/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;

import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests authentication, response ordering, and idle closure through real Kafka SASL sockets. */
class KafkaSaslListenerITCase {

    @ParameterizedTest
    @CsvSource({"-1, false", "0, false", "1, false", "0, true", "1, true"})
    void testNettyIdleTimeoutClosesEveryAuthenticationState(
            short handshakeVersion, boolean authenticate) throws Exception {
        try (Fixture fixture = new Fixture(Duration.ofSeconds(1))) {
            if (handshakeVersion >= 0) {
                fixture.handshake(handshakeVersion);
            }
            if (authenticate) {
                fixture.authenticate(handshakeVersion);
            }
            assertThat(fixture.input.read()).isEqualTo(-1);
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testAuthenticatedRequestsReturnResponsesInOrder(short handshakeVersion) throws Exception {
        try (Fixture fixture = new Fixture(Duration.ofSeconds(30))) {
            fixture.handshake(handshakeVersion);
            fixture.authenticate(handshakeVersion);
            ApiVersionsRequest body = new ApiVersionsRequest.Builder().build((short) 4);
            RequestHeader first = fixture.send(ApiKeys.API_VERSIONS, body);
            RequestHeader second = fixture.send(ApiKeys.API_VERSIONS, body);
            assertThat(fixture.response(first).errorCounts()).containsOnlyKeys(Errors.NONE);
            assertThat(fixture.response(second).errorCounts()).containsOnlyKeys(Errors.NONE);
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testInvalidCredentialsCloseWithVersionSpecificResponse(short handshakeVersion)
            throws Exception {
        try (Fixture fixture = new Fixture(Duration.ofSeconds(30))) {
            fixture.handshake(handshakeVersion);
            byte[] token = token("wrong-password");
            if (handshakeVersion == 0) {
                fixture.writeFrame(ByteBuffer.wrap(token));
            } else {
                RequestHeader header = fixture.sendAuthentication(token);
                assertThat(fixture.response(header).errorCounts())
                        .containsOnlyKeys(Errors.SASL_AUTHENTICATION_FAILED);
            }
            assertThat(fixture.input.read()).isEqualTo(-1);
        }
    }

    @Test
    void testUnauthenticatedProduceHeaderClosesWithoutResponse() throws Exception {
        try (Fixture fixture = new Fixture(Duration.ofSeconds(30))) {
            RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 3, "client", 1);
            ObjectSerializationCache cache = new ObjectSerializationCache();
            ByteBuffer bytes =
                    ByteBuffer.allocate(header.data().size(cache, header.headerVersion()));
            header.data().write(new ByteBufferAccessor(bytes), cache, header.headerVersion());
            bytes.flip();
            fixture.writeFrame(bytes);
            assertThat(fixture.input.read()).isEqualTo(-1);
        }
    }

    private static byte[] token(String password) {
        return ("\u0000writer\u0000" + password).getBytes(StandardCharsets.UTF_8);
    }

    private static final class Fixture implements AutoCloseable {
        private final NettyServer server;
        private final Socket socket = new Socket();
        private final DataInputStream input;
        private final DataOutputStream output;
        private int correlationId;

        private Fixture(Duration idleTimeout) throws Exception {
            Configuration conf = new Configuration();
            conf.set(ConfigOptions.KAFKA_ENABLED, true);
            conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
            conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
            conf.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, idleTimeout);
            conf.set(
                    ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                    Collections.singletonMap("KAFKA", "sasl"));
            conf.set(
                    ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                    Collections.singletonList("PLAIN"));
            conf.set(
                    ConfigOptions.SERVER_SASL_CREDENTIALS,
                    Collections.singletonMap("writer", "writer-secret"));
            MetricGroup metrics = NOPMetricsGroup.newInstance();
            server =
                    new NettyServer(
                            conf,
                            Collections.singletonList(new Endpoint("localhost", 0, "KAFKA")),
                            new TestingTabletGatewayService(),
                            metrics,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(metrics));
            try {
                server.start();
                Endpoint endpoint = server.getBindEndpoints().get(0);
                socket.connect(
                        new InetSocketAddress(endpoint.getHost(), endpoint.getPort()), 10_000);
                socket.setSoTimeout(10_000);
                input = new DataInputStream(socket.getInputStream());
                output = new DataOutputStream(socket.getOutputStream());
            } catch (Exception failure) {
                close();
                throw failure;
            }
        }

        private void handshake(short version) throws Exception {
            RequestHeader header =
                    send(
                            ApiKeys.SASL_HANDSHAKE,
                            new SaslHandshakeRequest(
                                    new SaslHandshakeRequestData().setMechanism("PLAIN"), version));
            assertThat(response(header).errorCounts()).containsOnlyKeys(Errors.NONE);
        }

        private void authenticate(short handshakeVersion) throws Exception {
            if (handshakeVersion == 0) {
                writeFrame(ByteBuffer.wrap(token("writer-secret")));
                assertThat(input.readInt()).isZero();
            } else {
                RequestHeader header = sendAuthentication(token("writer-secret"));
                assertThat(response(header).errorCounts()).containsOnlyKeys(Errors.NONE);
            }
        }

        private RequestHeader sendAuthentication(byte[] token) throws Exception {
            return send(
                    ApiKeys.SASL_AUTHENTICATE,
                    new SaslAuthenticateRequest(
                            new SaslAuthenticateRequestData().setAuthBytes(token), (short) 2));
        }

        private RequestHeader send(ApiKeys key, AbstractRequest request) throws Exception {
            RequestHeader header =
                    new RequestHeader(
                            key, request.version(), "sasl-listener-test", ++correlationId);
            writeFrame(
                    RequestUtils.serialize(
                            header.data(),
                            header.headerVersion(),
                            request.data(),
                            request.version()));
            return header;
        }

        private void writeFrame(ByteBuffer bytes) throws Exception {
            byte[] payload = new byte[bytes.remaining()];
            bytes.get(payload);
            output.writeInt(payload.length);
            output.write(payload);
            output.flush();
        }

        private AbstractResponse response(RequestHeader header) throws Exception {
            int length = input.readInt();
            assertThat(length).isBetween(1, 1024 * 1024);
            byte[] payload = new byte[length];
            input.readFully(payload);
            ByteBuffer bytes = ByteBuffer.wrap(payload);
            ResponseHeader responseHeader =
                    ResponseHeader.parse(
                            bytes.duplicate(), header.toResponseHeader().headerVersion());
            assertThat(responseHeader.correlationId()).isEqualTo(header.correlationId());
            AbstractResponse response = AbstractResponse.parseResponse(bytes, header);
            assertThat(bytes.hasRemaining()).isFalse();
            return response;
        }

        @Override
        public void close() throws Exception {
            try {
                socket.close();
            } finally {
                server.close();
            }
        }
    }
}
