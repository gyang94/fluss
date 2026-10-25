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
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
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
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
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

/** Tests authentication admission, failure responses, and credential rotation over real sockets. */
class KafkaSaslListenerITCase extends KafkaServerTestBase {

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testInvalidCredentialsCloseWithVersionSpecificResponse(short handshakeVersion)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.handshake(handshakeVersion);
            byte[] token = token("wrong-password");
            if (handshakeVersion == 0) {
                fixture.writeFrame(ByteBuffer.wrap(token));
            } else {
                RequestHeader header = fixture.sendAuthentication(token);
                SaslAuthenticateResponse response =
                        (SaslAuthenticateResponse) fixture.response(header);
                assertThat(response.errorCounts())
                        .containsOnlyKeys(Errors.SASL_AUTHENTICATION_FAILED);
                assertThat(response.data().errorMessage())
                        .doesNotContain("wrong-password", "writer-secret");
            }
            assertThat(fixture.input.read()).isEqualTo(-1);
        }
    }

    @Test
    void testUnauthenticatedProduceHeaderClosesWithoutResponse() throws Exception {
        try (Fixture fixture = new Fixture()) {
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

    @Test
    void testCredentialRotationAppliesToNewConnections() throws Exception {
        try (Fixture existing = new Fixture()) {
            existing.handshake((short) 1);
            existing.authenticate();
            Configuration changed = new Configuration(existing.configuration);
            changed.set(
                    ConfigOptions.SERVER_SASL_CREDENTIALS,
                    Collections.singletonMap("writer", "rotated-secret"));
            for (ServerReconfigurable component : existing.server.getServerReconfigurables()) {
                component.validate(changed);
                component.reconfigure(changed);
            }
            RequestHeader ready =
                    existing.send(
                            ApiKeys.API_VERSIONS,
                            new ApiVersionsRequest.Builder().build((short) 4));
            assertThat(existing.response(ready).errorCounts()).containsOnlyKeys(Errors.NONE);
            try (Fixture oldPassword = new Fixture(existing)) {
                oldPassword.handshake((short) 1);
                RequestHeader failed = oldPassword.sendAuthentication(token("writer-secret"));
                assertThat(oldPassword.response(failed).errorCounts())
                        .containsOnlyKeys(Errors.SASL_AUTHENTICATION_FAILED);
                assertThat(oldPassword.input.read()).isEqualTo(-1);
            }
            try (Fixture newPassword = new Fixture(existing)) {
                newPassword.handshake((short) 1);
                RequestHeader authenticated =
                        newPassword.sendAuthentication(token("rotated-secret"));
                assertThat(newPassword.response(authenticated).errorCounts())
                        .containsOnlyKeys(Errors.NONE);
            }
        }
    }

    private static byte[] token(String password) {
        return ("\u0000writer\u0000" + password).getBytes(StandardCharsets.UTF_8);
    }

    private final class Fixture implements AutoCloseable {
        private final NettyServer server;
        private final Configuration configuration;
        private boolean ownsServer = true;
        private final Socket socket = new Socket();
        private final DataInputStream input;
        private final DataOutputStream output;
        private int correlationId;

        private Fixture() throws Exception {
            Configuration conf = new Configuration();
            this.configuration = conf;
            conf.set(ConfigOptions.KAFKA_ENABLED, true);
            conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
            conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
            conf.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, Duration.ofSeconds(30));
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
                            createTabletService(),
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

        private Fixture(Fixture existing) throws Exception {
            server = existing.server;
            configuration = existing.configuration;
            ownsServer = false;
            try {
                Endpoint endpoint = server.getBindEndpoints().get(0);
                socket.connect(
                        new InetSocketAddress(endpoint.getHost(), endpoint.getPort()), 10_000);
                socket.setSoTimeout(10_000);
                input = new DataInputStream(socket.getInputStream());
                output = new DataOutputStream(socket.getOutputStream());
            } catch (Exception failure) {
                socket.close();
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

        private void authenticate() throws Exception {
            RequestHeader header = sendAuthentication(token("writer-secret"));
            assertThat(response(header).errorCounts()).containsOnlyKeys(Errors.NONE);
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
                if (ownsServer) {
                    server.close();
                }
            }
        }
    }
}
