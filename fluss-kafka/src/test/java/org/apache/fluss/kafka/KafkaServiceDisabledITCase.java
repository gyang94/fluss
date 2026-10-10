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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies disabled startup and dynamic transitions through actual PLAINTEXT/SASL listeners. */
class KafkaServiceDisabledITCase {
    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void testEveryDisabledApiVersionOnExistingAndNewSockets(boolean sasl, boolean dynamic)
            throws Exception {
        try (Fixture fixture = new Fixture(sasl, dynamic);
                Socket existing = fixture.connect()) {
            fixture.setEnabled(false);
            assertPipeline(existing);
            try (Socket fresh = fixture.connect()) {
                assertPipeline(fresh);
            }
            List<Arguments> noResponses =
                    KafkaServiceDisabledTest.noResponseRequests().collect(Collectors.toList());
            for (Arguments args : noResponses) {
                try (Socket socket = fixture.connect()) {
                    AbstractRequest request = (AbstractRequest) args.get()[0];
                    send(socket, request, 99);
                    assertThat(socket.getInputStream().read()).isEqualTo(-1);
                }
            }
            fixture.setEnabled(true);
            // Retired connections stay fenced even though new connections can use the service.
            assertPipeline(existing);
            try (Socket reopened = fixture.connect()) {
                RequestHeader header =
                        send(reopened, new ApiVersionsRequest.Builder().build((short) 4), 456);
                assertThat(response(reopened, header).errorCounts()).containsOnlyKeys(Errors.NONE);
            }
            fixture.setEnabled(false);
            try (Socket disabledAgain = fixture.connect()) {
                assertPipeline(disabledAgain);
            }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(shorts = {0, 1})
    void testDisableDuringRealSaslExchange(short version) throws Exception {
        try (Fixture fixture = new Fixture(true, true);
                Socket socket = fixture.connect()) {
            AbstractRequest handshake =
                    KafkaServiceDisabledTest.request(ApiKeys.SASL_HANDSHAKE, version, (short) 1);
            assertThat(response(socket, send(socket, handshake, 1)).errorCounts())
                    .containsOnlyKeys(Errors.NONE);
            fixture.setEnabled(false);
            if (version == 0) {
                assertThat(socket.getInputStream().read()).isEqualTo(-1);
            } else {
                AbstractRequest authentication =
                        KafkaServiceDisabledTest.request(
                                ApiKeys.SASL_AUTHENTICATE, (short) 2, (short) 1);
                assertThat(response(socket, send(socket, authentication, 2)).errorCounts())
                        .containsOnlyKeys(Errors.BROKER_NOT_AVAILABLE);
            }
        }
    }

    private static void assertPipeline(Socket socket) throws Exception {
        List<RequestHeader> headers = new ArrayList<>();
        int correlation = 100;
        for (Arguments args : KafkaServiceDisabledTest.requests().collect(Collectors.toList())) {
            Object[] values = args.get();
            if ((boolean) values[3] || (boolean) values[4]) {
                continue;
            }
            AbstractRequest body =
                    KafkaServiceDisabledTest.request(
                            (ApiKeys) values[0], (short) values[1], (short) values[2]);
            headers.add(send(socket, body, ++correlation));
        }
        for (RequestHeader header : headers) {
            assertThat(response(socket, header).errorCounts())
                    .containsOnlyKeys(Errors.BROKER_NOT_AVAILABLE);
        }
    }

    private static RequestHeader send(Socket socket, AbstractRequest body, int correlation)
            throws Exception {
        RequestHeader header =
                new RequestHeader(body.apiKey(), body.version(), "service-switch", correlation);
        ByteBuffer bytes =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), body.data(), body.version());
        byte[] payload = new byte[bytes.remaining()];
        bytes.get(payload);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(payload.length);
        output.write(payload);
        output.flush();
        return header;
    }

    private static AbstractResponse response(Socket socket, RequestHeader header) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
        assertThat(length).isBetween(1, 1024 * 1024);
        byte[] payload = new byte[length];
        input.readFully(payload);
        ByteBuffer bytes = ByteBuffer.wrap(payload);
        assertThat(
                        ResponseHeader.parse(bytes, header.toResponseHeader().headerVersion())
                                .correlationId())
                .isEqualTo(header.correlationId());
        AbstractResponse response =
                AbstractResponse.parseResponse(header.apiKey(), bytes, header.apiVersion());
        assertThat(bytes.remaining()).isZero();
        return response;
    }

    private static class Fixture implements AutoCloseable {
        private final Configuration config = new Configuration();
        private final NettyServer server;
        private final KafkaProtocolPlugin plugin;

        Fixture(boolean sasl, boolean enabled) throws Exception {
            config.set(ConfigOptions.KAFKA_ENABLED, enabled);
            config.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
            config.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
            config.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, Duration.ofSeconds(30));
            if (sasl) {
                config.set(
                        ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                        Collections.singletonMap("KAFKA", "sasl"));
                config.set(
                        ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                        Collections.singletonList("PLAIN"));
                config.set(
                        ConfigOptions.SERVER_SASL_CREDENTIALS,
                        Collections.singletonMap("writer", "secret"));
            }
            MetricGroup metrics = NOPMetricsGroup.newInstance();
            server =
                    new NettyServer(
                            config,
                            Collections.singletonList(new Endpoint("localhost", 0, "KAFKA")),
                            new TestingTabletGatewayService(),
                            metrics,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(metrics));
            plugin =
                    (KafkaProtocolPlugin)
                            server.getServerReconfigurables().stream()
                                    .filter(KafkaProtocolPlugin.class::isInstance)
                                    .findFirst()
                                    .get();
            try {
                server.start();
            } catch (Exception failure) {
                server.close();
                throw failure;
            }
        }

        Socket connect() throws Exception {
            Endpoint endpoint = server.getBindEndpoints().get(0);
            Socket socket = new Socket(endpoint.getHost(), endpoint.getPort());
            socket.setSoTimeout(10_000);
            return socket;
        }

        void setEnabled(boolean enabled) {
            config.set(ConfigOptions.KAFKA_ENABLED, enabled);
            plugin.reconfigure(config);
        }

        @Override
        public void close() throws Exception {
            server.close();
        }
    }
}
