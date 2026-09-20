/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.sasl.authenticator.SaslServerAuthenticator;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServerConfigManager;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;

import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests legacy SASL token framing, identity transitions, and buffer ownership. */
class KafkaSaslTokenDecoderTest {
    private static final String TOKEN = "\u0000writer\u0000writer-secret";

    @Test
    void testFragmentedRawTokenIsAuthenticatedOnlyWhenComplete() {
        try (Fixture fixture = new Fixture((short) 0)) {
            ByteBuf frame = tokenFrame(TOKEN);
            ByteBuf first = frame.readBytes(5);
            fixture.channel.writeInbound(first);
            assertThat(fixture.connection.isAuthenticatingWithRawTokens()).isTrue();
            assertThat(fixture.connection.principal()).isEqualTo(FlussPrincipal.ANONYMOUS);
            assertThat((Object) fixture.channel.readOutbound()).isNull();

            fixture.channel.writeInbound(frame);
            fixture.assertAuthenticated();
            fixture.assertEmptyChallenge();
            assertThat(fixture.requests.requestsCount()).isZero();
            assertThat(first.refCnt()).isZero();
            assertThat(frame.refCnt()).isZero();
        }
    }

    @Test
    void testRawTokenAndNextRequestInSameReadCarryAuthenticatedPrincipal() {
        try (Fixture fixture = new Fixture((short) 0)) {
            ApiVersionsRequest apiVersions = new ApiVersionsRequest.Builder().build((short) 0);
            RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "client", 2);
            ByteBuf input =
                    Unpooled.wrappedBuffer(tokenFrame(TOKEN), requestFrame(header, apiVersions));
            fixture.channel.writeInbound(input);
            fixture.assertAuthenticated();
            fixture.assertEmptyChallenge();
            KafkaRequest request = (KafkaRequest) fixture.requests.pollRequest(0);
            assertThat(request).isNotNull();
            assertThat(request.principal()).isEqualTo(new FlussPrincipal("writer", "User"));
            fixture.process(request);
            assertThat(fixture.response(header).errorCounts()).containsOnlyKeys(Errors.NONE);
            assertThat(input.refCnt()).isZero();
            assertThat(fixture.channel.isActive()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "malformed", "\u0000writer\u0000wrong-password"})
    void testInvalidRawTokenClosesWithoutKafkaErrorResponse(String token) {
        try (Fixture fixture = new Fixture((short) 0)) {
            ByteBuf frame = tokenFrame(token);
            fixture.channel.writeInbound(frame);
            fixture.channel.runPendingTasks();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(fixture.connection.shouldClose()).isTrue();
            assertThat(fixture.connection.principal()).isEqualTo(FlussPrincipal.ANONYMOUS);
            assertThat(fixture.requests.requestsCount()).isZero();
            assertThat((Object) fixture.channel.readOutbound()).isNull();
            assertThat(frame.refCnt()).isZero();
        }
    }

    @Test
    void testKafkaRequestDuringRawExchangeIsNotDispatched() {
        try (Fixture fixture = new Fixture((short) 0)) {
            ByteBuf frame =
                    requestFrame(
                            new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "client", 2),
                            new ApiVersionsRequest.Builder().build((short) 0));
            fixture.channel.writeInbound(frame);
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(fixture.requests.requestsCount()).isZero();
            assertThat((Object) fixture.channel.readOutbound()).isNull();
            assertThat(frame.refCnt()).isZero();
        }
    }

    @Test
    void testV1HandshakeDoesNotAcceptRawTokens() {
        try (Fixture fixture = new Fixture((short) 1)) {
            ByteBuf frame = tokenFrame(TOKEN);
            fixture.channel.writeInbound(frame);
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(fixture.connection.isReady()).isFalse();
            assertThat(fixture.requests.requestsCount()).isZero();
            assertThat(frame.refCnt()).isZero();
        }
    }

    private static ByteBuf tokenFrame(String token) {
        byte[] bytes = token.getBytes(StandardCharsets.UTF_8);
        return Unpooled.buffer(4 + bytes.length).writeInt(bytes.length).writeBytes(bytes);
    }

    private static ByteBuf requestFrame(RequestHeader header, AbstractRequest request) {
        ByteBuffer bytes =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        return Unpooled.buffer(4 + bytes.remaining()).writeInt(bytes.remaining()).writeBytes(bytes);
    }

    private static final class Fixture implements AutoCloseable {
        private final RequestChannel requests = new RequestChannel(100);
        private final EmbeddedChannel channel;
        private final KafkaRequestHandler handler;
        private final KafkaSaslConnection connection;

        private Fixture(short handshakeVersion) {
            Configuration conf = new Configuration();
            conf.set(
                    ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                    Collections.singletonList("PLAIN"));
            conf.set(
                    ConfigOptions.SERVER_SASL_CREDENTIALS,
                    Collections.singletonMap("writer", "writer-secret"));
            PlainSaslServerConfigManager manager = new PlainSaslServerConfigManager(conf);
            TestingTabletGatewayService service = new TestingTabletGatewayService();
            handler = new KafkaRequestHandler(service, service);
            channel =
                    new EmbeddedChannel(
                            new LengthFieldPrepender(4),
                            new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4),
                            new KafkaCommandDecoder(
                                    new RequestChannel[] {requests},
                                    "KAFKA",
                                    () -> new SaslServerAuthenticator(manager.getConfiguration())));
            RequestHeader header =
                    new RequestHeader(ApiKeys.SASL_HANDSHAKE, handshakeVersion, "client", 1);
            channel.writeInbound(
                    requestFrame(
                            header,
                            new SaslHandshakeRequest(
                                    new SaslHandshakeRequestData().setMechanism("PLAIN"),
                                    handshakeVersion)));
            KafkaRequest request = (KafkaRequest) requests.pollRequest(0);
            assertThat(request).isNotNull();
            connection = request.saslConnection();
            process(request);
            assertThat(response(header).errorCounts()).containsOnlyKeys(Errors.NONE);
        }

        private void process(KafkaRequest request) {
            try {
                handler.processRequest(request);
            } finally {
                request.releaseBuffer();
            }
            channel.runPendingTasks();
        }

        private AbstractResponse response(RequestHeader header) {
            ByteBuf size = channel.readOutbound();
            ByteBuf bytes = channel.readOutbound();
            try {
                assertThat(size).isNotNull();
                assertThat(bytes).isNotNull();
                assertThat(size.readInt()).isEqualTo(bytes.readableBytes());
                return AbstractResponse.parseResponse(bytes.nioBuffer(), header);
            } finally {
                if (size != null) {
                    size.release();
                }
                if (bytes != null) {
                    bytes.release();
                }
            }
        }

        private void assertAuthenticated() {
            assertThat(connection.isReady()).isTrue();
            assertThat(connection.isAuthenticatingWithRawTokens()).isFalse();
            assertThat(connection.principal()).isEqualTo(new FlussPrincipal("writer", "User"));
        }

        private void assertEmptyChallenge() {
            ByteBuf size = channel.readOutbound();
            ByteBuf bytes = channel.readOutbound();
            try {
                assertThat(size).isNotNull();
                assertThat(size.readInt()).isZero();
                assertThat(bytes).isNotNull();
                assertThat(bytes.readableBytes()).isZero();
                assertThat((Object) channel.readOutbound()).isNull();
            } finally {
                if (size != null) {
                    size.release();
                }
                if (bytes != null) {
                    bytes.release();
                }
            }
        }

        @Override
        public void close() {
            channel.finishAndReleaseAll();
            KafkaRequest request;
            while ((request = (KafkaRequest) requests.pollRequest(0)) != null) {
                request.releaseBuffer();
            }
        }
    }
}
