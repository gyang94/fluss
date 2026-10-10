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

import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Exercises complete request/response wire frames for every disabled-service API version. */
class KafkaServiceDisabledTest {
    static Stream<Arguments> requests() {
        List<Arguments> cases = new ArrayList<>();
        for (boolean sasl : new boolean[] {false, true}) {
            for (boolean dynamic : new boolean[] {false, true}) {
                for (ApiKeys key :
                        new ApiKeys[] {
                            ApiKeys.API_VERSIONS,
                            ApiKeys.METADATA,
                            ApiKeys.PRODUCE,
                            ApiKeys.SASL_HANDSHAKE,
                            ApiKeys.SASL_AUTHENTICATE
                        }) {
                    int min = key == ApiKeys.PRODUCE ? 3 : 0;
                    int max =
                            key == ApiKeys.API_VERSIONS
                                    ? 4
                                    : key == ApiKeys.SASL_HANDSHAKE
                                            ? 1
                                            : key == ApiKeys.SASL_AUTHENTICATE ? 2 : 11;
                    for (short version = (short) min; version <= max; version++) {
                        for (short acks :
                                key == ApiKeys.PRODUCE ? new short[] {1, -1} : new short[] {1}) {
                            cases.add(Arguments.of(key, version, acks, sasl, dynamic));
                        }
                    }
                }
            }
        }
        return cases.stream();
    }

    @ParameterizedTest
    @MethodSource("requests")
    void testDisabledWireContract(
            ApiKeys key, short version, short acks, boolean sasl, boolean dynamic) {
        try (Fixture fixture = new Fixture(dynamic, sasl)) {
            if (dynamic) {
                fixture.controller.setEnabled(false);
            }
            assertThat(fixture.channel.isActive()).isTrue();
            AbstractRequest body = request(key, version, acks);
            RequestHeader header = new RequestHeader(key, version, "disabled-client", 123);
            ByteBuf frame = frame(header, body);
            fixture.channel.writeInbound(frame);
            fixture.processAll();
            AbstractResponse response = fixture.response(header);
            assertThat(response.errorCounts()).containsOnlyKeys(Errors.BROKER_NOT_AVAILABLE);
            if (response instanceof MetadataResponse) {
                assertThat(((MetadataResponse) response).data().topics())
                        .extracting(topic -> topic.name())
                        .containsExactly("db.one", "db.two");
                assertThat(((MetadataResponse) response).data().brokers()).isEmpty();
            }
            if (response instanceof ProduceResponse) {
                assertThat(((ProduceResponse) response).data().responses()).hasSize(2);
                ((ProduceResponse) response)
                        .data()
                        .responses()
                        .forEach(
                                topic -> {
                                    assertThat(topic.partitionResponses())
                                            .extracting(partition -> partition.index())
                                            .containsExactlyInAnyOrder(0, 1);
                                    topic.partitionResponses()
                                            .forEach(
                                                    partition -> {
                                                        assertThat(partition.baseOffset())
                                                                .isEqualTo(-1L);
                                                        assertThat(partition.errorMessage())
                                                                .isEqualTo(
                                                                        version >= 8
                                                                                ? KafkaServiceUnavailableException
                                                                                        .MESSAGE
                                                                                : null);
                                                    });
                                });
            }
            if (response instanceof SaslAuthenticateResponse) {
                assertThat(((SaslAuthenticateResponse) response).data().errorMessage())
                        .isEqualTo(KafkaServiceUnavailableException.MESSAGE);
            }
            assertThat(frame.refCnt()).isZero();
            assertThat(KafkaServiceController.connection(fixture.channel).pendingRequests())
                    .isZero();
            assertThat(fixture.channel.isActive()).isTrue();
            fixture.assertNoBusinessOrAuthentication();
        }
    }

    static Stream<Arguments> noResponseRequests() {
        List<Arguments> cases = new ArrayList<>();
        for (short version = 0; version <= 11; version++) {
            cases.add(
                    Arguments.of(
                            new MetadataRequest(
                                    new MetadataRequestData()
                                            .setTopics(
                                                    version == 0 ? Collections.emptyList() : null),
                                    version)));
            cases.add(
                    Arguments.of(
                            new MetadataRequest(
                                    new MetadataRequestData().setTopics(Collections.emptyList()),
                                    version)));
            if (version >= 3) {
                cases.add(Arguments.of(request(ApiKeys.PRODUCE, version, (short) 0)));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest
    @MethodSource("noResponseRequests")
    void testClosesWithoutInventingResponse(AbstractRequest body) {
        try (Fixture fixture = new Fixture(false, true)) {
            ByteBuf input =
                    frame(new RequestHeader(body.apiKey(), body.version(), "client", 1), body);
            fixture.channel.writeInbound(input);
            fixture.processAll();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(fixture.readOutboundBytes()).isEmpty();
            assertThat(input.refCnt()).isZero();
            fixture.assertNoBusinessOrAuthentication();
        }
    }

    @Test
    void testMixedPipelinePreservesCorrelationAndOrderAcrossRepeatedToggles() {
        try (Fixture fixture = new Fixture(true, true)) {
            fixture.controller.setEnabled(false);
            fixture.controller.setEnabled(true);
            List<RequestHeader> headers = new ArrayList<>();
            List<ByteBuf> frames = new ArrayList<>();
            int correlation = 10;
            for (ApiKeys key :
                    new ApiKeys[] {
                        ApiKeys.PRODUCE,
                        ApiKeys.API_VERSIONS,
                        ApiKeys.METADATA,
                        ApiKeys.SASL_AUTHENTICATE,
                        ApiKeys.SASL_HANDSHAKE
                    }) {
                AbstractRequest body =
                        request(key, key == ApiKeys.PRODUCE ? (short) 8 : (short) 0, (short) 1);
                RequestHeader header =
                        new RequestHeader(key, body.version(), "pipeline", correlation++);
                headers.add(header);
                frames.add(frame(header, body));
            }
            ByteBuf combined = Unpooled.wrappedBuffer(frames.toArray(new ByteBuf[0]));
            fixture.channel.writeInbound(combined);
            // Complete the last request first: the ordered network queue must still wait.
            List<KafkaRequest> requests = new ArrayList<>();
            KafkaRequest pending;
            while ((pending = (KafkaRequest) fixture.requests.pollRequest(0)) != null) {
                requests.add(pending);
            }
            Collections.reverse(requests);
            for (KafkaRequest request : requests) {
                fixture.handler.processRequest(request);
                request.releaseBuffer();
            }
            fixture.channel.runPendingTasks();
            for (RequestHeader header : headers) {
                assertThat(fixture.response(header).errorCounts())
                        .containsOnlyKeys(Errors.BROKER_NOT_AVAILABLE);
            }
            assertThat(combined.refCnt()).isZero();
            fixture.assertNoBusinessOrAuthentication();
        }
    }

    @Test
    void testVersionAndApiValidationPrecedesDisabledAndSaslChecks() {
        try (Fixture fixture = new Fixture(false, true)) {
            short unsupported = (short) (ApiKeys.API_VERSIONS.latestVersion() + 1);
            RequestHeader header =
                    new RequestHeader(ApiKeys.API_VERSIONS, unsupported, "client", 91);
            // The normal unsupported ApiVersions fallback decodes only the header.
            ByteBuffer bytes =
                    RequestUtils.serialize(
                            header.data(),
                            header.headerVersion(),
                            new ApiVersionsRequest.Builder().build((short) 0).data(),
                            (short) 0);
            fixture.channel.writeInbound(
                    Unpooled.buffer().writeInt(bytes.remaining()).writeBytes(bytes));
            fixture.processAll();
            byte[] response = fixture.readOutboundBytes();
            ByteBuffer payload = ByteBuffer.wrap(response);
            assertThat(payload.getInt()).isEqualTo(payload.remaining());
            assertThat(ResponseHeader.parse(payload, (short) 0).correlationId()).isEqualTo(91);
            assertThat(
                            org.apache.kafka.common.requests.ApiVersionsResponse.parse(
                                            payload, (short) 0)
                                    .errorCounts())
                    .containsOnlyKeys(Errors.UNSUPPORTED_VERSION);
            fixture.assertNoBusinessOrAuthentication();
        }
    }

    @Test
    void testDisableClosesInProgressRawSaslWithoutKafkaResponse() {
        try (Fixture fixture = new Fixture(true, true)) {
            AbstractRequest handshake = request(ApiKeys.SASL_HANDSHAKE, (short) 0, (short) 1);
            RequestHeader header = new RequestHeader(handshake.apiKey(), (short) 0, "raw", 4);
            fixture.channel.writeInbound(frame(header, handshake));
            fixture.processAll();
            assertThat(fixture.response(header).errorCounts()).containsOnlyKeys(Errors.NONE);
            fixture.controller.setEnabled(false);
            fixture.channel.runPendingTasks();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(fixture.readOutboundBytes()).isEmpty();
            assertThat(fixture.authenticators.get()).isOne();
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({
        "PRODUCE, false",
        "METADATA, false",
        "PRODUCE, true",
        "METADATA, true"
    })
    void testAuthenticationAdmissionIsCapturedBeforeLaterAuthentication(
            ApiKeys key, boolean disable) {
        try (Fixture fixture = new Fixture(true, true)) {
            AbstractRequest early = request(key, (short) 9, (short) 1);
            RequestHeader earlyHeader = new RequestHeader(key, early.version(), "early", 41);
            fixture.channel.writeInbound(frame(earlyHeader, early));
            KafkaRequest queued = (KafkaRequest) fixture.requests.pollRequest(0);
            assertThat(queued.authenticationAllowed()).isFalse();
            completeLaterAuthentication(fixture);
            if (disable) {
                fixture.controller.setEnabled(false);
            }
            fixture.handler.processRequest(queued);
            queued.releaseBuffer();
            fixture.channel.runPendingTasks();
            if (disable) {
                assertThat(fixture.response(earlyHeader).errorCounts())
                        .containsOnlyKeys(Errors.BROKER_NOT_AVAILABLE);
            } else {
                assertThat(fixture.channel.isActive()).isFalse();
                assertThat(fixture.readOutboundBytes()).isEmpty();
            }
            verifyNoInteractions(fixture.service);
        }
    }

    @Test
    void testRequestBehindQueuedCloseCannotBeginBusinessAfterAuthentication() {
        try (Fixture fixture = new Fixture(true, true)) {
            AbstractRequest early = request(ApiKeys.METADATA, (short) 9, (short) 1);
            fixture.channel.writeInbound(
                    frame(new RequestHeader(early.apiKey(), early.version(), "early", 51), early));
            KafkaRequest first = (KafkaRequest) fixture.requests.pollRequest(0);
            completeLaterAuthentication(fixture);
            AbstractRequest late = request(ApiKeys.PRODUCE, (short) 9, (short) 1);
            fixture.channel.writeInbound(
                    frame(new RequestHeader(late.apiKey(), late.version(), "late", 54), late));
            KafkaRequest second = (KafkaRequest) fixture.requests.pollRequest(0);
            assertThat(second.authenticationAllowed()).isTrue();
            fixture.handler.processRequest(first);
            first.releaseBuffer();
            fixture.handler.processRequest(second);
            second.releaseBuffer();
            fixture.channel.runPendingTasks();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(fixture.readOutboundBytes()).isEmpty();
            verifyNoInteractions(fixture.service);
        }
    }

    private static void completeLaterAuthentication(Fixture fixture) {
        for (ApiKeys key : new ApiKeys[] {ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE}) {
            AbstractRequest body = request(key, (short) 1, (short) 1);
            fixture.channel.writeInbound(
                    frame(new RequestHeader(key, (short) 1, "later-auth", key.id), body));
            KafkaRequest request = (KafkaRequest) fixture.requests.pollRequest(0);
            fixture.handler.processRequest(request);
            request.releaseBuffer();
            assertThat(request.future().join().errorCounts()).containsOnlyKeys(Errors.NONE);
        }
    }

    @Test
    void testMalformedFrameClosesWithoutBusinessOrAuthentication() {
        try (Fixture fixture = new Fixture(false, true)) {
            ByteBuf frame = Unpooled.buffer().writeInt(1).writeByte(0);
            fixture.channel.writeInbound(frame);
            fixture.channel.runPendingTasks();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(frame.refCnt()).isZero();
            fixture.assertNoBusinessOrAuthentication();
        }
    }

    @Test
    void testOutOfOrderCloseDecisionPreservesEarlierDisabledResponse() {
        try (Fixture fixture = new Fixture(false, true)) {
            AbstractRequest first = request(ApiKeys.METADATA, (short) 9, (short) 1);
            RequestHeader header = new RequestHeader(first.apiKey(), first.version(), "first", 10);
            AbstractRequest closing = request(ApiKeys.PRODUCE, (short) 9, (short) 0);
            AbstractRequest last = request(ApiKeys.API_VERSIONS, (short) 4, (short) 1);
            ByteBuf bytes =
                    Unpooled.wrappedBuffer(
                            frame(header, first),
                            frame(
                                    new RequestHeader(
                                            closing.apiKey(), closing.version(), "close", 11),
                                    closing),
                            frame(
                                    new RequestHeader(last.apiKey(), last.version(), "last", 12),
                                    last));
            fixture.channel.writeInbound(bytes);
            KafkaRequest earlier = (KafkaRequest) fixture.requests.pollRequest(0);
            KafkaRequest close = (KafkaRequest) fixture.requests.pollRequest(0);
            KafkaRequest later = (KafkaRequest) fixture.requests.pollRequest(0);
            fixture.handler.processRequest(close);
            close.releaseBuffer();
            fixture.handler.processRequest(later);
            later.releaseBuffer();
            fixture.handler.processRequest(earlier);
            earlier.releaseBuffer();
            fixture.channel.runPendingTasks();
            assertThat(fixture.response(header).errorCounts())
                    .containsOnlyKeys(Errors.BROKER_NOT_AVAILABLE);
            assertThat(fixture.readOutboundBytes()).isEmpty();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(bytes.refCnt()).isZero();
            fixture.assertNoBusinessOrAuthentication();
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void testUnpublishedApiAndDecodableVersionBoundariesPrecedeAuthentication(boolean enabled) {
        try (Fixture fixture = new Fixture(enabled, true)) {
            List<AbstractRequest> bodies =
                    Arrays.asList(
                            new org.apache.kafka.common.requests.FindCoordinatorRequest.Builder(
                                            new org.apache.kafka.common.message
                                                            .FindCoordinatorRequestData()
                                                    .setKey("group"))
                                    .build((short) 0),
                            request(ApiKeys.METADATA, (short) 12, (short) 1),
                            request(ApiKeys.PRODUCE, (short) 2, (short) 1));
            int id = 200;
            for (AbstractRequest body : bodies) {
                RequestHeader header =
                        new RequestHeader(body.apiKey(), body.version(), "boundary", ++id);
                ByteBuf bytes = frame(header, body);
                fixture.channel.writeInbound(bytes);
                fixture.processAll();
                assertThat(fixture.response(header).errorCounts())
                        .containsOnlyKeys(Errors.UNSUPPORTED_VERSION);
                assertThat(bytes.refCnt()).isZero();
            }
            fixture.assertNoBusinessOrAuthentication();
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({
        "SASL_HANDSHAKE, false",
        "SASL_AUTHENTICATE, false",
        "SASL_HANDSHAKE, true",
        "SASL_AUTHENTICATE, true"
    })
    void testUndecodableSaslVersionsUseNormalClosure(ApiKeys key, boolean enabled) {
        try (Fixture fixture = new Fixture(enabled, true)) {
            short supported = key.latestVersion();
            RequestHeader header = new RequestHeader(key, (short) (supported + 1), "future", 300);
            AbstractRequest body = request(key, supported, (short) 1);
            ByteBuf bytes = frame(header, body);
            fixture.channel.writeInbound(bytes);
            fixture.processAll();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(fixture.readOutboundBytes()).isEmpty();
            assertThat(bytes.refCnt()).isZero();
            fixture.assertNoBusinessOrAuthentication();
        }
    }

    @Test
    void testDisabledDiagnosticDoesNotExposeCredentials() {
        org.apache.logging.log4j.core.Logger logger =
                (org.apache.logging.log4j.core.Logger)
                        org.apache.logging.log4j.LogManager.getLogger(
                                org.apache.fluss.kafka.dispatcher.KafkaRequestDispatcher.class);
        org.apache.logging.log4j.Level oldLevel = logger.getLevel();
        List<String> messages = new java.util.concurrent.CopyOnWriteArrayList<>();
        org.apache.logging.log4j.core.appender.AbstractAppender appender =
                new org.apache.logging.log4j.core.appender.AbstractAppender(
                        "service-diagnostic",
                        null,
                        org.apache.logging.log4j.core.layout.PatternLayout.createDefaultLayout(),
                        false,
                        org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
                    @Override
                    public void append(org.apache.logging.log4j.core.LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(org.apache.logging.log4j.Level.INFO);
        try (Fixture fixture = new Fixture(false, true)) {
            for (AbstractRequest body :
                    Arrays.asList(
                            request(ApiKeys.API_VERSIONS, (short) 0, (short) 1),
                            new SaslAuthenticateRequest(
                                    new SaslAuthenticateRequestData()
                                            .setAuthBytes(
                                                    "credential-never-logged"
                                                            .getBytes(
                                                                    java.nio.charset
                                                                            .StandardCharsets
                                                                            .UTF_8)),
                                    (short) 2))) {
                RequestHeader header =
                        new RequestHeader(body.apiKey(), body.version(), "diagnostic", 400);
                fixture.channel.writeInbound(frame(header, body));
                fixture.processAll();
                fixture.response(header);
            }
            assertThat(messages)
                    .anySatisfy(
                            message ->
                                    assertThat(message)
                                            .contains(KafkaServiceUnavailableException.MESSAGE)
                                            .contains("API_VERSIONS"));
            assertThat(messages)
                    .allSatisfy(
                            message ->
                                    assertThat(message).doesNotContain("credential-never-logged"));
            fixture.assertNoBusinessOrAuthentication();
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(oldLevel);
            appender.stop();
        }
    }

    static AbstractRequest request(ApiKeys key, short version, short acks) {
        switch (key) {
            case API_VERSIONS:
                return new ApiVersionsRequest.Builder().build(version);
            case METADATA:
                return new MetadataRequest(
                        new MetadataRequestData()
                                .setTopics(
                                        Arrays.asList(
                                                new MetadataRequestData.MetadataRequestTopic()
                                                        .setName("db.one"),
                                                new MetadataRequestData.MetadataRequestTopic()
                                                        .setName("db.two"))),
                        version);
            case PRODUCE:
                ProduceRequestData.TopicProduceDataCollection topics =
                        new ProduceRequestData.TopicProduceDataCollection();
                for (String name : new String[] {"db.one", "db.two"}) {
                    topics.add(
                            new ProduceRequestData.TopicProduceData()
                                    .setName(name)
                                    .setPartitionData(
                                            Arrays.asList(
                                                    new ProduceRequestData.PartitionProduceData()
                                                            .setIndex(0)
                                                            .setRecords(MemoryRecords.EMPTY),
                                                    new ProduceRequestData.PartitionProduceData()
                                                            .setIndex(1)
                                                            .setRecords(MemoryRecords.EMPTY))));
                }
                return new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks(acks)
                                .setTimeoutMs(1000)
                                .setTopicData(topics),
                        version);
            case SASL_HANDSHAKE:
                return new SaslHandshakeRequest(
                        new SaslHandshakeRequestData().setMechanism("PLAIN"), version);
            case SASL_AUTHENTICATE:
                return new SaslAuthenticateRequest(
                        new SaslAuthenticateRequestData().setAuthBytes(new byte[] {1, 2, 3}),
                        version);
            default:
                throw new IllegalArgumentException("Unexpected API " + key);
        }
    }

    static ByteBuf frame(RequestHeader header, AbstractRequest body) {
        ByteBuffer bytes =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), body.data(), body.version());
        return Unpooled.buffer().writeInt(bytes.remaining()).writeBytes(bytes);
    }

    static class Fixture implements AutoCloseable {
        final KafkaServiceController controller;
        final RequestChannel requests = new RequestChannel(100);
        final TestingTabletGatewayService service = spy(new TestingTabletGatewayService());
        final KafkaRequestHandler handler = new KafkaRequestHandler(service, service);
        final AtomicInteger authenticators = new AtomicInteger();
        final EmbeddedChannel channel;

        Fixture(boolean enabled, boolean sasl) {
            controller = new KafkaServiceController(enabled, Duration.ofSeconds(30));
            channel =
                    new EmbeddedChannel(
                            controller.newConnection(),
                            new LengthFieldPrepender(4),
                            new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4),
                            new KafkaCommandDecoder(
                                    new RequestChannel[] {requests},
                                    "KAFKA",
                                    sasl
                                            ? () -> {
                                                authenticators.incrementAndGet();
                                                ServerAuthenticator authenticator =
                                                        mock(ServerAuthenticator.class);
                                                when(authenticator.isCompleted()).thenReturn(true);
                                                when(authenticator.createPrincipal())
                                                        .thenReturn(
                                                                new org.apache.fluss.security.acl
                                                                        .FlussPrincipal(
                                                                        "writer", "User"));
                                                return authenticator;
                                            }
                                            : null));
        }

        void processAll() {
            KafkaRequest request;
            while ((request = (KafkaRequest) requests.pollRequest(0)) != null) {
                handler.processRequest(request);
                request.releaseBuffer();
            }
            channel.runPendingTasks();
        }

        AbstractResponse response(RequestHeader header) {
            ByteBuf length = channel.readOutbound();
            ByteBuf data = channel.readOutbound();
            assertThat(length).isNotNull();
            assertThat(data).isNotNull();
            try {
                assertThat(length.readInt()).isEqualTo(data.readableBytes());
                ByteBuffer bytes = data.nioBuffer();
                assertThat(
                                ResponseHeader.parse(
                                                bytes, header.toResponseHeader().headerVersion())
                                        .correlationId())
                        .isEqualTo(header.correlationId());
                return AbstractResponse.parseResponse(header.apiKey(), bytes, header.apiVersion());
            } finally {
                length.release();
                data.release();
            }
        }

        byte[] readOutboundBytes() {
            ByteBuf result = Unpooled.buffer();
            ByteBuf part;
            try {
                while ((part = channel.readOutbound()) != null) {
                    result.writeBytes(part);
                    part.release();
                }
                byte[] bytes = new byte[result.readableBytes()];
                result.readBytes(bytes);
                return bytes;
            } finally {
                result.release();
            }
        }

        void assertNoBusinessOrAuthentication() {
            verifyNoInteractions(service);
            assertThat(authenticators.get()).isZero();
        }

        @Override
        public void close() {
            KafkaRequest request;
            while ((request = (KafkaRequest) requests.pollRequest(0)) != null) {
                request.releaseBuffer();
            }
            channel.finishAndReleaseAll();
            controller.close();
        }
    }
}
