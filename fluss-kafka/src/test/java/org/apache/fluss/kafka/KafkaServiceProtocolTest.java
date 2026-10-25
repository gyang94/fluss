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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.backend.metadata.KafkaMetadataBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceBackend;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
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
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.utils.Crc32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Exercises complete request/response wire frames for every disabled-service API version. */
class KafkaServiceProtocolTest {

    @Test
    void testNonRetryableError() {
        assertThat(new KafkaServiceUnavailableException())
                .isNotInstanceOf(RetriableException.class);
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testDisableDuringSasl(short handshakeVersion) {
        TestingSaslAuthenticator authenticator = new TestingSaslAuthenticator();
        try (Fixture fixture = new Fixture(true, () -> authenticator)) {
            SaslHandshakeRequest handshake =
                    new SaslHandshakeRequest(
                            new SaslHandshakeRequestData().setMechanism("PLAIN"), handshakeVersion);
            RequestHeader header =
                    new RequestHeader(ApiKeys.SASL_HANDSHAKE, handshakeVersion, "sasl", 1);
            fixture.channel.writeInbound(frame(header, handshake));
            fixture.processAll();
            assertThat(fixture.response(header).errorCounts()).containsOnlyKeys(Errors.NONE);
            fixture.controller.setEnabled(false);
            ByteBuf input;
            if (handshakeVersion == 0) {
                input = Unpooled.buffer().writeInt(1).writeByte(0);
                fixture.channel.writeInbound(input);
                fixture.channel.runPendingTasks();
                assertThat(fixture.channel.isActive()).isFalse();
                assertThat(fixture.readOutboundBytes()).isEmpty();
                assertThat(authenticator.closed).isTrue();
            } else {
                SaslAuthenticateRequest authenticate =
                        new SaslAuthenticateRequest(
                                new SaslAuthenticateRequestData().setAuthBytes(new byte[] {0}),
                                (short) 2);
                header = new RequestHeader(ApiKeys.SASL_AUTHENTICATE, (short) 2, "sasl", 2);
                input = frame(header, authenticate);
                fixture.channel.writeInbound(input);
                fixture.processAll();
                assertThat(fixture.response(header).errorCounts())
                        .containsOnlyKeys(Errors.INVALID_REQUEST);
            }
            assertThat(authenticator.isCompleted()).isFalse();
            assertThat(input.refCnt()).isZero();
            assertThat(KafkaServiceController.connection(fixture.channel).pendingRequests())
                    .isZero();
            fixture.assertNoBusiness();
        }
    }

    static Stream<Arguments> supportedVersions() {
        List<Arguments> cases = new ArrayList<>();
        for (ApiKeys key :
                new ApiKeys[] {ApiKeys.API_VERSIONS, ApiKeys.METADATA, ApiKeys.PRODUCE}) {
            int min = key == ApiKeys.PRODUCE ? 3 : 0;
            int max = key == ApiKeys.API_VERSIONS ? 4 : 11;
            for (short version = (short) min; version <= max; version++) {
                cases.add(Arguments.of(key, version));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0} v{1}")
    @MethodSource("supportedVersions")
    void testDisabledResponses(ApiKeys key, short version) {
        try (Fixture fixture = new Fixture(false)) {
            assertThat(fixture.channel.isActive()).isTrue();
            AbstractRequest body = request(key, version, (short) 1);
            RequestHeader header = new RequestHeader(key, version, "disabled-client", 123);
            ByteBuf frame = frame(header, body);
            fixture.channel.writeInbound(frame);
            fixture.processAll();
            AbstractResponse response = fixture.response(header);
            assertThat(response.errorCounts()).containsOnlyKeys(Errors.INVALID_REQUEST);
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
            assertThat(frame.refCnt()).isZero();
            assertThat(KafkaServiceController.connection(fixture.channel).pendingRequests())
                    .isZero();
            assertThat(fixture.channel.isActive()).isTrue();
            fixture.assertNoBusiness();
        }
    }

    @ParameterizedTest
    @CsvSource({"GZIP,0", "GZIP,1", "GZIP,-1", "SNAPPY,-1", "LZ4,-1", "ZSTD,-1"})
    void testDisabledCompressedProduce(CompressionType codec, short acks) {
        try (Fixture fixture = new Fixture(true)) {
            MemoryRecords records =
                    MemoryRecords.withRecords(
                            Compression.of(codec).build(), new SimpleRecord(new byte[] {1, 2, 3}));
            // Keep the batch CRC valid so bypassing the service gate would reach decompression.
            ByteBuffer payload = records.buffer();
            for (int i = DefaultRecordBatch.RECORD_BATCH_OVERHEAD; i < payload.limit(); i++) {
                payload.put(i, (byte) 0);
            }
            int crcStart = DefaultRecordBatch.CRC_OFFSET + Integer.BYTES;
            payload.putInt(
                    DefaultRecordBatch.CRC_OFFSET,
                    (int) Crc32C.compute(payload, crcStart, payload.limit() - crcStart));
            records.batches().iterator().next().ensureValid();
            ProduceRequest body = (ProduceRequest) request(ApiKeys.PRODUCE, (short) 9, acks);
            body.data()
                    .topicData()
                    .forEach(
                            topic ->
                                    topic.partitionData()
                                            .forEach(partition -> partition.setRecords(records)));
            RequestHeader header =
                    new RequestHeader(ApiKeys.PRODUCE, (short) 9, "disabled-compressed", 456);
            ByteBuf bytes = frame(header, body);
            fixture.channel.writeInbound(bytes);
            // Exercise a request already queued when dynamic disable takes effect.
            fixture.controller.setEnabled(false);
            fixture.processAll();
            if (acks == 0) {
                assertThat(fixture.channel.isActive()).isFalse();
                assertThat(fixture.readOutboundBytes()).isEmpty();
            } else {
                assertThat(fixture.response(header).errorCounts())
                        .containsOnlyKeys(Errors.INVALID_REQUEST);
            }
            assertThat(bytes.refCnt()).isZero();
            assertThat(KafkaServiceController.connection(fixture.channel).pendingRequests())
                    .isZero();
            fixture.assertNoBusiness();
        }
    }

    static Stream<Arguments> noResponseRequests() {
        List<Arguments> cases = new ArrayList<>();
        // v0 treats an empty list as all topics; v9 introduces flexible encoding.
        for (short version : new short[] {0, 1, 8, 9, 11}) {
            cases.add(
                    Arguments.of(
                            new MetadataRequest(
                                    new MetadataRequestData().setTopics(Collections.emptyList()),
                                    version)));
            if (version > 0) {
                cases.add(
                        Arguments.of(
                                new MetadataRequest(
                                        new MetadataRequestData().setTopics(null), version)));
            }
        }
        for (short version : new short[] {3, 8, 9, 11}) {
            cases.add(Arguments.of(request(ApiKeys.PRODUCE, version, (short) 0)));
        }
        return cases.stream();
    }

    @ParameterizedTest
    @MethodSource("noResponseRequests")
    void testNoResponseRequestsClose(AbstractRequest body) {
        try (Fixture fixture = new Fixture(false)) {
            ByteBuf input =
                    frame(new RequestHeader(body.apiKey(), body.version(), "client", 1), body);
            fixture.channel.writeInbound(input);
            fixture.processAll();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(fixture.readOutboundBytes()).isEmpty();
            assertThat(input.refCnt()).isZero();
            fixture.assertNoBusiness();
        }
    }

    @Test
    void testResponseOrderAfterReenable() {
        try (Fixture fixture = new Fixture(true)) {
            fixture.controller.setEnabled(false);
            fixture.controller.setEnabled(true);
            List<RequestHeader> headers = new ArrayList<>();
            List<ByteBuf> frames = new ArrayList<>();
            int correlation = 10;
            for (ApiKeys key :
                    new ApiKeys[] {ApiKeys.PRODUCE, ApiKeys.API_VERSIONS, ApiKeys.METADATA}) {
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
                        .containsOnlyKeys(Errors.INVALID_REQUEST);
            }
            assertThat(combined.refCnt()).isZero();
            fixture.assertNoBusiness();
        }
    }

    @Test
    void testUnsupportedApiVersions() {
        try (Fixture fixture = new Fixture(false)) {
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
            fixture.assertNoBusiness();
        }
    }

    @Test
    void testMalformedFrame() {
        try (Fixture fixture = new Fixture(false)) {
            ByteBuf frame = Unpooled.buffer().writeInt(1).writeByte(0);
            fixture.channel.writeInbound(frame);
            fixture.channel.runPendingTasks();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(frame.refCnt()).isZero();
            fixture.assertNoBusiness();
        }
    }

    @Test
    void testClosePreservesEarlierResponse() {
        try (Fixture fixture = new Fixture(false)) {
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
                    .containsOnlyKeys(Errors.INVALID_REQUEST);
            assertThat(fixture.readOutboundBytes()).isEmpty();
            assertThat(fixture.channel.isActive()).isFalse();
            assertThat(bytes.refCnt()).isZero();
            fixture.assertNoBusiness();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnsupportedRequests(boolean enabled) {
        try (Fixture fixture = new Fixture(enabled)) {
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
            fixture.assertNoBusiness();
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
            default:
                throw new IllegalArgumentException("Unexpected API " + key);
        }
    }

    private static ByteBuf frame(RequestHeader header, AbstractRequest body) {
        ByteBuffer bytes =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), body.data(), body.version());
        return Unpooled.buffer().writeInt(bytes.remaining()).writeBytes(bytes);
    }

    private static class Fixture implements AutoCloseable {
        final KafkaServiceController controller;
        final RequestChannel requests = new RequestChannel(100);
        final KafkaMetadataBackend metadata = mock(KafkaMetadataBackend.class);
        final KafkaProduceBackend produce = mock(KafkaProduceBackend.class);
        final KafkaRequestHandler handler =
                new KafkaRequestHandler(
                        new KafkaTopicMapper(new Configuration()), metadata, produce);
        final EmbeddedChannel channel;

        Fixture(boolean enabled) {
            this(enabled, null);
        }

        Fixture(boolean enabled, Supplier<ServerAuthenticator> authenticatorSupplier) {
            controller = new KafkaServiceController(enabled, Duration.ofSeconds(30));
            channel =
                    new EmbeddedChannel(
                            controller.newConnection(),
                            new LengthFieldPrepender(4),
                            new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4),
                            new KafkaCommandDecoder(requests, "KAFKA", authenticatorSupplier));
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

        void assertNoBusiness() {
            verifyNoInteractions(metadata, produce);
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
