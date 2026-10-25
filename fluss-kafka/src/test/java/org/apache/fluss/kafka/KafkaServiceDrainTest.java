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
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Service drains wait for actual network completion without cancelling submitted native writes. */
class KafkaServiceDrainTest {
    @Test
    void testSubmittedAcksZero() {
        KafkaServiceController controller =
                new KafkaServiceController(true, Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(), new KafkaCommandDecoder(requests, "KAFKA"));
        try {
            ByteBuf buffer = produce((short) 0);
            channel.writeInbound(buffer);
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            handler(service).processRequest(request);
            request.releaseBuffer();
            controller.setEnabled(false);
            channel.runPendingTasks();
            assertThat(channel.isActive()).isTrue();
            service.result.complete(success());
            channel.runPendingTasks();
            assertThat(channel.isActive()).isTrue();
            assertThat(buffer.refCnt()).isZero();
            Object response = channel.readOutbound();
            assertThat(response).isNull();
        } finally {
            channel.finishAndReleaseAll();
            controller.close();
        }
    }

    @Test
    void testWaitsForResultAndFlush() {
        KafkaServiceController controller =
                new KafkaServiceController(true, Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        DelayedWrite writes = new DelayedWrite();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(),
                        writes,
                        new KafkaCommandDecoder(requests, "KAFKA"));
        try {
            ByteBuf buffer = produce();
            channel.writeInbound(buffer);
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            handler(service).processRequest(request);
            request.releaseBuffer();
            assertThat(service.append).isNotNull();
            controller.setEnabled(false);
            channel.runPendingTasks();
            assertThat(channel.isActive()).isTrue();
            assertThat(request.future()).isNotDone();
            service.result.complete(success());
            channel.runPendingTasks();
            assertThat(writes.buffer).isNotNull();
            assertThat(channel.isActive()).isTrue();
            writes.succeed();
            channel.runPendingTasks();
            assertThat(channel.isActive()).isTrue();
            assertThat(buffer.refCnt()).isZero();
        } finally {
            writes.release();
            channel.finishAndReleaseAll();
            controller.close();
        }
    }

    @Test
    void testDisconnectPreservesSubmittedWrite() {
        KafkaServiceController controller = new KafkaServiceController(true, Duration.ofMillis(1));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(), new KafkaCommandDecoder(requests, "KAFKA"));
        try {
            ByteBuf buffer = produce();
            channel.writeInbound(buffer);
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            handler(service).processRequest(request);
            request.releaseBuffer();
            controller.setEnabled(false);
            channel.close();
            channel.runPendingTasks();
            assertThat(channel.isActive()).isFalse();
            assertThat(service.result).isNotDone();
            assertThat(service.append).isEqualTo("value".getBytes(StandardCharsets.UTF_8));
            service.result.complete(success());
            channel.runPendingTasks();
            assertThat(request.future()).isCompleted();
            assertThat(buffer.refCnt()).isZero();
            Object response = channel.readOutbound();
            assertThat(response).isNull();
        } finally {
            channel.finishAndReleaseAll();
            controller.close();
        }
    }

    @Test
    void testQueuedRequestStaysRetired() {
        KafkaServiceController controller =
                new KafkaServiceController(true, Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(), new KafkaCommandDecoder(requests, "KAFKA"));
        try {
            channel.writeInbound(produce());
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            controller.setEnabled(false);
            controller.setEnabled(true);
            handler(service).processRequest(request);
            request.releaseBuffer();
            assertThat(request.future().join().errorCounts())
                    .containsOnlyKeys(org.apache.kafka.common.protocol.Errors.INVALID_REQUEST);
            assertThat(service.calls).isZero();
            assertThat(service.append).isNull();
            channel.runPendingTasks();
            assertThat(channel.isActive()).isTrue();
        } finally {
            channel.finishAndReleaseAll();
            controller.close();
        }
    }

    @Test
    void testQueuedBufferReleasedAfterDisconnect() {
        KafkaServiceController controller =
                new KafkaServiceController(true, Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(), new KafkaCommandDecoder(requests, "KAFKA"));
        KafkaRequest request = null;
        try {
            ByteBuf bytes = produce();
            channel.writeInbound(bytes);
            request = (KafkaRequest) requests.pollRequest(1000);
            channel.close();
            channel.runPendingTasks();
            assertThat(request.cancelled()).isTrue();
            assertThat(bytes.refCnt()).isOne();
            handler(service).processRequest(request);
            request.releaseBuffer();
            channel.runPendingTasks();
            assertThat(service.calls).isZero();
            assertThat(bytes.refCnt()).isZero();
            assertThat((Object) channel.readOutbound()).isNull();
            assertThat(KafkaServiceController.connection(channel).pendingRequests()).isZero();
        } finally {
            if (request != null) {
                request.releaseBuffer();
            }
            channel.finishAndReleaseAll();
            controller.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCloseWaitsForResultOrTimeout(boolean timeout) throws Exception {
        KafkaServiceController controller =
                new KafkaServiceController(
                        true, timeout ? Duration.ofMillis(1) : Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        KafkaRequestHandler handler = handler(service);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(), new KafkaCommandDecoder(requests, "KAFKA"));
        try {
            ByteBuf first = produce((short) -1);
            channel.writeInbound(first);
            KafkaRequest nativeRequest = (KafkaRequest) requests.pollRequest(1000);
            handler.processRequest(nativeRequest);
            nativeRequest.releaseBuffer();
            controller.setEnabled(false);
            ByteBuf second = produce((short) 0);
            channel.writeInbound(second);
            KafkaRequest closingRequest = (KafkaRequest) requests.pollRequest(1000);
            handler.processRequest(closingRequest);
            closingRequest.releaseBuffer();
            channel.runPendingTasks();
            if (timeout) {
                retry(
                        Duration.ofSeconds(5),
                        () -> {
                            channel.runScheduledPendingTasks();
                            channel.runPendingTasks();
                            assertThat(channel.isActive()).isFalse();
                        });
                assertThat(service.result).isNotDone();
            } else {
                assertThat(channel.isActive()).isTrue();
                assertThat((Object) channel.readOutbound()).isNull();
            }
            service.result.complete(success());
            assertThat(nativeRequest.future().get(10, TimeUnit.SECONDS).errorCounts())
                    .containsOnlyKeys(Errors.NONE);
            channel.runPendingTasks();
            assertThat(channel.isActive()).isFalse();
            ByteBuf response;
            int meaningfulResponses = 0;
            while ((response = channel.readOutbound()) != null) {
                if (response.isReadable()) {
                    meaningfulResponses++;
                    java.nio.ByteBuffer bytes = response.nioBuffer();
                    org.apache.kafka.common.requests.ResponseHeader.parse(bytes, (short) 1);
                    org.apache.kafka.common.requests.ProduceResponse parsed =
                            org.apache.kafka.common.requests.ProduceResponse.parse(
                                    bytes, (short) 9);
                    assertThat(parsed.errorCounts()).containsOnlyKeys(Errors.NONE);
                    assertThat(
                                    parsed.data()
                                            .responses()
                                            .iterator()
                                            .next()
                                            .partitionResponses()
                                            .get(0)
                                            .baseOffset())
                            .isEqualTo(42L);
                }
                response.release();
            }
            assertThat(meaningfulResponses).isEqualTo(timeout ? 0 : 1);
            assertThat(first.refCnt()).isZero();
            assertThat(second.refCnt()).isZero();
            assertThat(KafkaServiceController.connection(channel).pendingRequests()).isZero();
        } finally {
            channel.finishAndReleaseAll();
            controller.close();
        }
    }

    private static ByteBuf produce() {
        return produce((short) 1);
    }

    private static ByteBuf produce(short acks) {
        ProduceRequestData.TopicProduceData topic =
                new ProduceRequestData.TopicProduceData()
                        .setName("fluss.topic")
                        .setPartitionData(
                                Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(
                                                        MemoryRecords.withRecords(
                                                                Compression.NONE,
                                                                new SimpleRecord(
                                                                        "value"
                                                                                .getBytes(
                                                                                        StandardCharsets
                                                                                                .UTF_8))))));
        ProduceRequest request =
                new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks(acks)
                                .setTimeoutMs(1000)
                                .setTopicData(
                                        new ProduceRequestData.TopicProduceDataCollection(
                                                Collections.singletonList(topic).iterator())),
                        (short) 9);
        RequestHeader header =
                new RequestHeader(ApiKeys.PRODUCE, request.version(), "service-test", 1);
        ByteBuffer bytes =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        return Unpooled.wrappedBuffer(bytes);
    }

    private static KafkaProduceResult success() {
        return new KafkaProduceResult(
                Collections.singletonList(
                        new KafkaProduceResult.TopicResult(
                                "fluss.topic",
                                Collections.singletonList(
                                        new KafkaProduceResult.PartitionResult(
                                                0, Errors.NONE, 42L, null)))));
    }

    private static KafkaRequestHandler handler(PendingService service) {
        return new KafkaRequestHandler(
                new KafkaTopicMapper(new Configuration()),
                org.mockito.Mockito.mock(KafkaMetadataBackend.class),
                service);
    }

    private static class PendingService implements KafkaProduceBackend {
        private final CompletableFuture<KafkaProduceResult> result = new CompletableFuture<>();
        private byte[] append;
        private int calls;

        @Override
        public CompletableFuture<KafkaProduceResult> write(KafkaProduceCommand command) {
            return command.submitIfServiceAvailable(
                    () -> {
                        calls++;
                        ByteBuffer value =
                                command.topics()
                                        .get(0)
                                        .partitions()
                                        .get(0)
                                        .records()
                                        .get(0)
                                        .value();
                        append = new byte[value.remaining()];
                        value.get(append);
                        return result;
                    });
        }
    }

    private static class DelayedWrite extends ChannelOutboundHandlerAdapter {
        private ByteBuf buffer;
        private ChannelPromise promise;

        @Override
        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            buffer = (ByteBuf) message;
            this.promise = promise;
        }

        void succeed() {
            release();
            promise.setSuccess();
        }

        void release() {
            if (buffer != null) {
                buffer.release();
                buffer = null;
            }
        }
    }
}
