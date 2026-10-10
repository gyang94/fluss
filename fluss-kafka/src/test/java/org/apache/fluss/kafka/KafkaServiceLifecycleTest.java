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

import org.apache.fluss.kafka.backend.produce.KafkaProduceConversionExecutor;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.types.DataTypes;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Service drains wait for actual network completion without cancelling submitted native writes. */
class KafkaServiceLifecycleTest {
    @Test
    void testSubmittedAcksZeroCompletesWithoutAResponseAfterDisable() {
        KafkaServiceController controller =
                new KafkaServiceController(true, Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(),
                        new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
        try {
            ByteBuf buffer = produce((short) 0);
            channel.writeInbound(buffer);
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            new KafkaRequestHandler(service, service).processRequest(request);
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
    void testDisableWaitsForNativeResultAndNetworkFlush() {
        KafkaServiceController controller =
                new KafkaServiceController(true, Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        DelayedWrite writes = new DelayedWrite();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(),
                        writes,
                        new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
        try {
            ByteBuf buffer = produce();
            channel.writeInbound(buffer);
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            new KafkaRequestHandler(service, service).processRequest(request);
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
    void testDisconnectDoesNotCancelNativeFutureOrInvalidatePayload() {
        KafkaServiceController controller = new KafkaServiceController(true, Duration.ofMillis(1));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(),
                        new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
        try {
            ByteBuf buffer = produce();
            channel.writeInbound(buffer);
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            new KafkaRequestHandler(service, service).processRequest(request);
            request.releaseBuffer();
            controller.setEnabled(false);
            channel.close();
            channel.runPendingTasks();
            assertThat(channel.isActive()).isFalse();
            assertThat(service.result).isNotDone();
            ByteBuf nativeRecords = service.append.getBucketsReqsList().get(0).getRecordsSlice();
            try {
                MemoryLogRecords.pointToByteBuffer(nativeRecords.nioBuffer())
                        .batches()
                        .iterator()
                        .next()
                        .ensureValid();
            } finally {
                nativeRecords.release();
            }
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
    void testQueuedRequestCannotResumeOnOldConnectionAfterReenable() {
        KafkaServiceController controller =
                new KafkaServiceController(true, Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(),
                        new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
        try {
            channel.writeInbound(produce());
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            controller.setEnabled(false);
            controller.setEnabled(true);
            new KafkaRequestHandler(service, service).processRequest(request);
            request.releaseBuffer();
            assertThat(request.future().join().errorCounts())
                    .containsOnlyKeys(org.apache.kafka.common.protocol.Errors.BROKER_NOT_AVAILABLE);
            assertThat(service.metadataCalls).isZero();
            assertThat(service.append).isNull();
            channel.runPendingTasks();
            assertThat(channel.isActive()).isTrue();
        } finally {
            channel.finishAndReleaseAll();
            controller.close();
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, -1})
    void testDisableAfterLastCheckBeforeNativeSubmission(short acks) throws Exception {
        KafkaServiceController controller =
                new KafkaServiceController(true, Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        GatedService service = new GatedService(false);
        KafkaProduceConversionExecutor conversion = new KafkaProduceConversionExecutor(1, 2);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(),
                        new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
        try {
            ByteBuf bytes = produce(acks);
            channel.writeInbound(bytes);
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            new KafkaRequestHandler(service, service, conversion).processRequest(request);
            request.releaseBuffer();
            assertThat(service.entered.await(10, TimeUnit.SECONDS)).isTrue();
            controller.setEnabled(false);
            controller.setEnabled(true);
            service.resume.countDown();
            assertThat(request.future().get(10, TimeUnit.SECONDS).errorCounts())
                    .containsOnlyKeys(Errors.BROKER_NOT_AVAILABLE);
            assertThat(((PendingService) service).append).isNull();
            channel.runPendingTasks();
            assertThat(bytes.refCnt()).isZero();
            assertThat(channel.isActive()).isEqualTo(acks != 0);
            if (acks == 0) {
                ByteBuf output;
                while ((output = channel.readOutbound()) != null) {
                    assertThat(output.readableBytes()).isZero();
                    output.release();
                }
            }
        } finally {
            service.resume.countDown();
            conversion.closeAsync().join();
            channel.finishAndReleaseAll();
            controller.close();
        }
    }

    @Test
    void testNativeSubmissionSerializesDisableButDoesNotWaitForAcknowledgement() throws Exception {
        KafkaServiceController controller =
                new KafkaServiceController(true, Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        GatedService service = new GatedService(true);
        KafkaProduceConversionExecutor conversion = new KafkaProduceConversionExecutor(1, 2);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(),
                        new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
        Thread disable = new Thread(() -> controller.setEnabled(false));
        try {
            channel.writeInbound(produce((short) -1));
            KafkaRequest request = (KafkaRequest) requests.pollRequest(1000);
            new KafkaRequestHandler(service, service, conversion).processRequest(request);
            request.releaseBuffer();
            assertThat(service.entered.await(10, TimeUnit.SECONDS)).isTrue();
            disable.start();
            retry(
                    Duration.ofSeconds(10),
                    () -> assertThat(disable.getState()).isEqualTo(Thread.State.WAITING));
            assertThat(controller.isEnabled()).isTrue();
            service.resume.countDown();
            disable.join(10_000);
            assertThat(disable.isAlive()).isFalse();
            assertThat(controller.isEnabled()).isFalse();
            assertThat(((PendingService) service).result).isNotDone();
            assertThat(request.future()).isNotDone();
            ((PendingService) service).result.complete(success());
            assertThat(request.future().get(10, TimeUnit.SECONDS).errorCounts())
                    .containsOnlyKeys(Errors.NONE);
            channel.runPendingTasks();
        } finally {
            service.resume.countDown();
            disable.join(10_000);
            conversion.closeAsync().join();
            channel.finishAndReleaseAll();
            controller.close();
        }
    }

    private static class GatedService extends PendingService {
        private final boolean insideNative;
        private final AtomicInteger sessions = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch resume = new CountDownLatch(1);

        private GatedService(boolean insideNative) {
            this.insideNative = insideNative;
        }

        @Override
        public void setCurrentSession(Session session) {
            super.setCurrentSession(session);
            if (!insideNative && sessions.incrementAndGet() == 2) {
                pause();
            }
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            if (insideNative) {
                pause();
            }
            return super.produceLog(request);
        }

        private void pause() {
            entered.countDown();
            try {
                assertThat(resume.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNoResponseCloseWaitsForEarlierNativeResultOrDrainTimeout(boolean timeout)
            throws Exception {
        KafkaServiceController controller =
                new KafkaServiceController(
                        true, timeout ? Duration.ofMillis(1) : Duration.ofSeconds(30));
        RequestChannel requests = new RequestChannel(10);
        PendingService service = new PendingService();
        KafkaRequestHandler handler = new KafkaRequestHandler(service, service);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        controller.newConnection(),
                        new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
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
                        .setName("kafka.topic")
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

    private static ProduceLogResponse success() {
        return new ProduceLogResponse()
                .addAllBucketsResps(
                        Collections.singletonList(
                                new PbProduceLogRespForBucket().setBucketId(0).setBaseOffset(42L)));
    }

    private static class PendingService extends TestingTabletGatewayService {
        private final CompletableFuture<ProduceLogResponse> result = new CompletableFuture<>();
        private ProduceLogRequest append;
        private int metadataCalls;

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            metadataCalls++;
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(Schema.newBuilder().column("value", DataTypes.STRING()).build())
                            .distributedBy(1)
                            .logFormat(LogFormat.ARROW)
                            .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "string")
                            .build();
            return CompletableFuture.completedFuture(
                    new GetTableInfoResponse()
                            .setTableId(42L)
                            .setSchemaId(1)
                            .setTableJson(descriptor.toJsonBytes())
                            .setCreatedTime(1L)
                            .setModifiedTime(1L));
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            append = request;
            return result;
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
