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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaRequestAdmissionController;
import org.apache.fluss.kafka.backend.produce.GatewayKafkaProduceBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.network.KafkaFrameAdmission;
import org.apache.fluss.kafka.network.KafkaFrameAdmissionLease;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.kafka.transcode.KafkaArrowWriterManager;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseLease;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseReason;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests for Kafka protocol plugin resource lifecycle. */
class KafkaProtocolPluginTest {

    @Test
    void testCloseClosesAdmissionAndWaitsForArrowWriters() {
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
        plugin.setup(new Configuration());
        KafkaRequestAdmissionController controller = plugin.getAdmissionControllerForTesting();
        KafkaArrowWriterManager manager = plugin.getArrowWriterManagerForTesting();
        RequestChannel requestChannel = new RequestChannel(100);
        EmbeddedChannel channel = new EmbeddedChannel();
        requestChannel.registerChannel(channel);
        KafkaFrameAdmission admission = controller.createConnectionAdmission(requestChannel);
        admission.channelActive(channel);
        KafkaFrameAdmissionLease requestLease =
                admission.reserve(channel, 6L, ApiKeys.PRODUCE.id).getFuture().join();
        KafkaArrowWriterManager.WriterLease writerLease = manager.acquire(tableInfo());

        try {
            CompletableFuture<Void> firstClose = plugin.closeAsync();
            CompletableFuture<Void> secondClose = plugin.closeAsync();

            assertThat(secondClose).isSameAs(firstClose);
            assertThat(firstClose).isNotDone();
            assertThat(controller.connections()).isZero();
            assertThat(controller.produceAdmissionController().registeredConnections()).isZero();
            assertThat(controller.produceAdmissionController().liveRequests()).isOne();

            RequestChannel rejectedRequestChannel = new RequestChannel(100);
            EmbeddedChannel rejectedChannel = new EmbeddedChannel();
            rejectedRequestChannel.registerChannel(rejectedChannel);
            KafkaFrameAdmission rejectedAdmission =
                    controller.createConnectionAdmission(rejectedRequestChannel);
            try {
                assertThatThrownBy(() -> rejectedAdmission.channelActive(rejectedChannel))
                        .isInstanceOf(RejectedExecutionException.class)
                        .hasMessageContaining("controller is closed");
            } finally {
                rejectedAdmission.close();
                rejectedRequestChannel.unregisterChannel(rejectedChannel);
                rejectedChannel.runPendingTasks();
                rejectedChannel.finishAndReleaseAll();
            }

            requestLease.close();
            writerLease.close();
            firstClose.join();

            assertThat(controller.produceAdmissionController().liveRequests()).isZero();
            assertThatThrownBy(() -> manager.acquire(tableInfo()))
                    .isInstanceOf(FlussRuntimeException.class)
                    .hasMessageContaining("closing or closed");
        } finally {
            requestLease.close();
            writerLease.close();
            plugin.closeAsync().handle((ignored, failure) -> null).join();
            admission.close();
            requestChannel.unregisterChannel(channel);
            channel.runPendingTasks();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testAdmissionCloseFailureDoesNotSkipArrowManagerClose() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS, 1);
        configuration.set(
                ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION, 1);
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
        plugin.setup(configuration);
        KafkaRequestAdmissionController controller = plugin.getAdmissionControllerForTesting();
        KafkaArrowWriterManager manager = plugin.getArrowWriterManagerForTesting();

        RequestChannel pressureRequestChannel = new RequestChannel(100);
        EmbeddedChannel pressureChannel = new EmbeddedChannel();
        pressureRequestChannel.registerChannel(pressureChannel);
        KafkaProduceAdmissionController.ConnectionHandle pressureHandle =
                controller
                        .produceAdmissionController()
                        .registerConnection(pressureChannel, pressureRequestChannel);
        KafkaProduceAdmissionController.RequestLease pressureLease = pressureHandle.acquire(1L);

        ThrowingClosePauseRequestChannel requestChannel = new ThrowingClosePauseRequestChannel();
        EmbeddedChannel channel = new EmbeddedChannel();
        requestChannel.registerChannel(channel);
        KafkaFrameAdmission admission = controller.createConnectionAdmission(requestChannel);
        admission.channelActive(channel);
        KafkaFrameAdmission.Reservation waiting =
                admission.reserve(channel, 6L, ApiKeys.PRODUCE.id);
        KafkaArrowWriterManager.WriterLease writerLease = manager.acquire(tableInfo());

        try {
            CompletableFuture<Void> closeFuture = plugin.closeAsync();

            assertThat(closeFuture).isNotDone();
            assertThatThrownBy(() -> waiting.getFuture().join())
                    .isInstanceOf(CancellationException.class);
            writerLease.close();
            assertThatThrownBy(closeFuture::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("test pause close failure");
            assertThat(plugin.closeAsync()).isSameAs(closeFuture);
            assertThatThrownBy(() -> manager.acquire(tableInfo()))
                    .isInstanceOf(FlussRuntimeException.class)
                    .hasMessageContaining("closing or closed");
        } finally {
            writerLease.close();
            plugin.closeAsync().handle((ignored, failure) -> null).join();
            admission.close();
            requestChannel.unregisterChannel(channel);
            channel.runPendingTasks();
            channel.finishAndReleaseAll();
            pressureLease.close();
            pressureHandle.close();
            pressureRequestChannel.unregisterChannel(pressureChannel);
            pressureChannel.runPendingTasks();
            pressureChannel.finishAndReleaseAll();
        }
    }

    @Test
    void testCloseFailsQueuedGrantedConversionAndReleasesNativeLease() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.KAFKA_PRODUCE_ARROW_MAX_CONCURRENT_WRITERS, 1);
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
        plugin.setup(configuration);
        ExecutorService executor = plugin.getConversionExecutorForTesting();
        KafkaNativeProduceAdmissionController nativeController =
                plugin.getNativeAdmissionControllerForTesting();
        KafkaNativeProduceAdmissionController.ConnectionHandle connection =
                nativeController.registerConnection();
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CompletableFuture<Void> closeFuture = null;

        executor.execute(
                () -> {
                    workerStarted.countDown();
                    try {
                        releaseWorker.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
        assertThat(workerStarted.await(5, TimeUnit.SECONDS)).isTrue();

        TestingTabletGatewayService service = new TestingTabletGatewayService();
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service,
                        service,
                        "kafka",
                        mock(org.apache.fluss.kafka.transcode.KafkaRecordTranscoder.class),
                        KafkaProduceMetrics.noOp(),
                        executor,
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(5));
        TopicWrite topic =
                new TopicWrite(
                        "queued-topic",
                        Collections.singletonList(
                                new PartitionWrite(
                                        0,
                                        Collections.singletonList(
                                                new Record(
                                                        1L,
                                                        null,
                                                        new byte[] {1},
                                                        Collections.emptyList())))));
        KafkaProduceCommand command =
                new KafkaProduceCommand(
                        (short) 1,
                        1_000,
                        Collections.singletonList(topic),
                        "KAFKA",
                        null,
                        FlussPrincipal.ANONYMOUS,
                        connection,
                        null);
        CompletableFuture<KafkaProduceResult> result = backend.write(command);

        try {
            assertThat(result).isNotDone();
            assertThat(nativeController.inFlightRequests()).isOne();
            assertThat(executor).isInstanceOf(java.util.concurrent.ThreadPoolExecutor.class);
            assertThat(((java.util.concurrent.ThreadPoolExecutor) executor).getQueue()).hasSize(1);

            closeFuture = plugin.closeAsync();

            KafkaProduceResult completed = result.join();
            assertThat(completed.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            assertThat(command.nativeAdmissionTransferFuture()).isCompleted();
            assertThat(nativeController.inFlightRequests()).isZero();
            assertThat(nativeController.convertedBytes()).isZero();
            assertThat(closeFuture).isNotDone();
        } finally {
            releaseWorker.countDown();
            connection.close();
            if (closeFuture == null) {
                closeFuture = plugin.closeAsync();
            }
            closeFuture.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testCreateRequestHandlerUsesSharedTrackerDuringPluginClose() throws Exception {
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
        plugin.setup(new Configuration());
        NeverCompletingMetadataService service = new NeverCompletingMetadataService();
        KafkaRequestHandler handler = (KafkaRequestHandler) plugin.createRequestHandler(service);
        KafkaNativeProduceAdmissionController nativeController =
                plugin.getNativeAdmissionControllerForTesting();
        KafkaNativeProduceAdmissionController.ConnectionHandle connection =
                nativeController.registerConnection();
        java.util.concurrent.ScheduledExecutorService scheduler =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        short version = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest produceRequest = produceRequest(version);
        org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf buffer =
                org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator.DEFAULT.buffer();
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.PRODUCE,
                        version,
                        new RequestHeader(ApiKeys.PRODUCE, version, "client-id", 1),
                        produceRequest,
                        "KAFKA",
                        KafkaSaslConnection.plaintext(),
                        buffer,
                        new TestingChannelHandlerContext(),
                        new CompletableFuture<AbstractResponse>(),
                        System.nanoTime(),
                        buffer.readableBytes(),
                        connection,
                        scheduler);
        CompletableFuture<Void> closeFuture = null;

        try {
            handler.processRequest(request);

            assertThat(service.lookupStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(plugin.getNativeOperationTrackerForTesting().activeOperations()).isOne();
            assertThat(nativeController.inFlightRequests()).isOne();
            assertThat(nativeController.totalReservedBytes()).isPositive();

            closeFuture = plugin.closeAsync();

            ProduceResponse response = (ProduceResponse) request.future().get(5, TimeUnit.SECONDS);
            assertThat(response.errorCounts()).containsEntry(Errors.REQUEST_TIMED_OUT, 1);
            assertThatThrownBy(produceRequest::data)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("partition records are no longer available");
            assertThat(plugin.getNativeOperationTrackerForTesting().activeOperations()).isZero();
            assertThat(nativeController.inFlightRequests()).isZero();
            assertThat(nativeController.convertedBytes()).isZero();
            assertThat(nativeController.totalReservedBytes()).isZero();
            closeFuture.get(5, TimeUnit.SECONDS);
        } finally {
            connection.close();
            scheduler.shutdownNow();
            if (closeFuture == null) {
                closeFuture = plugin.closeAsync();
            }
            closeFuture.handle((ignored, failure) -> null).get(5, TimeUnit.SECONDS);
            buffer.release();
        }
    }

    @Test
    void testArrowBatchLimitUsesNativeAdmissionBytesInsteadOfWireLimit() {
        Configuration configuration = nativeLimitAboveWireConfiguration();
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
        plugin.setup(configuration);

        try {
            int maxWireBytes =
                    (int) configuration.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes();
            int maxNativeBytesPerConnection =
                    (int)
                            configuration
                                    .get(
                                            ConfigOptions
                                                    .KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES_PER_CONNECTION)
                                    .getBytes();

            assertThat(maxNativeBytesPerConnection).isGreaterThan(maxWireBytes);
            assertThat(plugin.getArrowWriterManagerForTesting().maxBatchSizeBytes())
                    .isEqualTo(maxNativeBytesPerConnection);
        } finally {
            plugin.closeAsync().join();
        }
    }

    @Test
    void testExactWireLimitProduceUsesNativeCopyBudget() throws Exception {
        Configuration configuration = nativeLimitAboveWireConfiguration();
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
        plugin.setup(configuration);
        NeverCompletingMetadataService service = new NeverCompletingMetadataService();
        KafkaRequestHandler handler = (KafkaRequestHandler) plugin.createRequestHandler(service);
        KafkaNativeProduceAdmissionController.ConnectionHandle connection =
                plugin.getNativeAdmissionControllerForTesting().registerConnection();
        java.util.concurrent.ScheduledExecutorService scheduler =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        short version = 3;
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, "client-id", 1);
        int maxWireBytes =
                (int) configuration.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes();
        ProduceRequest produceRequest = exactWireProduceRequest(version, header, maxWireBytes);
        MemoryRecords records =
                (MemoryRecords)
                        produceRequest
                                .data()
                                .topicData()
                                .iterator()
                                .next()
                                .partitionData()
                                .get(0)
                                .records();
        org.apache.kafka.common.record.Record exactRecord = records.records().iterator().next();
        assertThat(Record.estimateBaseCopiedBytes(exactRecord.keySize(), exactRecord.valueSize()))
                .isGreaterThan(maxWireBytes);
        org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf buffer =
                org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator.DEFAULT.buffer();
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.PRODUCE,
                        version,
                        header,
                        produceRequest,
                        "KAFKA",
                        KafkaSaslConnection.plaintext(),
                        buffer,
                        new TestingChannelHandlerContext(),
                        new CompletableFuture<AbstractResponse>(),
                        System.nanoTime(),
                        maxWireBytes,
                        connection,
                        scheduler);
        CompletableFuture<Void> closeFuture = null;

        try {
            handler.processRequest(request);

            assertThat(service.lookupStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(request.future()).isNotDone();

            closeFuture = plugin.closeAsync();
            ProduceResponse response = (ProduceResponse) request.future().get(5, TimeUnit.SECONDS);
            assertThat(response.errorCounts()).containsEntry(Errors.REQUEST_TIMED_OUT, 1);
            closeFuture.get(5, TimeUnit.SECONDS);
        } finally {
            connection.close();
            scheduler.shutdownNow();
            if (closeFuture == null) {
                closeFuture = plugin.closeAsync();
            }
            closeFuture.handle((ignored, failure) -> null).get(5, TimeUnit.SECONDS);
            buffer.release();
        }
    }

    private static Configuration nativeLimitAboveWireConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE, MemorySize.parse("1mb"));
        configuration.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES, MemorySize.parse("1mb"));
        configuration.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES, MemorySize.parse("4mb"));
        configuration.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES_PER_CONNECTION,
                MemorySize.parse("2mb"));
        return configuration;
    }

    private static ProduceRequest exactWireProduceRequest(
            short version, RequestHeader header, int targetWireBytes) {
        ProduceRequest emptyRequest = produceRequest(version, null, new byte[0]);
        int valueBytes = targetWireBytes - Integer.BYTES - serializedBytes(header, emptyRequest);
        for (int attempt = 0; attempt < 8; attempt++) {
            ProduceRequest candidate = produceRequest(version, null, new byte[valueBytes]);
            int candidateWireBytes = Integer.BYTES + serializedBytes(header, candidate);
            if (candidateWireBytes == targetWireBytes) {
                return candidate;
            }
            valueBytes += targetWireBytes - candidateWireBytes;
        }
        throw new IllegalArgumentException(
                "Could not construct an exact " + targetWireBytes + "-byte Produce frame.");
    }

    private static int serializedBytes(RequestHeader header, ProduceRequest request) {
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        return serialized.remaining();
    }

    private static ProduceRequest produceRequest(short version) {
        return produceRequest(
                version,
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));
    }

    private static ProduceRequest produceRequest(short version, byte[] key, byte[] value) {
        MemoryRecords records =
                MemoryRecords.withRecords(
                        org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2,
                        Compression.NONE,
                        new SimpleRecord(key, value));
        TopicProduceData topic =
                new TopicProduceData()
                        .setName("topic")
                        .setPartitionData(
                                Collections.singletonList(
                                        new PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(records)));
        ProduceRequestData data =
                new ProduceRequestData()
                        .setAcks((short) 1)
                        .setTimeoutMs(1_000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Collections.singletonList(topic).iterator()));
        return new ProduceRequest(data, version);
    }

    private static TableInfo tableInfo() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("payload", DataTypes.BYTES()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty(
                                KafkaDataFormat.VALUE_FORMAT_CONFIG, KafkaDataFormat.RAW.value())
                        .build();
        return TableInfo.of(
                TablePath.of("kafka", "plugin_close_test"), 1L, 1, descriptor, null, 1L, 1L);
    }

    private static final class ThrowingClosePauseRequestChannel extends RequestChannel {
        private ThrowingClosePauseRequestChannel() {
            super(100);
        }

        @Override
        public PauseLease pauseChannel(Channel channel, PauseReason reason) {
            return () -> {
                throw new IllegalStateException("test pause close failure");
            };
        }
    }

    private static final class NeverCompletingMetadataService extends TestingTabletGatewayService {
        private final CountDownLatch lookupStarted = new CountDownLatch(1);

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            lookupStarted.countDown();
            return new CompletableFuture<>();
        }
    }
}
