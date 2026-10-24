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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.EncodedRecords;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests append admission, error isolation, session propagation and asynchronous acknowledgements.
 */
class GatewayKafkaProduceBackendTest {

    private final BufferAllocator allocator = BufferAllocatorUtil.createBufferAllocator(null);
    private final LazyMemorySegmentPool memorySegmentPool =
            LazyMemorySegmentPool.createServerBufferPool(new Configuration());
    private final List<ArrowKafkaRecordTranscoder> transcoders = new ArrayList<>();
    private final List<KafkaProduceConversionExecutor> executors = new ArrayList<>();
    private final List<GatewayKafkaProduceBackend> backends = new ArrayList<>();
    private final List<TestingProduceService> services = new ArrayList<>();
    private final List<LazyMemorySegmentPool> additionalPools = new ArrayList<>();

    private ArrowKafkaRecordTranscoder createTranscoder() {
        return createTranscoder(memorySegmentPool);
    }

    private ArrowKafkaRecordTranscoder createTranscoder(LazyMemorySegmentPool pool) {
        ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder(allocator, pool);
        transcoders.add(transcoder);
        return transcoder;
    }

    @AfterEach
    void closeTranscoders() throws Exception {
        try {
            for (TestingProduceService service : services) {
                if (service.pendingMetadata != null) {
                    service.pendingMetadata.completeExceptionally(
                            new IllegalStateException("test cleanup"));
                }
                if (service.pendingAppend != null) {
                    service.pendingAppend.completeExceptionally(
                            new IllegalStateException("test cleanup"));
                }
            }
            for (KafkaProduceConversionExecutor executor : executors) {
                executor.closeAsync().get(10, TimeUnit.SECONDS);
            }
            for (GatewayKafkaProduceBackend backend : backends) {
                backend.closeAsync().get(10, TimeUnit.SECONDS);
            }
            assertThat(memorySegmentPool.usedPages()).isZero();
            for (LazyMemorySegmentPool pool : additionalPools) {
                assertThat(pool.usedPages()).isZero();
            }
        } finally {
            List<AutoCloseable> resources = new ArrayList<>(transcoders);
            resources.add(allocator);
            resources.add(memorySegmentPool);
            resources.addAll(additionalPools);
            IOUtils.closeAll(resources);
        }
    }

    @Test
    void testAsyncConversionOwnsPayloadsAfterWriteReturns() throws Exception {
        TestingProduceService service = service();
        service.pendingMetadata = new CompletableFuture<>();
        KafkaProduceConversionExecutor executor = executor(1, 2);
        List<Record> seen = new ArrayList<>();
        ArrowKafkaRecordTranscoder arrow = createTranscoder();
        GatewayKafkaProduceBackend backend =
                backend(
                        service,
                        (TestingTranscoder)
                                (partition, records, table) -> {
                                    seen.addAll(records);
                                    return arrow.transcode(partition, records, table);
                                },
                        executor);
        ByteBuffer key = ByteBuffer.allocateDirect(4);
        key.put(new byte[] {0, 1, 2, 0}).position(1).limit(3);
        byte[] value = {65, 66};
        byte[] header = {3, 4};
        Record record =
                new Record(
                        123L,
                        key.asReadOnlyBuffer(),
                        ByteBuffer.wrap(value),
                        Collections.singletonList(new RecordHeader("header", header)));
        try {
            CompletableFuture<KafkaProduceResult> result =
                    backend.write(
                            command(
                                    (short) 0,
                                    topic(
                                            "topic",
                                            new PartitionWrite(
                                                    0, Collections.singletonList(record)))));
            assertThat(result).isNotDone();
            // The network may reuse the request buffers before metadata lookup completes.
            key.put(1, (byte) 9).put(2, (byte) 9);
            Arrays.fill(value, (byte) 9);
            Arrays.fill(header, (byte) 9);
            CompletableFuture.runAsync(() -> service.pendingMetadata.complete(metadata(false)))
                    .get(10, TimeUnit.SECONDS);
            assertErrors(result.get(10, TimeUnit.SECONDS), Errors.NONE);
            assertThat(seen).hasSize(1);
            assertThat(seen.get(0).key()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2}));
            assertThat(seen.get(0).value()).isEqualTo(ByteBuffer.wrap(new byte[] {65, 66}));
            assertThat(seen.get(0).headers().get(0).value()).containsExactly((byte) 3, (byte) 4);
            assertThat(service.append.getAcks()).isZero();
        } finally {
            service.pendingMetadata.complete(metadata(false));
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testRevokedWritePermissionIsCheckedOnEveryAppend() throws Exception {
        TestingProduceService service = service();
        KafkaProduceConversionExecutor executor = executor(1, 2);
        GatewayKafkaProduceBackend backend = backend(service, createTranscoder(), executor);
        KafkaProduceCommand command = command((short) 1, topic("topic", nullValue(0)));
        try {
            assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE);
            service.denyWrite = true;
            assertErrors(
                    backend.write(command).get(10, TimeUnit.SECONDS),
                    Errors.TOPIC_AUTHORIZATION_FAILED);
            assertThat(service.writeChecks).isEqualTo(2);
            assertThat(service.appends).hasSize(1);
            assertThat(service.metadataThread).isNotSameAs(Thread.currentThread());
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testSharesMetadataAcrossPartitionsAndAliasesWithinEachRequest() throws Exception {
        TestingProduceService service = service();
        GatewayKafkaProduceBackend backend = backend(service);
        KafkaProduceCommand command =
                command(
                        (short) 1,
                        topic("topic", good(0), good(1)),
                        topic("kafka.topic", good(2)),
                        topic("other", good(0)));

        for (int request = 0; request < 2; request++) {
            KafkaProduceResult result = backend.write(command).get(10, TimeUnit.SECONDS);
            assertErrors(result, Errors.NONE, Errors.NONE);
            assertThat(result.topics().get(1).partitions().get(0).error()).isEqualTo(Errors.NONE);
            assertThat(result.topics().get(2).partitions().get(0).error()).isEqualTo(Errors.NONE);
            assertThat(service.metadataPaths)
                    .filteredOn(path -> path.equals(TablePath.of("kafka", "topic")))
                    .hasSize(request + 1);
            assertThat(service.metadataPaths)
                    .filteredOn(path -> path.equals(TablePath.of("kafka", "other")))
                    .hasSize(request + 1);
        }
        assertThat(service.appends).hasSize(8);
    }

    @Test
    void testRevokedMetadataPermissionIsCheckedAfterCacheHit() throws Exception {
        TestingProduceService service = service();
        GatewayKafkaProduceBackend backend = backend(service);
        KafkaProduceCommand command = command((short) 1, topic("topic", good(0), good(1)));

        assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE, Errors.NONE);
        service.denyMetadata = true;
        assertErrors(
                backend.write(command).get(10, TimeUnit.SECONDS),
                Errors.TOPIC_AUTHORIZATION_FAILED,
                Errors.TOPIC_AUTHORIZATION_FAILED);
        assertThat(service.metadataPaths).hasSize(2);
        assertThat(service.appends).hasSize(2);
    }

    @Test
    void testPropertyChangesInvalidateCachedAdmissionWithoutSchemaChange() throws Exception {
        TestingProduceService service = service();
        GatewayKafkaProduceBackend backend = backend(service);
        KafkaProduceCommand command = command((short) 1, topic("topic", good(0), good(1)));

        assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE, Errors.NONE);
        service.metadataResponse = metadata(true);
        assertErrors(
                backend.write(command).get(10, TimeUnit.SECONDS),
                Errors.INVALID_TOPIC_EXCEPTION,
                Errors.INVALID_TOPIC_EXCEPTION);
        service.metadataResponse =
                metadata(
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("_kafka_value", DataTypes.STRING())
                                                .build())
                                .distributedBy(3)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                        KafkaValueConversionType.STRING)
                                .build());
        assertErrors(
                backend.write(command).get(10, TimeUnit.SECONDS),
                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                Errors.UNKNOWN_TOPIC_OR_PARTITION);
        service.metadataResponse = metadata(false);
        assertErrors(backend.write(command).get(10, TimeUnit.SECONDS), Errors.NONE, Errors.NONE);
        assertThat(service.metadataPaths).hasSize(4);
        assertThat(service.appends).hasSize(4);
    }

    private static PartitionWrite nullValue(int partition) {
        return new PartitionWrite(
                partition,
                Collections.singletonList(
                        new Record(
                                -1L,
                                ByteBuffer.wrap(new byte[] {(byte) 0xff}),
                                null,
                                Collections.emptyList())));
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "failure", "cancel"})
    void testPooledOutputLivesUntilNativeAppendCompletes(String outcome) throws Exception {
        TestingProduceService service = service();
        service.pendingAppend = new CompletableFuture<>();
        service.appended = new CountDownLatch(2);
        KafkaProduceConversionExecutor executor = executor(1, 2);
        GatewayKafkaProduceBackend backend = backend(service, createTranscoder(), executor);
        try {
            CompletableFuture<KafkaProduceResult> result =
                    backend.write(command((short) -1, topic("topic", good(0), good(1))));
            assertThat(service.appended.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(service.append.getAcks()).isEqualTo(-1);
            assertThat(service.append.getTimeoutMs()).isEqualTo(4321);
            assertThat(service.append.getTableId()).isEqualTo(42L);
            executor.closeAsync().get(10, TimeUnit.SECONDS);
            assertThat(result).isNotDone();
            assertThat(service.pendingAppend).isNotCancelled();
            if (outcome.equals("cancel")) {
                result.cancel(false);
            }
            CompletableFuture<Void> closed = backend.closeAsync();
            assertThat(closed).isNotDone();
            assertThat(memorySegmentPool.usedPages()).isPositive();
            if (outcome.equals("failure")) {
                service.pendingAppend.completeExceptionally(
                        new IllegalStateException("append failed"));
                assertErrors(
                        result.get(10, TimeUnit.SECONDS),
                        Errors.UNKNOWN_SERVER_ERROR,
                        Errors.UNKNOWN_SERVER_ERROR);
            } else {
                service.pendingAppend.complete(
                        success(0).addAllBucketsResps(success(1).getBucketsRespsList()));
                if (!outcome.equals("cancel")) {
                    KafkaProduceResult completed = result.get(10, TimeUnit.SECONDS);
                    assertErrors(completed, Errors.NONE, Errors.NONE);
                    assertThat(completed.topics().get(0).partitions().get(0).baseOffset())
                            .isEqualTo(17L);
                }
            }
            closed.get(10, TimeUnit.SECONDS);
            assertThat(memorySegmentPool.usedPages()).isZero();
            assertThat(backend.closeAsync()).isSameAs(closed);
        } finally {
            service.pendingAppend.completeExceptionally(new IllegalStateException("test cleanup"));
            executor.closeAsync().get(10, TimeUnit.SECONDS);
            backend.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testConversionAndBucketErrorsAreIsolatedFromValidWrites() throws Exception {
        TestingProduceService service = service();
        ArrowKafkaRecordTranscoder arrow = createTranscoder();
        GatewayKafkaProduceBackend backend =
                backend(
                        service,
                        (TestingTranscoder)
                                (partitionId, records, table) -> {
                                    if (partitionId == 0) {
                                        throw new KafkaRecordEncodingException("invalid record");
                                    }
                                    return arrow.transcode(partitionId, records, table);
                                },
                        executor(2, 4));
        KafkaProduceResult result =
                backend.write(command((short) 1, topic("topic", good(0), good(8), good(1)))).join();
        assertErrors(result, Errors.INVALID_RECORD, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE);
        assertThat(service.append.getBucketsReqsList())
                .extracting(bucket -> bucket.getBucketId())
                .containsExactly(1);
    }

    @Test
    void testInvalidAndMissingTablesDoNotSuppressOtherTopics() throws Exception {
        TestingProduceService service = service();
        KafkaProduceResult result =
                backend(service)
                        .write(
                                command(
                                        (short) 1,
                                        topic("kafka.invalid", nullValue(0)),
                                        topic("kafka.missing", good(0)),
                                        topic("kafka.topic", good(0))))
                        .join();
        assertThat(result.topics())
                .extracting(topic -> topic.partitions().get(0).error())
                .containsExactly(
                        Errors.INVALID_TOPIC_EXCEPTION,
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.NONE);
        assertThat(service.appends).hasSize(1);
        assertThat(service.writeChecks).isEqualTo(1);
    }

    @Test
    void testMapsReplicationFailuresAndMissingResponsesPerPartition() throws Exception {
        org.apache.fluss.rpc.protocol.Errors flussError =
                org.apache.fluss.rpc.protocol.Errors.NOT_ENOUGH_REPLICAS_EXCEPTION;
        TestingProduceService service = service();
        service.pendingAppend =
                CompletableFuture.completedFuture(
                        success(1)
                                .addAllBucketsResps(
                                        Collections.singletonList(
                                                new PbProduceLogRespForBucket()
                                                        .setBucketId(0)
                                                        .setErrorCode(flussError.code()))));
        KafkaProduceResult result =
                backend(service)
                        .write(command((short) -1, topic("kafka.topic", good(0), good(1), good(2))))
                        .join();
        assertErrors(result, Errors.NOT_ENOUGH_REPLICAS, Errors.NONE, Errors.UNKNOWN_SERVER_ERROR);
        assertThat(result.topics().get(0).partitions().get(1).baseOffset()).isEqualTo(17L);
    }

    @Test
    void testAliasesSerializeConversionAndSubmitWithoutWaitingForAcknowledgement()
            throws Exception {
        KafkaProduceConversionExecutor executor = executor(2, 4);
        TestingProduceService service = service();
        service.pendingAppend = new CompletableFuture<>();
        service.appended = new CountDownLatch(2);
        CountDownLatch firstConverting = new CountDownLatch(1);
        CountDownLatch secondConverted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        ArrowKafkaRecordTranscoder arrow = createTranscoder();
        java.util.Map<Integer, byte[]> encodedBytes =
                new java.util.concurrent.ConcurrentHashMap<>();
        KafkaRecordTranscoder transcoder =
                (TestingTranscoder)
                        (partitionId, records, table) -> {
                            ByteBuffer buffer = records.get(0).value();
                            byte[] value = new byte[buffer.remaining()];
                            buffer.get(value);
                            if (value[0] == 1) {
                                firstConverting.countDown();
                                assertThat(releaseFirst.await(10, TimeUnit.SECONDS)).isTrue();
                            } else {
                                secondConverted.countDown();
                            }
                            EncodedRecords encoded = arrow.transcode(partitionId, records, table);
                            byte[] bytes = new byte[encoded.records().sizeInBytes()];
                            encoded.records()
                                    .getMemorySegment()
                                    .get(encoded.records().getPosition(), bytes);
                            encodedBytes.put((int) value[0], bytes);
                            return encoded;
                        };
        GatewayKafkaProduceBackend backend = backend(service, transcoder, executor);
        try {
            CompletableFuture<KafkaProduceResult> first =
                    backend.write(command((short) -1, topic("topic", good(0, 1))));
            assertThat(firstConverting.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<KafkaProduceResult> second =
                    backend.write(command((short) -1, topic("kafka.topic", good(0, 2))));
            executor.submit("unrelated", () -> CompletableFuture.completedFuture(null))
                    .get(10, TimeUnit.SECONDS);
            assertThat(secondConverted.getCount()).isEqualTo(1);
            assertThat(service.appends).isEmpty();
            releaseFirst.countDown();
            assertThat(service.appended.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(service.appends)
                    .extracting(request -> request.getBucketsReqsList().get(0).getRecords())
                    .containsExactly(encodedBytes.get(1), encodedBytes.get(2));
            assertThat(first).isNotDone();
            assertThat(second).isNotDone();
            service.pendingAppend.complete(success(0));
            assertErrors(first.get(10, TimeUnit.SECONDS), Errors.NONE);
            assertErrors(second.get(10, TimeUnit.SECONDS), Errors.NONE);
            assertThat(first.get().topics().get(0).topicName()).isEqualTo("topic");
            assertThat(second.get().topics().get(0).topicName()).isEqualTo("kafka.topic");
        } finally {
            releaseFirst.countDown();
            service.pendingAppend.completeExceptionally(new IllegalStateException("test cleanup"));
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testLaterWriteCannotReserveOnlyPoolPageBeforeEarlierWrite() throws Exception {
        Configuration poolConfig = new Configuration();
        MemorySize pageSize = MemorySize.parse("128kb");
        poolConfig.set(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE, pageSize);
        poolConfig.set(ConfigOptions.SERVER_BUFFER_PAGE_SIZE, pageSize);
        poolConfig.set(ConfigOptions.SERVER_BUFFER_PER_REQUEST_MEMORY_SIZE, pageSize);
        LazyMemorySegmentPool pool = LazyMemorySegmentPool.createServerBufferPool(poolConfig);
        additionalPools.add(pool);
        CompletableFuture<GetTableInfoResponse> firstMetadata = new CompletableFuture<>();
        CountDownLatch firstLookup = new CountDownLatch(1);
        AtomicInteger metadataCalls = new AtomicInteger();
        TestingProduceService service =
                new TestingProduceService() {
                    @Override
                    public CompletableFuture<GetTableInfoResponse> getTableInfo(
                            GetTableInfoRequest request) {
                        if (metadataCalls.getAndIncrement() == 0) {
                            firstLookup.countDown();
                            return firstMetadata;
                        }
                        return super.getTableInfo(request);
                    }
                };
        services.add(service);
        KafkaProduceConversionExecutor executor = executor(2, 4);
        GatewayKafkaProduceBackend backend = backend(service, createTranscoder(pool), executor);
        try {
            KafkaProduceCommand delayedCommand =
                    new KafkaProduceCommand(
                            (short) 1,
                            60_000,
                            Collections.singletonList(topic("topic", good(0, 1))),
                            "KAFKA",
                            InetAddress.getLoopbackAddress());
            CompletableFuture<KafkaProduceResult> first = backend.write(delayedCommand);
            assertThat(firstLookup.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<KafkaProduceResult> second =
                    backend.write(command((short) 1, topic("kafka.topic", good(0, 2))));

            // A same-bucket follower must neither reserve the only page nor occupy the spare
            // worker.
            executor.submit("unrelated", () -> CompletableFuture.completedFuture(null))
                    .get(10, TimeUnit.SECONDS);
            assertThat(pool.usedPages()).isZero();
            assertThat(service.appends).isEmpty();
            firstMetadata.complete(metadata(false));

            assertErrors(first.get(10, TimeUnit.SECONDS), Errors.NONE);
            assertErrors(second.get(10, TimeUnit.SECONDS), Errors.NONE);
            assertThat(service.appends).hasSize(2);
            assertThat(pool.usedPages()).isZero();
        } finally {
            firstMetadata.complete(metadata(false));
            executor.closeAsync().get(10, TimeUnit.SECONDS);
            backend.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testSaturationAndShutdownReturnRetriableErrorsWithoutAppend() throws Exception {
        KafkaProduceConversionExecutor executor = executor(1, 1);
        TestingProduceService service = service();
        service.pendingMetadata = new CompletableFuture<>();
        service.metadataEntered = new CountDownLatch(1);
        GatewayKafkaProduceBackend backend = backend(service, createTranscoder(), executor);
        KafkaProduceCommand command = command((short) 1, topic("kafka.topic", good(0)));
        try {
            CompletableFuture<KafkaProduceResult> running = backend.write(command);
            assertThat(service.metadataEntered.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<KafkaProduceResult> queued = backend.write(command);
            assertErrors(
                    backend.write(command).get(10, TimeUnit.SECONDS), Errors.REQUEST_TIMED_OUT);
            executor.closeAsync().get(10, TimeUnit.SECONDS);
            assertErrors(running.get(10, TimeUnit.SECONDS), Errors.REQUEST_TIMED_OUT);
            assertErrors(queued.get(10, TimeUnit.SECONDS), Errors.REQUEST_TIMED_OUT);
            assertErrors(
                    backend.write(command).get(10, TimeUnit.SECONDS), Errors.REQUEST_TIMED_OUT);
            service.pendingMetadata.complete(metadata(false));
            assertThat(service.appends).isEmpty();
            assertThat(service.metadataPaths).hasSize(1);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testDefaultBackendClosesItsConversionExecutor() throws Exception {
        TestingProduceService service = service();
        service.pendingMetadata = new CompletableFuture<>();
        service.metadataEntered = new CountDownLatch(1);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, service, mapper(), createTranscoder());
        backends.add(backend);
        CompletableFuture<KafkaProduceResult> result =
                backend.write(command((short) 1, topic("topic", good(0))));
        assertThat(service.metadataEntered.await(10, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Void> closed = backend.closeAsync();
        closed.get(10, TimeUnit.SECONDS);

        assertErrors(result.get(10, TimeUnit.SECONDS), Errors.REQUEST_TIMED_OUT);
        assertThat(backend.closeAsync()).isSameAs(closed);
        assertThat(service.appends).isEmpty();
        assertThat(memorySegmentPool.usedPages()).isZero();
    }

    private interface TestingTranscoder extends KafkaRecordTranscoder {
        @Override
        default void close() {}
    }

    private static KafkaTopicMapper mapper() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "kafka");
        return new KafkaTopicMapper(config);
    }

    private GatewayKafkaProduceBackend backend(TestingProduceService service) {
        return backend(service, createTranscoder(), executor(2, 8));
    }

    private GatewayKafkaProduceBackend backend(
            TestingProduceService service,
            KafkaRecordTranscoder transcoder,
            KafkaProduceConversionExecutor executor) {
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, service, mapper(), transcoder, executor);
        backends.add(backend);
        return backend;
    }

    private KafkaProduceConversionExecutor executor(int threads, int queueCapacity) {
        KafkaProduceConversionExecutor executor =
                new KafkaProduceConversionExecutor(threads, queueCapacity);
        executors.add(executor);
        return executor;
    }

    private TestingProduceService service() {
        TestingProduceService service = new TestingProduceService();
        services.add(service);
        return service;
    }

    private static KafkaProduceCommand command(short acks, TopicWrite... topics) throws Exception {
        return new KafkaProduceCommand(
                acks, 4321, Arrays.asList(topics), "KAFKA", InetAddress.getLoopbackAddress());
    }

    private static TopicWrite topic(String name, PartitionWrite... partitions) {
        return new TopicWrite(name, Arrays.asList(partitions));
    }

    private static PartitionWrite good(int id) {
        return good(id, 65);
    }

    private static PartitionWrite good(int id, int value) {
        return new PartitionWrite(
                id,
                Collections.singletonList(
                        new Record(
                                123L,
                                null,
                                ByteBuffer.wrap(new byte[] {(byte) value}),
                                Collections.emptyList())));
    }

    private static GetTableInfoResponse metadata(boolean invalid) {
        TableDescriptor descriptor =
                descriptor(
                        Schema.newBuilder().column("_kafka_value", DataTypes.STRING()).build(),
                        invalid
                                ? KafkaValueConversionType.BINARY
                                : KafkaValueConversionType.STRING);
        return metadata(descriptor);
    }

    private static GetTableInfoResponse metadata(TableDescriptor descriptor) {
        return new GetTableInfoResponse()
                .setTableId(42L)
                .setSchemaId(1)
                .setTableJson(descriptor.toJsonBytes())
                .setCreatedTime(1L)
                .setModifiedTime(1L);
    }

    private static TableDescriptor descriptor(Schema schema, KafkaValueConversionType format) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(3)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, format)
                .build();
    }

    private static ProduceLogResponse success(int id) {
        return new ProduceLogResponse()
                .addAllBucketsResps(
                        Collections.singletonList(
                                new PbProduceLogRespForBucket()
                                        .setBucketId(id)
                                        .setBaseOffset(17L)));
    }

    private static void assertErrors(KafkaProduceResult result, Errors... errors) {
        List<PartitionResult> partitions = result.topics().get(0).partitions();
        assertThat(partitions).extracting(PartitionResult::error).containsExactly(errors);
        for (PartitionResult partition : partitions) {
            if (partition.error() != Errors.NONE) {
                assertThat(partition.baseOffset()).isEqualTo(-1L);
            }
        }
    }

    private static class TestingProduceService extends TestingTabletGatewayService {
        private final List<TablePath> metadataPaths =
                Collections.synchronizedList(new ArrayList<>());
        private final List<ProduceLogRequest> appends =
                Collections.synchronizedList(new ArrayList<>());
        private ProduceLogRequest append;
        private CompletableFuture<ProduceLogResponse> pendingAppend;
        private CompletableFuture<GetTableInfoResponse> pendingMetadata;
        private GetTableInfoResponse metadataResponse;
        private int writeChecks;
        private boolean denyWrite;
        private boolean denyMetadata;
        private CountDownLatch metadataEntered;
        private CountDownLatch appended;
        private Thread metadataThread;

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            String database = request.getTablePath().getDatabaseName();
            String name = request.getTablePath().getTableName();
            metadataPaths.add(TablePath.of(database, name));
            metadataThread = Thread.currentThread();
            if (metadataEntered != null) {
                metadataEntered.countDown();
            }
            if (name.equals("missing")) {
                throw new TableNotExistException("missing");
            }
            if (denyMetadata) {
                throw new AuthorizationException("Describe denied");
            }
            return pendingMetadata == null
                    ? CompletableFuture.completedFuture(
                            metadataResponse == null
                                    ? GatewayKafkaProduceBackendTest.metadata(
                                            name.equals("invalid"))
                                    : metadataResponse)
                    : pendingMetadata;
        }

        private synchronized void checkWriteAuthorization(long tableId) {
            assertThat(tableId).isPositive();
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            assertThat(currentSession().getInetAddress())
                    .isEqualTo(InetAddress.getLoopbackAddress());
            writeChecks++;
            if (denyWrite) {
                throw new AuthorizationException("Write denied");
            }
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            checkWriteAuthorization(request.getTableId());
            assertThat(currentListenerName()).isEqualTo("KAFKA");
            assertThat(currentSession().getInetAddress())
                    .isEqualTo(InetAddress.getLoopbackAddress());
            ProduceLogRequest copiedRequest = new ProduceLogRequest().copyFrom(request);
            append = copiedRequest;
            appends.add(copiedRequest);
            if (appended != null) {
                appended.countDown();
            }
            return pendingAppend == null
                    ? CompletableFuture.completedFuture(
                            success(request.getBucketsReqsList().get(0).getBucketId()))
                    : pendingAppend;
        }
    }
}
