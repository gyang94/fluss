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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.AdmissionTimeoutException;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.AdmissionUnavailableException;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.RequestLease;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.RequestTooLargeException;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.Reservation;
import org.apache.fluss.kafka.backend.produce.KafkaNativeProduceOperationTracker.PreSubmitOperation;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.TopicResult;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaOutputMemoryBudget;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaTopicWritePlan;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.AdminOperationAuthorizer;
import org.apache.fluss.rpc.gateway.RoutedKvGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.OutOfMemoryException;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Adapts the local TabletServer write gateway to the Kafka Produce backend contract. */
@Internal
public final class GatewayKafkaProduceBackend implements KafkaProduceBackend {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayKafkaProduceBackend.class);
    private static final Duration DEFAULT_NATIVE_ADMISSION_ACQUIRE_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_NATIVE_COMPLETION_GRACE_TIMEOUT = Duration.ofMinutes(5);
    private static final Executor DIRECT_EXECUTOR = Runnable::run;

    private final KafkaTopicMapper topicMapper = new KafkaTopicMapper();
    private final RpcGatewayService service;
    private final TabletServerGateway gateway;
    private final KafkaRecordTranscoder transcoder;
    private final KafkaProduceMetrics produceMetrics;
    private final KafkaTableInfoCache tableInfoCache;
    private final Executor conversionExecutor;
    private final Duration nativeAdmissionAcquireTimeout;
    private final Duration nativeCompletionGraceTimeout;
    private final KafkaNativeProduceOperationTracker operationTracker;

    /** Creates a Produce backend backed by the local TabletServer gateway. */
    public GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaRecordTranscoder transcoder) {
        this(
                service,
                gateway,
                transcoder,
                KafkaProduceMetrics.noOp(),
                new KafkaTableInfoCache(),
                DIRECT_EXECUTOR,
                DEFAULT_NATIVE_ADMISSION_ACQUIRE_TIMEOUT,
                DEFAULT_NATIVE_COMPLETION_GRACE_TIMEOUT);
    }

    /** Creates a local gateway Produce backend with runtime metrics. */
    public GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaRecordTranscoder transcoder,
            KafkaProduceMetrics produceMetrics) {
        this(
                service,
                gateway,
                transcoder,
                produceMetrics,
                new KafkaTableInfoCache(),
                DIRECT_EXECUTOR,
                DEFAULT_NATIVE_ADMISSION_ACQUIRE_TIMEOUT,
                DEFAULT_NATIVE_COMPLETION_GRACE_TIMEOUT);
    }

    /** Creates a local gateway Produce backend with bounded native admission execution. */
    public GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaRecordTranscoder transcoder,
            KafkaProduceMetrics produceMetrics,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout) {
        this(
                service,
                gateway,
                transcoder,
                produceMetrics,
                conversionExecutor,
                nativeAdmissionAcquireTimeout,
                nativeCompletionGraceTimeout,
                new KafkaNativeProduceOperationTracker());
    }

    /** Creates a local gateway Produce backend with a shared shutdown lifecycle tracker. */
    public GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaRecordTranscoder transcoder,
            KafkaProduceMetrics produceMetrics,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout,
            KafkaNativeProduceOperationTracker operationTracker) {
        this(
                service,
                gateway,
                transcoder,
                produceMetrics,
                new KafkaTableInfoCache(),
                conversionExecutor,
                nativeAdmissionAcquireTimeout,
                nativeCompletionGraceTimeout,
                operationTracker);
    }

    GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaRecordTranscoder transcoder,
            KafkaProduceMetrics produceMetrics,
            KafkaTableInfoCache tableInfoCache) {
        this(
                service,
                gateway,
                transcoder,
                produceMetrics,
                tableInfoCache,
                DIRECT_EXECUTOR,
                DEFAULT_NATIVE_ADMISSION_ACQUIRE_TIMEOUT,
                DEFAULT_NATIVE_COMPLETION_GRACE_TIMEOUT);
    }

    GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaRecordTranscoder transcoder,
            KafkaProduceMetrics produceMetrics,
            KafkaTableInfoCache tableInfoCache,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout) {
        this(
                service,
                gateway,
                transcoder,
                produceMetrics,
                tableInfoCache,
                conversionExecutor,
                nativeAdmissionAcquireTimeout,
                nativeCompletionGraceTimeout,
                new KafkaNativeProduceOperationTracker());
    }

    GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaRecordTranscoder transcoder,
            KafkaProduceMetrics produceMetrics,
            KafkaTableInfoCache tableInfoCache,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout,
            KafkaNativeProduceOperationTracker operationTracker) {
        this.service = checkNotNull(service);
        this.gateway = checkNotNull(gateway);
        this.transcoder = checkNotNull(transcoder);
        this.produceMetrics = checkNotNull(produceMetrics);
        this.tableInfoCache = checkNotNull(tableInfoCache);
        this.conversionExecutor = checkNotNull(conversionExecutor);
        this.nativeAdmissionAcquireTimeout = checkNotNull(nativeAdmissionAcquireTimeout);
        this.nativeCompletionGraceTimeout = checkNotNull(nativeCompletionGraceTimeout);
        this.operationTracker = checkNotNull(operationTracker);
    }

    @Override
    public CompletableFuture<KafkaProduceResult> write(KafkaProduceCommand command) {
        List<CompletableFuture<TopicResult>> futures = new ArrayList<>();
        for (TopicWrite topic : command.topics()) {
            futures.add(writeTopic(command, topic));
        }
        CompletableFuture<Void> all =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
        CompletableFuture<KafkaProduceResult> result =
                all.thenApply(
                        ignored -> {
                            List<TopicResult> results = new ArrayList<>();
                            for (CompletableFuture<TopicResult> future : futures) {
                                results.add(future.join());
                            }
                            return new KafkaProduceResult(results);
                        });
        result.whenComplete((ignored, failure) -> command.completeNativeAdmissionTransfer());
        return result;
    }

    private CompletableFuture<TopicResult> writeTopic(
            KafkaProduceCommand command, TopicWrite topic) {
        CompletableFuture<TopicResult> result;
        CompletableFuture<RequestLease> admissionFuture;
        try {
            admissionFuture = acquireNativeAdmission(command, topic);
        } catch (Throwable failure) {
            // No native byte token protects this topic. Keep the PF raw owner until the whole
            // request is terminal so response metadata retained by a slow sibling stays charged.
            command.requireNativeAdmissionTransferFallback();
            result = CompletableFuture.completedFuture(failedTopic(command, topic, failure));
            return completeTopicOwnershipOnTerminal(command, topic, result);
        }
        result =
                admissionFuture
                        .thenCompose(lease -> executeAdmittedTopic(command, topic, lease))
                        .exceptionally(failure -> failedTopic(command, topic, failure));
        return completeTopicOwnershipOnTerminal(command, topic, result);
    }

    private CompletableFuture<TopicResult> completeTopicOwnershipOnTerminal(
            KafkaProduceCommand command, TopicWrite topic, CompletableFuture<TopicResult> result) {
        return result.whenComplete(
                (ignored, failure) -> {
                    topic.releaseCopiedRecords();
                    command.completeNativeAdmissionTransfer(topic);
                });
    }

    private CompletableFuture<RequestLease> acquireNativeAdmission(
            KafkaProduceCommand command, TopicWrite topic) {
        ConnectionHandle connection = command.nativeAdmissionConnection();
        if (connection == null) {
            return CompletableFuture.completedFuture(null);
        }

        long startedNanos = produceMetrics.nowNanos();
        Reservation reservation =
                connection.reserve(topic.estimatedConvertedBytes(), topic::releaseCopiedRecords);
        ScheduledFuture<?> timeoutTask = scheduleAdmissionTimeout(command, reservation);
        CompletableFuture<RequestLease> future = reservation.getFuture();
        if (reservation.ownsByteReservation()) {
            // A pending reservation already owns its estimated-byte token. A granted reservation
            // has atomically migrated the same token to converted bytes. Either state protects the
            // copied topic payload, so PF raw ownership may transfer before metadata/native waits.
            command.completeNativeAdmissionTransfer(topic);
        } else {
            // An immediate rejection owns no native bytes. The topic payload is cleared by the
            // reservation/result cleanup, but the request envelope can remain reachable from a
            // slow sibling until the aggregate result completes.
            command.requireNativeAdmissionTransferFallback();
        }
        future.whenComplete(
                (lease, failure) -> {
                    cancelTimer(timeoutTask);
                    Throwable cause = unwrap(failure);
                    if (cause == null) {
                        recordMetricsBestEffort(
                                () ->
                                        produceMetrics.recordNativeAdmissionGranted(
                                                startedNanos, topic.estimatedConvertedBytes()));
                    } else if (cause instanceof AdmissionTimeoutException) {
                        recordMetricsBestEffort(
                                () -> produceMetrics.recordNativeAdmissionTimeout(startedNanos));
                    } else if (cause instanceof CancellationException) {
                        recordMetricsBestEffort(produceMetrics::recordNativeAdmissionCancelled);
                    } else if (cause instanceof RequestTooLargeException) {
                        recordMetricsBestEffort(produceMetrics::recordNativeRequestTooLarge);
                    } else {
                        recordMetricsBestEffort(produceMetrics::recordNativeAdmissionRejected);
                    }
                });
        return future;
    }

    private @Nullable ScheduledFuture<?> scheduleAdmissionTimeout(
            KafkaProduceCommand command, Reservation reservation) {
        ScheduledExecutorService scheduler = command.admissionScheduler();
        if (scheduler == null) {
            return null;
        }
        try {
            return scheduler.schedule(
                    (Runnable) reservation::timeout,
                    nativeAdmissionAcquireTimeout.toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (RuntimeException schedulingFailure) {
            // A connection event loop that is already shutting down cannot own an unbounded
            // waiter. Linearize this as an admission timeout.
            reservation.timeout();
            return null;
        }
    }

    private CompletableFuture<TopicResult> executeAdmittedTopic(
            KafkaProduceCommand command, TopicWrite topic, @Nullable RequestLease lease) {
        NativeLeaseScope leaseScope =
                new NativeLeaseScope(
                        lease,
                        command.nativeAdmissionConnection(),
                        command.admissionScheduler(),
                        produceMetrics.nowNanos());
        CompletableFuture<TopicResult> result;
        try {
            result =
                    submitConversion(leaseScope, () -> lookupTableInfo(command, topic))
                            .thenCompose(
                                    response ->
                                            submitConversion(
                                                    leaseScope,
                                                    () ->
                                                            produceTopic(
                                                                    command,
                                                                    topic,
                                                                    toTableInfo(topic, response),
                                                                    leaseScope)));
        } catch (Throwable failure) {
            topic.releaseCopiedRecords();
            leaseScope.closeUnlessSubmitted();
            return failedFuture(failure);
        }
        result.whenComplete(
                (ignored, failure) -> {
                    // Quiesce the underlying conversion before copied records become unreachable
                    // and its byte token is returned to another waiter.
                    try {
                        topic.releaseCopiedRecords();
                        if (unwrap(failure) instanceof RejectedExecutionException) {
                            recordMetricsBestEffort(produceMetrics::recordNativeAdmissionRejected);
                        }
                    } finally {
                        leaseScope.closeUnlessSubmitted();
                    }
                });
        return leaseScope.guardPreSubmit(result);
    }

    private <T> CompletableFuture<T> submitConversion(
            NativeLeaseScope leaseScope, ConversionOperation<T> operation) {
        CompletableFuture<T> result = new CompletableFuture<>();
        ConversionTask<T> task = new ConversionTask<>(leaseScope, operation, result);
        leaseScope.registerTask(task);
        try {
            conversionExecutor.execute(task);
        } catch (Throwable failure) {
            task.cancel(failure);
        }
        return result;
    }

    private CompletableFuture<GetTableInfoResponse> lookupTableInfo(
            KafkaProduceCommand command, TopicWrite topic) {
        setCurrentSession(command);
        GetTableInfoRequest request = new GetTableInfoRequest();
        TablePath tablePath = topicMapper.toTablePath(topic.topicName());
        request.setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        long lookupStartedNanos = produceMetrics.nowNanos();
        CompletableFuture<GetTableInfoResponse> tableInfoFuture;
        try {
            tableInfoFuture = gateway.getTableInfo(request);
        } catch (Throwable failure) {
            recordMetricsBestEffort(() -> produceMetrics.recordTableInfoLookup(lookupStartedNanos));
            throw failure;
        }
        tableInfoFuture.whenComplete(
                (response, failure) ->
                        recordMetricsBestEffort(
                                () -> produceMetrics.recordTableInfoLookup(lookupStartedNanos)));
        return tableInfoFuture;
    }

    private CompletableFuture<TopicResult> produceTopic(
            KafkaProduceCommand command,
            TopicWrite topic,
            TableInfo tableInfo,
            NativeLeaseScope leaseScope) {
        leaseScope.checkpoint();
        if (tableInfo.hasPrimaryKey()) {
            return producePrimaryKeyTopic(command, topic, tableInfo, leaseScope);
        }
        ProduceLogRequest request =
                new ProduceLogRequest()
                        .setTableId(tableInfo.getTableId())
                        .setAcks(command.acks())
                        .setTimeoutMs(command.timeoutMs());
        List<BytesView> retainedRecords = new ArrayList<>();
        Map<Integer, PartitionResult> localFailures = new HashMap<>();
        TopicOutputMemoryBudget outputMemoryBudget =
                new TopicOutputMemoryBudget(leaseScope, topic.estimatedConvertedBytes());
        final KafkaTopicWritePlan writePlan;
        try {
            writePlan = transcoder.prepare(tableInfo);
            leaseScope.checkpoint();
        } catch (Exception e) {
            for (PartitionWrite partition : topic.partitions()) {
                localFailures.put(
                        partition.partitionId(),
                        failedPartition(command, partition.partitionId(), e));
            }
            return CompletableFuture.completedFuture(
                    toTopicResult(command, topic, null, localFailures));
        }
        for (PartitionWrite partition : topic.partitions()) {
            try {
                if (partition.nullValueCount() > 0
                        && partition.nullValueCount() == partition.recordCount()) {
                    leaseScope.checkpoint();
                    if (partition.partitionId() < 0
                            || partition.partitionId() >= tableInfo.getNumBuckets()) {
                        localFailures.put(
                                partition.partitionId(),
                                new PartitionResult(
                                        partition.partitionId(),
                                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                        -1L,
                                        null));
                        continue;
                    }
                    authorizeDroppedWrite(command, tableInfo);
                    localFailures.put(
                            partition.partitionId(),
                            new PartitionResult(partition.partitionId(), Errors.NONE, -1L, null));
                    continue;
                }
                BytesView records =
                        transcodePartition(topic, partition, writePlan, outputMemoryBudget);
                retainedRecords.add(records);
                request.addBucketsReq()
                        .setBucketId(partition.partitionId())
                        .setRecordsBytesView(records);
            } catch (RequestTooLargeException
                    | AdmissionUnavailableException
                    | CancellationException admissionFailure) {
                throw admissionFailure;
            } catch (Exception e) {
                localFailures.put(
                        partition.partitionId(),
                        failedPartition(command, partition.partitionId(), e));
            } finally {
                topic.releaseCopiedRecords(partition);
                outputMemoryBudget.releaseSource(partition.estimatedCopiedRecordBytes());
            }
        }
        outputMemoryBudget.completeConversion();
        if (retainedRecords.isEmpty()) {
            return CompletableFuture.completedFuture(
                    toTopicResult(command, topic, null, localFailures));
        }

        if (!leaseScope.tryMarkSubmitted()) {
            throw new CancellationException(
                    "Kafka connection closed before native Produce submission.");
        }

        setCurrentSession(command);
        long submitStartedNanos = produceMetrics.nowNanos();
        CompletableFuture<ProduceLogResponse> produceFuture;
        try {
            produceFuture = checkNotNull(gateway.produceLog(request), "native Produce future");
            leaseScope.handOffToOriginalFuture(
                    produceFuture, retainedRecords, () -> clearNativeRequestPayload(request));
        } finally {
            runPostSubmitMaintenance(submitStartedNanos);
        }
        if (command.acks() == -1) {
            long acksWaitStartedNanos = produceMetrics.nowNanos();
            produceFuture.whenComplete(
                    (response, failure) ->
                            recordMetricsBestEffort(
                                    () -> produceMetrics.recordAcksWait(acksWaitStartedNanos)));
        }
        return produceFuture.thenApply(
                response -> {
                    return toTopicResult(command, topic, response, localFailures);
                });
    }

    private CompletableFuture<TopicResult> producePrimaryKeyTopic(
            KafkaProduceCommand command,
            TopicWrite topic,
            TableInfo tableInfo,
            NativeLeaseScope leaseScope) {
        if (!(gateway instanceof RoutedKvGateway)) {
            throw new KafkaTopicSchemaException(
                    "The tablet gateway cannot route primary-key writes.");
        }
        KafkaTopicWritePlan writePlan = transcoder.prepare(tableInfo);
        Session session =
                new Session(
                        (short) 0,
                        command.listenerName(),
                        false,
                        command.clientAddress(),
                        command.principal());
        RoutedKvGateway.Route route =
                ((RoutedKvGateway) gateway)
                        .prepareKvWrite(tableInfo.getTablePath(), tableInfo.getTableId(), session);

        TopicOutputMemoryBudget budget =
                new TopicOutputMemoryBudget(leaseScope, topic.estimatedConvertedBytes());
        Map<Integer, PutKvRequest> requests = new java.util.LinkedHashMap<>();
        Map<Integer, PartitionResult> results = new HashMap<>();
        List<BytesView> retainedRecords = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            try {
                leaseScope.checkpoint();
                if (partition.partitionId() < 0
                        || partition.partitionId() >= tableInfo.getNumBuckets()) {
                    results.put(
                            partition.partitionId(),
                            new PartitionResult(
                                    partition.partitionId(),
                                    Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                    -1L,
                                    null));
                    continue;
                }
                Map<Integer, BytesView> buckets =
                        transcoder.transcodePrimaryKey(
                                nonNullRecords(topic, partition), writePlan, budget);
                if (buckets.isEmpty()) {
                    results.put(
                            partition.partitionId(),
                            new PartitionResult(partition.partitionId(), Errors.NONE, -1L, null));
                    continue;
                }
                PutKvRequest request =
                        new PutKvRequest()
                                .setTableId(tableInfo.getTableId())
                                .setAcks(command.acks())
                                .setTimeoutMs(command.timeoutMs());
                for (Map.Entry<Integer, BytesView> bucket : buckets.entrySet()) {
                    retainedRecords.add(bucket.getValue());
                    request.addBucketsReq()
                            .setBucketId(bucket.getKey())
                            .setRecordsBytesView(bucket.getValue());
                }
                requests.put(partition.partitionId(), request);
            } catch (RequestTooLargeException
                    | AdmissionUnavailableException
                    | CancellationException failure) {
                throw failure;
            } catch (Exception failure) {
                results.put(
                        partition.partitionId(),
                        failedPartition(command, partition.partitionId(), failure));
            } finally {
                topic.releaseCopiedRecords(partition);
                budget.releaseSource(partition.estimatedCopiedRecordBytes());
            }
        }
        budget.completeConversion();
        if (requests.isEmpty()) {
            return CompletableFuture.completedFuture(primaryKeyTopicResult(topic, results));
        }
        if (!leaseScope.tryMarkSubmitted()) {
            throw new CancellationException("Kafka connection closed before native KV submission.");
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(command.timeoutMs());
        long submitStarted = produceMetrics.nowNanos();
        CompletableFuture<Void> original = CompletableFuture.completedFuture(null);
        for (Map.Entry<Integer, PutKvRequest> entry : requests.entrySet()) {
            original =
                    original.thenCompose(
                            ignored -> {
                                long remainingMillis =
                                        TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                                if (remainingMillis <= 0) {
                                    results.put(
                                            entry.getKey(),
                                            new PartitionResult(
                                                    entry.getKey(),
                                                    Errors.REQUEST_TIMED_OUT,
                                                    -1L,
                                                    "Primary-key write timed out before native submission."));
                                    return CompletableFuture.completedFuture(null);
                                }
                                PutKvRequest request = entry.getValue();
                                request.setTimeoutMs(
                                        (int) Math.min(Integer.MAX_VALUE, remainingMillis));
                                try {
                                    return route.write(request)
                                            .handle(
                                                    (response, failure) -> {
                                                        results.put(
                                                                entry.getKey(),
                                                                failure == null
                                                                        ? primaryKeyPartitionResult(
                                                                                command,
                                                                                entry.getKey(),
                                                                                request,
                                                                                response)
                                                                        : failedPartition(
                                                                                command,
                                                                                entry.getKey(),
                                                                                failure));
                                                        return (Void) null;
                                                    });
                                } catch (Throwable failure) {
                                    results.put(
                                            entry.getKey(),
                                            failedPartition(command, entry.getKey(), failure));
                                    return CompletableFuture.completedFuture(null);
                                }
                            });
        }
        leaseScope.handOffToOriginalFuture(
                original,
                retainedRecords,
                () -> {
                    for (PutKvRequest request : requests.values()) {
                        for (int index = 0; index < request.getBucketsReqsCount(); index++) {
                            request.getBucketsReqAt(index).clearRecords();
                        }
                    }
                });
        runPostSubmitMaintenance(submitStarted);
        if (command.acks() == -1) {
            long started = produceMetrics.nowNanos();
            original.whenComplete(
                    (ignored, failure) ->
                            recordMetricsBestEffort(() -> produceMetrics.recordAcksWait(started)));
        }
        return original.thenApply(ignored -> primaryKeyTopicResult(topic, results));
    }

    private static TopicResult primaryKeyTopicResult(
            TopicWrite topic, Map<Integer, PartitionResult> results) {
        List<PartitionResult> ordered = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            ordered.add(results.get(partition.partitionId()));
        }
        return new TopicResult(topic.topicName(), ordered);
    }

    private static PartitionResult primaryKeyPartitionResult(
            KafkaProduceCommand command,
            int partition,
            PutKvRequest request,
            PutKvResponse response) {
        Map<Integer, PbPutKvRespForBucket> buckets = new HashMap<>();
        for (PbPutKvRespForBucket bucket : response.getBucketsRespsList()) {
            buckets.put(bucket.getBucketId(), bucket);
        }
        for (int index = 0; index < request.getBucketsReqsCount(); index++) {
            PbPutKvRespForBucket bucket = buckets.get(request.getBucketsReqAt(index).getBucketId());
            if (bucket == null) {
                return new PartitionResult(
                        partition,
                        Errors.UNKNOWN_SERVER_ERROR,
                        -1L,
                        "Native KV response omitted a requested bucket.");
            }
            if (bucket.hasErrorCode()) {
                Errors error =
                        toKafkaError(
                                org.apache.fluss.rpc.protocol.Errors.forCode(
                                        bucket.getErrorCode()));
                if (error != Errors.NONE) {
                    return new PartitionResult(
                            partition,
                            error,
                            -1L,
                            command.limitErrorMessage(
                                    bucket.hasErrorMessage() ? bucket.getErrorMessage() : null));
                }
            }
        }
        // A Kafka partition may span native buckets; no single contiguous Kafka offset exists.
        return new PartitionResult(partition, Errors.NONE, -1L, null);
    }

    private BytesView transcodePartition(
            TopicWrite topic,
            PartitionWrite partition,
            KafkaTopicWritePlan writePlan,
            KafkaOutputMemoryBudget outputMemoryBudget)
            throws Exception {
        List<KafkaProduceCommand.Record> copiedRecords = nonNullRecords(topic, partition);
        return transcoder.transcode(copiedRecords, writePlan, outputMemoryBudget);
    }

    private void authorizeDroppedWrite(KafkaProduceCommand command, TableInfo tableInfo) {
        if (!(gateway instanceof AdminOperationAuthorizer)) {
            throw new IllegalStateException("The tablet gateway cannot authorize a dropped write.");
        }
        TablePath path = tableInfo.getTablePath();
        ((AdminOperationAuthorizer) gateway)
                .authorize(
                        new Session(
                                (short) 0,
                                command.listenerName(),
                                false,
                                command.clientAddress(),
                                command.principal()),
                        OperationType.WRITE,
                        Resource.table(path.getDatabaseName(), path.getTableName()));
    }

    private static List<KafkaProduceCommand.Record> nonNullRecords(
            TopicWrite topic, PartitionWrite partition) {
        List<KafkaProduceCommand.Record> records = topic.copiedRecords(partition);
        if (partition.nullValueCount() == 0) {
            return records;
        }
        List<KafkaProduceCommand.Record> retained =
                new ArrayList<>(partition.recordCount() - partition.nullValueCount());
        for (KafkaProduceCommand.Record record : records) {
            if (record.borrowedValue() != null) {
                retained.add(record);
            }
        }
        return retained;
    }

    private static void clearNativeRequestPayload(ProduceLogRequest request) {
        for (int index = 0; index < request.getBucketsReqsCount(); index++) {
            request.getBucketsReqAt(index).clearRecords();
        }
    }

    private TableInfo toTableInfo(TopicWrite topic, GetTableInfoResponse response) {
        TablePath tablePath = topicMapper.toTablePath(topic.topicName());
        return tableInfoCache.getOrLoad(
                tablePath, response, () -> parseTableInfo(tablePath, response));
    }

    private TableInfo parseTableInfo(TablePath tablePath, GetTableInfoResponse response) {
        long parseStartedNanos = produceMetrics.nowNanos();
        try {
            return TableInfo.of(
                    tablePath,
                    response.getTableId(),
                    response.getSchemaId(),
                    TableDescriptor.fromJsonBytes(response.getTableJson()),
                    response.hasRemoteDataDir() ? response.getRemoteDataDir() : null,
                    response.getCreatedTime(),
                    response.getModifiedTime());
        } finally {
            recordMetricsBestEffort(() -> produceMetrics.recordTableInfoParse(parseStartedNanos));
        }
    }

    private static TopicResult toTopicResult(
            KafkaProduceCommand command,
            TopicWrite topic,
            ProduceLogResponse response,
            Map<Integer, PartitionResult> localFailures) {
        Map<Integer, PbProduceLogRespForBucket> responses = new HashMap<>();
        if (response != null) {
            for (PbProduceLogRespForBucket bucket : response.getBucketsRespsList()) {
                responses.put(bucket.getBucketId(), bucket);
            }
        }
        List<PartitionResult> partitions = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            PartitionResult localFailure = localFailures.get(partition.partitionId());
            if (localFailure != null) {
                partitions.add(localFailure);
                continue;
            }
            PbProduceLogRespForBucket bucket = responses.get(partition.partitionId());
            if (bucket == null) {
                partitions.add(
                        new PartitionResult(
                                partition.partitionId(),
                                Errors.UNKNOWN_SERVER_ERROR,
                                -1L,
                                command.limitErrorMessage(
                                        "Fluss Produce response omitted this bucket.")));
            } else if (bucket.hasErrorCode()) {
                partitions.add(
                        new PartitionResult(
                                partition.partitionId(),
                                toKafkaError(
                                        org.apache.fluss.rpc.protocol.Errors.forCode(
                                                bucket.getErrorCode())),
                                -1L,
                                command.limitErrorMessage(
                                        bucket.hasErrorMessage()
                                                ? bucket.getErrorMessage()
                                                : null)));
            } else {
                partitions.add(
                        new PartitionResult(
                                partition.partitionId(),
                                Errors.NONE,
                                partition.nullValueCount() == 0 && bucket.hasBaseOffset()
                                        ? bucket.getBaseOffset()
                                        : -1L,
                                null));
            }
        }
        return new TopicResult(topic.topicName(), partitions);
    }

    private static PartitionResult failedPartition(
            KafkaProduceCommand command, int partitionId, Throwable failure) {
        Throwable cause = unwrap(failure);
        Errors kafkaError;
        if (cause instanceof InvalidTopicException) {
            kafkaError = Errors.INVALID_TOPIC_EXCEPTION;
        } else if (cause instanceof KafkaRecordEncodingException) {
            kafkaError = Errors.INVALID_RECORD;
        } else if (cause instanceof KafkaTopicSchemaException) {
            kafkaError = Errors.INVALID_CONFIG;
        } else if (cause instanceof OutOfMemoryException) {
            kafkaError = Errors.REQUEST_TIMED_OUT;
        } else if (cause instanceof IllegalArgumentException) {
            kafkaError = Errors.INVALID_REQUEST;
        } else {
            kafkaError = toKafkaError(org.apache.fluss.rpc.protocol.Errors.forException(cause));
        }
        return new PartitionResult(
                partitionId, kafkaError, -1L, command.limitErrorMessage(cause.getMessage()));
    }

    private static TopicResult failedTopic(
            KafkaProduceCommand command, TopicWrite topic, Throwable failure) {
        Throwable cause = unwrap(failure);
        Errors kafkaError;
        if (cause instanceof InvalidTopicException) {
            kafkaError = Errors.INVALID_TOPIC_EXCEPTION;
        } else if (cause instanceof KafkaRecordEncodingException) {
            kafkaError = Errors.INVALID_RECORD;
        } else if (cause instanceof KafkaTopicSchemaException) {
            kafkaError = Errors.INVALID_CONFIG;
        } else if (cause instanceof RequestTooLargeException) {
            kafkaError = Errors.MESSAGE_TOO_LARGE;
        } else if (cause instanceof OutOfMemoryException
                || cause instanceof AdmissionUnavailableException
                || cause instanceof CancellationException
                || cause instanceof RejectedExecutionException) {
            kafkaError = Errors.REQUEST_TIMED_OUT;
        } else if (cause instanceof IllegalArgumentException) {
            kafkaError = Errors.INVALID_REQUEST;
        } else {
            kafkaError = toKafkaError(org.apache.fluss.rpc.protocol.Errors.forException(cause));
        }
        String errorMessage =
                KafkaProduceResult.limitErrorMessage(
                        cause.getMessage(), KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES);
        List<PartitionResult> partitions = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            partitions.add(
                    new PartitionResult(
                            partition.partitionId(),
                            kafkaError,
                            -1L,
                            command.limitErrorMessage(errorMessage)));
        }
        return new TopicResult(topic.topicName(), partitions);
    }

    private static Errors toKafkaError(org.apache.fluss.rpc.protocol.Errors error) {
        switch (error) {
            case NONE:
                return Errors.NONE;
            case DATABASE_NOT_EXIST:
            case TABLE_NOT_EXIST:
            case UNKNOWN_TABLE_OR_BUCKET_EXCEPTION:
                return Errors.UNKNOWN_TOPIC_OR_PARTITION;
            case NOT_LEADER_OR_FOLLOWER:
                return Errors.NOT_LEADER_OR_FOLLOWER;
            case LEADER_NOT_AVAILABLE_EXCEPTION:
                return Errors.LEADER_NOT_AVAILABLE;
            case RECORD_TOO_LARGE_EXCEPTION:
                return Errors.MESSAGE_TOO_LARGE;
            case CORRUPT_MESSAGE:
            case CORRUPT_RECORD_EXCEPTION:
                return Errors.CORRUPT_MESSAGE;
            case INVALID_REQUIRED_ACKS:
                return Errors.INVALID_REQUIRED_ACKS;
            case REQUEST_TIME_OUT:
                return Errors.REQUEST_TIMED_OUT;
            case NOT_ENOUGH_REPLICAS_EXCEPTION:
                return Errors.NOT_ENOUGH_REPLICAS;
            case NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION:
                return Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND;
            case AUTHORIZATION_EXCEPTION:
                return Errors.TOPIC_AUTHORIZATION_FAILED;
            case LOG_STORAGE_EXCEPTION:
            case STORAGE_EXCEPTION:
            case DISK_WRITE_LOCKED:
                return Errors.KAFKA_STORAGE_ERROR;
            default:
                return Errors.UNKNOWN_SERVER_ERROR;
        }
    }

    private void setCurrentSession(KafkaProduceCommand command) {
        service.setCurrentSession(
                new Session(
                        (short) 0,
                        command.listenerName(),
                        false,
                        command.clientAddress(),
                        command.principal()));
    }

    private void runPostSubmitMaintenance(long submitStartedNanos) {
        try {
            service.tryCompleteActions();
        } catch (Throwable maintenanceFailure) {
            recordInvariantBestEffort();
            LOG.warn(
                    "Failed to drain delayed actions after native Produce submission. The original Produce future remains authoritative.",
                    maintenanceFailure);
        }
        try {
            produceMetrics.recordNativeProduceSubmit(submitStartedNanos);
        } catch (Throwable metricFailure) {
            LOG.warn("Failed to record native Produce submission metrics.", metricFailure);
        }
    }

    private void recordInvariantBestEffort() {
        recordMetricsBestEffort(produceMetrics::recordNativeInvariantViolation);
    }

    private void recordMetricsBestEffort(Runnable metricOperation) {
        try {
            metricOperation.run();
        } catch (Throwable metricFailure) {
            LOG.debug("Failed to record native Produce metrics.", metricFailure);
        }
    }

    private final class NativeLeaseScope implements PreSubmitOperation {
        private final @Nullable RequestLease lease;
        private final @Nullable ConnectionHandle connection;
        private final @Nullable ScheduledExecutorService scheduler;
        private final long grantedNanos;
        private final Runnable connectionCloseListener = this::cancelBeforeSubmit;
        private final AtomicBoolean originalFutureOwnsLease = new AtomicBoolean();
        private final AtomicBoolean preSubmitResolved = new AtomicBoolean();
        private final AtomicBoolean leaseClosed = new AtomicBoolean();
        private final AtomicBoolean connectionCloseListenerRegistered = new AtomicBoolean();
        private final AtomicReference<Throwable> cancellationFailure = new AtomicReference<>();
        private final AtomicReference<ConversionTask<?>> activeTask = new AtomicReference<>();
        private volatile @Nullable ScheduledFuture<?> preSubmitDeadlineTask;

        private NativeLeaseScope(
                @Nullable RequestLease lease,
                @Nullable ConnectionHandle connection,
                @Nullable ScheduledExecutorService scheduler,
                long grantedNanos) {
            this.lease = lease;
            this.connection = connection;
            this.scheduler = scheduler;
            this.grantedNanos = grantedNanos;
            if (lease != null) {
                if (operationTracker.register(this)) {
                    if (connection == null || registerConnectionCloseListener(connection)) {
                        if (cancellationFailure.get() == null) {
                            preSubmitDeadlineTask = schedulePreSubmitDeadline();
                        }
                    } else {
                        requestCancellation(
                                new CancellationException(
                                        "Kafka connection closed before native Produce submission."),
                                false);
                    }
                } else {
                    requestCancellation(
                            new CancellationException(
                                    "Native Produce stopped before submission during protocol shutdown."),
                            false);
                }
            }
        }

        private void adjustReservation(long actualBytes) {
            if (lease == null) {
                return;
            }
            try {
                lease.resize(actualBytes);
            } catch (RequestTooLargeException failure) {
                recordMetricsBestEffort(produceMetrics::recordNativeRequestTooLarge);
                throw failure;
            } catch (AdmissionUnavailableException failure) {
                recordMetricsBestEffort(produceMetrics::recordNativeAdmissionRejected);
                throw failure;
            } catch (RuntimeException failure) {
                recordMetricsBestEffort(produceMetrics::recordNativeInvariantViolation);
                throw failure;
            }
        }

        private boolean hasLease() {
            return lease != null;
        }

        private CompletableFuture<TopicResult> guardPreSubmit(
                CompletableFuture<TopicResult> operationFuture) {
            if (lease == null) {
                return operationFuture;
            }
            CompletableFuture<TopicResult> guarded = new CompletableFuture<>();
            operationFuture.whenComplete(
                    (result, failure) -> {
                        Throwable cancellation = cancellationFailure.get();
                        if (cancellation != null) {
                            guarded.completeExceptionally(cancellation);
                        } else if (failure == null) {
                            guarded.complete(result);
                        } else {
                            guarded.completeExceptionally(failure);
                        }
                    });
            return guarded;
        }

        private void registerTask(ConversionTask<?> task) {
            if (!activeTask.compareAndSet(null, task)) {
                recordInvariantBestEffort();
                throw new IllegalStateException(
                        "A native Produce pre-submit conversion task is already active.");
            }
            if (cancellationFailure.get() != null) {
                task.cancelBeforeSubmit();
            }
        }

        private void unregisterTask(ConversionTask<?> task) {
            if (!activeTask.compareAndSet(task, null)) {
                recordInvariantBestEffort();
            }
        }

        private void checkpoint() {
            Throwable cancellation = cancellationFailure.get();
            if (cancellation instanceof RuntimeException) {
                throw (RuntimeException) cancellation;
            }
            if (cancellation != null) {
                throw new CancellationException(cancellation.getMessage());
            }
        }

        private boolean tryMarkSubmitted() {
            if (lease == null) {
                return true;
            }
            if (!preSubmitResolved.compareAndSet(false, true)) {
                return false;
            }
            cancelTimer(preSubmitDeadlineTask);
            removeConnectionCloseListener();
            if (!operationTracker.tryStartSubmit(this)) {
                return false;
            }
            return lease.tryMarkSubmitted();
        }

        private void handOffToOriginalFuture(
                CompletableFuture<?> produceFuture,
                List<BytesView> retainedRecords,
                Runnable releaseNativeRequestPayload) {
            if (!originalFutureOwnsLease.compareAndSet(false, true)) {
                recordInvariantBestEffort();
                throw new IllegalStateException(
                        "Native Produce admission ownership was already transferred.");
            }
            ScheduledFuture<?> graceTask = scheduleCompletionGrace(produceFuture);
            produceFuture.whenComplete(
                    (response, failure) -> {
                        // Keep converted buffers strongly reachable until the original native
                        // future, including delayed acks=all completion, is terminal.
                        retainedRecords.size();
                        try {
                            cancelTimer(graceTask);
                            if (lease != null) {
                                try {
                                    produceMetrics.recordNativeCompletion(grantedNanos);
                                } catch (Throwable metricFailure) {
                                    LOG.warn(
                                            "Failed to record native Produce completion metrics.",
                                            metricFailure);
                                }
                            }
                        } finally {
                            try {
                                releaseNativeRequestPayload.run();
                            } catch (Throwable cleanupFailure) {
                                recordInvariantBestEffort();
                                LOG.warn(
                                        "Failed to clear native Produce request payload references.",
                                        cleanupFailure);
                            } finally {
                                retainedRecords.clear();
                                closeLeaseBestEffort(
                                        "Failed to release native Produce admission after original future completion.");
                            }
                        }
                    });
        }

        private @Nullable ScheduledFuture<?> schedulePreSubmitDeadline() {
            if (lease == null || scheduler == null) {
                return null;
            }
            try {
                return scheduler.schedule(
                        this::timeoutBeforeSubmit,
                        nativeCompletionGraceTimeout.toMillis(),
                        TimeUnit.MILLISECONDS);
            } catch (RuntimeException schedulingFailure) {
                timeoutBeforeSubmit();
                return null;
            }
        }

        private void timeoutBeforeSubmit() {
            requestCancellation(
                    new CancellationException(
                            "Timed out before native Produce submission completed."),
                    true);
        }

        @Override
        public void cancelBeforeSubmit() {
            requestCancellation(
                    new CancellationException(
                            "Native Produce stopped before submission during protocol shutdown."),
                    false);
        }

        private void requestCancellation(Throwable failure, boolean recordTimeout) {
            if (!preSubmitResolved.compareAndSet(false, true)) {
                return;
            }
            cancellationFailure.set(checkNotNull(failure));
            operationTracker.unregister(this);
            cancelTimer(preSubmitDeadlineTask);
            removeConnectionCloseListener();
            if (recordTimeout) {
                try {
                    produceMetrics.recordNativeCompletionGraceTimeout();
                } catch (Throwable metricFailure) {
                    LOG.warn("Failed to record a native pre-submit timeout.", metricFailure);
                }
            }
            ConversionTask<?> task = activeTask.get();
            if (task != null) {
                task.cancelBeforeSubmit();
            }
        }

        private @Nullable ScheduledFuture<?> scheduleCompletionGrace(
                CompletableFuture<?> produceFuture) {
            if (lease == null || scheduler == null) {
                return null;
            }
            try {
                return scheduler.schedule(
                        () -> {
                            if (!produceFuture.isDone()) {
                                recordMetricsBestEffort(
                                        produceMetrics::recordNativeCompletionGraceTimeout);
                            }
                        },
                        nativeCompletionGraceTimeout.toMillis(),
                        TimeUnit.MILLISECONDS);
            } catch (RuntimeException schedulingFailure) {
                // Monitoring is best effort after submission. It must never release the native
                // lease or fail a write already handed to the gateway.
                recordInvariantBestEffort();
                LOG.debug(
                        "Unable to schedule the native Produce completion grace check.",
                        schedulingFailure);
                return null;
            }
        }

        private void closeUnlessSubmitted() {
            if (lease != null && !originalFutureOwnsLease.get()) {
                preSubmitResolved.set(true);
                operationTracker.unregister(this);
                cancelTimer(preSubmitDeadlineTask);
                removeConnectionCloseListener();
                closeLease();
            }
        }

        private boolean registerConnectionCloseListener(ConnectionHandle currentConnection) {
            boolean registered = currentConnection.addCloseListener(connectionCloseListener);
            connectionCloseListenerRegistered.set(registered);
            if (registered && cancellationFailure.get() != null) {
                removeConnectionCloseListener();
            }
            return registered;
        }

        private void removeConnectionCloseListener() {
            ConnectionHandle currentConnection = connection;
            if (currentConnection != null
                    && connectionCloseListenerRegistered.compareAndSet(true, false)) {
                currentConnection.removeCloseListener(connectionCloseListener);
            }
        }

        private void closeLease() {
            if (lease != null && leaseClosed.compareAndSet(false, true)) {
                lease.close();
            }
        }

        private void closeLeaseBestEffort(String message) {
            try {
                closeLease();
            } catch (RuntimeException invariantFailure) {
                recordInvariantBestEffort();
                LOG.warn(message, invariantFailure);
            }
        }
    }

    private final class TopicOutputMemoryBudget implements KafkaOutputMemoryBudget {
        private final NativeLeaseScope leaseScope;
        private final long estimatedBytes;
        private long sourceBytes;
        private long retainedOutputCapacityBytes;
        private boolean resizeMetricRecorded;

        private TopicOutputMemoryBudget(NativeLeaseScope leaseScope, long sourceBytes) {
            this.leaseScope = checkNotNull(leaseScope);
            this.estimatedBytes = sourceBytes;
            this.sourceBytes = sourceBytes;
        }

        @Override
        public void checkpoint() {
            leaseScope.checkpoint();
        }

        @Override
        public void reserve(long bytes) {
            leaseScope.checkpoint();
            long nextOutputCapacity = saturatedAdd(retainedOutputCapacityBytes, bytes);
            adjustOrRecordFailure(saturatedAdd(sourceBytes, nextOutputCapacity));
            retainedOutputCapacityBytes = nextOutputCapacity;
        }

        @Override
        public void release(long bytes) {
            if (bytes < 0 || bytes > retainedOutputCapacityBytes) {
                throw new IllegalStateException(
                        "Converted output budget released bytes that it does not own.");
            }
            retainedOutputCapacityBytes -= bytes;
            adjustOrRecordFailure(saturatedAdd(sourceBytes, retainedOutputCapacityBytes));
        }

        private void releaseSource(long bytes) {
            if (bytes < 0 || bytes > sourceBytes) {
                throw new IllegalStateException(
                        "Converted output budget released source bytes that it does not own.");
            }
            sourceBytes -= bytes;
            adjustOrRecordFailure(saturatedAdd(sourceBytes, retainedOutputCapacityBytes));
        }

        private void completeConversion() {
            if (leaseScope.hasLease() && !resizeMetricRecorded) {
                resizeMetricRecorded = true;
                recordMetricsBestEffort(
                        () ->
                                produceMetrics.recordNativeResize(
                                        estimatedBytes, retainedOutputCapacityBytes, true));
            }
        }

        private void adjustOrRecordFailure(long targetBytes) {
            try {
                leaseScope.adjustReservation(targetBytes);
            } catch (RuntimeException failure) {
                if (leaseScope.hasLease() && !resizeMetricRecorded) {
                    resizeMetricRecorded = true;
                    recordMetricsBestEffort(
                            () ->
                                    produceMetrics.recordNativeResize(
                                            estimatedBytes, targetBytes, false));
                }
                throw failure;
            }
        }
    }

    private static void cancelTimer(@Nullable ScheduledFuture<?> timer) {
        if (timer != null) {
            timer.cancel(false);
        }
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable failure) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(failure);
        return future;
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static @Nullable Throwable unwrap(@Nullable Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private interface ConversionOperation<T> {
        CompletableFuture<T> execute() throws Exception;
    }

    private final class ConversionTask<T>
            implements KafkaProduceConversionExecutor.CancellableTask {
        private final NativeLeaseScope leaseScope;
        private final AtomicReference<ConversionOperation<T>> operation;
        private final CompletableFuture<T> result;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final AtomicBoolean terminal = new AtomicBoolean();
        private final AtomicReference<CompletableFuture<T>> operationFuture =
                new AtomicReference<>();

        private ConversionTask(
                NativeLeaseScope leaseScope,
                ConversionOperation<T> operation,
                CompletableFuture<T> result) {
            this.leaseScope = checkNotNull(leaseScope);
            this.operation = new AtomicReference<>(checkNotNull(operation));
            this.result = result;
        }

        @Override
        public void run() {
            if (!claimed.compareAndSet(false, true)) {
                return;
            }
            try {
                leaseScope.checkpoint();
                ConversionOperation<T> currentOperation =
                        checkNotNull(operation.getAndSet(null), "conversion operation");
                CompletableFuture<T> future =
                        checkNotNull(currentOperation.execute(), "conversion operation future");
                operationFuture.set(future);
                future.whenComplete(this::finish);
                if (leaseScope.cancellationFailure.get() != null) {
                    cancelBeforeSubmit();
                }
            } catch (Throwable failure) {
                finish(null, failure);
            }
        }

        @Override
        public void cancel(Throwable failure) {
            if (claimed.compareAndSet(false, true)) {
                operation.set(null);
                finish(null, failure);
            }
        }

        private void cancelBeforeSubmit() {
            Throwable failure =
                    checkNotNull(leaseScope.cancellationFailure.get(), "cancellation failure");
            if (claimed.compareAndSet(false, true)) {
                operation.set(null);
                finish(null, failure);
                return;
            }
            CompletableFuture<T> currentFuture = operationFuture.get();
            if (currentFuture != null) {
                currentFuture.cancel(false);
                finish(null, failure);
            }
        }

        private void finish(@Nullable T value, @Nullable Throwable failure) {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            operation.set(null);
            operationFuture.set(null);
            leaseScope.unregisterTask(this);
            Throwable cancellation = leaseScope.cancellationFailure.get();
            if (cancellation != null) {
                result.completeExceptionally(cancellation);
            } else if (failure == null) {
                result.complete(value);
            } else {
                result.completeExceptionally(failure);
            }
        }
    }
}
