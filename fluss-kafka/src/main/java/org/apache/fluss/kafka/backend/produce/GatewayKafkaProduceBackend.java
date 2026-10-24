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
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.PartitionResult;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult.TopicResult;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.kafka.transcode.EncodedRecords;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.protocol.Errors;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Adapts the local TabletServer write gateway to the Kafka Produce backend contract. */
@Internal
public final class GatewayKafkaProduceBackend implements KafkaProduceBackend {

    private final Set<CompletableFuture<Void>> pendingWrites = new HashSet<>();
    private CompletableFuture<Void> closeFuture;
    private final RpcGatewayService service;
    private final TabletServerGateway gateway;
    private final KafkaTopicMapper topicMapper;
    private final KafkaTopicSchemaResolver schemaResolver = new KafkaTopicSchemaResolver();
    private final KafkaRecordTranscoder transcoder;
    private final KafkaTableInfoCache tableInfoCache = new KafkaTableInfoCache();
    private final KafkaProduceConversionExecutor conversionExecutor;
    private final boolean ownsConversionExecutor;

    /** Creates a Produce backend with its own bounded conversion executor. */
    public GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaTopicMapper topicMapper,
            KafkaRecordTranscoder transcoder) {
        this(service, gateway, topicMapper, transcoder, null);
    }

    /** Creates a backend that submits work to a caller-owned bounded conversion executor. */
    public GatewayKafkaProduceBackend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaTopicMapper topicMapper,
            KafkaRecordTranscoder transcoder,
            @Nullable KafkaProduceConversionExecutor conversionExecutor) {
        this.service = checkNotNull(service);
        this.gateway = checkNotNull(gateway);
        this.topicMapper = checkNotNull(topicMapper);
        this.transcoder = checkNotNull(transcoder);
        this.ownsConversionExecutor = conversionExecutor == null;
        this.conversionExecutor =
                ownsConversionExecutor
                        ? new KafkaProduceConversionExecutor(1, 1024)
                        : conversionExecutor;
    }

    @Override
    public CompletableFuture<KafkaProduceResult> write(KafkaProduceCommand command) {
        CompletableFuture<Void> finished = new CompletableFuture<>();
        synchronized (pendingWrites) {
            if (closeFuture != null) {
                throw new RejectedExecutionException("Kafka Produce backend is closed.");
            }
            pendingWrites.add(finished);
        }
        try {
            // The request owns the payloads only until write returns.
            KafkaProduceCommand ownedCommand = copyCommand(command);
            CompletableFuture<KafkaProduceResult> result = writePartitions(ownedCommand);
            // Caller cancellation must not release resources still held by native append.
            return result.whenComplete((ignored, failure) -> finishWrite(finished))
                    .thenApply(value -> value);
        } catch (RuntimeException | Error failure) {
            finishWrite(finished);
            throw failure;
        }
    }

    /** Stops admission and waits until accepted writes have released their encoded records. */
    public CompletableFuture<Void> closeAsync() {
        synchronized (pendingWrites) {
            if (closeFuture == null) {
                CompletableFuture<Void> drained =
                        CompletableFuture.allOf(pendingWrites.toArray(new CompletableFuture[0]));
                closeFuture = drained;
                if (ownsConversionExecutor) {
                    closeFuture = CompletableFuture.allOf(drained, conversionExecutor.closeAsync());
                }
            }
            return closeFuture;
        }
    }

    private void finishWrite(CompletableFuture<Void> finished) {
        synchronized (pendingWrites) {
            pendingWrites.remove(finished);
            finished.complete(null);
        }
    }

    private static KafkaProduceCommand copyCommand(KafkaProduceCommand command) {
        List<TopicWrite> topics = new ArrayList<>(command.topics().size());
        for (TopicWrite topic : command.topics()) {
            List<PartitionWrite> partitions = new ArrayList<>(topic.partitions().size());
            for (PartitionWrite partition : topic.partitions()) {
                List<Record> records = new ArrayList<>(partition.records().size());
                for (Record record : partition.records()) {
                    List<RecordHeader> headers = new ArrayList<>(record.headers().size());
                    for (RecordHeader header : record.headers()) {
                        byte[] value = header.value();
                        headers.add(
                                new RecordHeader(
                                        header.name(), value == null ? null : value.clone()));
                    }
                    records.add(
                            new Record(
                                    record.timestamp(),
                                    record.timestampType(),
                                    copyPayload(record.key()),
                                    copyPayload(record.value()),
                                    headers));
                }
                partitions.add(new PartitionWrite(partition.partitionId(), records));
            }
            topics.add(new TopicWrite(topic.topicName(), partitions));
        }
        return new KafkaProduceCommand(
                command.acks(),
                command.timeoutMs(),
                topics,
                command.listenerName(),
                command.clientAddress());
    }

    private static @Nullable ByteBuffer copyPayload(@Nullable ByteBuffer payload) {
        if (payload == null) {
            return null;
        }
        byte[] bytes = new byte[payload.remaining()];
        payload.get(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private CompletableFuture<KafkaProduceResult> writePartitions(KafkaProduceCommand command) {
        Map<TablePath, FutureTask<TableInfo>> metadataByTable = new HashMap<>();
        List<CompletableFuture<TopicResult>> topicFutures = new ArrayList<>();
        for (TopicWrite topic : command.topics()) {
            TablePath path;
            try {
                path = topicMapper.toTablePath(topic.topicName());
            } catch (Exception failure) {
                topicFutures.add(CompletableFuture.completedFuture(failedTopic(topic, failure)));
                continue;
            }
            // The first accepted partition loads this snapshot on a conversion worker. Aliases
            // and other partitions share it, while the next request always performs a fresh lookup.
            FutureTask<TableInfo> metadata =
                    metadataByTable.computeIfAbsent(
                            path, ignored -> new FutureTask<>(() -> loadTableInfo(command, path)));
            List<CompletableFuture<PartitionResult>> partitionFutures = new ArrayList<>();
            for (PartitionWrite partition : topic.partitions()) {
                partitionFutures.add(
                        conversionExecutor
                                .submit(
                                        Tuple2.of(path, partition.partitionId()),
                                        () -> writePartition(command, partition, metadata))
                                .exceptionally(
                                        failure ->
                                                failedPartition(partition.partitionId(), failure)));
            }
            topicFutures.add(
                    CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture<?>[0]))
                            .thenApply(
                                    ignored -> {
                                        List<PartitionResult> results = new ArrayList<>();
                                        for (CompletableFuture<PartitionResult> future :
                                                partitionFutures) {
                                            results.add(future.join());
                                        }
                                        return new TopicResult(topic.topicName(), results);
                                    }));
        }
        return CompletableFuture.allOf(topicFutures.toArray(new CompletableFuture<?>[0]))
                .thenApply(
                        ignored -> {
                            List<TopicResult> results = new ArrayList<>();
                            for (CompletableFuture<TopicResult> future : topicFutures) {
                                results.add(future.join());
                            }
                            return new KafkaProduceResult(results);
                        });
    }

    private TableInfo loadTableInfo(KafkaProduceCommand command, TablePath path) throws Exception {
        checkInterrupted();
        setCurrentSession(command);
        GetTableInfoRequest request = new GetTableInfoRequest();
        request.setTablePath()
                .setDatabaseName(path.getDatabaseName())
                .setTableName(path.getTableName());
        GetTableInfoResponse response =
                gateway.getTableInfo(request)
                        .get(Math.max(0, command.timeoutMs()), TimeUnit.MILLISECONDS);
        return toTableInfo(path, response);
    }

    private CompletableFuture<PartitionResult> writePartition(
            KafkaProduceCommand command, PartitionWrite partition, FutureTask<TableInfo> metadata) {
        try {
            checkInterrupted();
            metadata.run();
            TableInfo tableInfo =
                    metadata.get(Math.max(0, command.timeoutMs()), TimeUnit.MILLISECONDS);
            return producePartition(command, partition, tableInfo);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            return CompletableFuture.completedFuture(
                    failedPartition(partition.partitionId(), failure));
        } catch (Exception failure) {
            return CompletableFuture.completedFuture(
                    failedPartition(partition.partitionId(), failure));
        }
    }

    private CompletableFuture<PartitionResult> producePartition(
            KafkaProduceCommand command, PartitionWrite partition, TableInfo tableInfo) {
        int partitionId = partition.partitionId();
        if (partitionId < 0 || partitionId >= tableInfo.getNumBuckets()) {
            return CompletableFuture.completedFuture(
                    new PartitionResult(
                            partitionId,
                            Errors.UNKNOWN_TOPIC_OR_PARTITION,
                            -1L,
                            "Partition is outside the table bucket range."));
        }
        EncodedRecords converted = null;
        boolean submitted = false;
        try {
            converted = transcoder.transcode(partitionId, partition.records(), tableInfo);
            MemoryLogRecords records = converted.records();
            ProduceLogRequest request =
                    new ProduceLogRequest()
                            .setTableId(tableInfo.getTableId())
                            .setAcks(command.acks())
                            .setTimeoutMs(command.timeoutMs());
            request.addBucketsReq()
                    .setBucketId(partitionId)
                    .setRecordsBytesView(
                            new MemorySegmentBytesView(
                                    records.getMemorySegment(),
                                    records.getPosition(),
                                    records.sizeInBytes()));
            checkInterrupted();
            setCurrentSession(command);
            CompletableFuture<ProduceLogResponse> appended = gateway.produceLog(request);
            EncodedRecords retained = converted;
            CompletableFuture<PartitionResult> result =
                    appended.whenComplete((ignored, failure) -> retained.deallocate())
                            .handle(
                                    (response, failure) ->
                                            failure == null
                                                    ? toPartitionResult(partitionId, response)
                                                    : failedPartition(partitionId, failure));
            submitted = true;
            return result;
        } catch (Exception failure) {
            checkInterrupted();
            return CompletableFuture.completedFuture(failedPartition(partitionId, failure));
        } finally {
            if (!submitted && converted != null) {
                converted.deallocate();
            }
        }
    }

    private TableInfo toTableInfo(TablePath path, GetTableInfoResponse response) {
        return tableInfoCache.getOrLoad(
                path,
                response,
                () -> {
                    TableInfo tableInfo = parseTableInfo(path, response);
                    TableDescriptor descriptor = tableInfo.toTableDescriptor();
                    if (!schemaResolver.isEnabled(descriptor)) {
                        throw new TableNotExistException(
                                "Table " + path + " is not enabled for Kafka.");
                    }
                    schemaResolver.resolve(descriptor);
                    return tableInfo;
                });
    }

    private static TableInfo parseTableInfo(TablePath path, GetTableInfoResponse response) {
        return TableInfo.of(
                path,
                response.getTableId(),
                response.getSchemaId(),
                TableDescriptor.fromJsonBytes(response.getTableJson()),
                response.hasRemoteDataDir() ? response.getRemoteDataDir() : null,
                response.getCreatedTime(),
                response.getModifiedTime(),
                response.hasBucketCountEpoch() ? response.getBucketCountEpoch() : 0L);
    }

    private static PartitionResult toPartitionResult(int partitionId, ProduceLogResponse response) {
        for (PbProduceLogRespForBucket bucket : response.getBucketsRespsList()) {
            if (bucket.getBucketId() != partitionId) {
                continue;
            }
            if (bucket.hasErrorCode() && bucket.getErrorCode() != 0) {
                return new PartitionResult(
                        partitionId,
                        toKafkaError(
                                org.apache.fluss.rpc.protocol.Errors.forCode(
                                        bucket.getErrorCode())),
                        -1L,
                        bucket.hasErrorMessage() ? bucket.getErrorMessage() : null);
            }
            return new PartitionResult(
                    partitionId,
                    Errors.NONE,
                    bucket.hasBaseOffset() ? bucket.getBaseOffset() : -1L,
                    null);
        }
        return new PartitionResult(
                partitionId,
                Errors.UNKNOWN_SERVER_ERROR,
                -1L,
                "Fluss Produce response omitted this bucket.");
    }

    private static TopicResult failedTopic(TopicWrite topic, Throwable failure) {
        List<PartitionResult> partitions = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            partitions.add(failedPartition(partition.partitionId(), failure));
        }
        return new TopicResult(topic.topicName(), partitions);
    }

    private static PartitionResult failedPartition(int partitionId, Throwable failure) {
        Throwable cause = unwrap(failure);
        Errors kafkaError =
                cause instanceof RejectedExecutionException
                                || cause instanceof TimeoutException
                                || cause instanceof InterruptedException
                                || cause instanceof CancellationException
                        ? Errors.REQUEST_TIMED_OUT
                        : cause instanceof KafkaTopicSchemaException
                                        || cause instanceof InvalidTopicException
                                ? Errors.INVALID_TOPIC_EXCEPTION
                                : cause instanceof KafkaRecordEncodingException
                                        ? Errors.INVALID_RECORD
                                        : cause instanceof IllegalArgumentException
                                                ? Errors.INVALID_REQUEST
                                                : toKafkaError(
                                                        org.apache.fluss.rpc.protocol.Errors
                                                                .forException(cause));
        return new PartitionResult(partitionId, kafkaError, -1L, cause.getMessage());
    }

    private static Errors toKafkaError(org.apache.fluss.rpc.protocol.Errors error) {
        switch (error) {
            case NONE:
                return Errors.NONE;
            case TABLE_NOT_EXIST:
            case DATABASE_NOT_EXIST:
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
                        FlussPrincipal.ANONYMOUS));
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Kafka conversion worker was interrupted.");
        }
    }
}
