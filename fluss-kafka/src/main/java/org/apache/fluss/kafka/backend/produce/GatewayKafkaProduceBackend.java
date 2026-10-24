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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
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
    private final @Nullable KafkaProduceConversionExecutor conversionExecutor;

    /** Creates a Produce backend backed by the local TabletServer gateway. */
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
        this.conversionExecutor = conversionExecutor;
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
            // The request owns the payloads only until write returns. Metadata lookup can also
            // complete asynchronously when no conversion executor is configured.
            KafkaProduceCommand ownedCommand = copyCommand(command);
            CompletableFuture<KafkaProduceResult> result =
                    conversionExecutor != null
                            ? writePartitions(ownedCommand)
                            : writeTopics(ownedCommand);
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
                closeFuture =
                        CompletableFuture.allOf(pendingWrites.toArray(new CompletableFuture[0]));
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
        List<CompletableFuture<TopicResult>> topicFutures = new ArrayList<>();
        for (TopicWrite topic : command.topics()) {
            TablePath path;
            try {
                path = topicMapper.toTablePath(topic.topicName());
            } catch (Exception failure) {
                topicFutures.add(CompletableFuture.completedFuture(failedTopic(topic, failure)));
                continue;
            }
            List<CompletableFuture<TopicResult>> partitionFutures = new ArrayList<>();
            for (PartitionWrite partition : topic.partitions()) {
                TopicWrite singlePartition =
                        new TopicWrite(topic.topicName(), Collections.singletonList(partition));
                // Register before asynchronous metadata lookup. Aliases of the same table
                // share append order, while unrelated tables and partitions remain independent.
                partitionFutures.add(
                        conversionExecutor
                                .submit(
                                        Tuple2.of(path, partition.partitionId()),
                                        () -> writeTopic(command, singlePartition))
                                .exceptionally(failure -> failedTopic(singlePartition, failure)));
            }
            topicFutures.add(
                    CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture<?>[0]))
                            .thenApply(
                                    ignored -> {
                                        List<PartitionResult> results = new ArrayList<>();
                                        for (CompletableFuture<TopicResult> future :
                                                partitionFutures) {
                                            results.addAll(future.join().partitions());
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

    private CompletableFuture<KafkaProduceResult> writeTopics(KafkaProduceCommand command) {
        List<CompletableFuture<TopicResult>> futures = new ArrayList<>();
        for (TopicWrite topic : command.topics()) {
            futures.add(writeTopic(command, topic));
        }
        CompletableFuture<Void> all =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
        return all.thenApply(
                ignored -> {
                    List<TopicResult> results = new ArrayList<>();
                    for (CompletableFuture<TopicResult> future : futures) {
                        results.add(future.join());
                    }
                    return new KafkaProduceResult(results);
                });
    }

    private CompletableFuture<TopicResult> writeTopic(
            KafkaProduceCommand command, TopicWrite topic) {
        try {
            checkInterrupted();
            TablePath path = topicMapper.toTablePath(topic.topicName());
            setCurrentSession(command);
            GetTableInfoRequest request = new GetTableInfoRequest();
            request.setTablePath()
                    .setDatabaseName(path.getDatabaseName())
                    .setTableName(path.getTableName());
            CompletableFuture<GetTableInfoResponse> metadata = gateway.getTableInfo(request);
            if (conversionExecutor != null) {
                // Keep conversion and native submission on this worker even when metadata
                // completes asynchronously. The submission gate is scoped to the worker task.
                GetTableInfoResponse response =
                        metadata.get(Math.max(0, command.timeoutMs()), TimeUnit.MILLISECONDS);
                return produceTopic(command, topic, toTableInfo(path, response));
            }
            return metadata.thenCompose(
                            response -> produceTopic(command, topic, toTableInfo(path, response)))
                    .exceptionally(failure -> failedTopic(topic, failure));
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            return CompletableFuture.completedFuture(failedTopic(topic, failure));
        } catch (Exception failure) {
            return CompletableFuture.completedFuture(failedTopic(topic, failure));
        }
    }

    private CompletableFuture<TopicResult> produceTopic(
            KafkaProduceCommand command, TopicWrite topic, TableInfo tableInfo) {
        // Admission must match Metadata, including when the schema changes between requests.
        // Even a cached snapshot is selected only after fetching the authoritative descriptor.
        if (!schemaResolver.isEnabled(tableInfo.toTableDescriptor())) {
            throw new TableNotExistException(
                    "Table " + tableInfo.getTablePath() + " is not enabled for Kafka.");
        }
        schemaResolver.resolve(tableInfo.toTableDescriptor());
        ProduceLogRequest request =
                new ProduceLogRequest()
                        .setTableId(tableInfo.getTableId())
                        .setAcks(command.acks())
                        .setTimeoutMs(command.timeoutMs());
        Map<Integer, PartitionResult> failures = new HashMap<>();
        List<EncodedRecords> encoded = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            if (partition.partitionId() < 0
                    || partition.partitionId() >= tableInfo.getNumBuckets()) {
                failures.put(
                        partition.partitionId(),
                        new PartitionResult(
                                partition.partitionId(),
                                Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                -1L,
                                "Partition is outside the table bucket range."));
                continue;
            }
            try {
                EncodedRecords converted =
                        transcoder.transcode(
                                partition.partitionId(), partition.records(), tableInfo);
                encoded.add(converted);
                MemoryLogRecords records = converted.records();
                request.addBucketsReq()
                        .setBucketId(partition.partitionId())
                        .setRecordsBytesView(
                                new MemorySegmentBytesView(
                                        records.getMemorySegment(),
                                        records.getPosition(),
                                        records.sizeInBytes()));
            } catch (Exception failure) {
                failures.put(
                        partition.partitionId(), failedPartition(partition.partitionId(), failure));
            }
        }
        if (request.getBucketsReqsCount() == 0) {
            return CompletableFuture.completedFuture(
                    toTopicResult(topic, new ProduceLogResponse(), failures));
        }
        try {
            checkInterrupted();
            if (conversionExecutor != null) {
                conversionExecutor.awaitAppendTurn();
            }
            setCurrentSession(command);
            CompletableFuture<ProduceLogResponse> appended = gateway.produceLog(request);
            return appended.whenComplete(
                            (ignored, failure) -> encoded.forEach(EncodedRecords::deallocate))
                    .handle(
                            (response, failure) ->
                                    failure == null
                                            ? toTopicResult(topic, response, failures)
                                            : failedAppend(topic, failures, failure));
        } catch (Exception failure) {
            encoded.forEach(EncodedRecords::deallocate);
            return CompletableFuture.completedFuture(failedAppend(topic, failures, failure));
        }
    }

    private static TopicResult failedAppend(
            TopicWrite topic, Map<Integer, PartitionResult> failures, Throwable failure) {
        List<PartitionResult> results = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            PartitionResult local = failures.get(partition.partitionId());
            results.add(local == null ? failedPartition(partition.partitionId(), failure) : local);
        }
        return new TopicResult(topic.topicName(), results);
    }

    private TableInfo toTableInfo(TablePath path, GetTableInfoResponse response) {
        return tableInfoCache.getOrLoad(path, response, () -> parseTableInfo(path, response));
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

    private static TopicResult toTopicResult(
            TopicWrite topic, ProduceLogResponse response, Map<Integer, PartitionResult> failures) {
        Map<Integer, PbProduceLogRespForBucket> responses = new HashMap<>();
        for (PbProduceLogRespForBucket bucket : response.getBucketsRespsList()) {
            responses.put(bucket.getBucketId(), bucket);
        }
        List<PartitionResult> partitions = new ArrayList<>();
        for (PartitionWrite partition : topic.partitions()) {
            PartitionResult localFailure = failures.get(partition.partitionId());
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
                                "Fluss Produce response omitted this bucket."));
            } else if (bucket.hasErrorCode() && bucket.getErrorCode() != 0) {
                partitions.add(
                        new PartitionResult(
                                partition.partitionId(),
                                toKafkaError(
                                        org.apache.fluss.rpc.protocol.Errors.forCode(
                                                bucket.getErrorCode())),
                                -1L,
                                bucket.hasErrorMessage() ? bucket.getErrorMessage() : null));
            } else {
                partitions.add(
                        new PartitionResult(
                                partition.partitionId(),
                                Errors.NONE,
                                bucket.hasBaseOffset() ? bucket.getBaseOffset() : -1L,
                                null));
            }
        }
        return new TopicResult(topic.topicName(), partitions);
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
        if (conversionExecutor != null && Thread.currentThread().isInterrupted()) {
            throw new CancellationException("Kafka conversion worker was interrupted.");
        }
    }
}
