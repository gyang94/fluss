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

package org.apache.fluss.server.coordinator.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.CoordinatorNotAvailableException;
import org.apache.fluss.exception.InvalidGroupIdException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.FindCoordinatorRequest;
import org.apache.fluss.rpc.messages.FindCoordinatorResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.DataTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link GroupCoordinator} that validates requests and routes them to the correct
 * shard in the {@link CoordinatorRuntime}.
 *
 * <p>Each consumer group is mapped to a bucket via consistent hashing of the group ID. The bucket
 * ID determines which {@link GroupCoordinatorShard} handles the request.
 */
@Internal
public class GroupCoordinatorService implements GroupCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(GroupCoordinatorService.class);

    private static final TablePath CONSUMER_OFFSETS_TABLE_PATH =
            TablePath.of("sys", "consumer_offsets");

    private final CoordinatorRuntime runtime;
    private final TabletServerMetadataCache metadataCache;
    private final MetadataManager metadataManager;
    private final String defaultListenerName;
    private final CoordinatorGateway coordinatorGateway;
    private final Configuration conf;

    /** Dedup flag to prevent duplicate creation requests from the same TabletServer. */
    private final AtomicBoolean creationInflight = new AtomicBoolean(false);

    private volatile long consumerOffsetsTableId = TableInfo.UNKNOWN_TABLE_ID;
    private volatile int consumerOffsetsNumBuckets = -1;

    public GroupCoordinatorService(
            CoordinatorRuntime runtime,
            TabletServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            String defaultListenerName,
            CoordinatorGateway coordinatorGateway,
            Configuration conf) {
        this.runtime = runtime;
        this.metadataCache = metadataCache;
        this.metadataManager = metadataManager;
        this.defaultListenerName = defaultListenerName;
        this.coordinatorGateway = coordinatorGateway;
        this.conf = conf;
    }

    @Override
    public CompletableFuture<FindCoordinatorResponse> findCoordinator(
            FindCoordinatorRequest request) {
        return findCoordinator(request, defaultListenerName);
    }

    /**
     * Finds the coordinator endpoint for the given group using the specified listener name.
     *
     * <p>This overload is used by the tablet RPC service so the returned endpoint matches the
     * listener through which the request was received.
     */
    public CompletableFuture<FindCoordinatorResponse> findCoordinator(
            FindCoordinatorRequest request, String listenerName) {
        String groupId = request.getGroupId();
        try {
            validateGroupId(groupId);
        } catch (InvalidGroupIdException e) {
            return failedFuture(e);
        }

        try {
            ConsumerOffsetsTableMetadata tableMetadata = resolveConsumerOffsetsTableMetadata();
            int bucketId = computeBucketId(groupId, tableMetadata.numBuckets());
            BucketMetadata bucketMetadata =
                    metadataCache.getBucketMetadata(tableMetadata.tableId(), bucketId).orElse(null);
            if (bucketMetadata == null || !bucketMetadata.getLeaderId().isPresent()) {
                return CompletableFuture.completedFuture(
                        coordinatorNotAvailable(
                                "Coordinator leader is not available for bucket "
                                        + bucketId
                                        + "."));
            }

            int leaderId = bucketMetadata.getLeaderId().getAsInt();
            ServerNode leaderNode =
                    metadataCache.getTabletServer(leaderId, listenerName).orElse(null);
            if (leaderNode == null) {
                return CompletableFuture.completedFuture(
                        coordinatorNotAvailable(
                                "Coordinator endpoint is not available for leader "
                                        + leaderId
                                        + " and listener "
                                        + listenerName
                                        + "."));
            }

            FindCoordinatorResponse response = new FindCoordinatorResponse();
            response.setCoordinatorServerId(leaderNode.id());
            response.setHost(leaderNode.host());
            response.setPort(leaderNode.port());
            response.setErrorCode(Errors.NONE.code());
            return CompletableFuture.completedFuture(response);
        } catch (CoordinatorNotAvailableException e) {
            return CompletableFuture.completedFuture(coordinatorNotAvailable(e.getMessage()));
        }
    }

    @Override
    public CompletableFuture<CommitOffsetsResponse> commitOffsets(CommitOffsetsRequest request) {
        String groupId = request.getGroupId();
        try {
            validateGroupId(groupId);
            ConsumerOffsetsTableMetadata tableMetadata = resolveConsumerOffsetsTableMetadata();
            int bucketId = computeBucketId(groupId, tableMetadata.numBuckets());
            return runtime.scheduleWriteOperation(bucketId, shard -> shard.commitOffset(request));
        } catch (InvalidGroupIdException e) {
            return failedFuture(e);
        } catch (CoordinatorNotAvailableException e) {
            return failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<FetchOffsetsResponse> fetchOffsets(FetchOffsetsRequest request) {
        String groupId = request.getGroupId();
        try {
            validateGroupId(groupId);
            ConsumerOffsetsTableMetadata tableMetadata = resolveConsumerOffsetsTableMetadata();
            int bucketId = computeBucketId(groupId, tableMetadata.numBuckets());
            return runtime.scheduleReadOperation(bucketId, shard -> shard.fetchOffsets(request));
        } catch (InvalidGroupIdException e) {
            return failedFuture(e);
        } catch (CoordinatorNotAvailableException e) {
            return failedFuture(e);
        }
    }

    /**
     * Computes the bucket ID for a given group ID using consistent hashing.
     *
     * @param groupId the group ID
     * @param numBuckets the total number of buckets
     * @return the bucket ID in the range [0, numBuckets)
     */
    static int computeBucketId(String groupId, int numBuckets) {
        return (groupId.hashCode() & 0x7FFFFFFF) % numBuckets;
    }

    /**
     * Validates that the group ID is not null or empty.
     *
     * @param groupId the group ID to validate
     * @throws InvalidGroupIdException if the group ID is null or empty
     */
    private static void validateGroupId(String groupId) {
        if (groupId == null || groupId.trim().isEmpty()) {
            throw new InvalidGroupIdException("Group ID must not be null or empty.");
        }
    }

    private ConsumerOffsetsTableMetadata resolveConsumerOffsetsTableMetadata() {
        long tableId = consumerOffsetsTableId;
        int numBuckets = consumerOffsetsNumBuckets;
        if (tableId != TableInfo.UNKNOWN_TABLE_ID && numBuckets > 0) {
            return new ConsumerOffsetsTableMetadata(tableId, numBuckets);
        }

        synchronized (this) {
            if (consumerOffsetsTableId == TableInfo.UNKNOWN_TABLE_ID
                    || consumerOffsetsNumBuckets <= 0) {
                if (!metadataManager.tableExists(CONSUMER_OFFSETS_TABLE_PATH)) {
                    triggerConsumerOffsetsCreation();
                    throw new CoordinatorNotAvailableException(
                            "sys.consumer_offsets does not exist yet. "
                                    + "Creation has been triggered; please retry.");
                }

                TableInfo tableInfo = metadataManager.getTable(CONSUMER_OFFSETS_TABLE_PATH);
                if (tableInfo.getNumBuckets() <= 0) {
                    throw new CoordinatorNotAvailableException(
                            "sys.consumer_offsets has an invalid bucket count: "
                                    + tableInfo.getNumBuckets());
                }
                consumerOffsetsTableId = tableInfo.getTableId();
                consumerOffsetsNumBuckets = tableInfo.getNumBuckets();
            }

            return new ConsumerOffsetsTableMetadata(
                    consumerOffsetsTableId, consumerOffsetsNumBuckets);
        }
    }

    /**
     * Triggers the creation of the sys.consumer_offsets table via the coordinator RPC.
     *
     * <p>The {@code sys} database is created eagerly at coordinator startup. This method only
     * creates the {@code consumer_offsets} table. It is a fire-and-forget operation that uses an
     * {@link AtomicBoolean} to deduplicate concurrent creation attempts from the same TabletServer.
     * The coordinator's createTable RPC uses {@code ignoreIfExists=true} for idempotency across
     * multiple TabletServers.
     */
    private void triggerConsumerOffsetsCreation() {
        if (!creationInflight.compareAndSet(false, true)) {
            return;
        }

        LOG.info("Triggering auto-creation of sys.consumer_offsets via coordinator RPC.");

        int bucketCount = conf.getInt(ConfigOptions.CONSUMER_OFFSETS_BUCKET_COUNT);
        int replicationFactor = conf.getInt(ConfigOptions.CONSUMER_OFFSETS_REPLICATION_FACTOR);

        Schema schema =
                Schema.newBuilder()
                        .column("offset_key", DataTypes.BYTES())
                        .column("offset_value", DataTypes.BYTES())
                        .primaryKey("offset_key")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .property(
                                ConfigOptions.TABLE_REPLICATION_FACTOR.key(),
                                String.valueOf(replicationFactor))
                        .distributedBy(bucketCount)
                        .build();

        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest
                .setTableJson(tableDescriptor.toJsonBytes())
                .setIgnoreIfExists(true)
                .setTablePath()
                .setDatabaseName(CONSUMER_OFFSETS_TABLE_PATH.getDatabaseName())
                .setTableName(CONSUMER_OFFSETS_TABLE_PATH.getTableName());

        try {
            coordinatorGateway
                    .createTable(createTableRequest)
                    .whenComplete(
                            (resp, throwable) -> {
                                creationInflight.set(false);
                                if (throwable != null) {
                                    LOG.warn(
                                            "Failed to auto-create sys.consumer_offsets.",
                                            throwable);
                                } else {
                                    LOG.info(
                                            "Successfully triggered creation of "
                                                    + "sys.consumer_offsets with {} buckets, "
                                                    + "replication factor {}.",
                                            bucketCount,
                                            replicationFactor);
                                }
                            });
        } catch (Exception e) {
            creationInflight.set(false);
            LOG.warn("Failed to send auto-create RPC for sys.consumer_offsets.", e);
        }
    }

    private static FindCoordinatorResponse coordinatorNotAvailable(String message) {
        FindCoordinatorResponse response = new FindCoordinatorResponse();
        response.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE_EXCEPTION.code());
        response.setErrorMessage(message);
        return response;
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable throwable) {
        CompletableFuture<T> failed = new CompletableFuture<>();
        failed.completeExceptionally(throwable);
        return failed;
    }

    private static final class ConsumerOffsetsTableMetadata {
        private final long tableId;
        private final int numBuckets;

        private ConsumerOffsetsTableMetadata(long tableId, int numBuckets) {
            this.tableId = tableId;
            this.numBuckets = numBuckets;
        }

        private long tableId() {
            return tableId;
        }

        private int numBuckets() {
            return numBuckets;
        }
    }
}
