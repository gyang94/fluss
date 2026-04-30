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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.CoordinatorLoadInProgressException;
import org.apache.fluss.exception.NotCoordinatorException;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Manages the lifecycle of {@link GroupCoordinatorShard} instances and routes read/write operations
 * to the correct shard.
 *
 * <p>Write operations follow this flow:
 *
 * <ol>
 *   <li>Look up the active (loaded) shard for the given bucket
 *   <li>Execute the operation to produce a {@link CoordinatorResult}
 *   <li>Replay records into the shard's in-memory cache
 *   <li>If records are non-empty and a {@link KvWriter} is set, persist via the writer
 *   <li>Complete the future with the response once the write is acknowledged
 * </ol>
 *
 * <p>Read operations execute directly on the shard and return immediately.
 */
@Internal
@ThreadSafe
public class CoordinatorRuntime {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorRuntime.class);

    private final Map<Integer, GroupCoordinatorShard> shards;

    @Nullable private KvWriter kvWriter;

    /** Callback for writing KV records to the underlying state store. */
    public interface KvWriter {
        /**
         * Writes records and waits for the high watermark to advance past them.
         *
         * @param bucketId the bucket to write to
         * @param records the records to write
         * @return a future that completes when the write is durable
         */
        CompletableFuture<Void> writeAndWaitForHwm(
                int bucketId, List<CoordinatorResult.KvRecord> records);
    }

    public CoordinatorRuntime() {
        this.shards = MapUtils.newConcurrentMap();
    }

    /**
     * Sets the KV writer used for persisting coordinator state.
     *
     * @param kvWriter the writer to use
     */
    public void setKvWriter(KvWriter kvWriter) {
        this.kvWriter = kvWriter;
    }

    /**
     * Schedules a write operation on the shard for the given bucket.
     *
     * <p>The operation is executed, its records are replayed into the cache, and if there are
     * records to persist, they are written via the {@link KvWriter}. The returned future completes
     * with the operation's response once the write is durable (or immediately if there are no
     * records).
     *
     * @param bucketId the target bucket
     * @param op the operation to execute on the shard
     * @param <T> the response type
     * @return a future containing the operation's response
     */
    public <T> CompletableFuture<T> scheduleWriteOperation(
            int bucketId, Function<GroupCoordinatorShard, CoordinatorResult<T>> op) {
        try {
            GroupCoordinatorShard shard = getActiveShard(bucketId);
            CoordinatorResult<T> result = op.apply(shard);

            // Replay records into the in-memory cache.
            for (CoordinatorResult.KvRecord record : result.records()) {
                shard.replay(record.key(), record.value());
            }

            // If no records or no writer, complete immediately.
            if (result.records().isEmpty() || kvWriter == null) {
                return CompletableFuture.completedFuture(result.response());
            }

            // Persist via the KV writer and return the response after acknowledgement.
            return kvWriter.writeAndWaitForHwm(bucketId, result.records())
                    .thenApply(ignored -> result.response());
        } catch (Exception e) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    /**
     * Schedules a read operation on the shard for the given bucket.
     *
     * @param bucketId the target bucket
     * @param op the read operation to execute on the shard
     * @param <T> the response type
     * @return a future containing the operation's response
     */
    public <T> CompletableFuture<T> scheduleReadOperation(
            int bucketId, Function<GroupCoordinatorShard, T> op) {
        try {
            GroupCoordinatorShard shard = getActiveShard(bucketId);
            T result = op.apply(shard);
            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    /**
     * Creates and registers a new shard for the given bucket. If a shard already exists for this
     * bucket, it is replaced.
     *
     * @param bucketId the bucket ID
     * @param leaderEpoch the leader epoch
     */
    public void scheduleLoadOperation(int bucketId, int leaderEpoch) {
        shards.put(bucketId, new GroupCoordinatorShard(bucketId, leaderEpoch));
    }

    /**
     * Loads a shard and replays all KV entries from a consumer, then marks it as loaded.
     *
     * @param bucketId the bucket ID
     * @param leaderEpoch the leader epoch
     * @param kvScanner a consumer that scans all KV entries and invokes the callback for each
     */
    public void loadShardFromKv(
            int bucketId,
            int leaderEpoch,
            java.util.function.Consumer<java.util.function.BiConsumer<byte[], byte[]>> kvScanner) {
        GroupCoordinatorShard shard = new GroupCoordinatorShard(bucketId, leaderEpoch);
        shards.put(bucketId, shard);
        kvScanner.accept((key, value) -> shard.replay(key, value));
        shard.setLoaded(true);
        LOG.info(
                "Loaded GroupCoordinator shard for bucket {} with leader epoch {} from KV scan",
                bucketId,
                leaderEpoch);
    }

    /**
     * Removes the shard for the given bucket.
     *
     * @param bucketId the bucket ID to unload
     */
    public void scheduleUnloadOperation(int bucketId) {
        shards.remove(bucketId);
    }

    /**
     * Marks the shard for the given bucket as loaded (ready to serve requests).
     *
     * @param bucketId the bucket ID
     */
    public void markShardLoaded(int bucketId) {
        GroupCoordinatorShard shard = shards.get(bucketId);
        if (shard != null) {
            shard.setLoaded(true);
        }
    }

    /** Removes all shards from this runtime. */
    public void close() {
        shards.clear();
    }

    /** Returns the number of registered shards. */
    @VisibleForTesting
    int getShardCount() {
        return shards.size();
    }

    /** Returns the shard for the given bucket, or {@code null} if none exists. */
    @VisibleForTesting
    @Nullable
    GroupCoordinatorShard getShard(int bucketId) {
        return shards.get(bucketId);
    }

    /**
     * Returns the active (loaded) shard for the given bucket.
     *
     * @param bucketId the bucket ID
     * @return the active shard
     * @throws NotCoordinatorException if no shard is registered for the bucket
     * @throws CoordinatorLoadInProgressException if the shard exists but has not finished loading
     */
    private GroupCoordinatorShard getActiveShard(int bucketId) {
        GroupCoordinatorShard shard = shards.get(bucketId);
        if (shard == null) {
            throw new NotCoordinatorException("Not the coordinator for bucket " + bucketId + ".");
        }
        if (!shard.isLoaded()) {
            throw new CoordinatorLoadInProgressException(
                    "Coordinator for bucket " + bucketId + " is still loading.");
        }
        return shard;
    }
}
