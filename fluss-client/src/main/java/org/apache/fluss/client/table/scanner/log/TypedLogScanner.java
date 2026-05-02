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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;

/**
 * A typed scanner is used to scan log data as POJOs of specify table from Fluss.
 *
 * @param <T> the type of the POJO
 * @since 0.6
 */
@PublicEvolving
public interface TypedLogScanner<T> extends AutoCloseable {

    /**
     * Poll log data from tablet server.
     *
     * @param timeout the timeout to poll.
     * @return the result of poll.
     */
    TypedScanRecords<T> poll(Duration timeout);

    /**
     * Set the consumer group ID used by offset commit operations.
     *
     * @param groupId the consumer group ID
     */
    void setGroupId(String groupId);

    /**
     * Subscribe to the given table bucket from beginning dynamically. If the table bucket is
     * already subscribed, the start offset will be updated.
     *
     * @param bucket the table bucket to subscribe.
     */
    void subscribeFromBeginning(int bucket);

    /**
     * Subscribe to the given partitioned table bucket from beginning dynamically. If the table
     * bucket is already subscribed, the start offset will be updated.
     *
     * @param partitionId the partition id of the table partition to subscribe.
     * @param bucket the table bucket to subscribe.
     */
    void subscribeFromBeginning(long partitionId, int bucket);

    /**
     * Subscribe to the given table bucket in given offset dynamically. If the table bucket is
     * already subscribed, the offset will be updated.
     *
     * @param bucket the table bucket to subscribe.
     * @param offset the offset to start from.
     */
    void subscribe(int bucket, long offset);

    /**
     * Subscribe to the given partitioned table bucket in given offset dynamically. If the table
     * bucket is already subscribed, the offset will be updated.
     *
     * @param partitionId the partition id of the table partition to subscribe.
     * @param bucket the table bucket to subscribe.
     * @param offset the offset to start from.
     */
    void subscribe(long partitionId, int bucket, long offset);

    /**
     * Unsubscribe from the given bucket of given partition dynamically.
     *
     * @param partitionId the partition id of the table partition to unsubscribe.
     * @param bucket the table bucket to unsubscribe.
     */
    void unsubscribe(long partitionId, int bucket);

    /**
     * Commit the current next-fetch offsets for all subscribed buckets that have concrete scan
     * positions.
     */
    void commitSync();

    /**
     * Commit the provided offsets synchronously.
     *
     * @param offsets the offsets to commit
     */
    void commitSync(Map<TableBucket, Long> offsets);

    /** Asynchronously commit the current next-fetch offsets for all subscribed buckets. */
    void commitAsync();

    /**
     * Asynchronously commit the current next-fetch offsets with a callback.
     *
     * @param callback the callback to invoke on completion, or null for default error logging
     */
    void commitAsync(@Nullable OffsetCommitCallback callback);

    /**
     * Asynchronously commit the provided offsets with a callback.
     *
     * @param offsets the offsets to commit
     * @param callback the callback to invoke on completion, or null for default error logging
     */
    void commitAsync(Map<TableBucket, Long> offsets, @Nullable OffsetCommitCallback callback);

    /**
     * Wake up the log scanner in case the fetcher thread in log scanner is blocking in {@link
     * #poll(Duration timeout)}.
     */
    void wakeup();
}
