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
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidGroupIdException;
import org.apache.fluss.exception.WakeupException;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.Projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The default impl of {@link LogScanner}.
 *
 * <p>The {@link LogScannerImpl} is NOT thread-safe. It is the responsibility of the user to ensure
 * that multithreaded access is properly synchronized. Un-synchronized access will result in {@link
 * ConcurrentModificationException}.
 *
 * @since 0.1
 */
@PublicEvolving
public class LogScannerImpl implements LogScanner {
    private static final Logger LOG = LoggerFactory.getLogger(LogScannerImpl.class);

    private static final long NO_CURRENT_THREAD = -1L;
    private final TablePath tablePath;
    private final LogScannerStatus logScannerStatus;
    private final MetadataUpdater metadataUpdater;
    private final LogFetcher logFetcher;
    private final long tableId;
    private final boolean isPartitionedTable;
    private final long requestTimeoutMs;
    private final long retryBackoffMs;

    private @Nullable String groupId;
    private @Nullable OffsetCommitter offsetCommitter;
    private final ConcurrentLinkedQueue<OffsetCommitter.OffsetCommitCompletion>
            completedOffsetCommits = new ConcurrentLinkedQueue<>();

    private volatile boolean closed = false;

    // currentThread holds the threadId of the current thread accessing FlussLogScanner
    // and is used to prevent multithreaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refCount is used to allow reentrant access by the thread who has acquired currentThread.
    private final AtomicInteger refCount = new AtomicInteger(0);
    // metrics
    private final ScannerMetricGroup scannerMetricGroup;

    public LogScannerImpl(
            Configuration conf,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            ClientMetricGroup clientMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            @Nullable int[] projectedFields,
            SchemaGetter schemaGetter,
            @Nullable Predicate recordBatchFilter) {
        this.tablePath = tableInfo.getTablePath();
        this.tableId = tableInfo.getTableId();
        this.isPartitionedTable = tableInfo.isPartitioned();
        this.requestTimeoutMs = conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis();
        this.retryBackoffMs = conf.get(ConfigOptions.CLIENT_RETRY_BACKOFF).toMillis();
        // add this table to metadata updater.
        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
        this.logScannerStatus = new LogScannerStatus();
        this.metadataUpdater = metadataUpdater;
        Projection projection = sanityProjection(projectedFields, tableInfo);
        this.scannerMetricGroup = new ScannerMetricGroup(clientMetricGroup, tablePath);
        this.logFetcher =
                new LogFetcher(
                        tableInfo,
                        projection,
                        recordBatchFilter,
                        logScannerStatus,
                        conf,
                        metadataUpdater,
                        scannerMetricGroup,
                        remoteFileDownloader,
                        schemaGetter);
    }

    /**
     * Check if the projected fields are valid and returns {@link Projection} if the projection is
     * not null.
     */
    @Nullable
    private Projection sanityProjection(@Nullable int[] projectedFields, TableInfo tableInfo) {
        RowType tableRowType = tableInfo.getRowType();
        if (projectedFields != null) {
            for (int projectedField : projectedFields) {
                if (projectedField < 0 || projectedField >= tableRowType.getFieldCount()) {
                    throw new IllegalArgumentException(
                            "Projected field index "
                                    + projectedField
                                    + " is out of bound for schema "
                                    + tableRowType);
                }
            }
            return Projection.of(projectedFields);
        } else {
            return null;
        }
    }

    @Override
    public ScanRecords poll(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            invokeCompletedOffsetCommitCallbacks();

            if (!logScannerStatus.prepareToPoll()) {
                throw new IllegalStateException("LogScanner is not subscribed any buckets.");
            }

            scannerMetricGroup.recordPollStart(System.currentTimeMillis());
            long timeoutNanos = timeout.toNanos();
            long startNanos = System.nanoTime();
            do {
                ScanRecords scanRecords = pollForFetches();
                if (scanRecords.isEmpty()) {
                    try {
                        if (!logFetcher.awaitNotEmpty(startNanos + timeoutNanos)) {
                            // logFetcher waits for the timeout and no data in buffer,
                            // so we return empty
                            return scanRecords;
                        }
                    } catch (WakeupException e) {
                        // wakeup() is called, we need to return empty
                        return scanRecords;
                    }
                } else {
                    // before returning the fetched records, we can send off the next round of
                    // fetches and avoid block waiting for their responses to enable pipelining
                    // while the user is handling the fetched records.
                    logFetcher.sendFetches();

                    return scanRecords;
                }
            } while (System.nanoTime() - startNanos < timeoutNanos);

            return ScanRecords.EMPTY;
        } finally {
            release();
            scannerMetricGroup.recordPollEnd(System.currentTimeMillis());
        }
    }

    @Override
    public void setGroupId(String groupId) {
        acquireAndEnsureOpen();
        try {
            checkNotNull(groupId, "groupId must not be null");
            checkArgument(!groupId.trim().isEmpty(), "groupId must not be empty");
            this.groupId = groupId;
            if (offsetCommitter != null) {
                offsetCommitter.close();
                offsetCommitter = null;
            }
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(int bucket, long offset) {
        if (isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is a partitioned table, please use "
                            + "\"subscribe(long partitionId, int bucket, long offset)\" to "
                            + "subscribe a partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            this.metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
            this.logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(long partitionId, int bucket, long offset) {
        if (!isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is not a partitioned table, please use "
                            + "\"subscribe(int bucket, long offset)\" to "
                            + "subscribe a non-partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            // we make assumption that the partition id must belong to the current table
            // if we can't find the partition id from the table path, we'll consider the table
            // is not exist
            this.metadataUpdater.checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(partitionId));
            this.logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe(long partitionId, int bucket) {
        if (!isPartitionedTable) {
            throw new IllegalStateException(
                    "Can't unsubscribe a partition for a non-partitioned table.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            this.logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe(int bucket) {
        if (isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is a partitioned table, please use "
                            + "\"unsubscribe(long partitionId, int bucket)\" to "
                            + "unsubscribe a partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            this.logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
        } finally {
            release();
        }
    }

    @Override
    public void commitSync() {
        acquireAndEnsureOpen();
        try {
            invokeCompletedOffsetCommitCallbacks();
            ensureGroupIdConfigured();
            Map<TableBucket, Long> offsets = logScannerStatus.allCommittableOffsets();
            if (!offsets.isEmpty()) {
                getOrCreateOffsetCommitter().commitOffsetsSync(offsets, requestTimeoutMs);
            }
        } finally {
            release();
        }
    }

    @Override
    public void commitSync(Map<TableBucket, Long> offsets) {
        acquireAndEnsureOpen();
        try {
            invokeCompletedOffsetCommitCallbacks();
            ensureGroupIdConfigured();
            Map<TableBucket, Long> validatedOffsets = validateExplicitOffsets(offsets);
            if (!validatedOffsets.isEmpty()) {
                getOrCreateOffsetCommitter().commitOffsetsSync(validatedOffsets, requestTimeoutMs);
            }
        } finally {
            release();
        }
    }

    @Override
    public void wakeup() {
        logFetcher.wakeup();
    }

    @Override
    public void commitAsync() {
        commitAsync(new DefaultOffsetCommitCallback());
    }

    @Override
    public void commitAsync(@Nullable OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            invokeCompletedOffsetCommitCallbacks();
            ensureGroupIdConfigured();
            Map<TableBucket, Long> offsets = logScannerStatus.allCommittableOffsets();
            getOrCreateOffsetCommitter()
                    .commitOffsetsAsync(offsets, callback, completedOffsetCommits);
        } finally {
            release();
        }
    }

    @Override
    public void commitAsync(
            Map<TableBucket, Long> offsets, @Nullable OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            invokeCompletedOffsetCommitCallbacks();
            ensureGroupIdConfigured();
            Map<TableBucket, Long> validatedOffsets = validateExplicitOffsets(offsets);
            getOrCreateOffsetCommitter()
                    .commitOffsetsAsync(validatedOffsets, callback, completedOffsetCommits);
        } finally {
            release();
        }
    }

    private void invokeCompletedOffsetCommitCallbacks() {
        while (true) {
            OffsetCommitter.OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                return;
            }
            completion.invoke();
        }
    }

    private ScanRecords pollForFetches() {
        ScanRecords scanRecords = logFetcher.collectFetch();
        if (!scanRecords.isEmpty()) {
            return scanRecords;
        }

        // send any new fetches (won't resend pending fetches).
        logFetcher.sendFetches();

        return logFetcher.collectFetch();
    }

    private void ensureGroupIdConfigured() {
        if (groupId == null || groupId.trim().isEmpty()) {
            throw new InvalidGroupIdException(
                    "Consumer group ID is not set. Call setGroupId() before committing offsets.");
        }
    }

    private OffsetCommitter getOrCreateOffsetCommitter() {
        if (offsetCommitter == null) {
            offsetCommitter = new OffsetCommitter(groupId, metadataUpdater, retryBackoffMs);
        }
        return offsetCommitter;
    }

    private Map<TableBucket, Long> validateExplicitOffsets(Map<TableBucket, Long> offsets) {
        checkNotNull(offsets, "offsets must not be null");

        Map<TableBucket, Long> validatedOffsets = new HashMap<>();
        for (Map.Entry<TableBucket, Long> entry : offsets.entrySet()) {
            TableBucket tableBucket = checkNotNull(entry.getKey(), "offsets contains null bucket");
            Long offset =
                    checkNotNull(entry.getValue(), "offset for %s must not be null", tableBucket);

            checkArgument(offset >= 0, "offset for %s must be >= 0", tableBucket);
            checkArgument(
                    tableBucket.getTableId() == tableId,
                    "bucket %s does not belong to table %s",
                    tableBucket,
                    tablePath);

            if (isPartitionedTable) {
                checkArgument(
                        tableBucket.getPartitionId() != null,
                        "bucket %s must include partitionId for partitioned table %s",
                        tableBucket,
                        tablePath);
            } else {
                checkArgument(
                        tableBucket.getPartitionId() == null,
                        "bucket %s must not include partitionId for non-partitioned table %s",
                        tableBucket,
                        tablePath);
            }

            validatedOffsets.put(tableBucket, offset);
        }
        return validatedOffsets;
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
     * @throws IllegalStateException If the scanner has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (closed) {
            release();
            throw new IllegalStateException("This scanner has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this scanner from multithreaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multithreaded
     * usage is not supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        if (threadId != currentThread.get()
                && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId)) {
            throw new ConcurrentModificationException(
                    "Scanner is not safe for multithreaded access. "
                            + "currentThread(name: "
                            + thread.getName()
                            + ", id: "
                            + threadId
                            + ")"
                            + " otherThread(id: "
                            + currentThread.get()
                            + ")");
        }
        refCount.incrementAndGet();
    }

    /** Release the light lock protecting the consumer from multithreaded access. */
    private void release() {
        if (refCount.decrementAndGet() == 0) {
            currentThread.set(NO_CURRENT_THREAD);
        }
    }

    @Override
    public void close() {
        acquire();
        try {
            if (!closed) {
                LOG.trace("Closing log scanner for table: {}", tablePath);
                invokeCompletedOffsetCommitCallbacks();
                if (offsetCommitter != null) {
                    offsetCommitter.close();
                    offsetCommitter = null;
                }
                scannerMetricGroup.close();
                logFetcher.close();
            }
            LOG.debug("Log scanner for table: {} has been closed", tablePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to close log scanner for table " + tablePath, e);
        } finally {
            closed = true;
            release();
        }
    }

    /** Default callback that logs async commit errors at WARN level. */
    private static final class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        private static final Logger LOG =
                LoggerFactory.getLogger(DefaultOffsetCommitCallback.class);

        @Override
        public void onComplete(Map<TableBucket, Long> offsets, @Nullable Exception exception) {
            if (exception != null) {
                LOG.warn("Async offset commit of offsets {} failed.", offsets, exception);
            }
        }
    }
}
