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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.SingleThreadMultiplexSourceReaderBaseAdapter;
import org.apache.fluss.flink.lake.LakeSplitStateInitializer;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.event.FinishedKvSnapshotConsumeEvent;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.reader.fetcher.FlinkSourceFetcherManager;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

/** The source reader for Fluss. */
public class FlinkSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBaseAdapter<
                RecordAndPos, OUT, SourceSplitBase, SourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceReader.class);

    /** the tableBuckets ignore to send FinishedKvSnapshotConsumeEvent as it already sending. */
    private final Set<TableBucket> finishedKvSnapshotConsumeBuckets;

    /**
     * Offsets to commit on checkpoint completion, keyed by checkpoint ID. Access is synchronized
     * via Collections.synchronizedSortedMap.
     */
    private final SortedMap<Long, Map<TableBucket, Long>> offsetsToCommit;

    /** Whether offset commit on checkpoint is enabled. */
    private final boolean commitOffsetsOnCheckpoint;

    public FlinkSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue,
            Configuration flussConfig,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            FlinkSourceReaderMetrics flinkSourceReaderMetrics,
            FlinkRecordEmitter<OUT> recordEmitter,
            LakeSource<LakeSplit> lakeSource,
            @Nullable String groupId) {
        super(
                elementsQueue,
                new FlinkSourceFetcherManager(
                        elementsQueue,
                        () ->
                                new FlinkSourceSplitReader(
                                        flussConfig,
                                        tablePath,
                                        sourceOutputType,
                                        projectedFields,
                                        logRecordBatchFilter,
                                        lakeSource,
                                        flinkSourceReaderMetrics,
                                        groupId),
                        (ignore) -> {}),
                recordEmitter,
                context.getConfiguration(),
                context);
        this.finishedKvSnapshotConsumeBuckets = new HashSet<>();
        this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.commitOffsetsOnCheckpoint = (groupId != null);
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> map) {
        // do nothing
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        Set<TableBucket> bucketsFinishedConsumeKvSnapshot = new HashSet<>();

        // do not modify this state.
        List<SourceSplitBase> sourceSplitBases = super.snapshotState(checkpointId);

        // Capture offsets for commit on checkpoint completion
        if (commitOffsetsOnCheckpoint) {
            Map<TableBucket, Long> offsets = new HashMap<>();
            for (SourceSplitBase sourceSplitBase : sourceSplitBases) {
                TableBucket tableBucket = sourceSplitBase.getTableBucket();
                if (sourceSplitBase.isLogSplit()) {
                    LogSplit logSplit = sourceSplitBase.asLogSplit();
                    long offset = logSplit.getStartingOffset();
                    if (offset >= 0) {
                        offsets.put(tableBucket, offset);
                    }
                } else if (sourceSplitBase.isHybridSnapshotLogSplit()) {
                    HybridSnapshotLogSplit hybridSplit = sourceSplitBase.asHybridSnapshotLogSplit();
                    if (hybridSplit.isSnapshotFinished()) {
                        long offset = hybridSplit.getLogStartingOffset();
                        if (offset >= 0) {
                            offsets.put(tableBucket, offset);
                        }
                    }
                }
            }
            if (!offsets.isEmpty()) {
                offsetsToCommit.put(checkpointId, offsets);
            }
        }

        for (SourceSplitBase sourceSplitBase : sourceSplitBases) {
            TableBucket tableBucket = sourceSplitBase.getTableBucket();
            if (finishedKvSnapshotConsumeBuckets.contains(tableBucket)) {
                continue;
            }

            if (sourceSplitBase.isHybridSnapshotLogSplit()) {
                HybridSnapshotLogSplit hybridSnapshotLogSplit =
                        sourceSplitBase.asHybridSnapshotLogSplit();
                if (hybridSnapshotLogSplit.isSnapshotFinished()) {
                    bucketsFinishedConsumeKvSnapshot.add(tableBucket);
                }
            }
        }

        // report finished kv snapshot consume event.
        if (!bucketsFinishedConsumeKvSnapshot.isEmpty()) {
            LOG.info(
                    "reader has finished kv snapshot read for bucket: {}, checkpoint id {}",
                    bucketsFinishedConsumeKvSnapshot,
                    checkpointId);

            // TODO Reduce the external IO operation, trace by
            // https://github.com/apache/fluss/issues/2597.
            context.sendSourceEventToCoordinator(
                    new FinishedKvSnapshotConsumeEvent(
                            checkpointId, bucketsFinishedConsumeKvSnapshot));
            // It won't be sent anymore in the future for this table bucket, but will be resent
            // after failover recovery as ignoreBuckets is cleared.
            finishedKvSnapshotConsumeBuckets.addAll(bucketsFinishedConsumeKvSnapshot);
        }

        return sourceSplitBases;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        if (!commitOffsetsOnCheckpoint) {
            return;
        }

        Map<TableBucket, Long> offsets = offsetsToCommit.get(checkpointId);
        if (offsets == null || offsets.isEmpty()) {
            removeOffsetsToCommitUpTo(checkpointId);
            return;
        }

        ((FlinkSourceFetcherManager) splitFetcherManager)
                .commitOffsets(
                        offsets,
                        (committedOffsets, exception) -> {
                            if (exception != null) {
                                LOG.warn(
                                        "Failed to commit offsets for checkpoint {}",
                                        checkpointId,
                                        exception);
                            } else {
                                LOG.debug(
                                        "Successfully committed offsets for checkpoint {}",
                                        checkpointId);
                            }
                            removeOffsetsToCommitUpTo(checkpointId);
                        });
    }

    private void removeOffsetsToCommitUpTo(long checkpointId) {
        // headMap is exclusive of checkpointId, so +1 to include it
        offsetsToCommit.headMap(checkpointId + 1).clear();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof PartitionsRemovedEvent) {
            PartitionsRemovedEvent partitionsRemovedEvent = (PartitionsRemovedEvent) sourceEvent;
            Consumer<Set<TableBucket>> unsubscribeTableBucketsCallback =
                    (unsubscribedTableBuckets) -> {
                        // send remove partitions ack event to coordinator
                        context.sendSourceEventToCoordinator(
                                new PartitionBucketsUnsubscribedEvent(unsubscribedTableBuckets));
                    };
            ((FlinkSourceFetcherManager) splitFetcherManager)
                    .removePartitions(
                            partitionsRemovedEvent.getRemovedPartitions(),
                            unsubscribeTableBucketsCallback);
        }
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (split.isHybridSnapshotLogSplit()) {
            return new HybridSnapshotLogSplitState(split.asHybridSnapshotLogSplit());
        } else if (split.isLogSplit()) {
            return new LogSplitState(split.asLogSplit());
        } else if (split.isLakeSplit()) {
            return LakeSplitStateInitializer.initializedState(split);
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
        return splitState.toSourceSplit();
    }
}
