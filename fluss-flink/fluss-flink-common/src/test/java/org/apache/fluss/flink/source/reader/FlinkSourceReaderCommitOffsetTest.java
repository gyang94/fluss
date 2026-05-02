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
import org.apache.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for offset commit on checkpoint in {@link FlinkSourceReader}. */
class FlinkSourceReaderCommitOffsetTest extends FlinkTestBase {

    @Test
    void testSnapshotStateCapturesOffsetsForLogSplit() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_log_offset_capture");
        TableDescriptor tableDescriptor = DEFAULT_LOG_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        TestingReaderContext readerContext = new TestingReaderContext();
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        "test-group")) {
            reader.start();

            TableBucket bucket0 = new TableBucket(tableId, null, 0);
            long startOffset = 42L;
            reader.addSplits(Collections.singletonList(new LogSplit(bucket0, null, startOffset)));

            List<?> splits = reader.snapshotState(1L);
            assertThat(splits).isNotEmpty();

            // After snapshotState, the offsets should be captured internally.
            // We verify by calling notifyCheckpointComplete - it should not throw.
            reader.notifyCheckpointComplete(1L);
        }
    }

    @Test
    void testSnapshotStateCapturesOffsetsForHybridSplitAfterSnapshotFinished() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_hybrid_offset_capture");
        TableDescriptor tableDescriptor = DEFAULT_PK_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        TestingReaderContext readerContext = new TestingReaderContext();
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        "test-group")) {
            reader.start();

            TableBucket bucket0 = new TableBucket(tableId, null, 0);
            long logOffset = 100L;
            // Create a hybrid split with snapshot finished
            HybridSnapshotLogSplit hybridSplit =
                    new HybridSnapshotLogSplit(
                            bucket0, null, 0L, // snapshotId
                            0L, // recordsToSkip
                            true, // snapshot finished
                            logOffset);
            reader.addSplits(Collections.singletonList(hybridSplit));

            List<?> splits = reader.snapshotState(1L);
            assertThat(splits).isNotEmpty();

            // notifyCheckpointComplete should not throw
            reader.notifyCheckpointComplete(1L);
        }
    }

    @Test
    void testOffsetCommitDisabledWhenGroupIdIsNull() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_no_offset_commit");
        TableDescriptor tableDescriptor = DEFAULT_LOG_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        TestingReaderContext readerContext = new TestingReaderContext();
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        null)) {
            reader.start();

            TableBucket bucket0 = new TableBucket(tableId, null, 0);
            reader.addSplits(Collections.singletonList(new LogSplit(bucket0, null, 10L)));

            reader.snapshotState(1L);

            // Should be a no-op when groupId is null, should not throw
            reader.notifyCheckpointComplete(1L);
        }
    }

    @Test
    void testCheckpointSubsumption() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_checkpoint_subsumption");
        TableDescriptor tableDescriptor = DEFAULT_LOG_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        TestingReaderContext readerContext = new TestingReaderContext();
        try (FlinkSourceReader<RowData> reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        "test-group")) {
            reader.start();

            TableBucket bucket0 = new TableBucket(tableId, null, 0);
            reader.addSplits(Collections.singletonList(new LogSplit(bucket0, null, 10L)));

            // Take multiple snapshots
            reader.snapshotState(1L);
            reader.snapshotState(2L);
            reader.snapshotState(3L);

            // Completing checkpoint 3 should subsume checkpoints 1 and 2
            reader.notifyCheckpointComplete(3L);

            // Completing an old checkpoint should be a no-op
            reader.notifyCheckpointComplete(1L);
        }
    }

    private FlinkSourceReader<RowData> createReader(
            Configuration flussConf,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            String groupId)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        RowDataDeserializationSchema deserializationSchema = new RowDataDeserializationSchema();
        deserializationSchema.open(
                new DeserializerInitContextImpl(
                        context.metricGroup().addGroup("deserializer"),
                        context.getUserCodeClassLoader(),
                        sourceOutputType));
        FlinkRecordEmitter<RowData> recordEmitter = new FlinkRecordEmitter<>(deserializationSchema);

        return new FlinkSourceReader<>(
                elementsQueue,
                flussConf,
                tablePath,
                sourceOutputType,
                context,
                null,
                null,
                new FlinkSourceReaderMetrics(context.metricGroup()),
                recordEmitter,
                null,
                groupId);
    }
}
