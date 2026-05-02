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

import org.apache.fluss.client.metadata.TestingClientSchemaGetter;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidGroupIdException;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_ID;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_INFO;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Unit test for {@link LogScannerImpl#commitSync()}. */
class LogScannerCommitSyncTest {

    private Configuration configuration;
    private TestingMetadataUpdater metadataUpdater;
    private RemoteFileDownloader remoteFileDownloader;
    private LogScannerImpl logScanner;

    @BeforeEach
    void setUp() {
        configuration = new Configuration();
        configuration.set(ConfigOptions.CLIENT_REQUEST_TIMEOUT, Duration.ofSeconds(7));
        metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
        remoteFileDownloader = new RemoteFileDownloader(1);

        logScanner =
                new LogScannerImpl(
                        configuration,
                        DATA1_TABLE_INFO,
                        metadataUpdater,
                        TestingClientMetricGroup.newInstance(),
                        remoteFileDownloader,
                        null,
                        new TestingClientSchemaGetter(
                                DATA1_TABLE_PATH,
                                new SchemaInfo(DATA1_SCHEMA, 0),
                                metadataUpdater,
                                configuration),
                        null);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (logScanner != null) {
            logScanner.close();
        }
        if (remoteFileDownloader != null) {
            remoteFileDownloader.close();
        }
    }

    @Test
    void testCommitSyncWithoutGroupIdFails() {
        logScanner.subscribe(0, 5L);

        assertThatThrownBy(() -> logScanner.commitSync())
                .isInstanceOf(InvalidGroupIdException.class)
                .hasMessageContaining("setGroupId()");
    }

    @Test
    void testCommitSyncUsesAllCommittableOffsets() throws Exception {
        logScanner.setGroupId("group-1");
        logScanner.subscribe(0, 15L);
        logScanner.subscribe(1, LogScanner.EARLIEST_OFFSET);

        RecordingOffsetCommitter committer =
                new RecordingOffsetCommitter("group-1", metadataUpdater);
        setOffsetCommitter(committer);

        logScanner.commitSync();

        assertThat(committer.commitCalls).isEqualTo(1);
        assertThat(committer.lastOffsets)
                .containsOnly(entry(new TableBucket(DATA1_TABLE_ID, 0), 15L));
        assertThat(committer.timeoutMs)
                .isEqualTo(configuration.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis());
    }

    @Test
    void testCommitSyncAfterResubscribeUsesLatestExplicitOffset() throws Exception {
        logScanner.setGroupId("group-1");
        logScanner.subscribe(0, 15L);
        logScanner.subscribe(0, 27L);

        RecordingOffsetCommitter committer =
                new RecordingOffsetCommitter("group-1", metadataUpdater);
        setOffsetCommitter(committer);

        logScanner.commitSync();

        assertThat(committer.commitCalls).isEqualTo(1);
        assertThat(committer.lastOffsets)
                .containsOnly(entry(new TableBucket(DATA1_TABLE_ID, 0), 27L));
    }

    @Test
    void testCommitSyncAfterResubscribeToBeginningSkipsBucket() throws Exception {
        logScanner.setGroupId("group-1");
        logScanner.subscribe(0, 15L);
        logScanner.subscribeFromBeginning(0);

        RecordingOffsetCommitter committer =
                new RecordingOffsetCommitter("group-1", metadataUpdater);
        setOffsetCommitter(committer);

        logScanner.commitSync();

        assertThat(committer.commitCalls).isZero();
        assertThat(committer.lastOffsets).isNull();
    }

    @Test
    void testCommitSyncWithExplicitOffsetsUsesProvidedOffsets() throws Exception {
        logScanner.setGroupId("group-1");

        RecordingOffsetCommitter committer =
                new RecordingOffsetCommitter("group-1", metadataUpdater);
        setOffsetCommitter(committer);

        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(DATA1_TABLE_ID, 2), 6L);
        logScanner.commitSync(offsets);

        assertThat(committer.commitCalls).isEqualTo(1);
        assertThat(committer.lastOffsets).isEqualTo(offsets);
    }

    @Test
    void testSetGroupIdClosesExistingCommitter() throws Exception {
        logScanner.setGroupId("group-1");

        RecordingOffsetCommitter committer =
                new RecordingOffsetCommitter("group-1", metadataUpdater);
        setOffsetCommitter(committer);

        logScanner.setGroupId("group-2");

        assertThat(committer.closed).isTrue();
        assertThat(getOffsetCommitter()).isNull();
    }

    @Test
    void testCommitSyncRejectsNegativeExplicitOffset() {
        logScanner.setGroupId("group-1");

        assertThatThrownBy(
                        () ->
                                logScanner.commitSync(
                                        Collections.singletonMap(
                                                new TableBucket(DATA1_TABLE_ID, 0), -1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be >= 0");
    }

    @Test
    void testCommitSyncRejectsBucketFromDifferentTable() {
        logScanner.setGroupId("group-1");

        assertThatThrownBy(
                        () ->
                                logScanner.commitSync(
                                        Collections.singletonMap(
                                                new TableBucket(DATA1_TABLE_ID + 1, 0), 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not belong to table");
    }

    @Test
    void testCommitSyncRejectsPartitionedBucketForNonPartitionedTable() {
        logScanner.setGroupId("group-1");

        assertThatThrownBy(
                        () ->
                                logScanner.commitSync(
                                        Collections.singletonMap(
                                                new TableBucket(DATA1_TABLE_ID, 1L, 0), 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not include partitionId");
    }

    @Test
    void testCommitSyncRejectsMissingPartitionIdForPartitionedTable() throws Exception {
        LogScannerImpl partitionedScanner =
                createLogScanner(PARTITION_TABLE_INFO, PARTITION_TABLE_PATH);
        try {
            partitionedScanner.setGroupId("group-1");

            assertThatThrownBy(
                            () ->
                                    partitionedScanner.commitSync(
                                            Collections.singletonMap(
                                                    new TableBucket(PARTITION_TABLE_ID, 0), 1L)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must include partitionId");
        } finally {
            partitionedScanner.close();
        }
    }

    private LogScannerImpl createLogScanner(TableInfo tableInfo, TablePath tablePath) {
        TestingMetadataUpdater updater =
                new TestingMetadataUpdater(Collections.singletonMap(tablePath, tableInfo));
        return new LogScannerImpl(
                configuration,
                tableInfo,
                updater,
                TestingClientMetricGroup.newInstance(),
                remoteFileDownloader,
                null,
                new TestingClientSchemaGetter(
                        tablePath,
                        new SchemaInfo(tableInfo.getSchema(), tableInfo.getSchemaId()),
                        updater,
                        configuration),
                null);
    }

    private void setOffsetCommitter(OffsetCommitter offsetCommitter) throws Exception {
        Field field = LogScannerImpl.class.getDeclaredField("offsetCommitter");
        field.setAccessible(true);
        field.set(logScanner, offsetCommitter);
    }

    private OffsetCommitter getOffsetCommitter() throws Exception {
        Field field = LogScannerImpl.class.getDeclaredField("offsetCommitter");
        field.setAccessible(true);
        return (OffsetCommitter) field.get(logScanner);
    }

    private static final class RecordingOffsetCommitter extends OffsetCommitter {
        private Map<TableBucket, Long> lastOffsets;
        private long timeoutMs;
        private boolean closed;
        private int commitCalls;

        private RecordingOffsetCommitter(String groupId, TestingMetadataUpdater metadataUpdater) {
            super(groupId, metadataUpdater, 0L);
        }

        @Override
        void commitOffsetsSync(Map<TableBucket, Long> offsets, long timeoutMs) {
            this.commitCalls++;
            this.lastOffsets = new HashMap<>(offsets);
            this.timeoutMs = timeoutMs;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
