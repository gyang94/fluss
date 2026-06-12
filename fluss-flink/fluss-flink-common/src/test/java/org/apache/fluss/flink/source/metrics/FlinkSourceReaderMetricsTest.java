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

package org.apache.fluss.flink.source.metrics;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.source.reader.FlinkRecordsWithSplitIds;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics.BUCKET_GROUP;
import static org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics.FLUSS_METRIC_GROUP;
import static org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics.PARTITION_GROUP;
import static org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics.READER_METRIC_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics}. */
class FlinkSourceReaderMetricsTest {

    @Test
    void testCurrentOffsetTracking() {
        MetricListener metricListener = new MetricListener();

        final TableBucket t0 = new TableBucket(0, 0L, 1);
        final TableBucket t1 = new TableBucket(0, 0L, 2);
        final TableBucket t2 = new TableBucket(0, null, 1);
        final TableBucket t3 = new TableBucket(0, null, 2);

        final FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        flinkSourceReaderMetrics.registerTableBucket(t0);
        flinkSourceReaderMetrics.registerTableBucket(t1);
        flinkSourceReaderMetrics.registerTableBucket(t2);
        flinkSourceReaderMetrics.registerTableBucket(t3);

        flinkSourceReaderMetrics.recordCurrentOffset(t0, 15213L);
        flinkSourceReaderMetrics.recordCurrentOffset(t1, 18213L);
        flinkSourceReaderMetrics.recordCurrentOffset(t2, 18613L);
        flinkSourceReaderMetrics.recordCurrentOffset(t3, 15513L);

        assertCurrentOffset(t0, 15213L, metricListener);
        assertCurrentOffset(t1, 18213L, metricListener);
        assertCurrentOffset(t2, 18613L, metricListener);
        assertCurrentOffset(t3, 15513L, metricListener);
    }

    @Test
    void testNumBytesInCounter() {
        MetricListener metricListener = new MetricListener();
        InternalSourceReaderMetricGroup sourceReaderMetricGroup =
                InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup());

        FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(sourceReaderMetricGroup);

        Counter numBytesInCounter = flinkSourceReaderMetrics.getNumBytesInCounter();
        assertThat(numBytesInCounter.getCount()).isEqualTo(0);

        flinkSourceReaderMetrics.recordBytesIn(1024);
        assertThat(numBytesInCounter.getCount()).isEqualTo(1024);

        flinkSourceReaderMetrics.recordBytesIn(2048);
        assertThat(numBytesInCounter.getCount()).isEqualTo(3072);
    }

    @Test
    void testNumBytesInViaFlinkRecordsWithSplitIds() {
        MetricListener metricListener = new MetricListener();
        InternalSourceReaderMetricGroup sourceReaderMetricGroup =
                InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup());

        FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(sourceReaderMetricGroup);

        TableBucket tb = new TableBucket(0, 0);
        flinkSourceReaderMetrics.registerTableBucket(tb);

        ScanRecord r1 = new ScanRecord(0L, 1000L, ChangeType.APPEND_ONLY, GenericRow.of(1), 100);
        ScanRecord r2 = new ScanRecord(1L, 1001L, ChangeType.APPEND_ONLY, GenericRow.of(2), 200);
        ScanRecord unknownSize =
                new ScanRecord(2L, 1002L, ChangeType.APPEND_ONLY, GenericRow.of(3));

        CloseableIterator<RecordAndPos> records =
                CloseableIterator.wrap(
                        Arrays.asList(
                                        new RecordAndPos(r1),
                                        new RecordAndPos(r2),
                                        new RecordAndPos(unknownSize))
                                .iterator());

        FlinkRecordsWithSplitIds splitRecords =
                new FlinkRecordsWithSplitIds("split-0", tb, records, flinkSourceReaderMetrics);

        assertThat(splitRecords.nextSplit()).isEqualTo("split-0");
        splitRecords.nextRecordFromSplit();
        splitRecords.nextRecordFromSplit();
        splitRecords.nextRecordFromSplit();

        Counter numBytesInCounter = flinkSourceReaderMetrics.getNumBytesInCounter();
        assertThat(numBytesInCounter.getCount()).isEqualTo(300);
    }

    // ----------- Assertions --------------

    private void assertCurrentOffset(
            TableBucket tb, long expectedOffset, MetricListener metricListener) {
        final Optional<Gauge<Long>> currentOffsetGauge;
        if (tb.getPartitionId() == null) {
            currentOffsetGauge =
                    metricListener.getGauge(
                            FLUSS_METRIC_GROUP,
                            READER_METRIC_GROUP,
                            BUCKET_GROUP,
                            String.valueOf(tb.getBucket()),
                            FlinkSourceReaderMetrics.CURRENT_OFFSET_METRIC_GAUGE);
        } else {
            currentOffsetGauge =
                    metricListener.getGauge(
                            FLUSS_METRIC_GROUP,
                            READER_METRIC_GROUP,
                            PARTITION_GROUP,
                            String.valueOf(tb.getPartitionId()),
                            BUCKET_GROUP,
                            String.valueOf(tb.getBucket()),
                            FlinkSourceReaderMetrics.CURRENT_OFFSET_METRIC_GAUGE);
        }

        assertThat(currentOffsetGauge).isPresent();
        assertThat((long) currentOffsetGauge.get().getValue()).isEqualTo(expectedOffset);
    }
}
