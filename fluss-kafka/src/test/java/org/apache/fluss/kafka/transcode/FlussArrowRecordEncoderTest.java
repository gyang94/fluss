/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.bytesview.ByteBufBytesView;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.OutOfMemoryException;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlussArrowRecordEncoderTest {

    @Test
    void testWriterReuseDoesNotMutatePreviouslyBuiltOutputPages() throws Exception {
        KafkaArrowWriterManager manager = manager(1 << 20);
        FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder(manager);
        try {
            BytesView first =
                    encoder.encode(
                            Collections.singletonList(row(new byte[] {1, 2, 3})), tableInfo());
            byte[] firstSnapshot = bytes(first);

            BytesView second =
                    encoder.encode(
                            Collections.singletonList(row(new byte[] {9, 8, 7})), tableInfo());

            assertThat(bytes(first)).isEqualTo(firstSnapshot);
            assertThat(bytes(second)).isNotEqualTo(firstSnapshot);
            assertThat(manager.activeWriterCount()).isZero();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    @Test
    void testAppendFailureAbortsBuilderAndReturnsWriterPermit() {
        RuntimeException expected = new RuntimeException("append failure");
        AtomicInteger aborts = new AtomicInteger();
        assertFailureAbortsAndReleases(
                new TestingBuilder(aborts) {
                    @Override
                    public void append(GenericRow row) {
                        throw expected;
                    }
                },
                expected,
                aborts);
    }

    @Test
    void testBuildFailureAbortsBuilderAndReturnsWriterPermit() {
        RuntimeException expected = new RuntimeException("build failure");
        AtomicInteger aborts = new AtomicInteger();
        assertFailureAbortsAndReleases(
                new TestingBuilder(aborts) {
                    @Override
                    public BytesView build() {
                        throw expected;
                    }
                },
                expected,
                aborts);
    }

    @Test
    void testEncodedBatchLargerThanRequestLimitIsRejectedAndResourcesReturn() {
        KafkaArrowWriterManager manager = manager(64);
        FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder(manager);
        try {
            assertThatThrownBy(
                            () ->
                                    encoder.encode(
                                            Collections.singletonList(row(new byte[1024])),
                                            tableInfo()))
                    .isInstanceOf(RecordTooLargeException.class)
                    .hasMessageContaining("64 bytes");
            assertThat(manager.activeWriterCount()).isZero();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    @Test
    void testSuccessfulOutputRetainsWholePageCapacityInBudget() throws Exception {
        KafkaArrowWriterManager manager = manager(1 << 20);
        FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder(manager);
        AtomicLong retainedBytes = new AtomicLong();
        KafkaOutputMemoryBudget budget = trackingBudget(retainedBytes);
        try {
            BytesView output =
                    encoder.encode(
                            Collections.singletonList(row(new byte[] {1, 2, 3})),
                            tableInfo(),
                            budget);

            assertThat(output.getBytesLength()).isPositive();
            assertThat(retainedBytes).hasValueGreaterThanOrEqualTo(output.getBytesLength());
            assertThat(
                            retainedBytes.get()
                                    % BudgetedUnmanagedPagedOutputView.accountedPageBytes(4096))
                    .isZero();
        } finally {
            manager.close();
        }
    }

    @Test
    void testRejectedOutputReleasesEveryRetainedPageFromBudget() {
        KafkaArrowWriterManager manager = manager(64);
        FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder(manager);
        AtomicLong retainedBytes = new AtomicLong();
        try {
            assertThatThrownBy(
                            () ->
                                    encoder.encode(
                                            Collections.singletonList(row(new byte[1024])),
                                            tableInfo(),
                                            trackingBudget(retainedBytes)))
                    .isInstanceOf(RecordTooLargeException.class);
            assertThat(retainedBytes).hasValue(0);
        } finally {
            manager.close();
        }
    }

    @Test
    void testStreamingSourceAppendsEachRowBeforeProducingTheNext() throws Exception {
        int rowCount = 1_024;
        AtomicInteger producedRows = new AtomicInteger();
        AtomicInteger appendedRows = new AtomicInteger();
        KafkaArrowWriterManager manager = manager(1 << 20);
        FlussArrowRecordEncoder encoder =
                new FlussArrowRecordEncoder(
                        manager,
                        (tableInfo, writer, outputView) ->
                                new TestingBuilder(new AtomicInteger()) {
                                    @Override
                                    public void append(GenericRow row) {
                                        assertThat(producedRows).hasValue(appendedRows.get() + 1);
                                        appendedRows.incrementAndGet();
                                    }

                                    @Override
                                    public BytesView build() {
                                        return new ByteBufBytesView(new byte[0]);
                                    }
                                });
        try {
            encoder.encodeStreaming(
                    consumer -> {
                        for (int index = 0; index < rowCount; index++) {
                            assertThat(appendedRows).hasValue(index);
                            producedRows.incrementAndGet();
                            consumer.append(row(new byte[16 * 1_024]));
                        }
                    },
                    tableInfo(),
                    KafkaOutputMemoryBudget.UNBOUNDED);

            assertThat(producedRows).hasValue(rowCount);
            assertThat(appendedRows).hasValue(rowCount);
            assertThat(manager.activeWriterCount()).isZero();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    @Test
    void testStreamingFailureAbortsAndReleasesWriterAndOutputBudget() {
        RuntimeException expected = new RuntimeException("stream append failure");
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger appends = new AtomicInteger();
        AtomicLong retainedBytes = new AtomicLong();
        KafkaArrowWriterManager manager = manager(1 << 20);
        FlussArrowRecordEncoder encoder =
                new FlussArrowRecordEncoder(
                        manager,
                        (tableInfo, writer, outputView) ->
                                new TestingBuilder(aborts) {
                                    @Override
                                    public void append(GenericRow row) {
                                        appends.incrementAndGet();
                                    }
                                });
        try {
            assertThatThrownBy(
                            () ->
                                    encoder.encodeStreaming(
                                            consumer -> {
                                                for (int index = 0; index < 10; index++) {
                                                    if (index == 3) {
                                                        throw expected;
                                                    }
                                                    consumer.append(row(new byte[16 * 1_024]));
                                                }
                                            },
                                            tableInfo(),
                                            trackingBudget(retainedBytes)))
                    .isSameAs(expected);
            assertThat(appends).hasValue(3);
            assertThat(aborts).hasValue(1);
            assertThat(retainedBytes).hasValue(0);
            assertThat(manager.activeWriterCount()).isZero();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    @Test
    void testArrowMemoryExhaustionAbortsAndReleasesWriterAndOutputBudget() {
        OutOfMemoryException expected = new OutOfMemoryException("allocator exhausted");
        AtomicInteger aborts = new AtomicInteger();
        AtomicLong retainedBytes = new AtomicLong();
        KafkaArrowWriterManager manager = manager(1 << 20);
        FlussArrowRecordEncoder encoder =
                new FlussArrowRecordEncoder(
                        manager,
                        (tableInfo, writer, outputView) ->
                                new TestingBuilder(aborts) {
                                    @Override
                                    public void append(GenericRow row) {
                                        throw expected;
                                    }
                                });
        try {
            assertThatThrownBy(
                            () ->
                                    encoder.encodeStreaming(
                                            consumer -> consumer.append(row(new byte[1])),
                                            tableInfo(),
                                            trackingBudget(retainedBytes)))
                    .isSameAs(expected);
            assertThat(aborts).hasValue(1);
            assertThat(retainedBytes).hasValue(0);
            assertThat(manager.activeWriterCount()).isZero();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    @Test
    void testCancellationBeforeAppendAbortsAndReleasesWriterAndOutputBudget() {
        CancellationException expected = new CancellationException("cancelled");
        AtomicInteger checkpoints = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger appends = new AtomicInteger();
        AtomicLong retainedBytes = new AtomicLong();
        KafkaOutputMemoryBudget budget =
                new KafkaOutputMemoryBudget() {
                    @Override
                    public void reserve(long bytes) {
                        retainedBytes.addAndGet(bytes);
                    }

                    @Override
                    public void release(long bytes) {
                        retainedBytes.addAndGet(-bytes);
                    }

                    @Override
                    public void checkpoint() {
                        if (checkpoints.incrementAndGet() == 2) {
                            throw expected;
                        }
                    }
                };
        KafkaArrowWriterManager manager = manager(1 << 20);
        FlussArrowRecordEncoder encoder =
                new FlussArrowRecordEncoder(
                        manager,
                        (tableInfo, writer, outputView) ->
                                new TestingBuilder(aborts) {
                                    @Override
                                    public void append(GenericRow row) {
                                        appends.incrementAndGet();
                                    }
                                });
        try {
            assertThatThrownBy(
                            () ->
                                    encoder.encodeStreaming(
                                            consumer -> consumer.append(row(new byte[1])),
                                            tableInfo(),
                                            budget))
                    .isSameAs(expected);
            assertThat(checkpoints).hasValue(2);
            assertThat(appends).hasValue(0);
            assertThat(aborts).hasValue(1);
            assertThat(retainedBytes).hasValue(0);
            assertThat(manager.activeWriterCount()).isZero();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    private static void assertFailureAbortsAndReleases(
            FlussArrowRecordEncoder.ArrowBatchBuilder builder,
            RuntimeException expected,
            AtomicInteger aborts) {
        KafkaArrowWriterManager manager = manager(1 << 20);
        FlussArrowRecordEncoder encoder =
                new FlussArrowRecordEncoder(manager, (tableInfo, writer, outputView) -> builder);
        try {
            assertThatThrownBy(
                            () ->
                                    encoder.encode(
                                            Collections.singletonList(row(new byte[] {1})),
                                            tableInfo()))
                    .isSameAs(expected);
            assertThat(aborts).hasValue(1);
            assertThat(manager.activeWriterCount()).isZero();
            try (KafkaArrowWriterManager.WriterLease ignored = manager.acquire(tableInfo())) {
                assertThat(manager.activeWriterCount()).isOne();
            }
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    private static KafkaArrowWriterManager manager(int maxBatchSizeBytes) {
        return new KafkaArrowWriterManager(
                16L << 20,
                1,
                4,
                Duration.ofSeconds(1),
                maxBatchSizeBytes,
                KafkaProduceMetrics.noOp());
    }

    private static TableInfo tableInfo() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("payload", DataTypes.BYTES()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty(
                                KafkaDataFormat.VALUE_FORMAT_CONFIG, KafkaDataFormat.RAW.value())
                        .build();
        return TableInfo.of(TablePath.of("kafka", "topic"), 1L, 1, descriptor, null, 1L, 1L);
    }

    private static GenericRow row(byte[] value) {
        GenericRow row = new GenericRow(1);
        row.setField(0, value);
        return row;
    }

    private static KafkaOutputMemoryBudget trackingBudget(AtomicLong retainedBytes) {
        return new KafkaOutputMemoryBudget() {
            @Override
            public void reserve(long bytes) {
                retainedBytes.addAndGet(bytes);
            }

            @Override
            public void release(long bytes) {
                retainedBytes.addAndGet(-bytes);
            }
        };
    }

    private static byte[] bytes(BytesView bytesView) {
        byte[] bytes = new byte[bytesView.getBytesLength()];
        bytesView.getByteBuf().getBytes(bytesView.getByteBuf().readerIndex(), bytes);
        return bytes;
    }

    private static class TestingBuilder implements FlussArrowRecordEncoder.ArrowBatchBuilder {
        private final AtomicInteger aborts;

        private TestingBuilder(AtomicInteger aborts) {
            this.aborts = aborts;
        }

        @Override
        public boolean isFull() {
            return false;
        }

        @Override
        public void append(GenericRow row) {}

        @Override
        public BytesView build() {
            throw new AssertionError("The test must override build().");
        }

        @Override
        public void abort() {
            aborts.incrementAndGet();
        }
    }
}
