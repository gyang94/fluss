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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.compression.ArrowCompressionType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.OutOfMemoryException;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Verifies Arrow allocation limits, resource cleanup and independently owned output. */
class FlussArrowRecordEncoderTest {
    private static final int MIB = 1024 * 1024;

    @ParameterizedTest
    @EnumSource(
            value = ArrowCompressionType.class,
            names = {"NONE", "ZSTD", "LZ4_FRAME"})
    void testDefaultEncoderAcceptsRecordsLargerThanSixteenMiB(ArrowCompressionType compression)
            throws Exception {
        byte[] value = new byte[17 * MIB];
        new Random(42).nextBytes(value);
        TableInfo table = table(1, compression);
        byte[] lastValue = {7};
        BytesView output;
        try (FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder()) {
            output =
                    encoder.encode(
                            Arrays.asList(GenericRow.of(value), GenericRow.of(lastValue)), table);
        }
        assertThat(output.getBytesLength()).isGreaterThan(16 * MIB);
        assertValue(output, table, value, lastValue);
    }

    @ParameterizedTest
    @EnumSource(
            value = ArrowCompressionType.class,
            names = {"NONE", "ZSTD", "LZ4_FRAME"})
    void testReusesAllocatorAndWriterAndOutputOutlivesEncoder(ArrowCompressionType compression)
            throws Exception {
        TableInfo table = table(1, compression);
        byte[] value = {1};
        BytesView first;
        BytesView second;
        BufferAllocator allocator;
        ArrowWriterPool pool;
        try (FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder()) {
            allocator = encoder.allocator();
            pool = encoder.writerPool();
            assertThat(allocator.getLimit()).isEqualTo(Long.MAX_VALUE);
            first = encode(encoder, table, value);
            ArrowWriter writer = idleWriter(encoder);
            long epoch = writer.getEpoch();
            assertThat(allocator.getAllocatedMemory()).isZero();
            value[0] = 2;
            second = encode(encoder, table, value);
            value[0] = 3;
            assertThat(encoder.allocator()).isSameAs(allocator);
            assertThat(encoder.writerPool()).isSameAs(pool);
            assertThat(idleWriter(encoder)).isSameAs(writer);
            // build() and the finally block must recycle this batch only once.
            assertThat(writer.getEpoch()).isEqualTo(epoch + 1);
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
        assertThat(pool.freeWriters()).isEmpty();
        assertThatThrownBy(allocator::assertOpen).isInstanceOf(IllegalStateException.class);
        assertValue(first, table, new byte[] {1});
        assertValue(second, table, new byte[] {2});
    }

    @ParameterizedTest
    @EnumSource(
            value = ArrowCompressionType.class,
            names = {"NONE", "ZSTD", "LZ4_FRAME"})
    void testOversizedRecordRecyclesWriterAndNextBatchSucceeds(ArrowCompressionType compression)
            throws Exception {
        try (FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder(128 * 1024)) {
            TableInfo table = table(1, compression);
            Throwable failure = catchThrowable(() -> encode(encoder, table, new byte[MIB]));
            assertAllocationFailure(failure);
            ArrowWriter writer = idleWriter(encoder);
            assertThat(encoder.allocator().getAllocatedMemory()).isZero();
            assertValue(encode(encoder, table, new byte[] {7}), table, new byte[] {7});
            assertThat(idleWriter(encoder)).isSameAs(writer);
        }
    }

    @Test
    void testOutputPagesDoNotConsumeArrowAllocationLimit() throws Exception {
        try (FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder(128 * 1024)) {
            TableInfo table = table(1, ArrowCompressionType.NONE);
            byte[] value = new byte[64 * 1024];
            assertValue(encode(encoder, table, value), table, value);
            assertValue(encode(encoder, table, new byte[] {7}), table, new byte[] {7});
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0})
    void testRejectsInvalidLimit(long limit) {
        assertThatThrownBy(() -> new FlussArrowRecordEncoder(limit))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInFlightBatchesShareAllocatorLimit() throws Exception {
        try (FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder(256 * 1024)) {
            TableInfo table = table(1, ArrowCompressionType.NONE);
            byte[] value = new byte[128 * 1024];
            // This row fits by itself, but two active writers must share the same limit.
            assertValue(encode(encoder, table, value), table, value);
            BytesView output =
                    encoder.encodeStreaming(
                            consumer -> {
                                consumer.append(GenericRow.of(value));
                                long inUse = encoder.allocator().getAllocatedMemory();
                                assertAllocationFailure(
                                        catchThrowable(() -> encode(encoder, table, value)));
                                assertThat(encoder.allocator().getAllocatedMemory())
                                        .isEqualTo(inUse);
                            },
                            table);
            assertValue(output, table, value);
            assertThat(encoder.allocator().getAllocatedMemory()).isZero();
            assertValue(encode(encoder, table, value), table, value);
        }
    }

    @Test
    void testWritersAreSeparatedByTableSchemaAndCompression() throws Exception {
        try (FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder()) {
            TableInfo original = table(1, ArrowCompressionType.NONE);
            encode(encoder, original, new byte[] {1});
            ArrowWriter originalWriter = idleWriter(encoder);
            for (TableInfo changed :
                    Arrays.asList(
                            table(2, ArrowCompressionType.NONE),
                            table(1, 2, ArrowCompressionType.NONE),
                            table(1, ArrowCompressionType.LZ4_FRAME))) {
                assertValue(encode(encoder, changed, new byte[] {2}), changed, new byte[] {2});
            }
            assertThat(encoder.writerPool().freeWriters()).hasSize(4);
            assertThat(encoder.writerPool().freeWriters().values())
                    .allSatisfy(writers -> assertThat(writers).hasSize(1));
            long epoch = originalWriter.getEpoch();
            assertValue(encode(encoder, original, new byte[] {3}), original, new byte[] {3});
            assertThat(originalWriter.getEpoch()).isEqualTo(epoch + 1);
            assertThat(encoder.allocator().getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testConcurrentBatchesUseSeparateWritersAndCloseWaitsForThem() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch finish = new CountDownLatch(1);
        FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder();
        TableInfo table = table(1, ArrowCompressionType.NONE);
        try {
            Future<BytesView> first =
                    executor.submit(() -> encodeBlocked(encoder, table, (byte) 1, entered, finish));
            Future<BytesView> second =
                    executor.submit(() -> encodeBlocked(encoder, table, (byte) 2, entered, finish));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(encoder.allocator().getAllocatedMemory()).isPositive();
            CountDownLatch closing = new CountDownLatch(1);
            Future<?> close =
                    executor.submit(
                            () -> {
                                closing.countDown();
                                encoder.close();
                                return null;
                            });
            assertThat(closing.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> close.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            finish.countDown();
            BytesView firstOutput = first.get(10, TimeUnit.SECONDS);
            BytesView secondOutput = second.get(10, TimeUnit.SECONDS);
            close.get(10, TimeUnit.SECONDS);
            encoder.close();
            assertThat(encoder.allocator().getAllocatedMemory()).isZero();
            assertThat(encoder.writerPool().freeWriters()).isEmpty();
            assertThatThrownBy(encoder.allocator()::assertOpen)
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> encode(encoder, table, new byte[] {3}))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
            assertValue(firstOutput, table, new byte[] {1}, new byte[] {1});
            assertValue(secondOutput, table, new byte[] {2}, new byte[] {2});
        } finally {
            finish.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            encoder.close();
        }
    }

    private static BytesView encodeBlocked(
            FlussArrowRecordEncoder encoder,
            TableInfo table,
            byte value,
            CountDownLatch entered,
            CountDownLatch finish)
            throws Exception {
        return encoder.encodeStreaming(
                consumer -> {
                    consumer.append(GenericRow.of(new byte[] {value}));
                    entered.countDown();
                    assertThat(finish.await(10, TimeUnit.SECONDS)).isTrue();
                    consumer.append(GenericRow.of(new byte[] {value}));
                },
                table);
    }

    private static ArrowWriter idleWriter(FlussArrowRecordEncoder encoder) {
        assertThat(encoder.writerPool().freeWriters()).hasSize(1);
        return encoder.writerPool().freeWriters().values().iterator().next().getFirst();
    }

    private static void assertAllocationFailure(Throwable failure) {
        assertThat(failure)
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("memory limit exceeded")
                .hasCauseInstanceOf(OutOfMemoryException.class);
        // The original write failure must not be masked by recycling failures.
        assertThat(failure.getSuppressed()).isEmpty();
        assertThat(failure.getCause().getSuppressed()).isEmpty();
    }

    private static BytesView encode(FlussArrowRecordEncoder encoder, TableInfo table, byte[] value)
            throws Exception {
        return encoder.encode(Collections.singletonList(GenericRow.of(value)), table);
    }

    private static TableInfo table(long id, ArrowCompressionType compression) {
        return table(id, 1, compression);
    }

    private static TableInfo table(long id, int schemaId, ArrowCompressionType compression) {
        return TableInfo.of(
                TablePath.of("kafka", "topic"),
                id,
                schemaId,
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.BYTES())
                                        .build())
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .property(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_TYPE, compression)
                        .build(),
                null,
                1L,
                1L);
    }

    private static void assertValue(BytesView output, TableInfo table, byte[]... expected)
            throws Exception {
        ByteBuf buffer = output.getByteBuf();
        try {
            LogRecordBatch batch =
                    MemoryLogRecords.pointToByteBuffer(buffer.nioBuffer())
                            .batches()
                            .iterator()
                            .next();
            batch.ensureValid();
            assertThat(batch.getRecordCount()).isEqualTo(expected.length);
            try (LogRecordReadContext context =
                            LogRecordReadContext.createArrowReadContext(
                                    table.getRowType(),
                                    table.getSchemaId(),
                                    new TestingSchemaGetter(
                                            new SchemaInfo(
                                                    table.getSchema(), table.getSchemaId())));
                    CloseableIterator<LogRecord> records = batch.records(context)) {
                for (byte[] value : expected) {
                    assertThat(records.next().getRow().getBytes(0)).isEqualTo(value);
                }
                assertThat(records.hasNext()).isFalse();
            }
        } finally {
            buffer.release();
        }
    }
}
