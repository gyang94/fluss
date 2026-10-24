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
        BytesView output =
                new FlussArrowRecordEncoder()
                        .encode(
                                Arrays.asList(GenericRow.of(value), GenericRow.of(lastValue)),
                                table);
        assertThat(output.getBytesLength()).isGreaterThan(16 * MIB);
        assertValue(output, table, value, lastValue);
    }

    @ParameterizedTest
    @EnumSource(
            value = ArrowCompressionType.class,
            names = {"NONE", "ZSTD", "LZ4_FRAME"})
    void testOutputOutlivesBatchResourcesAndInput(ArrowCompressionType compression)
            throws Exception {
        FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder();
        TableInfo table = table(1, compression);
        byte[] value = {1};
        // Allocator and writer pool are already closed when each encode call returns.
        BytesView first = encode(encoder, table, value);
        value[0] = 2;
        BytesView second = encode(encoder, table, value);
        value[0] = 3;
        assertValue(first, table, new byte[] {1});
        assertValue(second, table, new byte[] {2});
    }

    @ParameterizedTest
    @EnumSource(
            value = ArrowCompressionType.class,
            names = {"NONE", "ZSTD", "LZ4_FRAME"})
    void testOversizedRecordClosesAllocatorAndNextBatchSucceeds(ArrowCompressionType compression)
            throws Exception {
        FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder(128 * 1024);
        TableInfo table = table(1, compression);
        Throwable failure = catchThrowable(() -> encode(encoder, table, new byte[MIB]));
        assertAllocationFailure(failure);
        assertValue(encode(encoder, table, new byte[] {7}), table, new byte[] {7});
    }

    @Test
    void testOutputPagesDoNotConsumeArrowAllocationLimit() throws Exception {
        FlussArrowRecordEncoder encoder = new FlussArrowRecordEncoder(128 * 1024);
        TableInfo table = table(1, ArrowCompressionType.NONE);
        byte[] value = new byte[64 * 1024];
        assertValue(encode(encoder, table, value), table, value);
        assertValue(encode(encoder, table, new byte[] {7}), table, new byte[] {7});
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0})
    void testRejectsInvalidLimit(long limit) {
        assertThatThrownBy(() -> new FlussArrowRecordEncoder(limit))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertAllocationFailure(Throwable failure) {
        assertThat(failure)
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("memory limit exceeded")
                .hasCauseInstanceOf(OutOfMemoryException.class);
        // try-with-resources must close cleanly even if creation or writing fails partway through.
        assertThat(failure.getSuppressed()).isEmpty();
        assertThat(failure.getCause().getSuppressed()).isEmpty();
    }

    private static BytesView encode(FlussArrowRecordEncoder encoder, TableInfo table, byte[] value)
            throws Exception {
        return encoder.encode(Collections.singletonList(GenericRow.of(value)), table);
    }

    private static TableInfo table(long id, ArrowCompressionType compression) {
        return TableInfo.of(
                TablePath.of("kafka", "topic"),
                id,
                1,
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
