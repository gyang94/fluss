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

package org.apache.fluss.record;

import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultLogRecordBatch}. */
public class DefaultLogRecordBatchTest extends LogTestBase {

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testRecordBatchSize(byte magic) throws Exception {
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject(magic, TestData.DATA1);
        int totalSize = 0;
        for (LogRecordBatch logRecordBatch : memoryLogRecords.batches()) {
            totalSize += logRecordBatch.sizeInBytes();
        }
        assertThat(totalSize).isEqualTo(memoryLogRecords.sizeInBytes());
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testIndexedRowWriteAndReadBatch(byte magic) throws Exception {
        int recordNumber = 50;
        RowType allRowType = TestInternalRowGenerator.createAllRowType();
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100));

        List<IndexedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
            builder.append(ChangeType.INSERT, row);
            rows.add(row);
        }

        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();

        logRecordBatch.ensureValid();

        assertThat(logRecordBatch.getRecordCount()).isEqualTo(recordNumber);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(baseLogOffset);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(baseLogOffset + recordNumber - 1);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(baseLogOffset + recordNumber);
        assertThat(logRecordBatch.magic()).isEqualTo(magic);
        assertThat(logRecordBatch.isValid()).isTrue();
        assertThat(logRecordBatch.schemaId()).isEqualTo(schemaId);

        SchemaGetter schemaGetter =
                new TestingSchemaGetter(
                        new SchemaInfo(
                                Schema.newBuilder().fromRowType(allRowType).build(), schemaId));
        // verify record.
        int i = 0;
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                allRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            while (iter.hasNext()) {
                LogRecord record = iter.next();
                assertThat(record.logOffset()).isEqualTo(i);
                assertThat(record.getChangeType()).isEqualTo(ChangeType.INSERT);
                assertThat(record.getRow()).isEqualTo(rows.get(i));
                i++;
            }
        }

        builder.close();
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testNoRecordAppend(byte magic) throws Exception {
        // 1. no record append with baseOffset as 0.
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L, schemaId, Integer.MAX_VALUE, magic, new UnmanagedPagedOutputView(100));
        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(recordBatchHeaderSize(magic));

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(0);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(1);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(0);
        SchemaGetter schemaGetter =
                new TestingSchemaGetter(
                        new SchemaInfo(
                                Schema.newBuilder().fromRowType(baseRowType).build(), schemaId));
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                baseRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }

        // 2. no record append with baseOffset as 100.
        builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        100L,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100));
        memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        iterator = memoryLogRecords.batches().iterator();
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(recordBatchHeaderSize(magic));

        assertThat(iterator.hasNext()).isTrue();
        logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(100);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(101);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(100);

        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                baseRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }
    }

    @Test
    void testPartialIndexedBatchReadbackWithOutputProjection() throws Exception {
        TestingSchemaGetter schemaGetter = createData1Data2SchemaGetter();
        LogRecordBatch batch =
                createPartialIndexedBatch(
                        TestData.DATA2_ROW_TYPE, 2, new int[] {1}, new Object[][] {{"a"}, {"b"}});

        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                TestData.DATA1_ROW_TYPE, 1, schemaGetter);
                CloseableIterator<LogRecord> records = batch.records(readContext)) {
            assertThat(extractRows(TestData.DATA1_ROW_TYPE, records))
                    .containsExactly(new Object[] {null, "a"}, new Object[] {null, "b"});
        }
    }

    @Test
    void testPartialCompactedBatchReadbackWithOutputProjection() throws Exception {
        TestingSchemaGetter schemaGetter = createData1Data2SchemaGetter();
        LogRecordBatch batch =
                createPartialCompactedBatch(
                        TestData.DATA2_ROW_TYPE, 2, new int[] {1}, new Object[][] {{"a"}, {"b"}});

        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createCompactedRowReadContext(
                                TestData.DATA1_ROW_TYPE, 1, schemaGetter);
                CloseableIterator<LogRecord> records = batch.records(readContext)) {
            assertThat(extractRows(TestData.DATA1_ROW_TYPE, records))
                    .containsExactly(new Object[] {null, "a"}, new Object[] {null, "b"});
        }
    }

    @Test
    void testPartialArrowBatchReadbackWithoutPushdown() throws Exception {
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(TestData.DATA2_SCHEMA, 2));
        LogRecordBatch batch =
                createPartialArrowBatch(
                        TestData.DATA2_ROW_TYPE, 2, new int[] {1}, new Object[][] {{"a"}, {"b"}});

        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createArrowReadContext(
                                TestData.DATA2_ROW_TYPE, 2, schemaGetter);
                CloseableIterator<LogRecord> records = batch.records(readContext)) {
            assertThat(extractRows(TestData.DATA2_ROW_TYPE, records))
                    .containsExactly(
                            new Object[] {null, "a", null}, new Object[] {null, "b", null});
        }
    }

    private TestingSchemaGetter createData1Data2SchemaGetter() {
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(TestData.DATA1_SCHEMA, 1));
        schemaGetter.updateLatestSchemaInfo(new SchemaInfo(TestData.DATA2_SCHEMA, 2));
        return schemaGetter;
    }

    private LogRecordBatch createPartialIndexedBatch(
            RowType fullRowType, int schemaId, int[] targetColumns, Object[][] partialRows)
            throws Exception {
        RowType storedRowType = fullRowType.project(targetColumns);
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(256),
                        true,
                        targetColumns);
        for (Object[] partialRow : partialRows) {
            IndexedRow row = DataTestUtils.indexedRow(storedRowType, partialRow);
            builder.append(ChangeType.APPEND_ONLY, row);
        }
        builder.close();
        return MemoryLogRecords.pointToBytesView(builder.build()).batches().iterator().next();
    }

    private LogRecordBatch createPartialCompactedBatch(
            RowType fullRowType, int schemaId, int[] targetColumns, Object[][] partialRows)
            throws Exception {
        RowType storedRowType = fullRowType.project(targetColumns);
        MemoryLogRecordsCompactedBuilder builder =
                MemoryLogRecordsCompactedBuilder.builder(
                        schemaId,
                        Integer.MAX_VALUE,
                        new UnmanagedPagedOutputView(256),
                        true,
                        targetColumns);
        for (Object[] partialRow : partialRows) {
            builder.append(
                    ChangeType.APPEND_ONLY, DataTestUtils.compactedRow(storedRowType, partialRow));
        }
        builder.close();
        return MemoryLogRecords.pointToBytesView(builder.build()).batches().iterator().next();
    }

    private LogRecordBatch createPartialArrowBatch(
            RowType fullRowType, int schemaId, int[] targetColumns, Object[][] partialRows)
            throws Exception {
        RowType storedRowType = fullRowType.project(targetColumns);
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool writerPool = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
                            1L, schemaId, Integer.MAX_VALUE, storedRowType, DEFAULT_COMPRESSION);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            schemaId,
                            writer,
                            new ManagedPagedOutputView(new TestingMemorySegmentPool(4 * 1024)),
                            true,
                            targetColumns);
            for (Object[] partialRow : partialRows) {
                builder.append(
                        ChangeType.APPEND_ONLY, DataTestUtils.row(storedRowType, partialRow));
            }
            builder.close();
            return MemoryLogRecords.pointToBytesView(builder.build()).batches().iterator().next();
        }
    }

    private List<Object[]> extractRows(RowType rowType, CloseableIterator<LogRecord> records) {
        List<Object[]> rows = new ArrayList<>();
        while (records.hasNext()) {
            rows.add(extractRow(rowType, records.next().getRow()));
        }
        return rows;
    }

    private Object[] extractRow(RowType rowType, InternalRow row) {
        Object[] values = new Object[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (row.isNullAt(i)) {
                values[i] = null;
                continue;
            }
            if (rowType.getTypeAt(i).getTypeRoot() == DataTypeRoot.INTEGER) {
                values[i] = row.getInt(i);
            } else if (rowType.getTypeAt(i).getTypeRoot() == DataTypeRoot.STRING) {
                values[i] = row.getString(i).toString();
            } else {
                throw new IllegalArgumentException(
                        "Unsupported type for test extraction: " + rowType.getTypeAt(i));
            }
        }
        return values;
    }
}
