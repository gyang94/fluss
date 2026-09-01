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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Encodes assembled physical Fluss rows into one native Arrow log batch. */
@Internal
public final class FlussArrowRecordEncoder {

    private static final int INITIAL_PAGE_SIZE = 4096;
    private static final ArrowBatchBuilderFactory DEFAULT_BUILDER_FACTORY =
            (tableInfo, writer, outputView) -> {
                MemoryLogRecordsArrowBuilder builder =
                        MemoryLogRecordsArrowBuilder.builder(
                                tableInfo.getSchemaId(), writer, outputView, true, null);
                return new ArrowBatchBuilder() {
                    @Override
                    public boolean isFull() {
                        return builder.isFull();
                    }

                    @Override
                    public void append(GenericRow row) throws Exception {
                        builder.append(ChangeType.APPEND_ONLY, row);
                    }

                    @Override
                    public BytesView build() throws Exception {
                        return builder.build();
                    }

                    @Override
                    public void abort() {
                        builder.abort();
                    }
                };
            };

    @Nullable private final KafkaArrowWriterManager writerManager;
    private final ArrowBatchBuilderFactory builderFactory;

    /** Creates a compatibility encoder with request-scoped Arrow resources. */
    public FlussArrowRecordEncoder() {
        this.writerManager = null;
        this.builderFactory = DEFAULT_BUILDER_FACTORY;
    }

    /** Creates an encoder backed by the supplied shared, bounded writer manager. */
    public FlussArrowRecordEncoder(KafkaArrowWriterManager writerManager) {
        this.writerManager = writerManager;
        this.builderFactory = DEFAULT_BUILDER_FACTORY;
    }

    FlussArrowRecordEncoder(
            KafkaArrowWriterManager writerManager, ArrowBatchBuilderFactory builderFactory) {
        this.writerManager = writerManager;
        this.builderFactory = builderFactory;
    }

    /** Encodes all rows using the table's current schema ID and Arrow compression settings. */
    public BytesView encode(List<GenericRow> rows, TableInfo tableInfo) throws Exception {
        return encode(rows, tableInfo, KafkaOutputMemoryBudget.UNBOUNDED);
    }

    /** Encodes all rows while reserving every retained output page before physical allocation. */
    public BytesView encode(
            List<GenericRow> rows, TableInfo tableInfo, KafkaOutputMemoryBudget outputMemoryBudget)
            throws Exception {
        checkNotNull(rows);
        return encodeStreaming(
                consumer -> {
                    for (GenericRow row : rows) {
                        consumer.append(row);
                    }
                },
                tableInfo,
                outputMemoryBudget);
    }

    BytesView encodeStreaming(
            RowSource rowSource, TableInfo tableInfo, KafkaOutputMemoryBudget outputMemoryBudget)
            throws Exception {
        checkNotNull(rowSource);
        if (writerManager != null) {
            return encodeWithSharedWriter(rowSource, tableInfo, outputMemoryBudget);
        }
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool provider = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    provider.getOrCreateWriter(
                            tableInfo.getTableId(),
                            tableInfo.getSchemaId(),
                            Integer.MAX_VALUE,
                            tableInfo.getRowType(),
                            tableInfo.getTableConfig().getArrowCompressionInfo());
            return encodeRows(
                    rowSource, tableInfo, writer, Integer.MAX_VALUE, null, outputMemoryBudget);
        }
    }

    private BytesView encodeWithSharedWriter(
            RowSource rowSource, TableInfo tableInfo, KafkaOutputMemoryBudget outputMemoryBudget)
            throws Exception {
        try (KafkaArrowWriterManager.WriterLease lease = writerManager.acquire(tableInfo)) {
            return encodeRows(
                    rowSource,
                    tableInfo,
                    lease.writer(),
                    writerManager.maxBatchSizeBytes(),
                    writerManager,
                    outputMemoryBudget);
        }
    }

    private BytesView encodeRows(
            RowSource rowSource,
            TableInfo tableInfo,
            ArrowWriter writer,
            int maxBatchSizeBytes,
            KafkaArrowWriterManager writerManager,
            KafkaOutputMemoryBudget outputMemoryBudget)
            throws Exception {
        BudgetedUnmanagedPagedOutputView outputView =
                new BudgetedUnmanagedPagedOutputView(
                        INITIAL_PAGE_SIZE, checkNotNull(outputMemoryBudget));
        ArrowBatchBuilder builder = null;
        try {
            builder = builderFactory.create(tableInfo, writer, outputView);
            ArrowBatchBuilder activeBuilder = builder;
            rowSource.produce(
                    row -> {
                        outputMemoryBudget.checkpoint();
                        if (activeBuilder.isFull()) {
                            throw batchTooLarge(maxBatchSizeBytes);
                        }
                        activeBuilder.append(row);
                    });
            outputMemoryBudget.checkpoint();
            BytesView output = builder.build();
            if (output.getBytesLength() > maxBatchSizeBytes) {
                throw batchTooLarge(maxBatchSizeBytes);
            }
            return output;
        } catch (Throwable failure) {
            if (writerManager != null) {
                writerManager.recordEncodeAbort();
            }
            if (builder != null) {
                try {
                    builder.abort();
                } catch (Throwable abortFailure) {
                    failure = ExceptionUtils.firstOrSuppressed(abortFailure, failure);
                }
            }
            try {
                outputView.releaseRetainedCapacity();
            } catch (Throwable releaseFailure) {
                failure = ExceptionUtils.firstOrSuppressed(releaseFailure, failure);
            }
            ExceptionUtils.rethrow(failure);
            throw new AssertionError("Unreachable");
        }
    }

    private static RecordTooLargeException batchTooLarge(int maxBatchSizeBytes) {
        return new RecordTooLargeException(
                "The converted Kafka Produce Arrow batch exceeds the configured maximum of "
                        + maxBatchSizeBytes
                        + " bytes.");
    }

    interface ArrowBatchBuilderFactory {
        ArrowBatchBuilder create(
                TableInfo tableInfo,
                ArrowWriter writer,
                BudgetedUnmanagedPagedOutputView outputView);
    }

    interface ArrowBatchBuilder {
        boolean isFull();

        void append(GenericRow row) throws Exception;

        BytesView build() throws Exception;

        void abort();
    }

    @FunctionalInterface
    interface RowSource {
        /** Produces rows synchronously and never retains the supplied consumer. */
        void produce(RowConsumer consumer) throws Exception;
    }

    @FunctionalInterface
    interface RowConsumer {
        /** Serializes one row before returning and never retains the row. */
        void append(GenericRow row) throws Exception;
    }
}
