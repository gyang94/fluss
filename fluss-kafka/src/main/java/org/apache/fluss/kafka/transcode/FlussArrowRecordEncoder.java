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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.OutOfMemoryException;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Encodes borrowed rows synchronously into independently owned Arrow log batches.
 *
 * <p>Each batch creates and closes its own allocator and writer pool. The allocator limit covers
 * Arrow vectors and compression buffers. Input data, Java objects and independently owned heap
 * output pages are outside this limit. Concurrent batches have independent allocators.
 */
@Internal
@ThreadSafe
public final class FlussArrowRecordEncoder {

    private static final int PAGE_SIZE = 4096;
    private static final long DEFAULT_ARROW_MEMORY_LIMIT = Integer.MAX_VALUE;

    private final long arrowMemoryLimit;

    /** Creates an encoder with an {@link Integer#MAX_VALUE} byte limit on Arrow allocations. */
    public FlussArrowRecordEncoder() {
        this(DEFAULT_ARROW_MEMORY_LIMIT);
    }

    /** Creates an encoder with a limit on each batch's Arrow allocations, in bytes. */
    public FlussArrowRecordEncoder(long arrowMemoryLimit) {
        checkArgument(arrowMemoryLimit > 0, "Arrow memory limit must be positive.");
        this.arrowMemoryLimit = arrowMemoryLimit;
    }

    /** Encodes all rows using the table's current schema ID and Arrow compression settings. */
    public BytesView encode(List<GenericRow> rows, TableInfo tableInfo) throws Exception {
        return encodeStreaming(
                consumer -> {
                    for (GenericRow row : rows) {
                        consumer.append(row);
                    }
                },
                tableInfo);
    }

    BytesView encodeStreaming(RowSource rowSource, TableInfo tableInfo) throws Exception {
        try (BufferAllocator allocator = BufferAllocatorUtil.createBufferAllocator(null);
                ArrowWriterPool provider = new ArrowWriterPool(allocator)) {
            allocator.setLimit(arrowMemoryLimit);
            ArrowWriter writer =
                    provider.getOrCreateWriter(
                            tableInfo.getTableId(),
                            tableInfo.getSchemaId(),
                            // Keep the whole conversion in one batch, as in the Put KV WAL path.
                            Integer.MAX_VALUE,
                            tableInfo.getRowType(),
                            tableInfo.getTableConfig().getArrowCompressionInfo());
            long epoch = writer.getEpoch();
            try {
                MemoryLogRecordsArrowBuilder builder =
                        MemoryLogRecordsArrowBuilder.builder(
                                tableInfo.getSchemaId(),
                                writer,
                                new UnmanagedPagedOutputView(PAGE_SIZE),
                                true,
                                null);
                rowSource.produce(row -> builder.append(ChangeType.APPEND_ONLY, row));
                // The result owns heap pages and remains valid after these resources close.
                return builder.build();
            } finally {
                writer.recycle(epoch);
            }
        } catch (OutOfMemoryException e) {
            throw new FlussRuntimeException(
                    "Kafka transcode Arrow memory limit exceeded (allocator limit "
                            + arrowMemoryLimit
                            + " bytes).",
                    e);
        }
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
