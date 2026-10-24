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
import org.apache.fluss.annotation.VisibleForTesting;
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
import org.apache.fluss.utils.IOUtils;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Encodes borrowed rows synchronously into independently owned Arrow log batches.
 *
 * <p>The encoder owns one allocator and an {@link ArrowWriterPool}, as in the Put KV path. Each
 * batch borrows a writer and recycles it on completion. Concurrent batches use distinct writers and
 * share the allocator limit for Arrow vectors and compression buffers. Input data, Java objects and
 * independently owned heap output pages are outside this limit. Recycling releases vector buffers
 * while retaining the writer's schema and field writers.
 *
 * <p>The owner must close the encoder when it is no longer needed. Closing waits for in-flight
 * batches before releasing the pool and allocator.
 */
@Internal
@ThreadSafe
public final class FlussArrowRecordEncoder implements AutoCloseable {

    private static final int PAGE_SIZE = 4096;
    private static final long DEFAULT_ARROW_MEMORY_LIMIT = Long.MAX_VALUE;

    private final long arrowMemoryLimit;
    private final BufferAllocator allocator;
    private final ArrowWriterPool writerPool;
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

    @GuardedBy("lifecycleLock")
    private boolean closed;

    /** Creates an encoder with a {@link Long#MAX_VALUE} byte limit, as in the Put KV path. */
    public FlussArrowRecordEncoder() {
        this(DEFAULT_ARROW_MEMORY_LIMIT);
    }

    /** Creates an encoder with a shared limit on all its Arrow allocations, in bytes. */
    public FlussArrowRecordEncoder(long arrowMemoryLimit) {
        checkArgument(arrowMemoryLimit > 0, "Arrow memory limit must be positive.");
        this.arrowMemoryLimit = arrowMemoryLimit;
        this.allocator = BufferAllocatorUtil.createBufferAllocator(null);
        this.allocator.setLimit(arrowMemoryLimit);
        this.writerPool = new ArrowWriterPool(allocator);
    }

    /**
     * Encodes all rows using the table's current schema ID and Arrow compression settings. A table
     * ID and schema ID must identify an immutable schema, as in native Fluss writes.
     */
    public BytesView encode(List<GenericRow> rows, TableInfo tableInfo) throws Exception {
        return encodeStreaming(
                consumer -> {
                    for (GenericRow row : rows) {
                        consumer.append(row);
                    }
                },
                tableInfo);
    }

    @Override
    public void close() throws Exception {
        lifecycleLock.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                IOUtils.closeAll(writerPool, allocator);
            }
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    BytesView encodeStreaming(RowSource rowSource, TableInfo tableInfo) throws Exception {
        lifecycleLock.readLock().lock();
        try {
            checkState(!closed, "Arrow record encoder is closed.");
            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
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
                // Heap output pages remain valid after the writer is recycled or encoder closed.
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
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @VisibleForTesting
    BufferAllocator allocator() {
        return allocator;
    }

    @VisibleForTesting
    ArrowWriterPool writerPool() {
        return writerPool;
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
