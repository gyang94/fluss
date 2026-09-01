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
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Bounded, instance-local lifecycle manager for Kafka Produce Arrow writers. */
@Internal
@ThreadSafe
public final class KafkaArrowWriterManager implements AutoCloseable {

    private static final long ACQUIRE_CLOSE_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    private final Object lock = new Object();
    private final long allocatorMemoryBytes;
    private final int maxSchemaKeys;
    private final int maxBatchSizeBytes;
    private final long acquireTimeoutNanos;
    private final Semaphore writerPermits;
    private final AtomicInteger waiters;
    private final KafkaProduceMetrics produceMetrics;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private BufferAllocator allocator;

    @GuardedBy("lock")
    private Generation currentGeneration;

    @GuardedBy("lock")
    private int activeWriters;

    @GuardedBy("lock")
    private boolean resourcesClosing;

    private volatile boolean closed;

    /** Creates a bounded writer manager. The Arrow allocator itself is created lazily. */
    public KafkaArrowWriterManager(
            long allocatorMemoryBytes,
            int maxConcurrentWriters,
            int maxSchemaKeys,
            Duration acquireTimeout,
            int maxBatchSizeBytes,
            KafkaProduceMetrics produceMetrics) {
        checkArgument(allocatorMemoryBytes > 0, "Arrow allocator memory must be greater than 0.");
        checkArgument(
                maxConcurrentWriters > 0,
                "Maximum concurrent Arrow writers must be greater than 0.");
        checkArgument(maxSchemaKeys > 0, "Maximum Arrow schema keys must be greater than 0.");
        checkArgument(maxBatchSizeBytes > 0, "Maximum Arrow batch size must be greater than 0.");
        checkNotNull(acquireTimeout);
        checkArgument(
                !acquireTimeout.isNegative() && !acquireTimeout.isZero(),
                "Arrow writer acquire timeout must be greater than 0.");
        this.allocatorMemoryBytes = allocatorMemoryBytes;
        this.maxSchemaKeys = maxSchemaKeys;
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.acquireTimeoutNanos = acquireTimeout.toNanos();
        this.writerPermits = new Semaphore(maxConcurrentWriters, true);
        this.waiters = new AtomicInteger();
        this.produceMetrics = checkNotNull(produceMetrics);
        recordMetricsBestEffort(
                () ->
                        this.produceMetrics.registerArrowResourceGauges(
                                this::allocatedMemoryBytes,
                                this::activeWriterCount,
                                this::waiterCount,
                                this::cachedSchemaKeyCount));
    }

    /** Acquires a writer for the supplied table schema or fails after the configured timeout. */
    public WriterLease acquire(TableInfo tableInfo) {
        checkNotNull(tableInfo);
        if (closed) {
            throw closedException();
        }

        long startedNanos = produceMetrics.nowNanos();
        boolean permitAcquired = false;
        waiters.incrementAndGet();
        try {
            permitAcquired = acquirePermit();
        } finally {
            waiters.decrementAndGet();
            // A reporting failure must not skip the permit hand-off below.
            recordMetricsBestEffort(
                    () -> produceMetrics.recordArrowWriterAcquireWait(startedNanos));
        }
        if (!permitAcquired) {
            recordMetricsBestEffort(produceMetrics::recordArrowAcquireTimeout);
            recordMetricsBestEffort(produceMetrics::recordArrowResourceError);
            throw new TimeoutException(
                    "Timed out waiting for a Kafka Produce Arrow writer after "
                            + TimeUnit.NANOSECONDS.toMillis(acquireTimeoutNanos)
                            + " ms.");
        }

        try {
            synchronized (lock) {
                if (closed) {
                    throw closedException();
                }
                Generation generation = generationFor(tableInfo);
                ArrowWriter writer =
                        generation.pool.getOrCreateWriter(
                                tableInfo.getTableId(),
                                tableInfo.getSchemaId(),
                                maxBatchSizeBytes,
                                tableInfo.getRowType(),
                                tableInfo.getTableConfig().getArrowCompressionInfo());
                // Compression adds fixed framing to each Arrow buffer. A tiny previous batch can
                // therefore report a body ratio above one, but that fixed overhead must not be
                // extrapolated as multiplicative expansion of this request.
                writer.capEstimatedCompressionRatio(1.0f);
                activeWriters++;
                return new WriterLease(this, writer);
            }
        } catch (Throwable failure) {
            writerPermits.release();
            recordMetricsBestEffort(produceMetrics::recordArrowResourceError);
            if (!(failure instanceof FlussRuntimeException) && !(failure instanceof Error)) {
                throw new TimeoutException(
                        "Failed to allocate or initialize a Kafka Produce Arrow writer.", failure);
            }
            ExceptionUtils.rethrow(failure);
            throw new AssertionError("Unreachable");
        }
    }

    /** Returns the configured maximum encoded Arrow batch size. */
    public int maxBatchSizeBytes() {
        return maxBatchSizeBytes;
    }

    /** Records that an acquired writer had to be aborted during encoding. */
    public void recordEncodeAbort() {
        recordMetricsBestEffort(produceMetrics::recordArrowEncodeAbort);
    }

    @Override
    public void close() {
        closeAsync().join();
    }

    /** Starts a non-blocking close that completes after all checked-out writers return. */
    public CompletableFuture<Void> closeAsync() {
        Resources resources;
        synchronized (lock) {
            if (closed) {
                return closeFuture;
            }
            closed = true;
            resources = detachResourcesIfReady();
        }
        if (resources != null) {
            closeResources(resources);
        }
        return closeFuture;
    }

    private void closeResources(Resources resources) {
        Throwable failure = null;
        try {
            if (resources.generation != null) {
                resources.generation.pool.close();
            }
        } catch (Throwable closeFailure) {
            failure = closeFailure;
        }
        try {
            if (resources.allocator != null) {
                resources.allocator.close();
            }
        } catch (Throwable closeFailure) {
            failure = ExceptionUtils.firstOrSuppressed(closeFailure, failure);
        }
        if (failure != null) {
            recordMetricsBestEffort(produceMetrics::recordArrowResourceError);
            closeFuture.completeExceptionally(failure);
        } else {
            closeFuture.complete(null);
        }
    }

    private boolean acquirePermit() {
        long deadline = System.nanoTime() + acquireTimeoutNanos;
        long remaining = acquireTimeoutNanos;
        while (remaining > 0) {
            if (closed) {
                throw closedException();
            }
            try {
                if (writerPermits.tryAcquire(
                        Math.min(remaining, ACQUIRE_CLOSE_POLL_NANOS), TimeUnit.NANOSECONDS)) {
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                recordMetricsBestEffort(produceMetrics::recordArrowResourceError);
                throw new TimeoutException(
                        "Interrupted while waiting for a Kafka Produce Arrow writer.", e);
            }
            remaining = deadline - System.nanoTime();
        }
        return false;
    }

    @GuardedBy("lock")
    private Generation generationFor(TableInfo tableInfo) {
        if (allocator == null) {
            allocator = new RootAllocator(allocatorMemoryBytes);
        }
        if (currentGeneration == null) {
            currentGeneration = new Generation(new ArrowWriterPool(allocator));
        }

        String schemaKey = writerKey(tableInfo);
        if (!currentGeneration.schemaKeys.contains(schemaKey)
                && currentGeneration.schemaKeys.size() >= maxSchemaKeys) {
            Generation previous = currentGeneration;
            currentGeneration = new Generation(new ArrowWriterPool(allocator));
            previous.pool.close();
            recordMetricsBestEffort(produceMetrics::recordArrowPoolRotation);
        }
        currentGeneration.schemaKeys.add(schemaKey);
        return currentGeneration;
    }

    private void release(ArrowWriter writer, long epoch) {
        Throwable failure = null;
        Resources resources;
        try {
            writer.recycle(epoch);
        } catch (Throwable recycleFailure) {
            failure = recycleFailure;
            recordMetricsBestEffort(produceMetrics::recordArrowResourceError);
        } finally {
            synchronized (lock) {
                activeWriters--;
                resources = detachResourcesIfReady();
            }
            writerPermits.release();
        }
        if (resources != null) {
            closeResources(resources);
        }
        if (failure != null) {
            ExceptionUtils.rethrow(failure);
        }
    }

    @GuardedBy("lock")
    private Resources detachResourcesIfReady() {
        if (!closed || activeWriters != 0 || resourcesClosing) {
            return null;
        }
        resourcesClosing = true;
        Resources resources = new Resources(currentGeneration, allocator);
        currentGeneration = null;
        allocator = null;
        return resources;
    }

    long allocatedMemoryBytes() {
        synchronized (lock) {
            return allocator == null ? 0L : allocator.getAllocatedMemory();
        }
    }

    int activeWriterCount() {
        synchronized (lock) {
            return activeWriters;
        }
    }

    int waiterCount() {
        return waiters.get();
    }

    int cachedSchemaKeyCount() {
        synchronized (lock) {
            return currentGeneration == null ? 0 : currentGeneration.schemaKeys.size();
        }
    }

    private static String writerKey(TableInfo tableInfo) {
        ArrowCompressionInfo compressionInfo = tableInfo.getTableConfig().getArrowCompressionInfo();
        return tableInfo.getTableId()
                + "-"
                + tableInfo.getSchemaId()
                + "-"
                + compressionInfo.toString();
    }

    private static FlussRuntimeException closedException() {
        return new FlussRuntimeException(
                "Kafka Produce Arrow writer manager is closing or closed.");
    }

    private static void recordMetricsBestEffort(Runnable metricOperation) {
        try {
            metricOperation.run();
        } catch (Throwable ignored) {
            // Metrics are observational. They must never retain a writer permit or prevent close.
        }
    }

    boolean isClosed() {
        return closed;
    }

    private static final class Generation {
        private final ArrowWriterPool pool;
        private final Set<String> schemaKeys = new HashSet<>();

        private Generation(ArrowWriterPool pool) {
            this.pool = pool;
        }
    }

    private static final class Resources {
        private final Generation generation;
        private final BufferAllocator allocator;

        private Resources(Generation generation, BufferAllocator allocator) {
            this.generation = generation;
            this.allocator = allocator;
        }
    }

    /** A checked-out Arrow writer that always returns its permit when closed. */
    public static final class WriterLease implements AutoCloseable {
        private final KafkaArrowWriterManager manager;
        private final ArrowWriter writer;
        private final long epoch;
        private final AtomicBoolean closed = new AtomicBoolean();

        private WriterLease(KafkaArrowWriterManager manager, ArrowWriter writer) {
            this.manager = manager;
            this.writer = writer;
            this.epoch = writer.getEpoch();
        }

        ArrowWriter writer() {
            return writer;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                manager.release(writer, epoch);
            }
        }
    }
}
