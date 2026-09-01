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

package org.apache.fluss.row.arrow;

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.compression.ArrowCompressionRatioEstimator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.AllocationListener;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.row.arrow.ArrowWriter.BUFFER_USAGE_RATIO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ArrowWriterPool}. */
public class ArrowWriterPoolTest {
    private BufferAllocator allocator;

    @BeforeEach
    void setup() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void testWriterMap() {
        ArrowWriterPool arrowWriterPool = new ArrowWriterPool(allocator);
        Map<String, Deque<ArrowWriter>> freeWritersMap = arrowWriterPool.freeWriters();
        ArrowWriter writer1 =
                arrowWriterPool.getOrCreateWriter(1L, 1, 1024, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
        assertThat(writer1.getWriteLimitInBytes()).isEqualTo((int) (1024 * BUFFER_USAGE_RATIO));
        assertThat(freeWritersMap.isEmpty()).isTrue();
        long epoch = writer1.getEpoch();
        writer1.recycle(epoch);
        assertThat(freeWritersMap.size()).isEqualTo(1);
        assertThat(freeWritersMap.get("1-1-ZSTD-3")).hasSize(1);
        assertThat(writer1.getEpoch()).isEqualTo(epoch + 1);
        // recycle the same epoch again, doesn't add it to pool
        writer1.recycle(epoch);
        assertThat(freeWritersMap.size()).isEqualTo(1);
        assertThat(freeWritersMap.get("1-1-ZSTD-3")).hasSize(1);

        ArrowWriter writer2 =
                arrowWriterPool.getOrCreateWriter(1L, 2, 10, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
        assertThat(freeWritersMap.size()).isEqualTo(1);
        writer2.recycle(writer2.getEpoch());
        assertThat(freeWritersMap.size()).isEqualTo(2);

        // test key1: "tableId_schemaId"
        Deque<ArrowWriter> arrowWriters = freeWritersMap.get("1-1-ZSTD-3");
        assertThat(arrowWriters.size()).isEqualTo(1);
        writer1 =
                arrowWriterPool.getOrCreateWriter(1L, 1, 1000, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
        assertThat(arrowWriters.size()).isEqualTo(0);
        assertThat(writer1.getWriteLimitInBytes()).isEqualTo((int) (1000 * BUFFER_USAGE_RATIO));
        ArrowWriter writer3WithKey1 =
                arrowWriterPool.getOrCreateWriter(1L, 1, 100, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
        writer3WithKey1.recycle(writer3WithKey1.getEpoch());
        writer1.recycle(writer1.getEpoch());
        assertThat(arrowWriters.size()).isEqualTo(2);
        arrowWriterPool.close();
    }

    @Test
    void testConstructorAllocationFailureClosesPartialRoot() {
        try (BufferAllocator limitedAllocator = new RootAllocator(1);
                ArrowWriterPool pool = new ArrowWriterPool(limitedAllocator)) {
            assertThatThrownBy(
                            () ->
                                    pool.getOrCreateWriter(
                                            1L, 1, 1024, DATA1_ROW_TYPE, DEFAULT_COMPRESSION))
                    .isInstanceOf(RuntimeException.class);
            assertThat(limitedAllocator.getAllocatedMemory()).isZero();
            assertThat(pool.compressionRatioEstimators()).isEmpty();
        }
    }

    @Test
    void testNewWriterResetFailureClosesConstructedWriter() {
        CountingAllocationListener countingListener = new CountingAllocationListener();
        int constructorAllocations;
        try (BufferAllocator countingAllocator =
                        new RootAllocator(countingListener, Long.MAX_VALUE);
                ArrowWriterPool pool = new ArrowWriterPool(countingAllocator)) {
            ArrowWriter writer =
                    new ArrowWriter(
                            "count-constructor-allocations",
                            1024,
                            DATA1_ROW_TYPE,
                            countingAllocator,
                            pool,
                            DEFAULT_COMPRESSION,
                            new ArrowCompressionRatioEstimator());
            constructorAllocations = countingListener.allocationAttempts.get();
            writer.root.close();
        }

        RuntimeException expected = new RuntimeException("reset allocation failure");
        CountingAllocationListener failingListener =
                new CountingAllocationListener(constructorAllocations + 1, expected);
        try (BufferAllocator failingAllocator = new RootAllocator(failingListener, Long.MAX_VALUE);
                ArrowWriterPool pool = new ArrowWriterPool(failingAllocator)) {
            assertThatThrownBy(
                            () ->
                                    pool.getOrCreateWriter(
                                            1L, 1, 1024, DATA1_ROW_TYPE, DEFAULT_COMPRESSION))
                    .isSameAs(expected);
            assertThat(failingAllocator.getAllocatedMemory()).isZero();
            assertThat(pool.freeWriters()).isEmpty();
            assertThat(pool.compressionRatioEstimators()).isEmpty();
        }
    }

    @Test
    void testIdleWriterResetFailureClosesPolledWriter() {
        final long allocatorLimit = 1L << 20;
        try (BufferAllocator limitedAllocator = new RootAllocator(allocatorLimit);
                ArrowWriterPool pool = new ArrowWriterPool(limitedAllocator)) {
            ArrowWriter writer =
                    pool.getOrCreateWriter(1L, 1, 1024, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
            writer.recycle(writer.getEpoch());
            assertThat(limitedAllocator.getAllocatedMemory()).isZero();

            try (ArrowBuf blocker = limitedAllocator.buffer(allocatorLimit)) {
                assertThatThrownBy(
                                () ->
                                        pool.getOrCreateWriter(
                                                1L, 1, 1024, DATA1_ROW_TYPE, DEFAULT_COMPRESSION))
                        .isInstanceOf(RuntimeException.class);
                assertThat(limitedAllocator.getAllocatedMemory()).isEqualTo(blocker.capacity());
                assertThat(pool.freeWriters()).isEmpty();
            }
            assertThat(limitedAllocator.getAllocatedMemory()).isZero();
        }
    }

    @Test
    void testRecycleProviderFailureClosesWriterAndInvalidatesEpoch() {
        AtomicInteger recycleCalls = new AtomicInteger();
        RuntimeException expected = new RuntimeException("recycle failure");
        ArrowWriterProvider failingProvider =
                new ArrowWriterProvider() {
                    @Override
                    public ArrowWriter getOrCreateWriter(
                            long tableId,
                            int schemaId,
                            int bufferSizeInBytes,
                            RowType schema,
                            ArrowCompressionInfo compressionInfo) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void recycleWriter(ArrowWriter arrowWriter) {
                        recycleCalls.incrementAndGet();
                        throw expected;
                    }

                    @Override
                    public void close() {}
                };

        ArrowWriter writer =
                new ArrowWriter(
                        "test",
                        1024,
                        DATA1_ROW_TYPE,
                        allocator,
                        failingProvider,
                        DEFAULT_COMPRESSION,
                        new ArrowCompressionRatioEstimator());
        long epoch = writer.getEpoch();
        assertThat(allocator.getAllocatedMemory()).isPositive();

        assertThatThrownBy(() -> writer.recycle(epoch)).isSameAs(expected);
        assertThat(allocator.getAllocatedMemory()).isZero();
        writer.recycle(epoch);
        assertThat(recycleCalls).hasValue(1);
    }

    private static final class CountingAllocationListener implements AllocationListener {
        private final AtomicInteger allocationAttempts = new AtomicInteger();
        private final int failureAttempt;
        private final RuntimeException failure;

        private CountingAllocationListener() {
            this(Integer.MAX_VALUE, null);
        }

        private CountingAllocationListener(int failureAttempt, RuntimeException failure) {
            this.failureAttempt = failureAttempt;
            this.failure = failure;
        }

        @Override
        public void onPreAllocation(long size) {
            if (allocationAttempts.incrementAndGet() == failureAttempt) {
                throw failure;
            }
        }
    }
}
