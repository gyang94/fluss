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

import org.apache.fluss.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BudgetedUnmanagedPagedOutputViewTest {

    @Test
    void testInitialPageBudgetRejectionHappensBeforeAllocation() {
        AtomicInteger allocations = new AtomicInteger();
        KafkaOutputMemoryBudget rejectingBudget =
                new KafkaOutputMemoryBudget() {
                    @Override
                    public void reserve(long bytes) {
                        throw new IllegalStateException("budget exhausted");
                    }

                    @Override
                    public void release(long bytes) {}
                };

        assertThatThrownBy(
                        () ->
                                new BudgetedUnmanagedPagedOutputView(
                                        8,
                                        rejectingBudget,
                                        pageSize -> {
                                            allocations.incrementAndGet();
                                            return MemorySegment.allocateHeapMemory(pageSize);
                                        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("budget exhausted");
        assertThat(allocations).hasValue(0);
    }

    @Test
    void testInitialAndFollowingPagesAreReservedBeforeAllocation() throws Exception {
        AtomicLong retainedBytes = new AtomicLong();
        AtomicInteger allocations = new AtomicInteger();
        KafkaOutputMemoryBudget budget = trackingBudget(retainedBytes);

        BudgetedUnmanagedPagedOutputView outputView =
                new BudgetedUnmanagedPagedOutputView(
                        8,
                        budget,
                        pageSize -> {
                            assertThat(retainedBytes)
                                    .hasValue(
                                            (allocations.get() + 1L)
                                                    * BudgetedUnmanagedPagedOutputView
                                                            .accountedPageBytes(pageSize));
                            allocations.incrementAndGet();
                            return MemorySegment.allocateHeapMemory(pageSize);
                        });
        outputView.write(new byte[9]);

        assertThat(allocations).hasValue(2);
        assertThat(outputView.retainedCapacityBytes())
                .isEqualTo(2 * BudgetedUnmanagedPagedOutputView.accountedPageBytes(8));
        assertThat(retainedBytes)
                .hasValue(2 * BudgetedUnmanagedPagedOutputView.accountedPageBytes(8));
        outputView.releaseRetainedCapacity();
        outputView.releaseRetainedCapacity();
        assertThat(retainedBytes).hasValue(0);
    }

    @Test
    void testBudgetRejectionHappensBeforeFollowingPageAllocation() throws Exception {
        AtomicLong retainedBytes = new AtomicLong();
        AtomicInteger allocations = new AtomicInteger();
        KafkaOutputMemoryBudget budget =
                new KafkaOutputMemoryBudget() {
                    @Override
                    public void reserve(long bytes) {
                        if (retainedBytes.get() + bytes
                                > BudgetedUnmanagedPagedOutputView.accountedPageBytes(8)) {
                            throw new IllegalStateException("budget exhausted");
                        }
                        retainedBytes.addAndGet(bytes);
                    }

                    @Override
                    public void release(long bytes) {
                        retainedBytes.addAndGet(-bytes);
                    }
                };
        BudgetedUnmanagedPagedOutputView outputView =
                new BudgetedUnmanagedPagedOutputView(
                        8,
                        budget,
                        pageSize -> {
                            allocations.incrementAndGet();
                            return MemorySegment.allocateHeapMemory(pageSize);
                        });

        assertThatThrownBy(() -> outputView.write(new byte[9]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("budget exhausted");
        assertThat(allocations).hasValue(1);
        assertThat(retainedBytes).hasValue(BudgetedUnmanagedPagedOutputView.accountedPageBytes(8));
        outputView.releaseRetainedCapacity();
        assertThat(retainedBytes).hasValue(0);
    }

    @Test
    void testAllocationFailureRollsBackOnlyFailedPageReservation() throws Exception {
        AtomicLong retainedBytes = new AtomicLong();
        AtomicInteger allocations = new AtomicInteger();
        BudgetedUnmanagedPagedOutputView outputView =
                new BudgetedUnmanagedPagedOutputView(
                        8,
                        trackingBudget(retainedBytes),
                        pageSize -> {
                            if (allocations.incrementAndGet() == 2) {
                                throw new IOException("allocation failed");
                            }
                            return MemorySegment.allocateHeapMemory(pageSize);
                        });

        assertThatThrownBy(() -> outputView.write(new byte[9]))
                .isInstanceOf(IOException.class)
                .hasMessage("allocation failed");
        assertThat(retainedBytes).hasValue(BudgetedUnmanagedPagedOutputView.accountedPageBytes(8));
        assertThat(outputView.retainedCapacityBytes())
                .isEqualTo(BudgetedUnmanagedPagedOutputView.accountedPageBytes(8));
        outputView.releaseRetainedCapacity();
        assertThat(retainedBytes).hasValue(0);
    }

    @Test
    void testCancellationCheckpointPrecedesEveryPageReservation() throws Exception {
        CancellationException expected = new CancellationException("cancelled");
        AtomicInteger checkpoints = new AtomicInteger();
        AtomicInteger reservations = new AtomicInteger();
        AtomicInteger allocations = new AtomicInteger();
        AtomicLong retainedBytes = new AtomicLong();
        KafkaOutputMemoryBudget budget =
                new KafkaOutputMemoryBudget() {
                    @Override
                    public void reserve(long bytes) {
                        reservations.incrementAndGet();
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
        BudgetedUnmanagedPagedOutputView outputView =
                new BudgetedUnmanagedPagedOutputView(
                        8,
                        budget,
                        pageSize -> {
                            allocations.incrementAndGet();
                            return MemorySegment.allocateHeapMemory(pageSize);
                        });

        assertThatThrownBy(() -> outputView.write(new byte[9])).isSameAs(expected);
        assertThat(checkpoints).hasValue(2);
        assertThat(reservations).hasValue(1);
        assertThat(allocations).hasValue(1);
        assertThat(retainedBytes).hasValue(BudgetedUnmanagedPagedOutputView.accountedPageBytes(8));
        outputView.releaseRetainedCapacity();
        assertThat(retainedBytes).hasValue(0);
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
}
