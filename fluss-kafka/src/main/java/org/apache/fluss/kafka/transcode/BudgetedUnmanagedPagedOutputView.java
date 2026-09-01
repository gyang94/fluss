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

import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.utils.ExceptionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An unmanaged paged output view that reserves every retained heap page before allocation. */
final class BudgetedUnmanagedPagedOutputView extends AbstractPagedOutputView {

    // Covers the MemorySegment wrapper, backing byte-array header, collection slot, and ordinary
    // object/array alignment retained for every output page.
    static final long PAGE_FIXED_ESTIMATE_BYTES = 128;

    private final KafkaOutputMemoryBudget budget;
    private final SegmentAllocator segmentAllocator;

    private long retainedCapacityBytes;
    private boolean released;

    BudgetedUnmanagedPagedOutputView(int pageSize, KafkaOutputMemoryBudget budget)
            throws IOException {
        this(pageSize, budget, MemorySegment::allocateHeapMemory);
    }

    BudgetedUnmanagedPagedOutputView(
            int pageSize, KafkaOutputMemoryBudget budget, SegmentAllocator segmentAllocator)
            throws IOException {
        this(
                pageSize,
                checkNotNull(budget),
                checkNotNull(segmentAllocator),
                allocatePage(pageSize, budget, segmentAllocator));
    }

    private BudgetedUnmanagedPagedOutputView(
            int pageSize,
            KafkaOutputMemoryBudget budget,
            SegmentAllocator segmentAllocator,
            MemorySegment initialSegment)
            throws IOException {
        super(initialSegment, pageSize);
        checkArgument(pageSize > 0, "Page size must be greater than 0.");
        this.budget = budget;
        this.segmentAllocator = segmentAllocator;
        this.retainedCapacityBytes = accountedPageBytes(pageSize);
    }

    @Override
    protected MemorySegment nextSegment() throws IOException {
        MemorySegment segment = allocatePage(pageSize, budget, segmentAllocator);
        retainedCapacityBytes += accountedPageBytes(pageSize);
        return segment;
    }

    @Override
    public List<MemorySegment> allocatedPooledSegments() {
        return Collections.emptyList();
    }

    long retainedCapacityBytes() {
        return retainedCapacityBytes;
    }

    void releaseRetainedCapacity() {
        if (released) {
            return;
        }
        released = true;
        long bytes = retainedCapacityBytes;
        retainedCapacityBytes = 0;
        budget.release(bytes);
    }

    private static MemorySegment allocatePage(
            int pageSize, KafkaOutputMemoryBudget budget, SegmentAllocator segmentAllocator)
            throws IOException {
        checkArgument(pageSize > 0, "Page size must be greater than 0.");
        budget.checkpoint();
        long accountedBytes = accountedPageBytes(pageSize);
        budget.reserve(accountedBytes);
        try {
            return segmentAllocator.allocate(pageSize);
        } catch (Throwable allocationFailure) {
            try {
                budget.release(accountedBytes);
            } catch (Throwable releaseFailure) {
                allocationFailure.addSuppressed(releaseFailure);
            }
            if (allocationFailure instanceof IOException) {
                throw (IOException) allocationFailure;
            }
            ExceptionUtils.rethrow(allocationFailure);
            throw new AssertionError("Unreachable");
        }
    }

    static long accountedPageBytes(int pageSize) {
        return pageSize + PAGE_FIXED_ESTIMATE_BYTES;
    }

    interface SegmentAllocator {
        MemorySegment allocate(int pageSize) throws IOException;
    }
}
