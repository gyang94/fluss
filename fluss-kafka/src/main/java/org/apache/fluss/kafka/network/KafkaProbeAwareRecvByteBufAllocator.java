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

package org.apache.fluss.kafka.network;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelConfig;
import org.apache.fluss.shaded.netty4.io.netty.channel.RecvByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.util.UncheckedBooleanSupplier;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A receive allocator that limits each socket read to the bytes currently requested by a Kafka
 * frame decoder.
 *
 * <p>During probing this allocator requests at most the bytes missing from the six-byte length/API
 * key probe. While admission is pending it refuses additional reads. After admission it delegates
 * sizing policy but clamps each allocation to the unread bytes in the admitted frame.
 */
@Internal
public final class KafkaProbeAwareRecvByteBufAllocator implements RecvByteBufAllocator {

    /** Test-only observer for proving actual allocation and socket-read bounds. */
    interface ReadObserver {
        void onAllocation(int bytes);

        void onBytesRead(int bytes);
    }

    private final RecvByteBufAllocator delegate;
    private final KafkaFrameReadState readState;
    private final @Nullable ReadObserver observer;

    KafkaProbeAwareRecvByteBufAllocator(
            RecvByteBufAllocator delegate, KafkaFrameReadState readState) {
        this(delegate, readState, null);
    }

    KafkaProbeAwareRecvByteBufAllocator(
            RecvByteBufAllocator delegate,
            KafkaFrameReadState readState,
            @Nullable ReadObserver observer) {
        this.delegate = checkNotNull(delegate, "delegate");
        this.readState = checkNotNull(readState, "readState");
        this.observer = observer;
    }

    @Override
    public Handle newHandle() {
        return new ProbeAwareHandle(delegate.newHandle());
    }

    private final class ProbeAwareHandle implements ExtendedHandle {
        private final Handle delegateHandle;

        private ProbeAwareHandle(Handle delegateHandle) {
            this.delegateHandle = delegateHandle;
        }

        @Override
        public ByteBuf allocate(ByteBufAllocator alloc) {
            if (!readState.canRead()) {
                throw new IllegalStateException(
                        "Kafka transport attempted to allocate while frame reads are stopped");
            }
            int allocationBytes = guess();
            if (allocationBytes <= 0) {
                throw new IllegalStateException(
                        "Kafka transport receive allocator returned a non-positive size: "
                                + allocationBytes);
            }
            if (observer != null) {
                observer.onAllocation(allocationBytes);
            }
            return alloc.ioBuffer(allocationBytes, allocationBytes);
        }

        @Override
        public int guess() {
            return readState.allocationSize(delegateHandle.guess());
        }

        @Override
        public void reset(ChannelConfig config) {
            delegateHandle.reset(config);
        }

        @Override
        public void incMessagesRead(int numMessages) {
            delegateHandle.incMessagesRead(numMessages);
        }

        @Override
        public void lastBytesRead(int bytes) {
            delegateHandle.lastBytesRead(bytes);
            if (observer != null && bytes > 0) {
                observer.onBytesRead(bytes);
            }
        }

        @Override
        public int lastBytesRead() {
            return delegateHandle.lastBytesRead();
        }

        @Override
        public void attemptedBytesRead(int bytes) {
            delegateHandle.attemptedBytesRead(bytes);
        }

        @Override
        public int attemptedBytesRead() {
            return delegateHandle.attemptedBytesRead();
        }

        @Override
        public boolean continueReading() {
            return readState.canRead() && delegateHandle.continueReading();
        }

        @Override
        public boolean continueReading(UncheckedBooleanSupplier maybeMoreDataSupplier) {
            if (!readState.canRead()) {
                return false;
            }
            if (delegateHandle instanceof ExtendedHandle) {
                return ((ExtendedHandle) delegateHandle).continueReading(maybeMoreDataSupplier);
            }
            return maybeMoreDataSupplier.get() && delegateHandle.continueReading();
        }

        @Override
        public void readComplete() {
            delegateHandle.readComplete();
        }
    }
}
