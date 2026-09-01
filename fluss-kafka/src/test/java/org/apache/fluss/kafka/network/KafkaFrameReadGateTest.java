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

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.FixedRecvByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.RecvByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaFrameReadGateTest {

    @Test
    void testReadGateForwardsOnlyAtOpenFrameBoundaries() {
        KafkaFrameReadState readState = new KafkaFrameReadState();
        AtomicInteger forwardedReads = new AtomicInteger();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new ChannelOutboundHandlerAdapter() {
                            @Override
                            public void read(ChannelHandlerContext ctx) throws Exception {
                                forwardedReads.incrementAndGet();
                                ctx.read();
                            }
                        },
                        new KafkaFrameReadGate(readState));
        int initialReads = forwardedReads.get();

        channel.read();
        assertThat(forwardedReads).hasValue(initialReads + 1);

        readState.waitForGrant(128);
        channel.read();
        assertThat(forwardedReads).hasValue(initialReads + 1);

        // Opening the state does not replay a swallowed read. The pause owner must restart it.
        readState.readBody(128 - KafkaFrameReadState.PROBE_BYTES);
        assertThat(forwardedReads).hasValue(initialReads + 1);
        channel.read();
        assertThat(forwardedReads).hasValue(initialReads + 2);

        readState.stop();
        channel.read();
        assertThat(forwardedReads).hasValue(initialReads + 2);

        readState.reset();
        channel.read();
        assertThat(forwardedReads).hasValue(initialReads + 3);
        channel.finishAndReleaseAll();
    }

    @Test
    void testAllocatorFailsBeforeAllocatingOrObservingWhenReadsAreStopped() {
        KafkaFrameReadState readState = new KafkaFrameReadState();
        AtomicInteger observedAllocations = new AtomicInteger();
        KafkaProbeAwareRecvByteBufAllocator allocator =
                new KafkaProbeAwareRecvByteBufAllocator(
                        new FixedRecvByteBufAllocator(16),
                        readState,
                        new KafkaProbeAwareRecvByteBufAllocator.ReadObserver() {
                            @Override
                            public void onAllocation(int bytes) {
                                observedAllocations.incrementAndGet();
                            }

                            @Override
                            public void onBytesRead(int bytes) {}
                        });
        RecvByteBufAllocator.Handle handle = allocator.newHandle();
        ByteBuf probe = handle.allocate(UnpooledByteBufAllocator.DEFAULT);
        assertThat(probe.capacity()).isEqualTo(KafkaFrameReadState.PROBE_BYTES);
        assertThat(observedAllocations).hasValue(1);
        probe.release();

        readState.waitForGrant(128);
        assertThatThrownBy(handle::guess)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("frame reads are stopped");
        assertThatThrownBy(() -> handle.allocate(UnpooledByteBufAllocator.DEFAULT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("frame reads are stopped");
        assertThat(observedAllocations).hasValue(1);
    }
}
