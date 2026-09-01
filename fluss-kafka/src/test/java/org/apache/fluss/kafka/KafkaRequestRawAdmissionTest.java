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

package org.apache.fluss.kafka;

import org.apache.fluss.kafka.network.KafkaFrameAdmissionLease;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests copied-payload raw admission ownership exposed through a request context. */
class KafkaRequestRawAdmissionTest {

    @Test
    void testContextDelegatesGrowthAndRequestLifecycleReleasesFinalOwnership() {
        short version = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest produceRequest =
                new ProduceRequest(
                        new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1_000), version);
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        TrackingLease lease = new TrackingLease();
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.PRODUCE,
                        version,
                        new RequestHeader(ApiKeys.PRODUCE, version, "client", 1),
                        produceRequest,
                        "KAFKA",
                        buffer,
                        new TestingChannelHandlerContext(),
                        new CompletableFuture<AbstractResponse>());

        try {
            request.attachAdmissionLease(lease);
            KafkaRequestContext context = KafkaRequestContext.fromRequest(request);

            context.growRawAdmissionBytes(12);
            context.releaseGrownRawAdmissionBytes(5);
            assertThat(lease.grownBytes).isEqualTo(7);
            assertThat(lease.rawReleased).isFalse();
            assertThat(lease.liveReleased).isFalse();

            request.releaseOrderedBuffer();
            assertThat(lease.rawReleased).isTrue();
            assertThat(lease.grownBytes).isZero();

            request.markProcessingCompleted();
            request.markNetworkCompleted();
            assertThat(lease.liveReleased).isTrue();
        } finally {
            request.releaseOrderedBuffer();
            request.markProcessingCompleted();
            request.markNetworkCompleted();
            buffer.release();
        }
    }

    @Test
    void testGrowthCannotPassAConcurrentRequestRawRelease() throws Exception {
        short version = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest produceRequest =
                new ProduceRequest(
                        new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1_000), version);
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        BlockingReleaseLease lease = new BlockingReleaseLease();
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.PRODUCE,
                        version,
                        new RequestHeader(ApiKeys.PRODUCE, version, "client", 1),
                        produceRequest,
                        "KAFKA",
                        buffer,
                        new TestingChannelHandlerContext(),
                        new CompletableFuture<AbstractResponse>());
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            request.attachAdmissionLease(lease);
            KafkaRequestContext context = KafkaRequestContext.fromRequest(request);
            Future<?> release = executor.submit(request::releaseOrderedBuffer);
            assertThat(lease.releaseEntered.await(5, TimeUnit.SECONDS)).isTrue();

            Future<?> growth = executor.submit(() -> context.growRawAdmissionBytes(1));
            assertThat(growth.isDone()).isFalse();
            lease.allowRelease.countDown();

            release.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> growth.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("Kafka frame raw-byte ownership is already released");
            assertThat(lease.grownBytes).isZero();
        } finally {
            lease.allowRelease.countDown();
            executor.shutdownNow();
            request.releaseOrderedBuffer();
            request.markProcessingCompleted();
            request.markNetworkCompleted();
            buffer.release();
        }
    }

    private static class TrackingLease implements KafkaFrameAdmissionLease {
        protected long grownBytes;
        protected boolean rawReleased;
        protected boolean liveReleased;

        @Override
        public void growFrameBytes(long additionalBytes) {
            if (rawReleased) {
                throw new IllegalStateException("raw bytes released");
            }
            grownBytes += additionalBytes;
        }

        @Override
        public void releaseGrownFrameBytes(long additionalBytes) {
            if (!rawReleased) {
                grownBytes -= additionalBytes;
            }
        }

        @Override
        public void releaseFrameBytes() {
            rawReleased = true;
            grownBytes = 0;
        }

        @Override
        public void releaseRequest() {
            liveReleased = true;
        }

        @Override
        public void close() {
            releaseFrameBytes();
            releaseRequest();
        }
    }

    private static final class BlockingReleaseLease extends TrackingLease {
        private final CountDownLatch releaseEntered = new CountDownLatch(1);
        private final CountDownLatch allowRelease = new CountDownLatch(1);

        @Override
        public void releaseFrameBytes() {
            releaseEntered.countDown();
            try {
                allowRelease.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while releasing raw ownership", e);
            }
            super.releaseFrameBytes();
        }
    }
}
