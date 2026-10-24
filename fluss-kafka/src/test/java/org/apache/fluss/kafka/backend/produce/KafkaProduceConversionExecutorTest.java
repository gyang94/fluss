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

package org.apache.fluss.kafka.backend.produce;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests concurrent conversion, ordered submission, bounded admission and executor shutdown. */
class KafkaProduceConversionExecutorTest {
    @Test
    void testFailedConversionDoesNotReleaseEarlierPendingAppend() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(2, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch lastConverted = new CountDownLatch(1);
        List<Integer> order = new ArrayList<>();
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                started.countDown();
                                await(release);
                                executor.awaitAppendTurn();
                                order.add(1);
                                return CompletableFuture.completedFuture(1);
                            });
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Object> failed =
                    executor.submit(
                            0,
                            () -> {
                                throw new IllegalArgumentException("invalid record");
                            });
            assertThatThrownBy(failed::join).hasCauseInstanceOf(IllegalArgumentException.class);
            CompletableFuture<Integer> last =
                    executor.submit(
                            0,
                            () -> {
                                lastConverted.countDown();
                                executor.awaitAppendTurn();
                                order.add(3);
                                return CompletableFuture.completedFuture(3);
                            });
            assertThat(lastConverted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> last.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            release.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(last.get(10, TimeUnit.SECONDS)).isEqualTo(3);
            assertThat(order).containsExactly(1, 3);
        } finally {
            release.countDown();
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testRejectedTaskDoesNotRunOrReleaseEarlierPendingAppend() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(2, 2);
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch releaseOther = new CountDownLatch(1);
        CountDownLatch lastConverted = new CountDownLatch(1);
        AtomicBoolean rejectedRan = new AtomicBoolean();
        List<Integer> order = new ArrayList<>();
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                started.countDown();
                                await(releaseFirst);
                                executor.awaitAppendTurn();
                                order.add(1);
                                return CompletableFuture.completedFuture(1);
                            });
            executor.submit(
                    1,
                    () -> {
                        started.countDown();
                        await(releaseOther);
                        return CompletableFuture.completedFuture(0);
                    });
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Integer> queued1 =
                    executor.submit(2, () -> CompletableFuture.completedFuture(0));
            CompletableFuture<Integer> queued2 =
                    executor.submit(3, () -> CompletableFuture.completedFuture(0));
            CompletableFuture<Integer> rejected =
                    executor.submit(
                            0,
                            () -> {
                                rejectedRan.set(true);
                                return CompletableFuture.completedFuture(0);
                            });
            assertThatThrownBy(rejected::join).hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(rejectedRan).isFalse();
            releaseOther.countDown();
            queued1.get(10, TimeUnit.SECONDS);
            queued2.get(10, TimeUnit.SECONDS);
            CompletableFuture<Integer> last =
                    executor.submit(
                            0,
                            () -> {
                                lastConverted.countDown();
                                executor.awaitAppendTurn();
                                order.add(2);
                                return CompletableFuture.completedFuture(2);
                            });
            assertThat(lastConverted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> last.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            releaseFirst.countDown();
            first.get(10, TimeUnit.SECONDS);
            last.get(10, TimeUnit.SECONDS);
            assertThat(order).containsExactly(1, 2);
        } finally {
            releaseFirst.countDown();
            releaseOther.countDown();
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testPendingAcknowledgementDoesNotOccupyWorker() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 1);
        CompletableFuture<Integer> acknowledgement = new CompletableFuture<>();
        CountDownLatch submitted = new CountDownLatch(1);
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                executor.awaitAppendTurn();
                                submitted.countDown();
                                return acknowledgement;
                            });
            assertThat(submitted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(
                            executor.submit(
                                            0,
                                            () -> {
                                                executor.awaitAppendTurn();
                                                return CompletableFuture.completedFuture(2);
                                            })
                                    .get(10, TimeUnit.SECONDS))
                    .isEqualTo(2);
            assertThat(first).isNotDone();
            acknowledgement.complete(1);
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(1);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testCloseInterruptsSubmissionWait() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(2, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch converted = new CountDownLatch(1);
        CompletableFuture<Void> first =
                executor.submit(
                        0,
                        () -> {
                            started.countDown();
                            await(new CountDownLatch(1));
                            return CompletableFuture.completedFuture(null);
                        });
        try {
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> second =
                    executor.submit(
                            0,
                            () -> {
                                converted.countDown();
                                executor.awaitAppendTurn();
                                throw new AssertionError("Must not append after closing.");
                            });
            assertThat(converted.await(10, TimeUnit.SECONDS)).isTrue();
            executor.closeAsync().get(10, TimeUnit.SECONDS);
            assertThatThrownBy(first::join).hasRootCauseInstanceOf(InterruptedException.class);
            assertThatThrownBy(second::join)
                    .isInstanceOf(java.util.concurrent.CancellationException.class);
        } finally {
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Test worker was not released.");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }
}
