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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests per-partition scheduling, bounded admission and executor shutdown. */
class KafkaProduceConversionExecutorTest {
    @Test
    void testSameKeyWaitsBeforeConversionWhileOtherKeysProgress() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(2, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean firstSubmitted = new AtomicBoolean();
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                started.countDown();
                                await(release);
                                firstSubmitted.set(true);
                                return CompletableFuture.completedFuture(1);
                            });
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Integer> second =
                    executor.submit(
                            0,
                            () -> {
                                assertThat(firstSubmitted).isTrue();
                                return CompletableFuture.completedFuture(2);
                            });
            assertThat(
                            executor.submit(1, () -> CompletableFuture.completedFuture(3))
                                    .get(10, TimeUnit.SECONDS))
                    .isEqualTo(3);
            assertThat(second).isNotDone();
            release.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo(2);
        } finally {
            release.countDown();
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testFailedAndCancelledQueuedTasksDoNotBlockFollowingTasks() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(2, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean cancelledRan = new AtomicBoolean();
        List<Integer> order = new ArrayList<>();
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                started.countDown();
                                await(release);
                                order.add(1);
                                return CompletableFuture.completedFuture(1);
                            });
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Object> failed =
                    executor.submit(
                            0,
                            () -> {
                                order.add(2);
                                throw new IllegalArgumentException("invalid record");
                            });
            CompletableFuture<Integer> cancelled =
                    executor.submit(
                            0,
                            () -> {
                                cancelledRan.set(true);
                                return CompletableFuture.completedFuture(3);
                            });
            assertThat(cancelled.cancel(false)).isTrue();
            CompletableFuture<Integer> last =
                    executor.submit(
                            0,
                            () -> {
                                order.add(4);
                                return CompletableFuture.completedFuture(4);
                            });
            release.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(1);
            assertThatThrownBy(failed::join).hasCauseInstanceOf(IllegalArgumentException.class);
            assertThat(last.get(10, TimeUnit.SECONDS)).isEqualTo(4);
            assertThat(cancelledRan).isFalse();
            assertThat(order).containsExactly(1, 2, 4);
        } finally {
            release.countDown();
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testKeyedFollowersCountTowardsCapacityAndRejectionPreservesOrder() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(2, 2);
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch releaseOther = new CountDownLatch(1);
        AtomicBoolean rejectedRan = new AtomicBoolean();
        List<Integer> order = new ArrayList<>();
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                started.countDown();
                                await(releaseFirst);
                                order.add(1);
                                return CompletableFuture.completedFuture(1);
                            });
            CompletableFuture<Integer> other =
                    executor.submit(
                            1,
                            () -> {
                                started.countDown();
                                await(releaseOther);
                                return CompletableFuture.completedFuture(0);
                            });
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Integer> second =
                    executor.submit(
                            0,
                            () -> {
                                order.add(2);
                                return CompletableFuture.completedFuture(2);
                            });
            CompletableFuture<Integer> third =
                    executor.submit(
                            0,
                            () -> {
                                order.add(3);
                                return CompletableFuture.completedFuture(3);
                            });
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
            other.get(10, TimeUnit.SECONDS);
            CompletableFuture<Integer> last =
                    executor.submit(
                            0,
                            () -> {
                                order.add(4);
                                return CompletableFuture.completedFuture(4);
                            });
            assertThat(last).isNotDone();
            releaseFirst.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            third.get(10, TimeUnit.SECONDS);
            last.get(10, TimeUnit.SECONDS);
            assertThat(order).containsExactly(1, 2, 3, 4);
        } finally {
            releaseFirst.countDown();
            releaseOther.countDown();
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testPendingAcknowledgementDoesNotBlockNextTaskForTheSameKey() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 1);
        CompletableFuture<Integer> acknowledgement = new CompletableFuture<>();
        CountDownLatch submitted = new CountDownLatch(1);
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                submitted.countDown();
                                return acknowledgement;
                            });
            assertThat(submitted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(
                            executor.submit(0, () -> CompletableFuture.completedFuture(2))
                                    .get(10, TimeUnit.SECONDS))
                    .isEqualTo(2);
            executor.closeAsync().get(10, TimeUnit.SECONDS);
            assertThat(first).isNotDone();
            acknowledgement.complete(1);
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(1);
        } finally {
            acknowledgement.complete(1);
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testCancellingRunningResultDoesNotStartNextConversion() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(2, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean firstSubmitted = new AtomicBoolean();
        try {
            CompletableFuture<Integer> first =
                    executor.submit(
                            0,
                            () -> {
                                started.countDown();
                                await(release);
                                firstSubmitted.set(true);
                                return CompletableFuture.completedFuture(1);
                            });
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(first.cancel(false)).isTrue();
            CompletableFuture<Integer> second =
                    executor.submit(
                            0,
                            () -> {
                                assertThat(firstSubmitted).isTrue();
                                return CompletableFuture.completedFuture(2);
                            });
            executor.submit(1, () -> CompletableFuture.completedFuture(3))
                    .get(10, TimeUnit.SECONDS);
            assertThat(second).isNotDone();
            release.countDown();
            assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo(2);
            assertThat(first).isCancelled();
        } finally {
            release.countDown();
            executor.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void testCloseRejectsAllQueuedTasksAndInterruptsRunningConversion() throws Exception {
        KafkaProduceConversionExecutor executor = new KafkaProduceConversionExecutor(1, 2);
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean queuedRan = new AtomicBoolean();
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
            CompletableFuture<Void> sameKey =
                    executor.submit(
                            0,
                            () -> {
                                queuedRan.set(true);
                                return CompletableFuture.completedFuture(null);
                            });
            CompletableFuture<Void> otherKey =
                    executor.submit(
                            1,
                            () -> {
                                queuedRan.set(true);
                                return CompletableFuture.completedFuture(null);
                            });
            CompletableFuture<Void> closed = executor.closeAsync();
            closed.get(10, TimeUnit.SECONDS);
            assertThatThrownBy(first::join).hasRootCauseInstanceOf(InterruptedException.class);
            assertThatThrownBy(sameKey::join).hasCauseInstanceOf(RejectedExecutionException.class);
            assertThatThrownBy(otherKey::join).hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(queuedRan).isFalse();
            assertThat(executor.closeAsync()).isSameAs(closed);
            assertThatThrownBy(
                            () ->
                                    executor.submit(0, () -> CompletableFuture.completedFuture(1))
                                            .join())
                    .hasCauseInstanceOf(RejectedExecutionException.class);
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
