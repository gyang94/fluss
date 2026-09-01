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

package org.apache.fluss.kafka.admission;

import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController.AdmissionPauseReason;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController.RequestLease;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController.Reservation;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseLease;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseReason;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaProduceAdmissionControllerTest {

    @Test
    void testCopiedPayloadGrowthUsesGlobalAndPerConnectionRawBudgetOnly() {
        KafkaProduceAdmissionController controller = controller(10, 20, 10, 20);
        try (TestingConnection first = new TestingConnection(controller);
                TestingConnection second = new TestingConnection(controller)) {
            RequestLease firstLease = granted(first.handle.reserve(4));
            RequestLease secondLease = granted(second.handle.reserve(4));

            firstLease.growFrameBytes(10);
            assertThat(controller.rawBytes()).isEqualTo(18);
            assertThat(first.handle.rawBytes()).isEqualTo(14);
            assertThat(second.handle.rawBytes()).isEqualTo(4);
            assertThat(controller.liveBytes()).isEqualTo(8);
            assertThat(first.handle.liveBytes()).isEqualTo(4);

            assertThatThrownBy(() -> secondLease.growFrameBytes(3))
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasMessageContaining("currently unavailable");
            assertThat(controller.rawBytes()).isEqualTo(18);
            assertThat(controller.liveBytes()).isEqualTo(8);

            firstLease.releaseGrownFrameBytes(10);
            assertThat(controller.rawBytes()).isEqualTo(8);
            assertThat(controller.liveBytes()).isEqualTo(8);

            firstLease.close();
            secondLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testCopiedPayloadGrowthThatCanNeverFitIsAtomic() {
        KafkaProduceAdmissionController controller = controller(10, 20, 10, 20);
        try (TestingConnection connection = new TestingConnection(controller)) {
            RequestLease lease = granted(connection.handle.reserve(4));

            assertThatThrownBy(() -> lease.growFrameBytes(17))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("copied payload");
            assertThat(controller.rawBytes()).isEqualTo(4);
            assertThat(controller.liveBytes()).isEqualTo(4);
            assertThat(connection.handle.rawBytes()).isEqualTo(4);

            lease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testGrowVersusDisconnectAndRawReleaseIsLinearizable() throws Exception {
        KafkaProduceAdmissionController controller = controller(10, 100, 10, 100);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int index = 0; index < 200; index++) {
                TestingConnection connection = new TestingConnection(controller);
                RequestLease lease = granted(connection.handle.reserve(4));
                CountDownLatch start = new CountDownLatch(1);
                Future<Boolean> growth =
                        executor.submit(
                                () -> {
                                    start.await();
                                    try {
                                        lease.growFrameBytes(8);
                                        return true;
                                    } catch (IllegalStateException expected) {
                                        return false;
                                    }
                                });
                Future<?> disconnectAndRelease =
                        executor.submit(
                                () -> {
                                    start.await();
                                    connection.handle.close();
                                    lease.releaseRaw();
                                    return null;
                                });

                start.countDown();
                growth.get(5, TimeUnit.SECONDS);
                disconnectAndRelease.get(5, TimeUnit.SECONDS);
                assertThat(controller.rawBytes()).isZero();
                assertThat(connection.handle.rawBytes()).isZero();
                assertThat(controller.liveBytes()).isEqualTo(4);

                lease.close();
                connection.close();
                assertThat(controller.liveBytes()).isZero();
            }
        } finally {
            executor.shutdownNow();
        }
        assertEmpty(controller);
    }

    @Test
    void testGrowthRollbackVersusRawReleaseNeverUnderflows() throws Exception {
        KafkaProduceAdmissionController controller = controller(10, 100, 10, 100);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (TestingConnection connection = new TestingConnection(controller)) {
            for (int index = 0; index < 200; index++) {
                RequestLease lease = granted(connection.handle.reserve(4));
                lease.growFrameBytes(8);
                CountDownLatch start = new CountDownLatch(1);
                Future<?> rollback =
                        executor.submit(
                                () -> {
                                    start.await();
                                    lease.releaseGrownFrameBytes(8);
                                    return null;
                                });
                Future<?> release =
                        executor.submit(
                                () -> {
                                    start.await();
                                    lease.releaseRaw();
                                    return null;
                                });

                start.countDown();
                rollback.get(5, TimeUnit.SECONDS);
                release.get(5, TimeUnit.SECONDS);
                assertThat(controller.rawBytes()).isZero();
                assertThat(connection.handle.rawBytes()).isZero();
                lease.close();
            }
        } finally {
            executor.shutdownNow();
        }
        assertEmpty(controller);
    }

    @Test
    void testPreFrameReservationIsAtomicAndNeverOvershoots() {
        KafkaProduceAdmissionController controller = controller(1, 10, 1, 10);
        try (TestingConnection first = new TestingConnection(controller);
                TestingConnection second = new TestingConnection(controller)) {
            RequestLease firstLease = granted(first.handle.reserve(10));
            Reservation waiting = second.handle.reserve(1);

            assertThat(waiting.getFuture()).isNotDone();
            assertThat(controller.liveRequests()).isOne();
            assertThat(controller.rawBytes()).isEqualTo(10);
            assertThat(controller.pendingReservations()).isOne();
            assertThat(controller.liveRequestOvershootEvents()).isZero();
            assertThat(controller.rawBytesOvershootEvents()).isZero();

            firstLease.releaseRaw();
            assertThat(controller.rawBytes()).isZero();
            assertThat(waiting.getFuture()).isNotDone();

            firstLease.releaseLive();
            RequestLease secondLease = granted(waiting);
            assertThat(controller.liveRequests()).isOne();
            assertThat(controller.rawBytes()).isOne();
            assertThat(controller.pendingReservations()).isZero();

            secondLease.close();
            firstLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testLiveWireBytesRemainBoundedAfterRawOwnershipTransfers() {
        KafkaProduceAdmissionController controller = controller(10, 10, 10, 10);
        try (TestingConnection first = new TestingConnection(controller);
                TestingConnection second = new TestingConnection(controller)) {
            RequestLease firstLease = granted(first.handle.reserve(10));
            firstLease.releaseRaw();
            assertThat(controller.rawBytes()).isZero();
            assertThat(controller.liveBytes()).isEqualTo(10);

            Reservation waiting = second.handle.reserve(1);
            assertThat(waiting.getFuture()).isNotDone();
            assertThat(controller.pendingReservations()).isOne();

            firstLease.releaseLive();
            RequestLease secondLease = granted(waiting);
            assertThat(controller.liveBytes()).isOne();
            assertThat(controller.rawBytes()).isOne();
            assertThat(controller.rawBytesOvershootEvents()).isZero();

            firstLease.close();
            secondLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testStrictFifoDoesNotSkipLargeHeadReservation() {
        KafkaProduceAdmissionController controller = controller(10, 10, 10, 10);
        try (TestingConnection holder = new TestingConnection(controller);
                TestingConnection headConnection = new TestingConnection(controller);
                TestingConnection followerConnection = new TestingConnection(controller)) {
            RequestLease holderLease = granted(holder.handle.reserve(6));
            Reservation head = headConnection.handle.reserve(8);
            Reservation follower = followerConnection.handle.reserve(4);

            assertThat(head.getFuture()).isNotDone();
            assertThat(follower.getFuture()).isNotDone();
            assertThat(controller.pendingReservations()).isEqualTo(2);

            holderLease.releaseRaw();
            assertThat(head.getFuture()).isNotDone();
            holderLease.releaseLive();
            RequestLease headLease = granted(head);
            assertThat(follower.getFuture()).isNotDone();
            assertThat(controller.rawBytes()).isEqualTo(8);

            headLease.releaseRaw();
            assertThat(follower.getFuture()).isNotDone();
            headLease.releaseLive();
            RequestLease followerLease = granted(follower);
            assertThat(controller.rawBytes()).isEqualTo(4);

            holderLease.close();
            headLease.close();
            followerLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testCancellationRemovesHeadAndUnblocksFollower() {
        KafkaProduceAdmissionController controller = controller(10, 10, 10, 10);
        try (TestingConnection holder = new TestingConnection(controller);
                TestingConnection headConnection = new TestingConnection(controller);
                TestingConnection followerConnection = new TestingConnection(controller)) {
            RequestLease holderLease = granted(holder.handle.reserve(8));
            Reservation head = headConnection.handle.reserve(4);
            Reservation follower = followerConnection.handle.reserve(2);

            assertThat(head.cancel()).isTrue();
            assertThat(head.cancel()).isFalse();
            assertThatThrownBy(() -> head.getFuture().join())
                    .isInstanceOf(CancellationException.class);
            RequestLease followerLease = granted(follower);
            assertThat(controller.pendingReservations()).isZero();
            assertThat(controller.liveRequests()).isEqualTo(2);
            assertThat(controller.rawBytes()).isEqualTo(10);

            holderLease.close();
            followerLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testPerConnectionBlockedHeadDoesNotBlockAnotherConnection() {
        KafkaProduceAdmissionController controller = controller(3, 30, 1, 10);
        try (TestingConnection slow = new TestingConnection(controller);
                TestingConnection other = new TestingConnection(controller)) {
            RequestLease slowLease = granted(slow.handle.reserve(5));
            Reservation slowHead = slow.handle.reserve(5);
            Reservation otherFollower = other.handle.reserve(5);

            RequestLease otherLease = granted(otherFollower);
            assertThat(slowHead.getFuture()).isNotDone();
            assertThat(controller.pendingReservations()).isOne();
            assertThat(controller.liveRequests()).isEqualTo(2);
            assertThat(controller.rawBytes()).isEqualTo(10);

            slowLease.close();
            RequestLease slowHeadLease = granted(slowHead);
            otherLease.close();
            slowHeadLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testConnectionAllowsOnlyOneWaitingReservation() {
        KafkaProduceAdmissionController controller = controller(2, 10, 1, 10);
        try (TestingConnection connection = new TestingConnection(controller)) {
            RequestLease held = granted(connection.handle.reserve(5));
            Reservation waiting = connection.handle.reserve(5);
            Reservation duplicate = connection.handle.reserve(1);

            assertThat(waiting.getFuture()).isNotDone();
            assertThatThrownBy(() -> duplicate.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(controller.pendingReservations()).isOne();

            assertThat(waiting.getFuture().cancel(false)).isTrue();
            assertThat(controller.pendingReservations()).isZero();
            held.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testImpossibleFrameAndBoundedWaiterQueueFailWithoutAccounting() {
        ManualClock clock = new ManualClock();
        KafkaProduceAdmissionController controller =
                new KafkaProduceAdmissionController(1, 10, 1, 5, clock, 1);
        try (TestingConnection first = new TestingConnection(controller);
                TestingConnection second = new TestingConnection(controller);
                TestingConnection third = new TestingConnection(controller)) {
            Reservation impossible = first.handle.reserve(6);
            assertThatThrownBy(() -> impossible.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(IllegalArgumentException.class);
            assertThat(controller.liveRequests()).isZero();
            assertThat(controller.rawBytes()).isZero();

            RequestLease held = granted(first.handle.reserve(5));
            Reservation queued = second.handle.reserve(5);
            Reservation overflow = third.handle.reserve(1);
            assertThat(controller.pendingReservations()).isOne();
            assertThatThrownBy(() -> overflow.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(RejectedExecutionException.class);

            assertThat(queued.cancel()).isTrue();
            held.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testConnectionCloseCancelsOnlyWaitingReservations() {
        KafkaProduceAdmissionController controller = controller(1, 10, 1, 10);
        TestingConnection first = new TestingConnection(controller);
        try (TestingConnection second = new TestingConnection(controller)) {
            RequestLease held = granted(first.handle.reserve(10));
            Reservation cancelled = first.handle.reserve(1);
            Reservation follower = second.handle.reserve(1);

            first.handle.close();
            assertThatThrownBy(() -> cancelled.getFuture().join())
                    .isInstanceOf(CancellationException.class);
            assertThat(follower.getFuture()).isNotDone();
            assertThat(controller.liveRequests()).isOne();
            assertThat(controller.rawBytes()).isEqualTo(10);

            held.close();
            RequestLease followerLease = granted(follower);
            followerLease.close();
        } finally {
            first.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testGrantCompletesFutureOutsideControllerLock() throws Exception {
        KafkaProduceAdmissionController controller = controller(1, 1, 1, 1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (TestingConnection connection = new TestingConnection(controller)) {
            RequestLease held = granted(connection.handle.reserve(1));
            Reservation waiting = connection.handle.reserve(1);
            CompletableFuture<Boolean> callback =
                    waiting.getFuture()
                            .thenApply(
                                    lease -> {
                                        try {
                                            assertThat(
                                                            executor.submit(
                                                                            controller
                                                                                    ::liveRequests)
                                                                    .get(5, TimeUnit.SECONDS))
                                                    .isOne();
                                        } catch (Exception e) {
                                            throw new AssertionError(
                                                    "Reservation callback ran under controller lock",
                                                    e);
                                        }
                                        lease.close();
                                        return true;
                                    });

            held.close();
            assertThat(callback.get(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
        }
        assertEmpty(controller);
    }

    @Test
    void testGrantVersusTimeoutCancellationRaceDoesNotLeak() throws Exception {
        KafkaProduceAdmissionController controller = controller(1, 1, 1, 1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (TestingConnection connection = new TestingConnection(controller)) {
            for (int i = 0; i < 200; i++) {
                RequestLease held = granted(connection.handle.reserve(1));
                Reservation waiting = connection.handle.reserve(1);
                CountDownLatch start = new CountDownLatch(1);
                Future<Boolean> cancellation =
                        executor.submit(
                                () -> {
                                    start.await();
                                    return waiting.cancel();
                                });
                Future<?> release =
                        executor.submit(
                                () -> {
                                    start.await();
                                    held.close();
                                    return null;
                                });

                start.countDown();
                boolean cancellationWon = cancellation.get(5, TimeUnit.SECONDS);
                release.get(5, TimeUnit.SECONDS);
                if (cancellationWon) {
                    assertThatThrownBy(() -> waiting.getFuture().join())
                            .isInstanceOf(CancellationException.class);
                } else {
                    assertThat(waiting.cancel()).isFalse();
                    granted(waiting).close();
                }
                assertThat(controller.pendingReservations()).isZero();
                assertThat(controller.liveRequests()).isZero();
                assertThat(controller.rawBytes()).isZero();
            }
        } finally {
            executor.shutdownNow();
        }
        assertEmpty(controller);
    }

    @Test
    void testGlobalLiveRequestHighAndLowWatermarks() {
        KafkaProduceAdmissionController controller = controller(4, 1_000, 4, 1_000);
        try (TestingConnection first = new TestingConnection(controller);
                TestingConnection second = new TestingConnection(controller)) {
            RequestLease firstOne = first.handle.acquire(1);
            RequestLease firstTwo = first.handle.acquire(1);
            RequestLease secondOne = second.handle.acquire(1);
            RequestLease secondTwo = second.handle.acquire(1);

            assertThat(controller.liveRequests()).isEqualTo(4);
            assertThat(controller.liveRequestPauseEvents()).isOne();
            assertPausedFor(first, AdmissionPauseReason.LIVE_REQUEST_COUNT);
            assertPausedFor(second, AdmissionPauseReason.LIVE_REQUEST_COUNT);

            firstOne.releaseLive();
            assertThat(controller.liveRequests()).isEqualTo(3);
            assertPausedFor(first, AdmissionPauseReason.LIVE_REQUEST_COUNT);
            assertPausedFor(second, AdmissionPauseReason.LIVE_REQUEST_COUNT);

            firstTwo.releaseLive();
            assertThat(controller.liveRequests()).isEqualTo(2);
            assertResumed(first);
            assertResumed(second);

            firstOne.close();
            firstTwo.close();
            secondOne.close();
            secondTwo.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testConcurrentConnectionCloseAndLeaseReleaseDoesNotLeak() throws Exception {
        final int connectionCount = 8;
        KafkaProduceAdmissionController controller =
                controller(connectionCount, connectionCount * 8L, 4, 64);
        TestingConnection[] connections = new TestingConnection[connectionCount];
        ExecutorService executor = Executors.newFixedThreadPool(connectionCount);
        CountDownLatch acquired = new CountDownLatch(connectionCount);
        CountDownLatch release = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < connectionCount; i++) {
                connections[i] = new TestingConnection(controller);
            }
            for (TestingConnection connection : connections) {
                futures.add(
                        executor.submit(
                                () -> {
                                    RequestLease lease = connection.handle.acquire(8);
                                    acquired.countDown();
                                    try {
                                        release.await();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new AssertionError(
                                                "Interrupted while awaiting release", e);
                                    }
                                    connection.handle.close();
                                    lease.close();
                                }));
            }

            assertThat(acquired.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(controller.registeredConnections()).isEqualTo(connectionCount);
            assertThat(controller.liveRequests()).isEqualTo(connectionCount);
            assertThat(controller.liveBytes()).isEqualTo(connectionCount * 8L);
            assertThat(controller.rawBytes()).isEqualTo(connectionCount * 8L);
            assertThat(controller.pausedConnections()).isEqualTo(connectionCount);
            assertThat(controller.maxLiveRequestOvershoot()).isZero();

            release.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }

            assertEmpty(controller);
        } finally {
            release.countDown();
            executor.shutdownNow();
            for (TestingConnection connection : connections) {
                if (connection != null) {
                    connection.close();
                }
            }
        }
        assertEmpty(controller);
    }

    @Test
    void testGlobalRawBytesHighAndLowWatermarksWithoutOvershoot() {
        KafkaProduceAdmissionController controller = controller(100, 11, 100, 11);
        try (TestingConnection first = new TestingConnection(controller);
                TestingConnection second = new TestingConnection(controller)) {
            RequestLease firstLease = first.handle.acquire(6);
            RequestLease secondLease = second.handle.acquire(5);

            assertThat(controller.rawBytes()).isEqualTo(11);
            assertThat(controller.rawBytesPauseEvents()).isOne();
            assertThat(controller.rawBytesOvershootEvents()).isZero();
            assertThat(controller.maxRawBytesOvershoot()).isZero();
            assertPausedFor(first, AdmissionPauseReason.RAW_BYTES);
            assertPausedFor(second, AdmissionPauseReason.RAW_BYTES);

            secondLease.releaseRaw();
            assertThat(controller.rawBytes()).isEqualTo(6);
            assertPausedFor(first, AdmissionPauseReason.RAW_BYTES);
            assertPausedFor(second, AdmissionPauseReason.RAW_BYTES);

            firstLease.releaseRaw();
            assertThat(controller.rawBytes()).isZero();
            assertResumed(first);
            assertResumed(second);

            firstLease.close();
            secondLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testGlobalPauseIsReestablishedWhenLeaseCloseReentersAdmission() {
        ManualClock clock = new ManualClock();
        KafkaProduceAdmissionController controller =
                new KafkaProduceAdmissionController(2, 1_000, 2, 1_000, clock);
        ReentrantPauseRequestChannel firstRequestChannel = new ReentrantPauseRequestChannel();
        ReentrantPauseRequestChannel secondRequestChannel = new ReentrantPauseRequestChannel();
        EmbeddedChannel firstChannel = new EmbeddedChannel();
        EmbeddedChannel secondChannel = new EmbeddedChannel();
        firstRequestChannel.registerChannel(firstChannel);
        secondRequestChannel.registerChannel(secondChannel);
        ConnectionHandle firstHandle =
                controller.registerConnection(firstChannel, firstRequestChannel);
        ConnectionHandle secondHandle =
                controller.registerConnection(secondChannel, secondRequestChannel);
        RequestLease firstLease = firstHandle.acquire(1);
        RequestLease secondLease = secondHandle.acquire(1);
        AtomicBoolean reentered = new AtomicBoolean();
        AtomicReference<RequestLease> reentrantLease = new AtomicReference<>();
        Runnable resumeAction =
                () -> {
                    if (reentered.compareAndSet(false, true)) {
                        reentrantLease.set(firstHandle.acquire(1));
                    }
                };
        firstRequestChannel.setResumeAction(resumeAction);
        secondRequestChannel.setResumeAction(resumeAction);

        try {
            clock.advanceTime(5, TimeUnit.MILLISECONDS);
            firstLease.releaseLive();

            assertThat(reentrantLease.get()).isNotNull();
            assertThat(controller.liveRequests()).isEqualTo(2);
            assertThat(controller.liveRequestPauseEvents()).isEqualTo(2);
            assertThat(controller.maxLiveRequestOvershoot()).isZero();
            assertThat(controller.cumulativeLiveRequestPauseTimeMicros()).isEqualTo(5_000);
            assertThat(firstRequestChannel.activePauseReasons(firstChannel))
                    .contains(AdmissionPauseReason.LIVE_REQUEST_COUNT);
            assertThat(secondRequestChannel.activePauseReasons(secondChannel))
                    .contains(AdmissionPauseReason.LIVE_REQUEST_COUNT);

            clock.advanceTime(2, TimeUnit.MILLISECONDS);
            assertThat(controller.cumulativeLiveRequestPauseTimeMicros()).isEqualTo(7_000);
        } finally {
            RequestLease acquiredDuringResume = reentrantLease.get();
            if (acquiredDuringResume != null) {
                acquiredDuringResume.close();
            }
            firstLease.close();
            secondLease.close();
            firstHandle.close();
            secondHandle.close();
            firstRequestChannel.unregisterChannel(firstChannel);
            secondRequestChannel.unregisterChannel(secondChannel);
            firstChannel.finishAndReleaseAll();
            secondChannel.finishAndReleaseAll();
        }
        assertEmpty(controller);
    }

    @Test
    void testGlobalPauseCloseIsBestEffortBeforeRethrowing() {
        KafkaProduceAdmissionController controller = controller(2, 1_000, 2, 1_000);
        ThrowingCloseRequestChannel firstRequestChannel = new ThrowingCloseRequestChannel(true);
        ThrowingCloseRequestChannel secondRequestChannel = new ThrowingCloseRequestChannel(false);
        EmbeddedChannel firstChannel = new EmbeddedChannel();
        EmbeddedChannel secondChannel = new EmbeddedChannel();
        firstRequestChannel.registerChannel(firstChannel);
        secondRequestChannel.registerChannel(secondChannel);
        ConnectionHandle firstHandle =
                controller.registerConnection(firstChannel, firstRequestChannel);
        ConnectionHandle secondHandle =
                controller.registerConnection(secondChannel, secondRequestChannel);
        RequestLease firstLease = firstHandle.acquire(1);
        RequestLease secondLease = secondHandle.acquire(1);

        try {
            assertThatThrownBy(firstLease::releaseLive)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("expected close failure");

            assertThat(firstRequestChannel.closeCount).isOne();
            assertThat(secondRequestChannel.closeCount).isOne();
            assertThat(controller.liveRequests()).isOne();
            assertThat(controller.pausedConnections()).isZero();
            assertThat(firstRequestChannel.activePauseReasons(firstChannel)).isEmpty();
            assertThat(secondRequestChannel.activePauseReasons(secondChannel)).isEmpty();
        } finally {
            firstLease.close();
            secondLease.close();
            firstHandle.close();
            secondHandle.close();
            firstRequestChannel.unregisterChannel(firstChannel);
            secondRequestChannel.unregisterChannel(secondChannel);
            firstChannel.finishAndReleaseAll();
            secondChannel.finishAndReleaseAll();
        }
        assertEmpty(controller);
    }

    @Test
    void testPerConnectionLiveRequestLimitIsIsolated() {
        KafkaProduceAdmissionController controller = controller(100, 1_000, 4, 1_000);
        try (TestingConnection pressured = new TestingConnection(controller);
                TestingConnection unaffected = new TestingConnection(controller)) {
            RequestLease one = pressured.handle.acquire(1);
            RequestLease two = pressured.handle.acquire(1);
            RequestLease three = pressured.handle.acquire(1);
            RequestLease four = pressured.handle.acquire(1);

            assertPausedFor(pressured, AdmissionPauseReason.LIVE_REQUEST_COUNT);
            assertResumed(unaffected);
            assertThat(controller.liveRequestPauseEvents()).isOne();

            one.releaseLive();
            assertPausedFor(pressured, AdmissionPauseReason.LIVE_REQUEST_COUNT);
            two.releaseLive();
            assertResumed(pressured);
            assertResumed(unaffected);

            one.close();
            two.close();
            three.close();
            four.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testPerConnectionRawByteLimitIsIsolated() {
        KafkaProduceAdmissionController controller = controller(100, 100, 100, 11);
        try (TestingConnection pressured = new TestingConnection(controller);
                TestingConnection unaffected = new TestingConnection(controller)) {
            RequestLease first = pressured.handle.acquire(6);
            RequestLease second = pressured.handle.acquire(5);

            assertPausedFor(pressured, AdmissionPauseReason.RAW_BYTES);
            assertResumed(unaffected);
            assertThat(controller.rawBytesPauseEvents()).isOne();
            assertThat(controller.rawBytesOvershootEvents()).isZero();

            second.releaseRaw();
            assertPausedFor(pressured, AdmissionPauseReason.RAW_BYTES);
            first.releaseRaw();
            assertResumed(pressured);
            assertResumed(unaffected);

            first.close();
            second.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testOverlappingReasonsResumeIndependently() {
        KafkaProduceAdmissionController controller = controller(2, 11, 2, 11);
        try (TestingConnection connection = new TestingConnection(controller)) {
            RequestLease first = connection.handle.acquire(6);
            RequestLease second = connection.handle.acquire(5);

            assertThat(connection.requestChannel.activePauseReasons(connection.channel))
                    .containsExactlyInAnyOrder(
                            AdmissionPauseReason.LIVE_REQUEST_COUNT,
                            AdmissionPauseReason.RAW_BYTES);
            assertThat(connection.autoRead()).isFalse();

            second.releaseRaw();
            assertThat(connection.requestChannel.activePauseReasons(connection.channel))
                    .containsExactlyInAnyOrder(
                            AdmissionPauseReason.LIVE_REQUEST_COUNT,
                            AdmissionPauseReason.RAW_BYTES);
            first.releaseRaw();
            assertPausedFor(connection, AdmissionPauseReason.LIVE_REQUEST_COUNT);

            first.releaseLive();
            assertResumed(connection);
            first.close();
            second.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testNewConnectionInheritsGlobalPressure() {
        KafkaProduceAdmissionController controller = controller(1, 100, 1, 100);
        try (TestingConnection first = new TestingConnection(controller)) {
            RequestLease lease = first.handle.acquire(1);
            assertPausedFor(first, AdmissionPauseReason.LIVE_REQUEST_COUNT);

            try (TestingConnection inherited = new TestingConnection(controller)) {
                assertPausedFor(inherited, AdmissionPauseReason.LIVE_REQUEST_COUNT);

                lease.releaseLive();
                assertResumed(first);
                assertResumed(inherited);
                lease.close();
            }
        }
        assertEmpty(controller);
    }

    @Test
    void testDisconnectKeepsOutstandingRequestAccounted() {
        KafkaProduceAdmissionController controller = controller(1, 100, 1, 100);
        TestingConnection disconnected = new TestingConnection(controller);
        RequestLease outstanding = disconnected.handle.acquire(7);
        assertPausedFor(disconnected, AdmissionPauseReason.LIVE_REQUEST_COUNT);

        disconnected.handle.close();
        assertThat(disconnected.handle.isRegistered()).isFalse();
        assertThat(controller.registeredConnections()).isZero();
        assertThat(controller.liveRequests()).isOne();
        assertThat(controller.liveBytes()).isEqualTo(7);
        assertThat(controller.rawBytes()).isEqualTo(7);
        assertThat(disconnected.autoRead()).isTrue();
        assertThatThrownBy(() -> disconnected.handle.acquire(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");

        try (TestingConnection inherited = new TestingConnection(controller)) {
            assertPausedFor(inherited, AdmissionPauseReason.LIVE_REQUEST_COUNT);
            outstanding.releaseLive();
            assertResumed(inherited);
            assertThat(controller.liveBytes()).isZero();
            assertThat(controller.rawBytes()).isEqualTo(7);
            outstanding.releaseRaw();
        } finally {
            disconnected.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testRequestLeaseReleasesAreIdempotentAndNeverNegative() {
        KafkaProduceAdmissionController controller = controller(100, 1_000, 100, 1_000);
        try (TestingConnection connection = new TestingConnection(controller)) {
            RequestLease lease = connection.handle.acquire(17);
            assertThat(connection.handle.liveBytes()).isEqualTo(17);
            assertThat(controller.liveBytes()).isEqualTo(17);
            lease.releaseRaw();
            lease.releaseRaw();
            assertThat(connection.handle.liveBytes()).isEqualTo(17);
            assertThat(controller.liveBytes()).isEqualTo(17);
            lease.releaseLive();
            lease.releaseLive();
            lease.close();
            lease.close();

            assertThat(connection.handle.liveRequests()).isZero();
            assertThat(connection.handle.liveBytes()).isZero();
            assertThat(connection.handle.rawBytes()).isZero();
            assertThat(controller.liveRequests()).isZero();
            assertThat(controller.liveBytes()).isZero();
            assertThat(controller.rawBytes()).isZero();
        }
        assertEmpty(controller);
    }

    @Test
    void testOverflowFailsWithoutChangingAccounting() {
        KafkaProduceAdmissionController controller =
                controller(100, Long.MAX_VALUE, 100, Long.MAX_VALUE);
        try (TestingConnection connection = new TestingConnection(controller)) {
            RequestLease retained = connection.handle.acquire(Long.MAX_VALUE);
            assertThat(controller.liveRequests()).isOne();
            assertThat(controller.liveBytes()).isEqualTo(Long.MAX_VALUE);
            assertThat(controller.rawBytes()).isEqualTo(Long.MAX_VALUE);

            assertThatThrownBy(() -> connection.handle.acquire(1))
                    .isInstanceOf(RejectedExecutionException.class);
            assertThat(controller.liveRequests()).isOne();
            assertThat(controller.liveBytes()).isEqualTo(Long.MAX_VALUE);
            assertThat(controller.rawBytes()).isEqualTo(Long.MAX_VALUE);
            assertThat(connection.handle.liveRequests()).isOne();
            assertThat(connection.handle.liveBytes()).isEqualTo(Long.MAX_VALUE);
            assertThat(connection.handle.rawBytes()).isEqualTo(Long.MAX_VALUE);

            retained.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testUpdateLimitsReconcilesImmediatelyAndRecordsPauseDuration() {
        ManualClock clock = new ManualClock();
        KafkaProduceAdmissionController controller =
                new KafkaProduceAdmissionController(10, 100, 10, 100, clock);
        try (TestingConnection connection = new TestingConnection(controller)) {
            RequestLease lease = connection.handle.acquire(7);

            controller.updateLimits(1, 7, 1, 7);

            assertThat(controller.maxLiveRequestsLimit()).isOne();
            assertThat(controller.maxRawBytesLimit()).isEqualTo(7);
            assertThat(controller.maxLiveRequestsPerConnectionLimit()).isOne();
            assertThat(controller.maxRawBytesPerConnectionLimit()).isEqualTo(7);
            assertThat(controller.liveRequestPauseEvents()).isEqualTo(2);
            assertThat(controller.rawBytesPauseEvents()).isEqualTo(2);
            assertThat(connection.requestChannel.activePauseReasons(connection.channel))
                    .containsExactlyInAnyOrder(
                            AdmissionPauseReason.LIVE_REQUEST_COUNT,
                            AdmissionPauseReason.RAW_BYTES);

            clock.advanceTime(5, TimeUnit.MILLISECONDS);
            assertThat(controller.longestActiveLiveRequestPauseTimeMillis()).isEqualTo(5);
            assertThat(controller.longestActiveRawBytesPauseTimeMillis()).isEqualTo(5);
            assertThat(controller.cumulativeLiveRequestPauseTimeMicros()).isEqualTo(10_000);
            assertThat(controller.cumulativeRawBytesPauseTimeMicros()).isEqualTo(10_000);

            controller.updateLimits(10, 100, 10, 100);

            assertThat(controller.maxLiveRequestsLimit()).isEqualTo(10);
            assertThat(controller.maxRawBytesLimit()).isEqualTo(100);
            assertThat(controller.maxLiveRequestsPerConnectionLimit()).isEqualTo(10);
            assertThat(controller.maxRawBytesPerConnectionLimit()).isEqualTo(100);
            assertResumed(connection);
            assertThat(controller.longestActiveLiveRequestPauseTimeMillis()).isZero();
            assertThat(controller.longestActiveRawBytesPauseTimeMillis()).isZero();
            assertThat(controller.cumulativeLiveRequestPauseTimeMicros()).isEqualTo(10_000);
            assertThat(controller.cumulativeRawBytesPauseTimeMicros()).isEqualTo(10_000);

            lease.releaseRaw();
            assertThat(controller.rawBytes()).isZero();
            assertThat(controller.liveBytes()).isEqualTo(7);
            lease.releaseLive();
            assertThat(controller.liveBytes()).isZero();
        }
        assertEmpty(controller);
    }

    @Test
    void testLimitsAreValidated() {
        assertThatThrownBy(() -> controller(0, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> controller(1, 0, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> controller(1, 1, 2, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> controller(1, 1, 1, 2))
                .isInstanceOf(IllegalArgumentException.class);

        KafkaProduceAdmissionController controller = controller(4, 4, 4, 4);
        assertThatThrownBy(() -> controller.updateLimits(3, 3, 4, 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(controller.maxLiveRequestsLimit()).isEqualTo(4);
        assertThat(controller.maxRawBytesLimit()).isEqualTo(4);
    }

    private static KafkaProduceAdmissionController controller(
            long maxLiveRequests,
            long maxRawBytes,
            long maxLiveRequestsPerConnection,
            long maxRawBytesPerConnection) {
        return new KafkaProduceAdmissionController(
                maxLiveRequests,
                maxRawBytes,
                maxLiveRequestsPerConnection,
                maxRawBytesPerConnection);
    }

    private static RequestLease granted(Reservation reservation) {
        return reservation.getFuture().join();
    }

    private static void assertPausedFor(TestingConnection connection, AdmissionPauseReason reason) {
        assertThat(connection.requestChannel.activePauseReasons(connection.channel))
                .contains(reason);
        assertThat(connection.autoRead()).isFalse();
    }

    private static void assertResumed(TestingConnection connection) {
        assertThat(connection.requestChannel.activePauseReasons(connection.channel)).isEmpty();
        assertThat(connection.autoRead()).isTrue();
    }

    private static void assertEmpty(KafkaProduceAdmissionController controller) {
        assertThat(controller.liveRequests()).isZero();
        assertThat(controller.liveBytes()).isZero();
        assertThat(controller.rawBytes()).isZero();
        assertThat(controller.registeredConnections()).isZero();
        assertThat(controller.pausedConnections()).isZero();
        assertThat(controller.pendingReservations()).isZero();
    }

    private static final class TestingConnection implements AutoCloseable {
        private final RequestChannel requestChannel = new RequestChannel(1_000);
        private final EmbeddedChannel channel = new EmbeddedChannel();
        private final ConnectionHandle handle;

        private TestingConnection(KafkaProduceAdmissionController controller) {
            requestChannel.registerChannel(channel);
            handle = controller.registerConnection(channel, requestChannel);
            channel.runPendingTasks();
        }

        private boolean autoRead() {
            channel.runPendingTasks();
            return channel.config().isAutoRead();
        }

        @Override
        public void close() {
            handle.close();
            requestChannel.unregisterChannel(channel);
            channel.runPendingTasks();
            channel.finishAndReleaseAll();
        }
    }

    private static final class ReentrantPauseRequestChannel extends RequestChannel {
        private Runnable resumeAction;

        private ReentrantPauseRequestChannel() {
            super(1_000);
        }

        private void setResumeAction(Runnable resumeAction) {
            this.resumeAction = resumeAction;
        }

        @Override
        public PauseLease pauseChannel(Channel channel, PauseReason reason) {
            PauseLease delegate = super.pauseChannel(channel, reason);
            return () -> {
                delegate.close();
                Runnable action = resumeAction;
                if (action != null) {
                    action.run();
                }
            };
        }
    }

    private static final class ThrowingCloseRequestChannel extends RequestChannel {
        private final boolean failOnClose;
        private int closeCount;

        private ThrowingCloseRequestChannel(boolean failOnClose) {
            super(1_000);
            this.failOnClose = failOnClose;
        }

        @Override
        public PauseLease pauseChannel(Channel channel, PauseReason reason) {
            PauseLease delegate = super.pauseChannel(channel, reason);
            return () -> {
                delegate.close();
                closeCount++;
                if (failOnClose) {
                    throw new IllegalStateException("expected close failure");
                }
            };
        }
    }
}
