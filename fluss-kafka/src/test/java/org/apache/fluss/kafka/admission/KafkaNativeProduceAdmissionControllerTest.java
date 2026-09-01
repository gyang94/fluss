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

import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.AdmissionTimeoutException;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.AdmissionUnavailableException;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.RequestLease;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.RequestTooLargeException;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.Reservation;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaNativeProduceAdmissionControllerTest {

    @Test
    void testExactGlobalAndConnectionReservationsNeverOvershoot() {
        KafkaNativeProduceAdmissionController controller = controller(2, 20, 1, 10, 10);
        try (ConnectionHandle first = controller.registerConnection();
                ConnectionHandle second = controller.registerConnection()) {
            RequestLease firstLease = granted(first.reserve(6));
            RequestLease secondLease = granted(second.reserve(4));
            Reservation waiting = second.reserve(1);

            assertThat(controller.inFlightRequests()).isEqualTo(2);
            assertThat(controller.convertedBytes()).isEqualTo(10);
            assertThat(controller.pendingReservedBytes()).isOne();
            assertThat(controller.totalReservedBytes()).isEqualTo(11);
            assertThat(controller.pendingReservations()).isOne();
            assertThat(first.inFlightRequests()).isOne();
            assertThat(first.convertedBytes()).isEqualTo(6);
            assertThat(second.inFlightRequests()).isOne();
            assertThat(second.convertedBytes()).isEqualTo(4);

            secondLease.close();
            RequestLease waitingLease = granted(waiting);
            assertThat(controller.inFlightRequests()).isEqualTo(2);
            assertThat(controller.convertedBytes()).isEqualTo(7);

            firstLease.close();
            waitingLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testMaterializedWaitersCannotExceedGlobalOrConnectionByteBudget() {
        KafkaNativeProduceAdmissionController controller = controller(1, 100, 1, 70, 10);
        ConnectionHandle saturated = controller.registerConnection();
        ConnectionHandle second = controller.registerConnection();
        ConnectionHandle third = controller.registerConnection();
        ConnectionHandle rejectedConnection = controller.registerConnection();
        try {
            RequestLease held = granted(saturated.reserve(40));
            Reservation sameConnectionWaiter = saturated.reserve(30);
            Reservation perConnectionRejected = saturated.reserve(1);
            Reservation secondWaiter = second.reserve(20);
            Reservation thirdWaiter = third.reserve(10);
            Reservation globallyRejected = rejectedConnection.reserve(1);

            assertThat(sameConnectionWaiter.getFuture()).isNotDone();
            assertThat(secondWaiter.getFuture()).isNotDone();
            assertThat(thirdWaiter.getFuture()).isNotDone();
            assertThatThrownBy(() -> perConnectionRejected.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(AdmissionUnavailableException.class)
                    .hasMessageContaining("pending byte reservation");
            assertThatThrownBy(() -> globallyRejected.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(AdmissionUnavailableException.class)
                    .hasMessageContaining("pending byte reservation");
            assertThat(controller.convertedBytes()).isEqualTo(40);
            assertThat(controller.pendingReservedBytes()).isEqualTo(60);
            assertThat(controller.totalReservedBytes()).isEqualTo(100);
            assertThat(saturated.pendingReservedBytes()).isEqualTo(30);
            assertThat(saturated.totalReservedBytes()).isEqualTo(70);
            assertThat(controller.maxConnectionPendingReservedBytes()).isEqualTo(30);
            assertThat(controller.maxConnectionTotalReservedBytes()).isEqualTo(70);

            assertThat(sameConnectionWaiter.timeout()).isTrue();
            assertThat(controller.pendingReservedBytes()).isEqualTo(30);
            assertThat(controller.totalReservedBytes()).isEqualTo(70);

            held.close();
            RequestLease secondLease = granted(secondWaiter);
            assertThat(thirdWaiter.getFuture()).isNotDone();
            secondLease.close();
            granted(thirdWaiter).close();
        } finally {
            saturated.close();
            second.close();
            third.close();
            rejectedConnection.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testGlobalPressurePreservesFifoForSmallerFollower() {
        KafkaNativeProduceAdmissionController controller = controller(1, 30, 1, 20, 10);
        try (ConnectionHandle holder = controller.registerConnection();
                ConnectionHandle headConnection = controller.registerConnection();
                ConnectionHandle followerConnection = controller.registerConnection()) {
            RequestLease holderLease = granted(holder.reserve(6));
            Reservation head = headConnection.reserve(8);
            Reservation follower = followerConnection.reserve(4);

            assertThat(head.getFuture()).isNotDone();
            assertThat(follower.getFuture()).isNotDone();

            holderLease.close();
            RequestLease headLease = granted(head);
            assertThat(follower.getFuture()).isNotDone();
            assertThat(controller.convertedBytes()).isEqualTo(8);

            headLease.close();
            RequestLease followerLease = granted(follower);
            followerLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testPureConnectionPressureMaySkipHead() {
        KafkaNativeProduceAdmissionController controller = controller(3, 30, 1, 10, 10);
        try (ConnectionHandle slow = controller.registerConnection();
                ConnectionHandle other = controller.registerConnection()) {
            RequestLease slowLease = granted(slow.reserve(5));
            Reservation slowHead = slow.reserve(5);
            Reservation otherFollower = other.reserve(5);

            RequestLease otherLease = granted(otherFollower);
            assertThat(slowHead.getFuture()).isNotDone();
            assertThat(controller.pendingReservations()).isOne();

            slowLease.close();
            RequestLease slowHeadLease = granted(slowHead);
            otherLease.close();
            slowHeadLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testConnectionBlockedHeadPreventsSameConnectionOvertaking() {
        KafkaNativeProduceAdmissionController controller = controller(4, 100, 1, 20, 10);
        try (ConnectionHandle saturated = controller.registerConnection();
                ConnectionHandle other = controller.registerConnection()) {
            RequestLease held = granted(saturated.reserve(6));
            Reservation blockedHead = saturated.reserve(5);
            Reservation sameConnectionFollower = saturated.reserve(4);
            Reservation otherConnectionFollower = other.reserve(4);

            RequestLease otherLease = granted(otherConnectionFollower);
            assertThat(blockedHead.getFuture()).isNotDone();
            assertThat(sameConnectionFollower.getFuture()).isNotDone();
            assertThat(controller.pendingReservations()).isEqualTo(2);

            held.close();
            RequestLease headLease = granted(blockedHead);
            assertThat(sameConnectionFollower.getFuture()).isNotDone();

            headLease.close();
            RequestLease sameConnectionLease = granted(sameConnectionFollower);
            sameConnectionLease.close();
            otherLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testFullQueueDoesNotRejectImmediatelyGrantableOtherConnection() {
        KafkaNativeProduceAdmissionController controller = controller(3, 30, 1, 10, 1);
        try (ConnectionHandle saturated = controller.registerConnection();
                ConnectionHandle other = controller.registerConnection()) {
            RequestLease held = granted(saturated.reserve(5));
            Reservation saturatedWaiter = saturated.reserve(5);
            assertThat(controller.pendingReservations()).isOne();

            Reservation otherReservation = other.reserve(5);
            RequestLease otherLease = granted(otherReservation);
            assertThat(saturatedWaiter.getFuture()).isNotDone();
            assertThat(controller.pendingReservations()).isOne();

            held.close();
            RequestLease saturatedLease = granted(saturatedWaiter);
            saturatedLease.close();
            otherLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testTypedTooLargeAndBoundedQueueFailuresDoNotChangeAccounting() {
        KafkaNativeProduceAdmissionController controller = controller(1, 10, 1, 6, 1);
        try (ConnectionHandle first = controller.registerConnection();
                ConnectionHandle second = controller.registerConnection();
                ConnectionHandle third = controller.registerConnection()) {
            Reservation tooLarge = first.reserve(7);
            assertThatThrownBy(() -> tooLarge.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(RequestTooLargeException.class);
            RequestTooLargeException failure =
                    (RequestTooLargeException) failure(tooLarge.getFuture());
            assertThat(failure.requestedBytes()).isEqualTo(7);
            assertThat(failure.byteLimit()).isEqualTo(6);

            RequestLease held = granted(first.reserve(6));
            Reservation queued = second.reserve(1);
            Reservation queueFull = third.reserve(1);
            assertThatThrownBy(() -> queueFull.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(AdmissionUnavailableException.class)
                    .hasMessageContaining("queue is full");
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.convertedBytes()).isEqualTo(6);
            assertThat(controller.pendingReservations()).isOne();

            queued.cancel();
            held.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testCancellationAndTimeoutRemoveWaiterAndUnblockFollower() {
        KafkaNativeProduceAdmissionController controller = controller(1, 20, 1, 20, 10);
        try (ConnectionHandle holder = controller.registerConnection();
                ConnectionHandle headConnection = controller.registerConnection();
                ConnectionHandle followerConnection = controller.registerConnection()) {
            RequestLease held = granted(holder.reserve(10));
            Reservation cancelled = headConnection.reserve(1);
            Reservation timedOut = followerConnection.reserve(1);

            assertThat(cancelled.getFuture().cancel(false)).isTrue();
            assertThat(cancelled.cancel()).isFalse();
            assertThatThrownBy(() -> cancelled.getFuture().join())
                    .isInstanceOf(CancellationException.class);
            assertThat(timedOut.timeout()).isTrue();
            assertThat(timedOut.timeout()).isFalse();
            assertThatThrownBy(() -> timedOut.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(AdmissionTimeoutException.class);
            assertThat(controller.pendingReservations()).isZero();

            held.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testDisconnectCancelsWaitersButRetainsGrantedLease() {
        KafkaNativeProduceAdmissionController controller = controller(1, 10, 1, 10, 10);
        ConnectionHandle first = controller.registerConnection();
        try (ConnectionHandle follower = controller.registerConnection()) {
            RequestLease held = granted(first.reserve(8));
            Reservation cancelled = first.reserve(1);
            Reservation waitingFollower = follower.reserve(1);

            first.close();
            assertThat(first.isClosed()).isTrue();
            assertThat(controller.registeredConnections()).isOne();
            assertThatThrownBy(() -> cancelled.getFuture().join())
                    .isInstanceOf(CancellationException.class);
            assertThat(waitingFollower.getFuture()).isNotDone();
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.convertedBytes()).isEqualTo(8);
            assertThat(controller.maxConnectionConvertedBytes()).isEqualTo(8);
            assertThat(held.tryMarkSubmitted()).isFalse();

            Reservation afterClose = first.reserve(1);
            assertThatThrownBy(() -> afterClose.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(AdmissionUnavailableException.class)
                    .hasMessageContaining("connection is closed");

            held.close();
            RequestLease followerLease = granted(waitingFollower);
            followerLease.close();
        } finally {
            first.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testConnectionCloseReportsInflightExactlyOnce() {
        KafkaNativeProduceAdmissionController controller = controller(1, 10, 1, 10, 10);
        ConnectionHandle connection = controller.registerConnection();
        RequestLease lease = granted(connection.reserve(5));

        assertThat(connection.closeAndGetHadInFlightRequests()).isTrue();
        assertThat(connection.closeAndGetHadInFlightRequests()).isFalse();
        connection.close();
        assertThat(controller.registeredConnections()).isZero();
        assertThat(controller.inFlightRequests()).isOne();

        lease.close();
        assertEmpty(controller);
    }

    @Test
    void testControllerCloseCancelsWaitersButRetainsGrantedLeases() {
        KafkaNativeProduceAdmissionController controller = controller(1, 10, 1, 10, 10);
        ConnectionHandle holder = controller.registerConnection();
        ConnectionHandle waiter = controller.registerConnection();
        RequestLease held = granted(holder.reserve(8));
        Reservation waiting = waiter.reserve(1);

        controller.close();
        controller.close();

        assertThat(controller.isClosed()).isTrue();
        assertThat(controller.registeredConnections()).isZero();
        assertThat(holder.isClosed()).isTrue();
        assertThat(waiter.isClosed()).isTrue();
        assertThatThrownBy(() -> waiting.getFuture().join())
                .isInstanceOf(CancellationException.class);
        assertThat(controller.inFlightRequests()).isOne();
        assertThat(controller.convertedBytes()).isEqualTo(8);
        assertThatThrownBy(controller::registerConnection)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("controller is closed");

        held.close();
        assertThat(controller.inFlightRequests()).isZero();
        assertThat(controller.convertedBytes()).isZero();
        assertThat(controller.maxConnectionInFlightRequests()).isZero();
        assertThat(controller.maxConnectionConvertedBytes()).isZero();
    }

    @Test
    void testPendingTokenCleanupPrecedesEveryPendingTokenRelease() {
        KafkaNativeProduceAdmissionController controller = controller(1, 100, 1, 100, 1);
        ConnectionHandle holder = controller.registerConnection();
        RequestLease held = granted(holder.reserve(1));

        ConnectionHandle timedOutConnection = controller.registerConnection();
        assertPendingCleanup(
                controller,
                timedOutConnection,
                2,
                reservation -> assertThat(reservation.timeout()).isTrue());

        ConnectionHandle cancelledConnection = controller.registerConnection();
        assertPendingCleanup(
                controller,
                cancelledConnection,
                3,
                reservation -> assertThat(reservation.cancel()).isTrue());

        ConnectionHandle futureCancelledConnection = controller.registerConnection();
        assertPendingCleanup(
                controller,
                futureCancelledConnection,
                4,
                reservation -> assertThat(reservation.getFuture().cancel(false)).isTrue());

        ConnectionHandle closedConnection = controller.registerConnection();
        assertPendingCleanup(
                controller, closedConnection, 5, reservation -> closedConnection.close());

        AtomicInteger grantedCleanupCalls = new AtomicInteger();
        ConnectionHandle grantedConnection = controller.registerConnection();
        Reservation eventuallyGranted =
                grantedConnection.reserve(6, grantedCleanupCalls::incrementAndGet);
        AtomicInteger overflowCleanupCalls = new AtomicInteger();
        AtomicBoolean overflowSawToken = new AtomicBoolean();
        ConnectionHandle overflowConnection = controller.registerConnection();
        Reservation overflow =
                overflowConnection.reserve(
                        7,
                        () -> {
                            overflowCleanupCalls.incrementAndGet();
                            overflowSawToken.set(
                                    controller.pendingReservedBytes() >= 7
                                            && overflowConnection.pendingReservedBytes() >= 7);
                        });
        assertThatThrownBy(() -> overflow.getFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(AdmissionUnavailableException.class)
                .hasMessageContaining("queue is full");
        assertThat(overflowCleanupCalls).hasValue(1);
        assertThat(overflowSawToken).isTrue();

        held.close();
        RequestLease grantedLease = granted(eventuallyGranted);
        assertThat(grantedCleanupCalls).hasValue(0);
        grantedLease.close();

        RequestLease closingLease = granted(holder.reserve(1));
        AtomicInteger controllerCloseCleanupCalls = new AtomicInteger();
        AtomicBoolean controllerCloseSawToken = new AtomicBoolean();
        ConnectionHandle controllerCloseConnection = controller.registerConnection();
        Reservation cancelledByController =
                controllerCloseConnection.reserve(
                        8,
                        () -> {
                            controllerCloseCleanupCalls.incrementAndGet();
                            controllerCloseSawToken.set(
                                    controller.pendingReservedBytes() >= 8
                                            && controllerCloseConnection.pendingReservedBytes()
                                                    >= 8);
                        });
        controller.close();
        assertThatThrownBy(() -> cancelledByController.getFuture().join())
                .isInstanceOf(CancellationException.class);
        assertThat(controllerCloseCleanupCalls).hasValue(1);
        assertThat(controllerCloseSawToken).isTrue();
        closingLease.close();
        assertEmpty(controller);
    }

    @Test
    void testSubmitBoundaryIsAtomicWithDisconnect() {
        KafkaNativeProduceAdmissionController controller = controller(1, 10, 1, 10, 10);
        ConnectionHandle connection = controller.registerConnection();
        RequestLease lease = granted(connection.reserve(5));

        assertThat(lease.tryMarkSubmitted()).isTrue();
        assertThat(lease.tryMarkSubmitted()).isTrue();
        assertThatThrownBy(() -> lease.resize(6)).isInstanceOf(IllegalStateException.class);
        connection.close();
        assertThat(connection.isClosed()).isTrue();
        assertThat(lease.tryMarkSubmitted()).isTrue();
        assertThat(controller.inFlightRequests()).isOne();
        assertThat(controller.convertedBytes()).isEqualTo(5);

        lease.close();
        assertThat(lease.tryMarkSubmitted()).isFalse();
        assertEmpty(controller);
    }

    @Test
    void testResizeWinsConcurrentLeaseCloseAtControllerLock() throws Exception {
        CountDownLatch mutationEntered = new CountDownLatch(1);
        CountDownLatch allowMutation = new CountDownLatch(1);
        AtomicInteger hookInvocations = new AtomicInteger();
        KafkaNativeProduceAdmissionController controller =
                controller(
                        1,
                        10,
                        1,
                        10,
                        10,
                        blockFirstMutation(hookInvocations, mutationEntered, allowMutation));
        ConnectionHandle connection = controller.registerConnection();
        RequestLease lease = granted(connection.reserve(4));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicReference<Thread> closeThread = new AtomicReference<>();
        CountDownLatch closeStarted = new CountDownLatch(1);
        try {
            Future<?> resize = executor.submit(() -> lease.resize(7));
            assertThat(mutationEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<?> close =
                    executor.submit(
                            () -> {
                                closeThread.set(Thread.currentThread());
                                closeStarted.countDown();
                                lease.close();
                            });
            assertThat(closeStarted.await(5, TimeUnit.SECONDS)).isTrue();
            waitUntilBlocked(closeThread.get());

            allowMutation.countDown();
            resize.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);

            assertThat(lease.reservedBytes()).isZero();
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.convertedBytes()).isZero();
        } finally {
            allowMutation.countDown();
            executor.shutdownNow();
            connection.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testDisconnectWinsConcurrentSubmitMarkAtControllerLock() throws Exception {
        CountDownLatch mutationEntered = new CountDownLatch(1);
        CountDownLatch allowMutation = new CountDownLatch(1);
        AtomicInteger hookInvocations = new AtomicInteger();
        KafkaNativeProduceAdmissionController controller =
                controller(
                        1,
                        10,
                        1,
                        10,
                        10,
                        blockFirstMutation(hookInvocations, mutationEntered, allowMutation));
        ConnectionHandle connection = controller.registerConnection();
        RequestLease lease = granted(connection.reserve(4));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicReference<Thread> submitThread = new AtomicReference<>();
        CountDownLatch submitStarted = new CountDownLatch(1);
        try {
            Future<Boolean> disconnect =
                    executor.submit(connection::closeAndGetHadInFlightRequests);
            assertThat(mutationEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<Boolean> submit =
                    executor.submit(
                            () -> {
                                submitThread.set(Thread.currentThread());
                                submitStarted.countDown();
                                return lease.tryMarkSubmitted();
                            });
            assertThat(submitStarted.await(5, TimeUnit.SECONDS)).isTrue();
            waitUntilBlocked(submitThread.get());

            allowMutation.countDown();
            assertThat(disconnect.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(submit.get(5, TimeUnit.SECONDS)).isFalse();
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.convertedBytes()).isEqualTo(4);

            lease.close();
        } finally {
            allowMutation.countDown();
            executor.shutdownNow();
            connection.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testResizeUsesExactActualBytesAndAccountsForPendingTokens() {
        KafkaNativeProduceAdmissionController controller = controller(1, 10, 1, 10, 10);
        try (ConnectionHandle first = controller.registerConnection();
                ConnectionHandle second = controller.registerConnection()) {
            RequestLease firstLease = granted(first.reserve(5));
            Reservation waiting = second.reserve(4);

            assertThat(firstLease.estimatedBytes()).isEqualTo(5);
            assertThat(firstLease.reservedBytes()).isEqualTo(5);
            assertThat(controller.pendingReservedBytes()).isEqualTo(4);
            assertThat(controller.totalReservedBytes()).isEqualTo(9);
            firstLease.resize(4);
            assertThat(waiting.getFuture()).isNotDone();
            assertThat(controller.convertedBytes()).isEqualTo(4);
            assertThat(controller.pendingReservedBytes()).isEqualTo(4);
            assertThat(controller.totalReservedBytes()).isEqualTo(8);

            Reservation unavailable = second.reserve(3);
            assertThatThrownBy(() -> unavailable.getFuture().join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(AdmissionUnavailableException.class)
                    .hasMessageContaining("pending byte reservation");
            assertThat(controller.totalReservedBytes()).isEqualTo(8);

            firstLease.close();
            firstLease.close();
            RequestLease waitingLease = granted(waiting);
            assertThat(controller.convertedBytes()).isEqualTo(4);
            assertThat(controller.pendingReservedBytes()).isZero();
            waitingLease.resize(9);
            assertThat(waitingLease.reservedBytes()).isEqualTo(9);
            waitingLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testFailedResizeLeavesOriginalReservationUnchanged() {
        KafkaNativeProduceAdmissionController controller = controller(2, 10, 2, 8, 10);
        try (ConnectionHandle first = controller.registerConnection();
                ConnectionHandle second = controller.registerConnection()) {
            RequestLease firstLease = granted(first.reserve(4));
            RequestLease secondLease = granted(second.reserve(6));

            assertThatThrownBy(() -> firstLease.resize(5))
                    .isInstanceOf(AdmissionUnavailableException.class)
                    .hasMessageContaining("not currently available");
            assertThat(firstLease.reservedBytes()).isEqualTo(4);
            assertThat(controller.convertedBytes()).isEqualTo(10);

            assertThatThrownBy(() -> firstLease.resize(9))
                    .isInstanceOf(RequestTooLargeException.class);
            assertThat(firstLease.reservedBytes()).isEqualTo(4);
            assertThat(first.convertedBytes()).isEqualTo(4);

            firstLease.close();
            secondLease.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testFutureCallbacksAndQueueDrainRunOutsideControllerLock() throws Exception {
        KafkaNativeProduceAdmissionController controller = controller(1, 2, 1, 2, 10);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ConnectionHandle connection = controller.registerConnection()) {
            RequestLease held = granted(connection.reserve(1));
            Reservation waiting = connection.reserve(1);
            CompletableFuture<Boolean> callback =
                    waiting.getFuture()
                            .thenApply(
                                    lease -> {
                                        try {
                                            assertThat(
                                                            executor.submit(
                                                                            controller
                                                                                    ::inFlightRequests)
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
    void testReservationFutureRejectsExternalCompletion() {
        KafkaNativeProduceAdmissionController controller = controller(1, 2, 1, 2, 10);
        try (ConnectionHandle holder = controller.registerConnection();
                ConnectionHandle waiter = controller.registerConnection()) {
            RequestLease held = granted(holder.reserve(1));
            Reservation reservation = waiter.reserve(1);
            CompletableFuture<RequestLease> future = reservation.getFuture();

            assertThat(future.complete(null)).isFalse();
            assertThat(future.completeExceptionally(new RuntimeException("external"))).isFalse();
            assertThatThrownBy(() -> future.obtrudeValue(null))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> future.obtrudeException(new RuntimeException("external")))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThat(future).isNotDone();
            assertThat(controller.pendingReservations()).isOne();

            held.close();
            granted(reservation).close();
        }
        assertEmpty(controller);
    }

    @Test
    void testFailedInternalGrantCompletionImmediatelyReleasesLease() throws Exception {
        KafkaNativeProduceAdmissionController controller = controller(1, 2, 1, 2, 10);
        try (ConnectionHandle holder = controller.registerConnection();
                ConnectionHandle waiter = controller.registerConnection()) {
            RequestLease held = granted(holder.reserve(1));
            Reservation reservation = waiter.reserve(1);
            CompletableFuture<RequestLease> future = reservation.getFuture();
            RuntimeException injectedFailure = new RuntimeException("injected completion");
            Method completeFailure =
                    future.getClass().getDeclaredMethod("completeFailure", Throwable.class);
            completeFailure.setAccessible(true);

            assertThat(completeFailure.invoke(future, injectedFailure)).isEqualTo(true);
            assertThatThrownBy(future::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCause(injectedFailure);

            held.close();
            assertThat(controller.pendingReservations()).isZero();
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.convertedBytes()).isZero();
        }
        assertEmpty(controller);
    }

    @Test
    void testGrantCancellationRaceDoesNotLeak() throws Exception {
        KafkaNativeProduceAdmissionController controller = controller(1, 2, 1, 2, 10);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (ConnectionHandle connection = controller.registerConnection()) {
            for (int i = 0; i < 200; i++) {
                RequestLease held = granted(connection.reserve(1));
                AtomicBoolean payloadOwned = new AtomicBoolean(true);
                AtomicInteger cleanupCalls = new AtomicInteger();
                Reservation waiting =
                        connection.reserve(
                                1,
                                () -> {
                                    cleanupCalls.incrementAndGet();
                                    payloadOwned.set(false);
                                });
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
                    assertThat(cleanupCalls).hasValue(1);
                    assertThat(payloadOwned).isFalse();
                } else {
                    assertThat(cleanupCalls).hasValue(0);
                    assertThat(payloadOwned).isTrue();
                    payloadOwned.set(false);
                    granted(waiting).close();
                }
                assertThat(controller.pendingReservations()).isZero();
                assertThat(controller.inFlightRequests()).isZero();
                assertThat(controller.convertedBytes()).isZero();
            }
        } finally {
            executor.shutdownNow();
        }
        assertEmpty(controller);
    }

    @Test
    void testGrantConnectionCloseRacePreservesEitherPayloadOwner() throws Exception {
        KafkaNativeProduceAdmissionController controller = controller(1, 2, 1, 2, 10);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ConnectionHandle holderConnection = controller.registerConnection();
        try {
            for (int i = 0; i < 200; i++) {
                RequestLease held = granted(holderConnection.reserve(1));
                ConnectionHandle waitingConnection = controller.registerConnection();
                AtomicBoolean payloadOwned = new AtomicBoolean(true);
                AtomicInteger cleanupCalls = new AtomicInteger();
                Reservation waiting =
                        waitingConnection.reserve(
                                1,
                                () -> {
                                    cleanupCalls.incrementAndGet();
                                    payloadOwned.set(false);
                                });
                CountDownLatch start = new CountDownLatch(1);
                Future<?> release =
                        executor.submit(
                                () -> {
                                    start.await();
                                    held.close();
                                    return null;
                                });
                Future<?> disconnect =
                        executor.submit(
                                () -> {
                                    start.await();
                                    waitingConnection.close();
                                    return null;
                                });

                start.countDown();
                release.get(5, TimeUnit.SECONDS);
                disconnect.get(5, TimeUnit.SECONDS);
                if (waiting.getFuture().isCancelled()) {
                    assertThat(cleanupCalls).hasValue(1);
                    assertThat(payloadOwned).isFalse();
                } else {
                    RequestLease lease = granted(waiting);
                    assertThat(cleanupCalls).hasValue(0);
                    assertThat(payloadOwned).isTrue();
                    payloadOwned.set(false);
                    lease.close();
                }
                assertThat(controller.pendingReservations()).isZero();
                assertThat(controller.inFlightRequests()).isZero();
                assertThat(controller.totalReservedBytes()).isZero();
            }
        } finally {
            executor.shutdownNow();
            holderConnection.close();
        }
        assertEmpty(controller);
    }

    @Test
    void testConstructorAndByteArgumentsAreValidated() {
        assertThatThrownBy(() -> controller(0, 1, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> controller(1, 0, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> controller(1, 1, 2, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> controller(1, 1, 1, 2, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> controller(1, 1, 1, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);

        KafkaNativeProduceAdmissionController controller = controller(1, 1, 1, 1, 1);
        try (ConnectionHandle connection = controller.registerConnection()) {
            assertThatThrownBy(() -> connection.reserve(-1))
                    .isInstanceOf(IllegalArgumentException.class);
            RequestLease lease = granted(connection.reserve(1));
            assertThatThrownBy(() -> lease.resize(-1)).isInstanceOf(IllegalArgumentException.class);
            lease.close();
            assertThatThrownBy(() -> lease.resize(0)).isInstanceOf(IllegalStateException.class);
        }
        assertEmpty(controller);
    }

    private static KafkaNativeProduceAdmissionController controller(
            long globalRequests,
            long globalBytes,
            long connectionRequests,
            long connectionBytes,
            int pendingReservations) {
        return new KafkaNativeProduceAdmissionController(
                globalRequests,
                globalBytes,
                connectionRequests,
                connectionBytes,
                pendingReservations);
    }

    private static KafkaNativeProduceAdmissionController controller(
            long globalRequests,
            long globalBytes,
            long connectionRequests,
            long connectionBytes,
            int pendingReservations,
            Runnable admissionMutationHookForTesting) {
        return new KafkaNativeProduceAdmissionController(
                globalRequests,
                globalBytes,
                connectionRequests,
                connectionBytes,
                pendingReservations,
                admissionMutationHookForTesting);
    }

    private static Runnable blockFirstMutation(
            AtomicInteger invocations, CountDownLatch entered, CountDownLatch release) {
        return () -> {
            if (invocations.incrementAndGet() == 1) {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting to release admission mutation");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Admission mutation hook was interrupted", e);
                }
            }
        };
    }

    private static void assertPendingCleanup(
            KafkaNativeProduceAdmissionController controller,
            ConnectionHandle connection,
            long bytes,
            ReservationTerminator terminator) {
        AtomicInteger cleanupCalls = new AtomicInteger();
        AtomicBoolean cleanupSawToken = new AtomicBoolean();
        Reservation reservation =
                connection.reserve(
                        bytes,
                        () -> {
                            cleanupCalls.incrementAndGet();
                            cleanupSawToken.set(
                                    controller.pendingReservedBytes() >= bytes
                                            && connection.pendingReservedBytes() >= bytes);
                        });
        terminator.terminate(reservation);
        assertThat(cleanupCalls).hasValue(1);
        assertThat(cleanupSawToken).isTrue();
        assertThat(reservation.ownsByteReservation()).isFalse();
    }

    private static void waitUntilBlocked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertThat(thread.getState()).isEqualTo(Thread.State.BLOCKED);
    }

    private static RequestLease granted(Reservation reservation) {
        return reservation.getFuture().join();
    }

    private static Throwable failure(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("Expected future to fail");
        } catch (CompletionException failure) {
            return failure.getCause();
        }
    }

    private static void assertEmpty(KafkaNativeProduceAdmissionController controller) {
        assertThat(controller.inFlightRequests()).isZero();
        assertThat(controller.convertedBytes()).isZero();
        assertThat(controller.pendingReservedBytes()).isZero();
        assertThat(controller.totalReservedBytes()).isZero();
        assertThat(controller.pendingReservations()).isZero();
        assertThat(controller.registeredConnections()).isZero();
        assertThat(controller.maxConnectionInFlightRequests()).isZero();
        assertThat(controller.maxConnectionConvertedBytes()).isZero();
        assertThat(controller.maxConnectionPendingReservedBytes()).isZero();
        assertThat(controller.maxConnectionTotalReservedBytes()).isZero();
    }

    private interface ReservationTerminator {
        void terminate(Reservation reservation);
    }
}
