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

import org.apache.fluss.kafka.network.KafkaFrameAdmission;
import org.apache.fluss.kafka.network.KafkaFrameAdmissionLease;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseLease;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseReason;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaRequestAdmissionControllerTest {

    @Test
    void testLanesAreLazyAndControlRemainsAvailableUnderProducePressure() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(produce, control, 10);

        try (TestingConnection first = new TestingConnection(controller);
                TestingConnection second = new TestingConnection(controller)) {
            assertThat(controller.connections()).isEqualTo(2);
            assertThat(produce.registeredConnections()).isZero();
            assertThat(control.registeredConnections()).isZero();

            KafkaFrameAdmissionLease produceLease =
                    granted(first.admission.reserve(first.channel, 10, ApiKeys.PRODUCE.id));
            KafkaFrameAdmission.Reservation waitingProduce =
                    second.admission.reserve(second.channel, 1, ApiKeys.PRODUCE.id);

            assertThat(produce.registeredConnections()).isEqualTo(2);
            assertThat(produce.liveRequests()).isOne();
            assertThat(produce.pendingReservations()).isOne();
            assertThat(control.registeredConnections()).isZero();

            KafkaFrameAdmissionLease controlLease =
                    granted(second.admission.reserve(second.channel, 10, ApiKeys.API_VERSIONS.id));
            assertThat(control.registeredConnections()).isOne();
            assertThat(control.liveRequests()).isOne();
            assertThat(waitingProduce.getFuture()).isNotDone();

            controlLease.close();
            produceLease.releaseFrameBytes();
            assertThat(waitingProduce.getFuture()).isNotDone();
            produceLease.releaseRequest();
            granted(waitingProduce).close();
            produceLease.close();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testUnknownApiKeyUsesControlLane() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(produce, control, 1);

        try (TestingConnection connection = new TestingConnection(controller)) {
            KafkaFrameAdmissionLease lease =
                    granted(connection.admission.reserve(connection.channel, 1, Short.MAX_VALUE));
            assertThat(produce.registeredConnections()).isZero();
            assertThat(control.registeredConnections()).isOne();
            assertThat(control.liveRequests()).isOne();
            lease.close();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testControlFrameAtExactLimitIsAdmitted() {
        KafkaProduceAdmissionController produce = lane(1, 100, 1, 100);
        KafkaProduceAdmissionController control = lane(1, 100, 1, 100);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(produce, control, 1, 10L);

        try (TestingConnection connection = new TestingConnection(controller)) {
            KafkaFrameAdmissionLease lease =
                    granted(
                            connection.admission.reserve(
                                    connection.channel, 10, ApiKeys.METADATA.id));

            assertThat(controller.controlFrameLimit()).isEqualTo(10);
            assertThat(control.registeredConnections()).isOne();
            assertThat(control.liveRequests()).isOne();
            assertThat(control.rawBytes()).isEqualTo(10);
            lease.close();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testOversizedControlFrameIsRejectedBeforeLaneRegistrationOrQueueing() {
        KafkaProduceAdmissionController produce = lane(1, 100, 1, 100);
        KafkaProduceAdmissionController control = lane(1, 100, 1, 100);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(produce, control, 1, 10L);

        try (TestingConnection connection = new TestingConnection(controller)) {
            assertThatThrownBy(
                            () ->
                                    connection.admission.reserve(
                                            connection.channel, 11, ApiKeys.API_VERSIONS.id))
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasMessageContaining("control-plane frame size 11")
                    .hasMessageContaining("limit 10");

            assertThat(control.registeredConnections()).isZero();
            assertThat(control.pendingReservations()).isZero();
            assertThat(control.liveRequests()).isZero();
            assertThat(control.rawBytes()).isZero();

            KafkaFrameAdmissionLease produceLease =
                    granted(
                            connection.admission.reserve(
                                    connection.channel, 100, ApiKeys.PRODUCE.id));
            assertThat(produce.liveRequests()).isOne();
            assertThat(produce.rawBytes()).isEqualTo(100);
            produceLease.close();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testControlFrameLimitMustContainKafkaProbe() {
        KafkaProduceAdmissionController produce = lane(1, 100, 1, 100);
        KafkaProduceAdmissionController control = lane(1, 100, 1, 100);

        assertThatThrownBy(() -> new KafkaRequestAdmissionController(produce, control, 1, 5L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxControlFrameBytes must be at least 6");
    }

    @Test
    void testConnectionLimitRejectsWithoutRegisteringEitherLaneAndRecovers() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        AtomicInteger rejectionNotifications = new AtomicInteger();
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(
                        produce, control, 1, rejectionNotifications::incrementAndGet);

        TestingConnection accepted = new TestingConnection(controller);
        TestingConnection rejected = new TestingConnection(controller, false);
        try {
            assertThatThrownBy(rejected::activate)
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasMessageContaining("connection limit");
            assertThat(controller.connections()).isOne();
            assertThat(controller.connectionLimit()).isOne();
            assertThat(controller.connectionRejections()).isOne();
            assertThat(rejectionNotifications).hasValue(1);
            assertThat(produce.registeredConnections()).isZero();
            assertThat(control.registeredConnections()).isZero();

            rejected.close();
            accepted.close();
            try (TestingConnection replacement = new TestingConnection(controller)) {
                assertThat(controller.connections()).isOne();
            }
        } finally {
            rejected.close();
            accepted.close();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testClosingConnectionCancelsPendingReservationsInEveryUsedLane() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(produce, control, 10);

        try (TestingConnection holder = new TestingConnection(controller)) {
            KafkaFrameAdmissionLease heldProduce =
                    granted(holder.admission.reserve(holder.channel, 10, ApiKeys.PRODUCE.id));
            KafkaFrameAdmissionLease heldControl =
                    granted(holder.admission.reserve(holder.channel, 10, ApiKeys.METADATA.id));

            TestingConnection waiting = new TestingConnection(controller);
            KafkaFrameAdmission.Reservation produceWaiter =
                    waiting.admission.reserve(waiting.channel, 1, ApiKeys.PRODUCE.id);
            KafkaFrameAdmission.Reservation controlWaiter =
                    waiting.admission.reserve(waiting.channel, 1, ApiKeys.API_VERSIONS.id);
            waiting.close();

            assertCancelled(produceWaiter);
            assertCancelled(controlWaiter);
            assertThat(produce.pendingReservations()).isZero();
            assertThat(control.pendingReservations()).isZero();
            assertThat(controller.connections()).isOne();

            heldProduce.close();
            heldControl.close();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testControllerCloseCancelsWaitingReservations() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(produce, control, 10);

        try (TestingConnection holder = new TestingConnection(controller);
                TestingConnection waiting = new TestingConnection(controller)) {
            KafkaFrameAdmissionLease held =
                    granted(holder.admission.reserve(holder.channel, 10, ApiKeys.PRODUCE.id));
            KafkaFrameAdmission.Reservation waiter =
                    waiting.admission.reserve(waiting.channel, 1, ApiKeys.PRODUCE.id);

            controller.close();

            assertCancelled(waiter);
            assertThat(controller.connections()).isZero();
            assertThat(produce.registeredConnections()).isZero();
            assertThat(produce.pendingReservations()).isZero();
            assertThat(produce.liveRequests()).isOne();
            assertThat(produce.rawBytes()).isEqualTo(10);

            held.close();
        }

        controller.close();
        assertEmpty(controller, produce, control);
    }

    @Test
    void testControllerCloseDoesNotReleaseGrantedRequestBeforeItsTerminalOwners() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(produce, control, 10);

        try (TestingConnection connection = new TestingConnection(controller)) {
            KafkaFrameAdmissionLease lease =
                    granted(
                            connection.admission.reserve(
                                    connection.channel, 10, ApiKeys.PRODUCE.id));

            controller.close();

            assertThat(controller.connections()).isZero();
            assertThat(produce.registeredConnections()).isZero();
            assertThat(produce.liveRequests()).isOne();
            assertThat(produce.rawBytes()).isEqualTo(10);

            lease.releaseFrameBytes();
            assertThat(produce.rawBytes()).isZero();
            assertThat(produce.liveRequests()).isOne();
            assertThat(produce.liveBytes()).isEqualTo(10);

            lease.releaseRequest();
            assertLaneEmpty(produce);
            lease.close();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testChannelActivationIsRejectedAfterControllerClose() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        AtomicInteger rejectionNotifications = new AtomicInteger();
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(
                        produce, control, 10, rejectionNotifications::incrementAndGet);

        TestingConnection connection = new TestingConnection(controller, false);
        try {
            controller.close();

            assertThatThrownBy(connection::activate)
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasMessageContaining("controller is closed");
            assertThat(controller.connections()).isZero();
            assertThat(controller.connectionRejections()).isZero();
            assertThat(rejectionNotifications).hasValue(0);
        } finally {
            connection.close();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testControllerCloseAttemptsEveryRegistrationWhenOneHandleCloseFails() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(produce, control, 10);
        RequestChannel pressureRequestChannel = new RequestChannel(100);
        EmbeddedChannel pressureChannel = new EmbeddedChannel();
        pressureRequestChannel.registerChannel(pressureChannel);
        KafkaProduceAdmissionController.ConnectionHandle pressureHandle =
                produce.registerConnection(pressureChannel, pressureRequestChannel);
        KafkaProduceAdmissionController.RequestLease pressureLease = pressureHandle.acquire(10);
        TestingConnection normal = new TestingConnection(controller);
        TestingConnection failing =
                new TestingConnection(controller, new ThrowingClosePauseRequestChannel());
        KafkaFrameAdmission.Reservation normalWaiter =
                normal.admission.reserve(normal.channel, 1, ApiKeys.PRODUCE.id);
        KafkaFrameAdmission.Reservation failingWaiter =
                failing.admission.reserve(failing.channel, 1, ApiKeys.PRODUCE.id);

        try {
            assertThatThrownBy(controller::close)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("pause close failure");

            assertCancelled(normalWaiter);
            assertCancelled(failingWaiter);
            assertThat(controller.connections()).isZero();
            assertThat(produce.registeredConnections()).isOne();
            assertThat(produce.pendingReservations()).isZero();
        } finally {
            normal.close();
            failing.close();
            pressureLease.close();
            pressureHandle.close();
            pressureRequestChannel.unregisterChannel(pressureChannel);
            pressureChannel.runPendingTasks();
            pressureChannel.finishAndReleaseAll();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testLazyLaneRegistrationFailureDoesNotInstallHandleOrLeakOnClose() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(produce, control, 10);
        ThrowingPauseRequestChannel requestChannel = new ThrowingPauseRequestChannel();
        EmbeddedChannel channel = new EmbeddedChannel();
        requestChannel.registerChannel(channel);
        KafkaFrameAdmission admission = controller.createConnectionAdmission(requestChannel);
        admission.channelActive(channel);

        KafkaProduceAdmissionController.ConnectionHandle pressureHandle = null;
        KafkaProduceAdmissionController.RequestLease pressureLease = null;
        RequestChannel pressureRequestChannel = new RequestChannel(100);
        EmbeddedChannel pressureChannel = new EmbeddedChannel();
        try {
            pressureRequestChannel.registerChannel(pressureChannel);
            pressureHandle = control.registerConnection(pressureChannel, pressureRequestChannel);
            pressureLease = pressureHandle.acquire(10);

            assertThatThrownBy(() -> admission.reserve(channel, 1, ApiKeys.API_VERSIONS.id))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("pause failure");
            assertThat(controller.connections()).isOne();
            assertThat(control.registeredConnections()).isOne();
            assertThat(produce.registeredConnections()).isZero();
        } finally {
            admission.close();
            requestChannel.unregisterChannel(channel);
            channel.runPendingTasks();
            channel.finishAndReleaseAll();
            if (pressureLease != null) {
                pressureLease.close();
            }
            if (pressureHandle != null) {
                pressureHandle.close();
            }
            pressureRequestChannel.unregisterChannel(pressureChannel);
            pressureChannel.runPendingTasks();
            pressureChannel.finishAndReleaseAll();
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testConcurrentConnectionAdmissionNeverExceedsLimit() throws Exception {
        int connectionLimit = 4;
        int attempts = 24;
        KafkaProduceAdmissionController produce = lane(10, 100, 10, 100);
        KafkaProduceAdmissionController control = lane(10, 100, 10, 100);
        AtomicInteger rejectionNotifications = new AtomicInteger();
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(
                        produce, control, connectionLimit, rejectionNotifications::incrementAndGet);
        RequestChannel requestChannel = new RequestChannel(1_000);
        List<EmbeddedChannel> channels = new ArrayList<>(attempts);
        List<KafkaFrameAdmission> admissions = new ArrayList<>(attempts);
        ExecutorService executor = Executors.newFixedThreadPool(attempts);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>(attempts);

        try {
            for (int i = 0; i < attempts; i++) {
                EmbeddedChannel channel = new EmbeddedChannel();
                requestChannel.registerChannel(channel);
                KafkaFrameAdmission admission =
                        controller.createConnectionAdmission(requestChannel);
                channels.add(channel);
                admissions.add(admission);
                results.add(
                        executor.submit(
                                () -> {
                                    start.await();
                                    try {
                                        admission.channelActive(channel);
                                        return true;
                                    } catch (RejectedExecutionException expected) {
                                        return false;
                                    }
                                }));
            }
            start.countDown();

            int accepted = 0;
            for (Future<Boolean> result : results) {
                if (result.get(10, TimeUnit.SECONDS)) {
                    accepted++;
                }
            }
            assertThat(accepted).isEqualTo(connectionLimit);
            assertThat(controller.connections()).isEqualTo(connectionLimit);
            assertThat(controller.connectionRejections()).isEqualTo(attempts - connectionLimit);
            assertThat(rejectionNotifications).hasValue(attempts - connectionLimit);
            assertThat(produce.registeredConnections()).isZero();
            assertThat(control.registeredConnections()).isZero();
        } finally {
            start.countDown();
            executor.shutdownNow();
            for (KafkaFrameAdmission admission : admissions) {
                admission.close();
            }
            for (EmbeddedChannel channel : channels) {
                requestChannel.unregisterChannel(channel);
                channel.runPendingTasks();
                channel.finishAndReleaseAll();
            }
        }

        assertEmpty(controller, produce, control);
    }

    @Test
    void testRejectionListenerFailureDoesNotChangeAdmissionOutcome() {
        KafkaProduceAdmissionController produce = lane(1, 10, 1, 10);
        KafkaProduceAdmissionController control = lane(1, 10, 1, 10);
        KafkaRequestAdmissionController controller =
                new KafkaRequestAdmissionController(
                        produce,
                        control,
                        1,
                        () -> {
                            throw new IllegalStateException("metric failure");
                        });

        try (TestingConnection accepted = new TestingConnection(controller)) {
            TestingConnection rejected = new TestingConnection(controller, false);
            try {
                assertThatThrownBy(rejected::activate)
                        .isInstanceOf(RejectedExecutionException.class)
                        .satisfies(
                                failure ->
                                        assertThat(failure.getSuppressed())
                                                .singleElement()
                                                .isInstanceOf(IllegalStateException.class));
                assertThat(controller.connections()).isOne();
                assertThat(controller.connectionRejections()).isOne();
            } finally {
                rejected.close();
            }
        }

        assertEmpty(controller, produce, control);
    }

    private static KafkaProduceAdmissionController lane(
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

    private static KafkaFrameAdmissionLease granted(KafkaFrameAdmission.Reservation reservation) {
        return reservation.getFuture().join();
    }

    private static void assertCancelled(KafkaFrameAdmission.Reservation reservation) {
        assertThatThrownBy(() -> reservation.getFuture().join())
                .isInstanceOf(CancellationException.class);
    }

    private static void assertEmpty(
            KafkaRequestAdmissionController controller,
            KafkaProduceAdmissionController produce,
            KafkaProduceAdmissionController control) {
        assertThat(controller.connections()).isZero();
        assertLaneEmpty(produce);
        assertLaneEmpty(control);
    }

    private static void assertLaneEmpty(KafkaProduceAdmissionController lane) {
        assertThat(lane.liveRequests()).isZero();
        assertThat(lane.liveBytes()).isZero();
        assertThat(lane.rawBytes()).isZero();
        assertThat(lane.registeredConnections()).isZero();
        assertThat(lane.pendingReservations()).isZero();
    }

    private static final class TestingConnection implements AutoCloseable {
        private final RequestChannel requestChannel;
        private final EmbeddedChannel channel = new EmbeddedChannel();
        private final KafkaFrameAdmission admission;
        private boolean requestChannelRegistered = true;

        private TestingConnection(KafkaRequestAdmissionController controller) {
            this(controller, true);
        }

        private TestingConnection(
                KafkaRequestAdmissionController controller, boolean activateImmediately) {
            this(controller, new RequestChannel(1_000), activateImmediately);
        }

        private TestingConnection(
                KafkaRequestAdmissionController controller, RequestChannel requestChannel) {
            this(controller, requestChannel, true);
        }

        private TestingConnection(
                KafkaRequestAdmissionController controller,
                RequestChannel requestChannel,
                boolean activateImmediately) {
            this.requestChannel = requestChannel;
            requestChannel.registerChannel(channel);
            admission = controller.createConnectionAdmission(requestChannel);
            if (activateImmediately) {
                activate();
            }
        }

        private void activate() {
            admission.channelActive(channel);
        }

        @Override
        public void close() {
            admission.close();
            if (requestChannelRegistered) {
                requestChannelRegistered = false;
                requestChannel.unregisterChannel(channel);
                channel.runPendingTasks();
                channel.finishAndReleaseAll();
            }
        }
    }

    private static final class ThrowingPauseRequestChannel extends RequestChannel {
        private ThrowingPauseRequestChannel() {
            super(100);
        }

        @Override
        public PauseLease pauseChannel(Channel channel, PauseReason reason) {
            throw new IllegalStateException("test pause failure");
        }
    }

    private static final class ThrowingClosePauseRequestChannel extends RequestChannel {
        private ThrowingClosePauseRequestChannel() {
            super(100);
        }

        @Override
        public PauseLease pauseChannel(Channel channel, PauseReason reason) {
            return () -> {
                throw new IllegalStateException("test pause close failure");
            };
        }
    }
}
