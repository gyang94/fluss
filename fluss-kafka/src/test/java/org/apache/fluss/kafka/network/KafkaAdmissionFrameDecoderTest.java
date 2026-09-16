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

import org.apache.fluss.kafka.network.KafkaFrameAdmission.Reservation;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.AdaptiveRecvByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaAdmissionFrameDecoder} over real NIO sockets. */
class KafkaAdmissionFrameDecoderTest {
    private static final int MAX_FRAME_BYTES = 1024 * 1024;
    private static final Duration SHORT_TIMEOUT = Duration.ofMillis(100);
    private static final Duration NORMAL_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration LONG_TIMEOUT = Duration.ofSeconds(10);

    @Test
    void testBodyIsNotReadOrAllocatedBeforeGrant() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        try (RunningServer server = new RunningServer(admission, readStats);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 64 * 1024, (byte) 0x5A);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            TestReservation reservation = admission.awaitReservation(0);
            server.awaitEventLoopBarrier();

            assertThat(reservation.frameBytes).isEqualTo(request.length);
            assertThat(reservation.apiKey).isEqualTo(ApiKeys.PRODUCE.id);
            assertThat(readStats.bytesRead).hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(readStats.maxAllocation).hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(readStats.pauseCalls).hasValue(1);
            assertThat(readStats.pauseCloseCalls).hasValue(0);
            assertThat(server.receivedFrames).isEmpty();

            TestLease lease = reservation.grant();
            KafkaFrame decoded = server.awaitFrame(0);

            assertThat(readStats.bytesRead).hasValue(request.length);
            assertThat(decoded.apiKey()).isEqualTo(ApiKeys.PRODUCE.id);
            assertThat(decoded.wireBytes()).isEqualTo(request.length);
            assertThat(decoded.content().readableBytes()).isEqualTo(request.length - 4);
            assertThat(decoded.content().getShort(decoded.content().readerIndex()))
                    .isEqualTo(ApiKeys.PRODUCE.id);
            assertThat(readStats.pauseCloseCalls).hasValue(1);

            decoded.release();
            assertThat(lease.closeCalls).hasValue(1);
        }
    }

    @Test
    void testImmediateGrantDoesNotPauseOrHopEventLoop() throws Exception {
        ImmediateAdmission admission = new ImmediateAdmission();
        ReadStats readStats = new ReadStats();
        TestMetrics metrics = new TestMetrics();
        try (RunningServer server =
                        new RunningServer(
                                admission, readStats, LONG_TIMEOUT, LONG_TIMEOUT, metrics);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 16 * 1024, (byte) 0x6A);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            KafkaFrame decoded = server.awaitFrame(0);

            assertThat(admission.reservations).hasValue(1);
            assertThat(readStats.bytesRead).hasValue(request.length);
            assertThat(readStats.pauseCalls).hasValue(0);
            assertThat(readStats.pauseCloseCalls).hasValue(0);
            assertThat(metrics.preFrameWaitCompletions).hasValue(1);
            assertThat(metrics.preFrameWaitTimeouts).hasValue(0);
            assertThat(metrics.bodyReadCompletions).hasValue(1);
            assertThat(metrics.bodyReadTimeouts).hasValue(0);

            decoded.release();
            assertThat(admission.lastLease.get().closeCalls).hasValue(1);
        }
    }

    @Test
    void testSuccessfulFrameIgnoresMetricFailuresWithoutLeakingGrant() throws Exception {
        ImmediateAdmission admission = new ImmediateAdmission();
        ReadStats readStats = new ReadStats();
        try (RunningServer server =
                        new RunningServer(
                                admission,
                                readStats,
                                LONG_TIMEOUT,
                                LONG_TIMEOUT,
                                new ThrowingMetrics());
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 16 * 1024, (byte) 0x6D);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            KafkaFrame decoded = server.awaitFrame(0);

            assertThat(server.acceptedChannel.get().isActive()).isTrue();
            assertThat(decoded.wireBytes()).isEqualTo(request.length);
            decoded.release();
            assertThat(admission.lastLease.get().closeCalls).hasValue(1);
        }
    }

    @Test
    void testImmediateGrantSchedulesBodyReadWhenAutoReadIsDisabled() throws Exception {
        ImmediateAdmission admission = new ImmediateAdmission();
        ReadStats readStats = new ReadStats();
        TestMetrics metrics = new TestMetrics();
        try (RunningServer server =
                        new RunningServer(
                                admission, readStats, LONG_TIMEOUT, LONG_TIMEOUT, metrics, false);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 16 * 1024, (byte) 0x6B);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();
            server.readOnce();

            KafkaFrame decoded = server.awaitFrame(0);

            assertThat(readStats.bytesRead).hasValue(request.length);
            assertThat(readStats.pauseCalls).hasValue(1);
            assertThat(readStats.pauseCloseCalls).hasValue(1);
            assertThat(metrics.preFrameWaitCompletions).hasValue(1);

            decoded.release();
            assertThat(admission.lastLease.get().closeCalls).hasValue(1);
        }
    }

    @Test
    void testGrantRacingWithPauseFailureIsReleased() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        AtomicReference<TestLease> racedLease = new AtomicReference<>();
        readStats.failPauseAfter(
                () -> racedLease.set(admission.reservations.get(0).grant()),
                new IllegalStateException("test pause failure"));

        try (RunningServer server = new RunningServer(admission, readStats);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 16 * 1024, (byte) 0x6C);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            server.awaitAcceptedChannelClosed();
            waitUntil(
                    () -> racedLease.get() != null && racedLease.get().closeCalls.get() == 1,
                    Duration.ofSeconds(10),
                    "grant that raced with pause failure was not released");

            assertThat(server.receivedFrames).isEmpty();
            assertThat(readStats.pauseCalls).hasValue(1);
            assertThat(readStats.pauseCloseCalls).hasValue(0);
        }
    }

    @Test
    void testFragmentedProbeStillStopsAtExactlySixBytes() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        try (RunningServer server = new RunningServer(admission, readStats);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 32 * 1024, (byte) 0x33);
            OutputStream output = client.getOutputStream();
            output.write(request, 0, 2);
            output.flush();
            waitUntil(
                    () -> readStats.bytesRead.get() == 2,
                    Duration.ofSeconds(10),
                    "server did not read the first probe fragment");
            assertThat(admission.reservations).isEmpty();

            output.write(request, 2, request.length - 2);
            output.flush();
            TestReservation reservation = admission.awaitReservation(0);
            server.awaitEventLoopBarrier();

            assertThat(readStats.bytesRead).hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(server.receivedFrames).isEmpty();

            client.close();
            // With auto-read disabled, a portable NIO channel does not consume the peer FIN. The
            // PF2 wait timeout is therefore responsible for eventually closing a remotely
            // disconnected waiter. A locally observed close must still cancel immediately.
            server.closeAcceptedChannel();
            waitUntil(
                    reservation.cancelled::get,
                    Duration.ofSeconds(10),
                    "pending reservation was not cancelled on disconnect");

            // Model a controller grant that raced with cancellation. The decoder no longer owns
            // this reservation, so the late lease must be closed without reading the body.
            TestLease lateLease = reservation.grant();
            waitUntil(
                    () -> lateLease.closeCalls.get() == 1,
                    Duration.ofSeconds(10),
                    "late admission grant was not released");
            assertThat(readStats.bytesRead).hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(readStats.pauseCloseCalls).hasValue(1);
        }
    }

    @Test
    void testFailedGrantReleasesPauseAndClosesChannel() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        TestMetrics metrics = new TestMetrics();
        try (RunningServer server =
                        new RunningServer(
                                admission, readStats, LONG_TIMEOUT, LONG_TIMEOUT, metrics);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 16 * 1024, (byte) 0x44);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            TestReservation reservation = admission.awaitReservation(0);
            reservation.fail(new IllegalStateException("test grant failure"));

            server.awaitAcceptedChannelClosed();
            assertThat(readStats.bytesRead).hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(readStats.pauseCalls).hasValue(1);
            assertThat(readStats.pauseCloseCalls).hasValue(1);
            assertThat(server.receivedFrames).isEmpty();
            assertThat(metrics.reservationRejections).hasValue(1);
            assertThat(metrics.reservationCancellations).hasValue(0);
        }
    }

    @Test
    void testProbeOnlyFrameIsRejectedBeforeAdmission() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        try (RunningServer server = new RunningServer(admission, readStats);
                Socket client = server.connect()) {
            ByteBuffer probeOnly = ByteBuffer.allocate(KafkaFrameReadState.PROBE_BYTES);
            probeOnly.putInt(2).putShort(ApiKeys.PRODUCE.id);
            client.getOutputStream().write(probeOnly.array());
            client.getOutputStream().flush();

            server.awaitAcceptedChannelClosed();

            assertThat(readStats.bytesRead).hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(admission.reservations).isEmpty();
            assertThat(server.receivedFrames).isEmpty();
            assertThat(readStats.pauseCalls).hasValue(0);
        }
    }

    @Test
    void testPreFrameWaitTimeoutCancelsWithoutReadingOrEmittingRequest() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        TestMetrics metrics = new TestMetrics();
        try (RunningServer server =
                        new RunningServer(
                                admission, readStats, SHORT_TIMEOUT, LONG_TIMEOUT, metrics);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 32 * 1024, (byte) 0x66);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            TestReservation reservation = admission.awaitReservation(0);
            server.awaitAcceptedChannelClosed();

            assertThat(reservation.cancelled).isTrue();
            assertThat(readStats.bytesRead).hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(server.receivedFrames).isEmpty();
            assertThat(metrics.reservationCancellations).hasValue(1);
            assertThat(metrics.preFrameWaitTimeouts).hasValue(1);
            assertThat(metrics.lastPreFrameWaitTimeoutStart).hasValueGreaterThan(0);
            assertThat(metrics.preFrameWaitCompletions).hasValue(0);
            assertThat(metrics.bodyReadTimeouts).hasValue(0);

            TestLease lateLease = reservation.grant();
            waitUntil(
                    () -> lateLease.closeCalls.get() == 1,
                    Duration.ofSeconds(10),
                    "late timeout-racing grant was not released");
            assertThat(server.receivedFrames).isEmpty();
        }
    }

    @Test
    void testPreFrameTimeoutIgnoresMetricFailuresAndCancelsReservation() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        try (RunningServer server =
                        new RunningServer(
                                admission,
                                readStats,
                                SHORT_TIMEOUT,
                                LONG_TIMEOUT,
                                new ThrowingMetrics());
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 16 * 1024, (byte) 0x6E);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            TestReservation reservation = admission.awaitReservation(0);
            server.awaitAcceptedChannelClosed();

            assertThat(reservation.cancelled).isTrue();
            assertThat(readStats.pauseCloseCalls).hasValue(1);
            TestLease lateLease = reservation.grant();
            waitUntil(
                    () -> lateLease.closeCalls.get() == 1,
                    Duration.ofSeconds(10),
                    "late grant after metric-failing timeout was not released");
        }
    }

    @Test
    void testBodyReadTimeoutReleasesGrantWithoutEmittingRequest() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        TestMetrics metrics = new TestMetrics();
        try (RunningServer server =
                        new RunningServer(
                                admission, readStats, LONG_TIMEOUT, SHORT_TIMEOUT, metrics);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 16 * 1024, (byte) 0x77);
            client.getOutputStream().write(request, 0, KafkaFrameReadState.PROBE_BYTES);
            client.getOutputStream().flush();

            TestLease lease = admission.awaitReservation(0).grant();
            server.awaitAcceptedChannelClosed();

            assertThat(readStats.bytesRead).hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(server.receivedFrames).isEmpty();
            assertThat(lease.closeCalls).hasValue(1);
            assertThat(metrics.preFrameWaitCompletions).hasValue(1);
            assertThat(metrics.lastPreFrameWaitStart).hasValueGreaterThan(0);
            assertThat(metrics.bodyReadTimeouts).hasValue(1);
            assertThat(metrics.lastBodyReadTimeoutStart).hasValueGreaterThan(0);
            assertThat(metrics.bodyReadCompletions).hasValue(0);
            assertThat(metrics.reservationCancellations).hasValue(0);
        }
    }

    @Test
    void testBodyTimeoutIgnoresMetricFailuresAndReleasesGrant() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        try (RunningServer server =
                        new RunningServer(
                                admission,
                                readStats,
                                LONG_TIMEOUT,
                                SHORT_TIMEOUT,
                                new ThrowingMetrics());
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 16 * 1024, (byte) 0x6F);
            client.getOutputStream().write(request, 0, KafkaFrameReadState.PROBE_BYTES);
            client.getOutputStream().flush();

            TestLease lease = admission.awaitReservation(0).grant();
            server.awaitAcceptedChannelClosed();

            assertThat(lease.closeCalls).hasValue(1);
            assertThat(server.receivedFrames).isEmpty();
        }
    }

    @Test
    void testCompletedStagesCancelBothTimeoutTasks() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        TestMetrics metrics = new TestMetrics();
        try (RunningServer server =
                        new RunningServer(
                                admission, readStats, NORMAL_TIMEOUT, NORMAL_TIMEOUT, metrics);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 8 * 1024, (byte) 0x12);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            TestLease lease = admission.awaitReservation(0).grant();
            KafkaFrame decoded = server.awaitFrame(0);
            server.awaitEventLoopDelay(Duration.ofMillis(1200));

            assertThat(server.acceptedChannel.get().isActive()).isTrue();
            assertThat(metrics.preFrameWaitCompletions).hasValue(1);
            assertThat(metrics.bodyReadCompletions).hasValue(1);
            assertThat(metrics.lastBodyReadStart).hasValueGreaterThan(0);
            assertThat(metrics.preFrameWaitTimeouts).hasValue(0);
            assertThat(metrics.bodyReadTimeouts).hasValue(0);

            decoded.release();
            assertThat(lease.closeCalls).hasValue(1);
        }
    }

    @Test
    void testGrantThatWinsTimeoutCancelRaceIsAccepted() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        TestMetrics metrics = new TestMetrics();
        try (RunningServer server =
                        new RunningServer(
                                admission, readStats, NORMAL_TIMEOUT, LONG_TIMEOUT, metrics);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 8 * 1024, (byte) 0x23);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            TestReservation reservation = admission.awaitReservation(0);
            reservation.grantWhenCancelIsAttempted.set(true);
            KafkaFrame decoded = server.awaitFrame(0);
            TestLease racedLease = reservation.racedLease.get();

            assertThat(racedLease).isNotNull();
            assertThat(reservation.cancelled).isFalse();
            assertThat(metrics.reservationCancellations).hasValue(0);
            assertThat(metrics.preFrameWaitTimeouts).hasValue(0);
            assertThat(metrics.preFrameWaitCompletions).hasValue(1);
            assertThat(metrics.bodyReadCompletions).hasValue(1);

            decoded.release();
            assertThat(racedLease.closeCalls).hasValue(1);
        }
    }

    @Test
    void testDisconnectCancellationDoesNotCountAsTimeout() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        TestMetrics metrics = new TestMetrics();
        try (RunningServer server =
                        new RunningServer(
                                admission, readStats, LONG_TIMEOUT, LONG_TIMEOUT, metrics);
                Socket client = server.connect()) {
            byte[] request = frame(ApiKeys.PRODUCE.id, 8 * 1024, (byte) 0x34);
            client.getOutputStream().write(request);
            client.getOutputStream().flush();

            TestReservation reservation = admission.awaitReservation(0);
            server.closeAcceptedChannel();
            waitUntil(
                    reservation.cancelled::get,
                    Duration.ofSeconds(10),
                    "reservation was not cancelled on local disconnect");
            // cancel() becomes visible before channelInactive finishes updating its metrics.
            server.awaitEventLoopBarrier();

            assertThat(metrics.reservationCancellations).hasValue(1);
            assertThat(metrics.preFrameWaitTimeouts).hasValue(0);
            assertThat(metrics.bodyReadTimeouts).hasValue(0);
        }
    }

    @Test
    void testEachFrameRequiresItsOwnProbeAndReservation() throws Exception {
        ControllableAdmission admission = new ControllableAdmission();
        ReadStats readStats = new ReadStats();
        try (RunningServer server = new RunningServer(admission, readStats);
                Socket client = server.connect()) {
            byte[] produce = frame(ApiKeys.PRODUCE.id, 8 * 1024, (byte) 0x11);
            byte[] apiVersions = frame(ApiKeys.API_VERSIONS.id, 4 * 1024, (byte) 0x22);
            ByteBuffer both = ByteBuffer.allocate(produce.length + apiVersions.length);
            both.put(produce).put(apiVersions);
            client.getOutputStream().write(both.array());
            client.getOutputStream().flush();

            TestReservation first = admission.awaitReservation(0);
            assertThat(first.apiKey).isEqualTo(ApiKeys.PRODUCE.id);
            TestLease firstLease = first.grant();
            KafkaFrame firstFrame = server.awaitFrame(0);

            TestReservation second = admission.awaitReservation(1);
            server.awaitEventLoopBarrier();
            assertThat(second.apiKey).isEqualTo(ApiKeys.API_VERSIONS.id);
            assertThat(readStats.bytesRead)
                    .hasValue(produce.length + KafkaFrameReadState.PROBE_BYTES);
            assertThat(server.receivedFrames).hasSize(1);

            TestLease secondLease = second.grant();
            KafkaFrame secondFrame = server.awaitFrame(1);
            assertThat(readStats.bytesRead).hasValue(produce.length + apiVersions.length);
            assertThat(secondFrame.apiKey()).isEqualTo(ApiKeys.API_VERSIONS.id);

            firstFrame.release();
            secondFrame.release();
            assertThat(firstLease.closeCalls).hasValue(1);
            assertThat(secondLease.closeCalls).hasValue(1);
        }
    }

    @Test
    void testAdmissionLeaseCanBeTransferredExactlyOnce() {
        TestLease lease = new TestLease();
        KafkaFrame frame =
                new KafkaFrame(
                        Unpooled.buffer(2).writeShort(ApiKeys.PRODUCE.id),
                        ApiKeys.PRODUCE.id,
                        6,
                        lease);

        KafkaFrameAdmissionLease transferred = frame.takeAdmissionLease();
        assertThatThrownBy(frame::takeAdmissionLease)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no longer owned");

        frame.release();
        assertThat(lease.closeCalls).hasValue(0);
        transferred.close();
        assertThat(lease.closeCalls).hasValue(1);
    }

    private static byte[] frame(short apiKey, int bodyBytes, byte fill) {
        if (bodyBytes < 2) {
            throw new IllegalArgumentException("bodyBytes must include the API key");
        }
        byte[] frame = new byte[4 + bodyBytes];
        ByteBuffer buffer = ByteBuffer.wrap(frame);
        buffer.putInt(bodyBytes);
        buffer.putShort(apiKey);
        while (buffer.hasRemaining()) {
            buffer.put(fill);
        }
        return frame;
    }

    private static final class RunningServer implements AutoCloseable {
        private final EventLoopGroup acceptGroup = new NioEventLoopGroup(1);
        private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        private final AtomicReference<Channel> acceptedChannel = new AtomicReference<>();
        private final CountDownLatch accepted = new CountDownLatch(1);
        private final List<KafkaFrame> receivedFrames =
                Collections.synchronizedList(new ArrayList<>());
        private final Channel serverChannel;

        private RunningServer(KafkaFrameAdmission admission, ReadStats readStats) {
            this(admission, readStats, null, null, null);
        }

        private RunningServer(
                KafkaFrameAdmission admission,
                ReadStats readStats,
                Duration preFrameWaitTimeout,
                Duration bodyReadTimeout,
                KafkaFrameAdmissionMetrics metrics) {
            this(admission, readStats, preFrameWaitTimeout, bodyReadTimeout, metrics, true);
        }

        private RunningServer(
                KafkaFrameAdmission admission,
                ReadStats readStats,
                Duration preFrameWaitTimeout,
                Duration bodyReadTimeout,
                KafkaFrameAdmissionMetrics metrics,
                boolean autoRead) {
            serverChannel =
                    new ServerBootstrap()
                            .group(acceptGroup, workerGroup)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(
                                    new ChannelInitializer<SocketChannel>() {
                                        @Override
                                        protected void initChannel(SocketChannel channel) {
                                            channel.config().setAutoRead(autoRead);
                                            KafkaAdmissionFrameDecoder decoder;
                                            if (preFrameWaitTimeout == null) {
                                                decoder =
                                                        new KafkaAdmissionFrameDecoder(
                                                                MAX_FRAME_BYTES,
                                                                true,
                                                                admission,
                                                                readStats);
                                            } else {
                                                decoder =
                                                        new KafkaAdmissionFrameDecoder(
                                                                MAX_FRAME_BYTES,
                                                                true,
                                                                admission,
                                                                readStats,
                                                                preFrameWaitTimeout,
                                                                bodyReadTimeout,
                                                                metrics);
                                            }
                                            channel.config()
                                                    .setRecvByteBufAllocator(
                                                            decoder.newRecvByteBufAllocator(
                                                                    new AdaptiveRecvByteBufAllocator(
                                                                            1,
                                                                            16 * 1024,
                                                                            256 * 1024),
                                                                    readStats));
                                            channel.pipeline()
                                                    .addLast(
                                                            "frameReadGate", decoder.newReadGate());
                                            channel.pipeline().addLast("frameDecoder", decoder);
                                            channel.pipeline()
                                                    .addLast(
                                                            "collector",
                                                            new SimpleChannelInboundHandler<
                                                                    KafkaFrame>() {
                                                                @Override
                                                                protected void channelRead0(
                                                                        ChannelHandlerContext
                                                                                unused,
                                                                        KafkaFrame frame) {
                                                                    receivedFrames.add(
                                                                            frame.retain());
                                                                }
                                                            });
                                            acceptedChannel.set(channel);
                                            accepted.countDown();
                                        }
                                    })
                            .bind("127.0.0.1", 0)
                            .syncUninterruptibly()
                            .channel();
        }

        private Socket connect() throws Exception {
            InetSocketAddress address = (InetSocketAddress) serverChannel.localAddress();
            Socket socket = new Socket();
            socket.connect(address);
            assertThat(accepted.await(10, TimeUnit.SECONDS)).isTrue();
            return socket;
        }

        private void awaitEventLoopBarrier() {
            acceptedChannel.get().eventLoop().submit((Runnable) () -> {}).syncUninterruptibly();
        }

        private void awaitEventLoopDelay(Duration duration) throws Exception {
            CountDownLatch elapsed = new CountDownLatch(1);
            acceptedChannel
                    .get()
                    .eventLoop()
                    .schedule(elapsed::countDown, duration.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(elapsed.await(10, TimeUnit.SECONDS)).isTrue();
        }

        private KafkaFrame awaitFrame(int index) throws Exception {
            waitUntil(
                    () -> receivedFrames.size() > index,
                    Duration.ofSeconds(10),
                    "server did not decode Kafka frame " + index);
            return receivedFrames.get(index);
        }

        private void closeAcceptedChannel() {
            acceptedChannel.get().close().syncUninterruptibly();
        }

        private void readOnce() {
            acceptedChannel.get().read();
        }

        private void awaitAcceptedChannelClosed() throws Exception {
            waitUntil(
                    () -> !acceptedChannel.get().isActive(),
                    Duration.ofSeconds(10),
                    "server did not close the failed admission channel");
        }

        @Override
        public void close() {
            Channel channel = acceptedChannel.get();
            if (channel != null) {
                channel.close().syncUninterruptibly();
            }
            serverChannel.close().syncUninterruptibly();
            acceptGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
            workerGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
            for (KafkaFrame frame : receivedFrames) {
                if (frame.refCnt() > 0) {
                    frame.release(frame.refCnt());
                }
            }
        }
    }

    private static final class ControllableAdmission implements KafkaFrameAdmission {
        private final List<TestReservation> reservations =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public Reservation reserve(Channel channel, long frameBytes, short apiKey) {
            TestReservation reservation = new TestReservation(frameBytes, apiKey);
            reservations.add(reservation);
            return reservation;
        }

        private TestReservation awaitReservation(int index) throws Exception {
            waitUntil(
                    () -> reservations.size() > index,
                    Duration.ofSeconds(10),
                    "admission did not receive reservation " + index);
            return reservations.get(index);
        }
    }

    private static final class ImmediateAdmission implements KafkaFrameAdmission {
        private final AtomicInteger reservations = new AtomicInteger();
        private final AtomicReference<TestLease> lastLease = new AtomicReference<>();

        @Override
        public Reservation reserve(Channel channel, long frameBytes, short apiKey) {
            TestLease lease = new TestLease();
            lastLease.set(lease);
            reservations.incrementAndGet();
            return new Reservation() {
                @Override
                public CompletableFuture<KafkaFrameAdmissionLease> getFuture() {
                    return CompletableFuture.completedFuture(lease);
                }

                @Override
                public boolean cancel() {
                    return false;
                }
            };
        }
    }

    private static final class TestReservation implements Reservation {
        private final long frameBytes;
        private final short apiKey;
        private final CompletableFuture<KafkaFrameAdmissionLease> future =
                new CompletableFuture<>();
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final AtomicBoolean grantWhenCancelIsAttempted = new AtomicBoolean(false);
        private final AtomicReference<TestLease> racedLease = new AtomicReference<>();

        private TestReservation(long frameBytes, short apiKey) {
            this.frameBytes = frameBytes;
            this.apiKey = apiKey;
        }

        @Override
        public CompletableFuture<KafkaFrameAdmissionLease> getFuture() {
            return future;
        }

        @Override
        public boolean cancel() {
            if (grantWhenCancelIsAttempted.compareAndSet(true, false) && !future.isDone()) {
                TestLease lease = new TestLease();
                racedLease.set(lease);
                future.complete(lease);
                return false;
            }
            return !future.isDone() && cancelled.compareAndSet(false, true);
        }

        private TestLease grant() {
            TestLease lease = new TestLease();
            future.complete(lease);
            return lease;
        }

        private void fail(Throwable failure) {
            future.completeExceptionally(failure);
        }
    }

    private static final class TestLease implements KafkaFrameAdmissionLease {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public void releaseFrameBytes() {}

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                closeCalls.incrementAndGet();
            }
        }
    }

    private static final class ReadStats
            implements KafkaProbeAwareRecvByteBufAllocator.ReadObserver, KafkaFrameReadPauser {
        private final AtomicLong bytesRead = new AtomicLong();
        private final AtomicInteger maxAllocation = new AtomicInteger();
        private final AtomicInteger pauseCalls = new AtomicInteger();
        private final AtomicInteger pauseCloseCalls = new AtomicInteger();
        private Runnable pauseHook;
        private RuntimeException pauseFailure;

        private void failPauseAfter(Runnable hook, RuntimeException failure) {
            pauseHook = hook;
            pauseFailure = failure;
        }

        @Override
        public void onAllocation(int bytes) {
            maxAllocation.updateAndGet(previous -> Math.max(previous, bytes));
        }

        @Override
        public void onBytesRead(int bytes) {
            bytesRead.addAndGet(bytes);
        }

        @Override
        public PauseLease pause(Channel channel) {
            boolean restoreAutoRead = channel.config().isAutoRead();
            channel.config().setAutoRead(false);
            pauseCalls.incrementAndGet();
            Runnable hook = pauseHook;
            if (hook != null) {
                hook.run();
            }
            RuntimeException failure = pauseFailure;
            if (failure != null) {
                throw failure;
            }
            AtomicBoolean closed = new AtomicBoolean(false);
            return () -> {
                if (!closed.compareAndSet(false, true)) {
                    return;
                }
                pauseCloseCalls.incrementAndGet();
                if (!channel.isActive()) {
                    return;
                }
                if (restoreAutoRead) {
                    channel.config().setAutoRead(true);
                } else {
                    channel.read();
                }
            };
        }
    }

    private static final class TestMetrics implements KafkaFrameAdmissionMetrics {
        private final AtomicInteger reservationRejections = new AtomicInteger();
        private final AtomicInteger reservationCancellations = new AtomicInteger();
        private final AtomicInteger preFrameWaitCompletions = new AtomicInteger();
        private final AtomicInteger preFrameWaitTimeouts = new AtomicInteger();
        private final AtomicInteger bodyReadCompletions = new AtomicInteger();
        private final AtomicInteger bodyReadTimeouts = new AtomicInteger();
        private final AtomicLong lastPreFrameWaitStart = new AtomicLong();
        private final AtomicLong lastPreFrameWaitTimeoutStart = new AtomicLong();
        private final AtomicLong lastBodyReadStart = new AtomicLong();
        private final AtomicLong lastBodyReadTimeoutStart = new AtomicLong();

        @Override
        public long nowNanos() {
            return System.nanoTime();
        }

        @Override
        public void recordReservationRejected() {
            reservationRejections.incrementAndGet();
        }

        @Override
        public void recordReservationCancelled() {
            reservationCancellations.incrementAndGet();
        }

        @Override
        public void recordPreFrameWait(long startedNanos) {
            lastPreFrameWaitStart.set(startedNanos);
            preFrameWaitCompletions.incrementAndGet();
        }

        @Override
        public void recordPreFrameWaitTimeout(long startedNanos) {
            lastPreFrameWaitTimeoutStart.set(startedNanos);
            preFrameWaitTimeouts.incrementAndGet();
        }

        @Override
        public void recordBodyRead(long startedNanos) {
            lastBodyReadStart.set(startedNanos);
            bodyReadCompletions.incrementAndGet();
        }

        @Override
        public void recordBodyReadTimeout(long startedNanos) {
            lastBodyReadTimeoutStart.set(startedNanos);
            bodyReadTimeouts.incrementAndGet();
        }
    }

    private static final class ThrowingMetrics implements KafkaFrameAdmissionMetrics {
        @Override
        public long nowNanos() {
            throw new IllegalStateException("test metrics clock failure");
        }

        @Override
        public void recordReservationRejected() {
            throw new IllegalStateException("test reservation rejection metric failure");
        }

        @Override
        public void recordReservationCancelled() {
            throw new IllegalStateException("test reservation cancellation metric failure");
        }

        @Override
        public void recordPreFrameWait(long startedNanos) {
            throw new IllegalStateException("test pre-frame metric failure");
        }

        @Override
        public void recordPreFrameWaitTimeout(long startedNanos) {
            throw new IllegalStateException("test pre-frame timeout metric failure");
        }

        @Override
        public void recordBodyRead(long startedNanos) {
            throw new IllegalStateException("test body-read metric failure");
        }

        @Override
        public void recordBodyReadTimeout(long startedNanos) {
            throw new IllegalStateException("test body-read timeout metric failure");
        }
    }
}
