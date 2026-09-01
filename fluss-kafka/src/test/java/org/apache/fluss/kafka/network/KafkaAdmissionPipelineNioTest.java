/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaRequestAdmissionController;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseLease;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseReason;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.fluss.shaded.netty4.io.netty.channel.AdaptiveRecvByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.fluss.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.flow.FlowControlHandler;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real-socket composition tests for pre-frame decoding and the two admission lanes. */
class KafkaAdmissionPipelineNioTest {
    private static final int MAX_FRAME_BYTES = 1024 * 1024;
    private static final int PRODUCE_FRAME_BYTES = 64 * 1024;
    private static final int CONTROL_FRAME_BYTES = 4 * 1024;
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(10);

    @Test
    void testPipelinedLargeFramesWaitAtSecondProbeWithoutClosingConnection() throws Exception {
        int frameBytes = 655_526;
        int perConnectionRawBytes = 1024 * 1024;
        KafkaProduceAdmissionController produce =
                lane(64, 16L * 1024 * 1024, 64, perConnectionRawBytes);
        KafkaProduceAdmissionController control = lane(8, MAX_FRAME_BYTES, 8, MAX_FRAME_BYTES);
        KafkaRequestAdmissionController admission =
                new KafkaRequestAdmissionController(produce, control, 16);

        try (RunningServer server = new RunningServer(admission, true)) {
            ClientConnection client = server.connect(1).get(0);
            client.send(frame(ApiKeys.PRODUCE.id, frameBytes, (byte) 0x51));
            ReceivedFrame first = client.state.awaitFrame(0);
            server.awaitEventLoopBarriers();
            client.send(frame(ApiKeys.PRODUCE.id, frameBytes, (byte) 0x52));
            waitUntil(
                    () -> produce.pendingReservations() == 1,
                    WAIT_TIMEOUT,
                    "second pipelined Produce frame did not wait at admission");
            server.awaitEventLoopBarriers();

            assertThat(client.state.channel.isActive()).isTrue();
            assertThat(client.state.failure).hasValue(null);
            assertThat(client.state.frameCount()).isOne();
            assertThat(client.state.readStats.bytesRead)
                    .hasValue(frameBytes + KafkaFrameReadState.PROBE_BYTES);

            first.complete();
            ReceivedFrame second = client.state.awaitFrame(1);
            assertThat(client.state.channel.isActive()).isTrue();
            assertThat(client.state.failure).hasValue(null);
            assertThat(client.state.readStats.bytesRead).hasValue(2L * frameBytes);

            second.complete();
            waitForEmptyLane(produce);
            server.closeConnections();
            waitForEmptyController(admission, produce, control);
            assertThat(server.failures()).isEmpty();
        }
    }

    @Test
    void testNativeEpollDoesNotReadWhileSecondFrameWaitsForAdmission() throws Exception {
        assumeTrue(
                Epoll.isAvailable(),
                "requires Linux native epoll: " + String.valueOf(Epoll.unavailabilityCause()));
        int frameBytes = 655_526;
        int perConnectionRawBytes = 1024 * 1024;
        KafkaProduceAdmissionController produce =
                lane(64, 16L * 1024 * 1024, 64, perConnectionRawBytes);
        KafkaProduceAdmissionController control = lane(8, MAX_FRAME_BYTES, 8, MAX_FRAME_BYTES);
        KafkaRequestAdmissionController admission =
                new KafkaRequestAdmissionController(produce, control, 16);

        try (RunningServer server = new RunningServer(admission, true, true)) {
            ClientConnection client = server.connect(1).get(0);
            client.send(frame(ApiKeys.PRODUCE.id, frameBytes, (byte) 0x61));
            ReceivedFrame first = client.state.awaitFrame(0);
            // Put the second probe in a new native read/readComplete cycle. The first lease remains
            // held, so the second reservation cannot acquire the per-connection raw budget.
            server.awaitEventLoopBarriers();

            client.send(frame(ApiKeys.PRODUCE.id, frameBytes, (byte) 0x62));
            waitUntil(
                    () -> produce.pendingReservations() == 1,
                    WAIT_TIMEOUT,
                    "second native-epoll Produce frame did not wait at admission");
            server.awaitEventLoopDelay(Duration.ofMillis(250));

            assertThat(client.state.channel.isActive()).isTrue();
            assertThat(client.state.failure).hasValue(null);
            assertThat(client.state.frameCount()).isOne();
            assertThat(client.state.readStats.bytesRead)
                    .hasValue(frameBytes + KafkaFrameReadState.PROBE_BYTES);

            first.complete();
            ReceivedFrame second = client.state.awaitFrame(1);
            assertThat(client.state.channel.isActive()).isTrue();
            assertThat(client.state.failure).hasValue(null);
            assertThat(client.state.readStats.bytesRead).hasValue(2L * frameBytes);

            second.complete();
            waitForEmptyLane(produce);
            server.closeConnections();
            waitForEmptyController(admission, produce, control);
            assertThat(server.failures()).isEmpty();
        }
    }

    @Test
    void testGlobalLiveLimitReadsOnlyOneBodyAndAdvancesWithoutLeaks() throws Exception {
        KafkaProduceAdmissionController produce = lane(1, MAX_FRAME_BYTES, 1, MAX_FRAME_BYTES);
        KafkaProduceAdmissionController control = lane(8, MAX_FRAME_BYTES, 8, MAX_FRAME_BYTES);
        KafkaRequestAdmissionController admission =
                new KafkaRequestAdmissionController(produce, control, 16);

        try (RunningServer server = new RunningServer(admission)) {
            List<ClientConnection> clients = server.connect(4);
            clients.get(0).send(frame(ApiKeys.PRODUCE.id, PRODUCE_FRAME_BYTES, (byte) 0x11));
            ReceivedFrame first = clients.get(0).state.awaitFrame(0);

            for (int i = 1; i < clients.size(); i++) {
                clients.get(i)
                        .send(frame(ApiKeys.PRODUCE.id, PRODUCE_FRAME_BYTES, (byte) (0x11 + i)));
                final int expectedWaiters = i;
                waitUntil(
                        () -> produce.pendingReservations() == expectedWaiters,
                        WAIT_TIMEOUT,
                        "Produce reservation did not enter the global live waiter queue");
            }
            server.awaitEventLoopBarriers();

            assertThat(first.apiKey).isEqualTo(ApiKeys.PRODUCE.id);
            assertThat(produce.liveRequests()).isOne();
            assertThat(produce.rawBytes()).isEqualTo(PRODUCE_FRAME_BYTES);
            assertThat(produce.pendingReservations()).isEqualTo(3);
            assertThat(produce.maxLiveRequestOvershoot()).isZero();
            assertThat(produce.liveRequestOvershootEvents()).isZero();
            assertThat(clients.get(0).state.readStats.bytesRead).hasValue(PRODUCE_FRAME_BYTES);
            assertWaitingAtProbe(clients, 1);

            first.complete();
            for (int i = 1; i < clients.size(); i++) {
                ReceivedFrame admitted = clients.get(i).state.awaitFrame(0);
                assertThat(admitted.apiKey).isEqualTo(ApiKeys.PRODUCE.id);
                assertThat(produce.liveRequests()).isOne();
                assertThat(produce.rawBytes()).isEqualTo(PRODUCE_FRAME_BYTES);
                assertThat(produce.maxLiveRequestOvershoot()).isZero();
                assertThat(clients.get(i).state.readStats.bytesRead).hasValue(PRODUCE_FRAME_BYTES);
                assertWaitingAtProbe(clients, i + 1);
                admitted.complete();
            }

            waitForEmptyLane(produce);
            server.closeConnections();
            waitForEmptyController(admission, produce, control);
            assertThat(server.failures()).isEmpty();
        }
    }

    @Test
    void testControlLaneCompletesWhileProduceLaneIsFull() throws Exception {
        KafkaProduceAdmissionController produce = lane(1, MAX_FRAME_BYTES, 1, MAX_FRAME_BYTES);
        KafkaProduceAdmissionController control = lane(1, MAX_FRAME_BYTES, 1, MAX_FRAME_BYTES);
        KafkaRequestAdmissionController admission =
                new KafkaRequestAdmissionController(produce, control, 16);

        try (RunningServer server = new RunningServer(admission)) {
            List<ClientConnection> clients = server.connect(3);
            ClientConnection produceHolder = clients.get(0);
            ClientConnection produceWaiter = clients.get(1);
            ClientConnection controlClient = clients.get(2);

            produceHolder.send(frame(ApiKeys.PRODUCE.id, PRODUCE_FRAME_BYTES, (byte) 0x21));
            ReceivedFrame heldProduce = produceHolder.state.awaitFrame(0);
            produceWaiter.send(frame(ApiKeys.PRODUCE.id, PRODUCE_FRAME_BYTES, (byte) 0x22));
            waitUntil(
                    () -> produce.pendingReservations() == 1,
                    WAIT_TIMEOUT,
                    "second Produce request did not wait on the full Produce lane");

            controlClient.send(frame(ApiKeys.API_VERSIONS.id, CONTROL_FRAME_BYTES, (byte) 0x23));
            ReceivedFrame controlFrame = controlClient.state.awaitFrame(0);
            server.awaitEventLoopBarriers();

            assertThat(controlFrame.apiKey).isEqualTo(ApiKeys.API_VERSIONS.id);
            assertThat(produce.liveRequests()).isOne();
            assertThat(produce.pendingReservations()).isOne();
            assertThat(produceWaiter.state.readStats.bytesRead)
                    .hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(control.liveRequests()).isOne();
            assertThat(control.rawBytes()).isEqualTo(CONTROL_FRAME_BYTES);
            assertThat(control.pendingReservations()).isZero();
            assertThat(controlClient.state.readStats.bytesRead).hasValue(CONTROL_FRAME_BYTES);

            controlFrame.complete();
            waitForEmptyLane(control);
            heldProduce.complete();
            produceWaiter.state.awaitFrame(0).complete();
            waitForEmptyLane(produce);

            server.closeConnections();
            waitForEmptyController(admission, produce, control);
            assertThat(server.failures()).isEmpty();
        }
    }

    @Test
    void testGlobalWireByteLimitIsReservedBeforeAnyAdditionalBodyRead() throws Exception {
        KafkaProduceAdmissionController produce =
                lane(4, PRODUCE_FRAME_BYTES, 1, PRODUCE_FRAME_BYTES);
        KafkaProduceAdmissionController control = lane(4, MAX_FRAME_BYTES, 4, MAX_FRAME_BYTES);
        KafkaRequestAdmissionController admission =
                new KafkaRequestAdmissionController(produce, control, 16);

        try (RunningServer server = new RunningServer(admission)) {
            List<ClientConnection> clients = server.connect(4);
            clients.get(0).send(frame(ApiKeys.PRODUCE.id, PRODUCE_FRAME_BYTES, (byte) 0x31));
            clients.get(0).state.awaitFrame(0);

            for (int i = 1; i < clients.size(); i++) {
                clients.get(i)
                        .send(frame(ApiKeys.PRODUCE.id, PRODUCE_FRAME_BYTES, (byte) (0x31 + i)));
                final int expectedWaiters = i;
                waitUntil(
                        () -> produce.pendingReservations() == expectedWaiters,
                        WAIT_TIMEOUT,
                        "Produce reservation did not enter the global raw waiter queue");
            }
            server.awaitEventLoopBarriers();

            assertThat(produce.liveRequests()).isOne();
            assertThat(produce.rawBytes()).isEqualTo(PRODUCE_FRAME_BYTES);
            assertThat(produce.maxRawBytesOvershoot()).isZero();
            assertThat(produce.rawBytesOvershootEvents()).isZero();
            assertWaitingAtProbe(clients, 1);

            for (int i = 0; i < clients.size(); i++) {
                ReceivedFrame admitted = clients.get(i).state.awaitFrame(0);
                assertThat(produce.rawBytes()).isEqualTo(PRODUCE_FRAME_BYTES);
                assertThat(produce.liveRequests()).isOne();
                assertThat(produce.maxRawBytesOvershoot()).isZero();
                assertThat(clients.get(i).state.readStats.bytesRead).hasValue(PRODUCE_FRAME_BYTES);
                assertWaitingAtProbe(clients, i + 1);
                // The same strict byte limit independently bounds the raw/copy lifetime and the
                // wire-envelope live lifetime. Complete both before the next body may be read.
                admitted.complete();
            }

            assertThat(produce.rawBytes()).isZero();
            assertThat(produce.liveRequests()).isZero();
            waitForEmptyLane(produce);

            server.closeConnections();
            waitForEmptyController(admission, produce, control);
            assertThat(server.failures()).isEmpty();
        }
    }

    @Test
    void testControlFrameAtExactLimitReadsAndDecodesCompleteBody() throws Exception {
        KafkaProduceAdmissionController produce = lane(1, MAX_FRAME_BYTES, 1, MAX_FRAME_BYTES);
        KafkaProduceAdmissionController control = lane(1, MAX_FRAME_BYTES, 1, MAX_FRAME_BYTES);
        KafkaRequestAdmissionController admission =
                new KafkaRequestAdmissionController(produce, control, 16, CONTROL_FRAME_BYTES);

        try (RunningServer server = new RunningServer(admission)) {
            ClientConnection client = server.connect(1).get(0);
            client.send(frame(ApiKeys.METADATA.id, CONTROL_FRAME_BYTES, (byte) 0x41));
            ReceivedFrame admitted = client.state.awaitFrame(0);

            assertThat(admitted.apiKey).isEqualTo(ApiKeys.METADATA.id);
            assertThat(client.state.readStats.bytesRead).hasValue(CONTROL_FRAME_BYTES);
            assertThat(control.liveRequests()).isOne();
            assertThat(control.rawBytes()).isEqualTo(CONTROL_FRAME_BYTES);

            admitted.complete();
            waitForEmptyLane(control);
            server.closeConnections();
            waitForEmptyController(admission, produce, control);
            assertThat(server.failures()).isEmpty();
        }
    }

    @Test
    void testOversizedControlFrameClosesAfterProbeWithoutQueueingOrReadingBody() throws Exception {
        KafkaProduceAdmissionController produce = lane(1, MAX_FRAME_BYTES, 1, MAX_FRAME_BYTES);
        KafkaProduceAdmissionController control = lane(1, MAX_FRAME_BYTES, 1, MAX_FRAME_BYTES);
        KafkaRequestAdmissionController admission =
                new KafkaRequestAdmissionController(produce, control, 16, CONTROL_FRAME_BYTES);
        CountingAdmissionMetrics metrics = new CountingAdmissionMetrics();

        try (RunningServer server = new RunningServer(admission, metrics)) {
            ClientConnection client = server.connect(1).get(0);
            client.send(frame(ApiKeys.API_VERSIONS.id, CONTROL_FRAME_BYTES + 1, (byte) 0x42));

            waitUntil(
                    () -> !client.state.channel.isActive(),
                    WAIT_TIMEOUT,
                    "oversized control-plane connection was not closed");
            server.awaitEventLoopBarriers();

            assertThat(client.state.readStats.bytesRead).hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(client.state.readStats.maxAllocation)
                    .hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(client.state.frameCount()).isZero();
            assertThat(control.registeredConnections()).isZero();
            assertThat(control.pendingReservations()).isZero();
            assertThat(control.liveRequests()).isZero();
            assertThat(control.rawBytes()).isZero();
            assertThat(metrics.reservationRejections).hasValue(1);
            List<Throwable> failures = server.failures();
            assertThat(failures).hasSize(1);
            assertThat(failures.get(0).getCause()).isInstanceOf(RejectedExecutionException.class);

            waitForEmptyController(admission, produce, control);
        }
    }

    private static KafkaProduceAdmissionController lane(
            long live, long raw, long livePerConnection, long rawPerConnection) {
        return new KafkaProduceAdmissionController(
                live, raw, livePerConnection, rawPerConnection, 32);
    }

    private static byte[] frame(short apiKey, int wireBytes, byte fill) {
        assertThat(wireBytes).isGreaterThanOrEqualTo(KafkaFrameReadState.PROBE_BYTES);
        ByteBuffer buffer = ByteBuffer.allocate(wireBytes);
        buffer.putInt(wireBytes - Integer.BYTES);
        buffer.putShort(apiKey);
        while (buffer.hasRemaining()) {
            buffer.put(fill);
        }
        return buffer.array();
    }

    private static void assertWaitingAtProbe(List<ClientConnection> clients, int startIndex) {
        for (int i = startIndex; i < clients.size(); i++) {
            assertThat(clients.get(i).state.readStats.bytesRead)
                    .as("bytes read by waiting connection %s", i)
                    .hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(clients.get(i).state.readStats.maxAllocation)
                    .as("largest allocation by waiting connection %s", i)
                    .hasValue(KafkaFrameReadState.PROBE_BYTES);
            assertThat(clients.get(i).state.frameCount())
                    .as("decoded frames on waiting connection %s", i)
                    .isZero();
        }
    }

    private static void waitForEmptyLane(KafkaProduceAdmissionController lane) throws Exception {
        waitUntil(
                () ->
                        lane.liveRequests() == 0
                                && lane.rawBytes() == 0
                                && lane.pendingReservations() == 0,
                WAIT_TIMEOUT,
                "admission lane retained request resources");
    }

    private static void waitForEmptyController(
            KafkaRequestAdmissionController admission,
            KafkaProduceAdmissionController produce,
            KafkaProduceAdmissionController control)
            throws Exception {
        waitUntil(
                () ->
                        admission.connections() == 0
                                && produce.registeredConnections() == 0
                                && control.registeredConnections() == 0,
                WAIT_TIMEOUT,
                "admission controller retained connection registrations");
        waitForEmptyLane(produce);
        waitForEmptyLane(control);
    }

    private enum PreFramePauseReason implements PauseReason {
        ADMISSION_WAIT
    }

    private static final class RunningServer implements AutoCloseable {
        private final KafkaRequestAdmissionController admission;
        private final KafkaFrameAdmissionMetrics admissionMetrics;
        private final boolean includeFlowControlHandler;
        private final RequestChannel requestChannel = new RequestChannel(1_000);
        private final EventLoopGroup acceptGroup;
        private final EventLoopGroup workerGroup;
        private final List<ConnectionState> states =
                Collections.synchronizedList(new ArrayList<>());
        private final List<ClientConnection> clients =
                Collections.synchronizedList(new ArrayList<>());
        private final Channel serverChannel;

        private RunningServer(KafkaRequestAdmissionController admission) {
            this(admission, null);
        }

        private RunningServer(
                KafkaRequestAdmissionController admission, boolean includeFlowControlHandler) {
            this(admission, null, includeFlowControlHandler);
        }

        private RunningServer(
                KafkaRequestAdmissionController admission,
                boolean includeFlowControlHandler,
                boolean useNativeEpoll) {
            this(admission, null, includeFlowControlHandler, useNativeEpoll);
        }

        private RunningServer(
                KafkaRequestAdmissionController admission,
                KafkaFrameAdmissionMetrics admissionMetrics) {
            this(admission, admissionMetrics, false);
        }

        private RunningServer(
                KafkaRequestAdmissionController admission,
                KafkaFrameAdmissionMetrics admissionMetrics,
                boolean includeFlowControlHandler) {
            this(admission, admissionMetrics, includeFlowControlHandler, false);
        }

        private RunningServer(
                KafkaRequestAdmissionController admission,
                KafkaFrameAdmissionMetrics admissionMetrics,
                boolean includeFlowControlHandler,
                boolean useNativeEpoll) {
            this.admission = admission;
            this.admissionMetrics = admissionMetrics;
            this.includeFlowControlHandler = includeFlowControlHandler;
            acceptGroup = useNativeEpoll ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
            workerGroup = useNativeEpoll ? new EpollEventLoopGroup(4) : new NioEventLoopGroup(4);
            ServerBootstrap bootstrap =
                    new ServerBootstrap()
                            .group(acceptGroup, workerGroup)
                            .childHandler(
                                    new ChannelInitializer<SocketChannel>() {
                                        @Override
                                        protected void initChannel(SocketChannel channel) {
                                            initializeConnection(channel);
                                        }
                                    });
            if (useNativeEpoll) {
                bootstrap.channel(EpollServerSocketChannel.class);
            } else {
                bootstrap.channel(NioServerSocketChannel.class);
            }
            serverChannel = bootstrap.bind("127.0.0.1", 0).syncUninterruptibly().channel();
        }

        private void initializeConnection(SocketChannel channel) {
            ConnectionState state = new ConnectionState(channel);
            states.add(state);
            KafkaFrameAdmission frameAdmission =
                    admission.createConnectionAdmission(requestChannel);
            KafkaFrameReadPauser readPauser =
                    pausedChannel -> {
                        PauseLease pauseLease =
                                requestChannel.pauseChannel(
                                        pausedChannel, PreFramePauseReason.ADMISSION_WAIT);
                        return pauseLease::close;
                    };
            KafkaAdmissionFrameDecoder decoder;
            if (admissionMetrics == null) {
                decoder =
                        new KafkaAdmissionFrameDecoder(
                                MAX_FRAME_BYTES, true, frameAdmission, readPauser);
            } else {
                decoder =
                        new KafkaAdmissionFrameDecoder(
                                MAX_FRAME_BYTES,
                                true,
                                frameAdmission,
                                readPauser,
                                WAIT_TIMEOUT,
                                WAIT_TIMEOUT,
                                admissionMetrics);
            }
            channel.config()
                    .setRecvByteBufAllocator(
                            decoder.newRecvByteBufAllocator(
                                    new AdaptiveRecvByteBufAllocator(1, 16 * 1024, MAX_FRAME_BYTES),
                                    state.readStats));
            channel.pipeline().addLast("frameReadGate", decoder.newReadGate());
            channel.pipeline().addLast("frameDecoder", decoder);
            if (includeFlowControlHandler) {
                channel.pipeline().addLast("flowController", new FlowControlHandler());
            }
            channel.pipeline().addLast("collector", new FrameCollector(state, requestChannel));
        }

        private List<ClientConnection> connect(int count) throws Exception {
            List<ClientConnection> clients = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int stateIndex = states.size();
                InetSocketAddress address = (InetSocketAddress) serverChannel.localAddress();
                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.connect(address);
                waitUntil(
                        () -> states.size() > stateIndex,
                        WAIT_TIMEOUT,
                        "server did not initialize accepted Kafka connection");
                ConnectionState state = state(stateIndex);
                waitUntil(
                        () -> admission.connections() == stateIndex + 1,
                        WAIT_TIMEOUT,
                        "Kafka request admission did not register accepted connection");
                ClientConnection client = new ClientConnection(socket, state);
                clients.add(client);
                this.clients.add(client);
            }
            return clients;
        }

        private ConnectionState state(int index) {
            synchronized (states) {
                return states.get(index);
            }
        }

        private void awaitEventLoopBarriers() {
            synchronized (states) {
                for (ConnectionState state : states) {
                    state.channel.eventLoop().submit((Runnable) () -> {}).syncUninterruptibly();
                }
            }
        }

        private void awaitEventLoopDelay(Duration duration) throws Exception {
            AtomicBoolean elapsed = new AtomicBoolean();
            synchronized (states) {
                for (ConnectionState state : states) {
                    state.channel
                            .eventLoop()
                            .schedule(
                                    () -> elapsed.set(true),
                                    duration.toMillis(),
                                    TimeUnit.MILLISECONDS)
                            .syncUninterruptibly();
                }
            }
            assertThat(elapsed).isTrue();
        }

        private void closeConnections() {
            synchronized (states) {
                for (ConnectionState state : states) {
                    state.channel.close().syncUninterruptibly();
                }
            }
        }

        private List<Throwable> failures() {
            List<Throwable> failures = new ArrayList<>();
            synchronized (states) {
                for (ConnectionState state : states) {
                    Throwable failure = state.failure.get();
                    if (failure != null) {
                        failures.add(failure);
                    }
                }
            }
            return failures;
        }

        @Override
        public void close() {
            closeConnections();
            synchronized (clients) {
                for (ClientConnection client : clients) {
                    try {
                        client.close();
                    } catch (IOException ignored) {
                        // The server side may already have reset the test connection.
                    }
                }
            }
            serverChannel.close().syncUninterruptibly();
            acceptGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
            workerGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
            synchronized (states) {
                for (ConnectionState state : states) {
                    state.releaseFrames();
                }
            }
        }
    }

    private static final class FrameCollector extends SimpleChannelInboundHandler<KafkaFrame> {
        private final ConnectionState state;
        private final RequestChannel requestChannel;

        private FrameCollector(ConnectionState state, RequestChannel requestChannel) {
            this.state = state;
            this.requestChannel = requestChannel;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            requestChannel.registerChannel(ctx.channel());
            super.channelActive(ctx);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext unused, KafkaFrame frame) {
            KafkaFrameAdmissionLease lease = frame.takeAdmissionLease();
            state.addFrame(new ReceivedFrame(frame.retain(), lease));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            requestChannel.unregisterChannel(ctx.channel());
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            state.failure.compareAndSet(null, cause);
            ctx.close();
        }
    }

    private static final class ClientConnection implements AutoCloseable {
        private final Socket socket;
        private final ConnectionState state;

        private ClientConnection(Socket socket, ConnectionState state) {
            this.socket = socket;
            this.state = state;
        }

        private void send(byte[] frame) throws Exception {
            OutputStream output = socket.getOutputStream();
            output.write(frame);
            output.flush();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static final class ConnectionState {
        private final Channel channel;
        private final ReadStats readStats = new ReadStats();
        private final List<ReceivedFrame> frames = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private ConnectionState(Channel channel) {
            this.channel = channel;
        }

        private void addFrame(ReceivedFrame frame) {
            frames.add(frame);
        }

        private ReceivedFrame awaitFrame(int index) throws Exception {
            waitUntil(
                    () -> frameCount() > index,
                    WAIT_TIMEOUT,
                    "server did not decode admitted Kafka frame");
            return frame(index);
        }

        private int frameCount() {
            return frames.size();
        }

        private ReceivedFrame frame(int index) {
            synchronized (frames) {
                return frames.get(index);
            }
        }

        private void releaseFrames() {
            synchronized (frames) {
                for (ReceivedFrame frame : frames) {
                    frame.complete();
                }
            }
        }
    }

    private static final class ReceivedFrame {
        private final short apiKey;
        private final KafkaFrame frame;
        private final KafkaFrameAdmissionLease lease;
        private final AtomicBoolean frameBytesReleased = new AtomicBoolean();
        private final AtomicBoolean requestReleased = new AtomicBoolean();

        private ReceivedFrame(KafkaFrame frame, KafkaFrameAdmissionLease lease) {
            this.apiKey = frame.apiKey();
            this.frame = frame;
            this.lease = lease;
        }

        private void releaseFrameBytes() {
            if (frameBytesReleased.compareAndSet(false, true)) {
                frame.release();
                lease.releaseFrameBytes();
            }
        }

        private void releaseRequest() {
            if (requestReleased.compareAndSet(false, true)) {
                lease.releaseRequest();
                lease.close();
            }
        }

        private void complete() {
            releaseFrameBytes();
            releaseRequest();
        }
    }

    private static final class ReadStats
            implements KafkaProbeAwareRecvByteBufAllocator.ReadObserver {
        private final AtomicLong bytesRead = new AtomicLong();
        private final AtomicInteger maxAllocation = new AtomicInteger();

        @Override
        public void onAllocation(int bytes) {
            maxAllocation.updateAndGet(previous -> Math.max(previous, bytes));
        }

        @Override
        public void onBytesRead(int bytes) {
            bytesRead.addAndGet(bytes);
        }
    }

    private static final class CountingAdmissionMetrics implements KafkaFrameAdmissionMetrics {
        private final AtomicInteger reservationRejections = new AtomicInteger();

        @Override
        public long nowNanos() {
            return System.nanoTime();
        }

        @Override
        public void recordReservationRejected() {
            reservationRejections.incrementAndGet();
        }

        @Override
        public void recordReservationCancelled() {}

        @Override
        public void recordPreFrameWait(long startedNanos) {}

        @Override
        public void recordPreFrameWaitTimeout(long startedNanos) {}

        @Override
        public void recordBodyRead(long startedNanos) {}

        @Override
        public void recordBodyReadTimeout(long startedNanos) {}
    }
}
