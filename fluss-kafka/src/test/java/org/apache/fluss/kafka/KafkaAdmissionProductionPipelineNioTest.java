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

package org.apache.fluss.kafka;

import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaRequestAdmissionController;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseLease;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseReason;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Deterministic real-NIO tests for PF2 through the production Kafka channel pipeline. */
class KafkaAdmissionProductionPipelineNioTest {
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration NORMAL_STAGE_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration SHORT_STAGE_TIMEOUT = Duration.ofMillis(250);

    @Test
    void testExactMaximumProduceCompletesAndMaximumPlusOneStopsBeforeAdmission() throws Exception {
        byte[] exactFrame = produceFrame(1, 64 * 1024, (byte) 0x41);
        int maxFrameBytes = exactFrame.length;
        KafkaRequestAdmissionController admission = admission(8, maxFrameBytes, 8);
        KafkaProduceAdmissionController produce = admission.produceAdmissionController();

        try (RunningServer server =
                new RunningServer(
                        admission,
                        maxFrameBytes,
                        NORMAL_STAGE_TIMEOUT,
                        NORMAL_STAGE_TIMEOUT,
                        false)) {
            try (ClientConnection exact = server.connect()) {
                exact.send(exactFrame);
                KafkaRequest request = server.awaitRequest();

                assertThat(request.header().correlationId()).isEqualTo(1);
                assertThat(produce.liveRequests()).isOne();
                assertThat(produce.rawBytes()).isEqualTo(maxFrameBytes);
                assertThat(produce.maxRawBytesOvershoot()).isZero();

                complete(request);
                server.awaitEmptyLanes();
            }
            server.awaitNoConnections();

            byte[] oversizedFrame = Arrays.copyOf(exactFrame, maxFrameBytes + 1);
            ByteBuffer.wrap(oversizedFrame).putInt(oversizedFrame.length - Integer.BYTES);
            try (ClientConnection oversized = server.connect()) {
                oversized.send(oversizedFrame, 0, 6);
                oversized.awaitServerClosed();
            }

            assertThat(server.requestChannel.requestsCount()).isZero();
            assertThat(produce.registeredConnections()).isZero();
            assertThat(produce.pendingReservations()).isZero();
            assertThat(produce.liveRequests()).isZero();
            assertThat(produce.rawBytes()).isZero();
            assertThat(produce.maxRawBytesOvershoot()).isZero();
            assertThat(admission.controlAdmissionController().registeredConnections()).isZero();
        }
    }

    @Test
    void testPreFrameTimeoutCancelsWaiterThroughProductionPipeline() throws Exception {
        byte[] frame = produceFrame(2, 8 * 1024, (byte) 0x42);
        KafkaRequestAdmissionController admission = admission(1, frame.length * 2L, 2);
        KafkaProduceAdmissionController produce = admission.produceAdmissionController();

        try (RunningServer server =
                        new RunningServer(
                                admission,
                                frame.length,
                                SHORT_STAGE_TIMEOUT,
                                NORMAL_STAGE_TIMEOUT,
                                false);
                ClientConnection holder = server.connect();
                ClientConnection waiter = server.connect()) {
            holder.send(frame);
            KafkaRequest heldRequest = server.awaitRequest();
            waiter.send(frame);
            waitUntil(
                    () -> produce.pendingReservations() == 1,
                    WAIT_TIMEOUT,
                    "second Produce request did not enter the pre-frame wait queue");

            waiter.awaitServerClosed();
            waitUntil(
                    () -> produce.pendingReservations() == 0 && admission.connections() == 1,
                    WAIT_TIMEOUT,
                    "timed-out pre-frame waiter retained admission ownership");

            assertThat(server.requestChannel.requestsCount()).isZero();
            assertThat(produce.liveRequests()).isOne();
            assertThat(produce.rawBytes()).isEqualTo(frame.length);

            complete(heldRequest);
            server.awaitEmptyLanes();
        }
    }

    @Test
    void testBodyTimeoutReleasesReservationThroughProductionPipeline() throws Exception {
        byte[] frame = produceFrame(3, 8 * 1024, (byte) 0x43);
        KafkaRequestAdmissionController admission = admission(1, frame.length, 1);
        KafkaProduceAdmissionController produce = admission.produceAdmissionController();

        try (RunningServer server =
                        new RunningServer(
                                admission,
                                frame.length,
                                NORMAL_STAGE_TIMEOUT,
                                SHORT_STAGE_TIMEOUT,
                                false);
                ClientConnection client = server.connect()) {
            client.send(frame, 0, 6);
            waitUntil(
                    () -> produce.liveRequests() == 1 && produce.rawBytes() == frame.length,
                    WAIT_TIMEOUT,
                    "partial Produce frame did not acquire exact body admission");

            client.awaitServerClosed();
            server.awaitNoConnections();
            server.awaitEmptyLanes();

            assertThat(server.requestChannel.requestsCount()).isZero();
            assertThat(produce.registeredConnections()).isZero();
            assertThat(produce.maxLiveRequestOvershoot()).isZero();
            assertThat(produce.maxRawBytesOvershoot()).isZero();
        }
    }

    @Test
    void testLiveLeaseWaitsForDelayedResponseWriteCompletion() throws Exception {
        byte[] frame = produceFrame(4, 8 * 1024, (byte) 0x44);
        KafkaRequestAdmissionController admission = admission(1, frame.length, 1);
        KafkaProduceAdmissionController produce = admission.produceAdmissionController();

        try (RunningServer server =
                        new RunningServer(
                                admission,
                                frame.length,
                                NORMAL_STAGE_TIMEOUT,
                                NORMAL_STAGE_TIMEOUT,
                                true);
                ClientConnection client = server.connect()) {
            client.send(frame);
            KafkaRequest request = server.awaitRequest();
            request.detachProducePayload();
            request.releaseBuffer();
            request.complete(new ProduceResponse(new ProduceResponseData()));

            DelayedWriteHandler delayedWrite = server.awaitDelayedWrite();
            assertThat(produce.rawBytes()).isZero();
            assertThat(produce.liveRequests()).isOne();
            assertThat(server.requestChannel.requestsCount()).isZero();

            delayedWrite.release();
            server.awaitEmptyLanes();

            assertThat(client.serverChannel.isActive()).isTrue();
            assertThat(delayedWrite.isPending()).isFalse();
        }
    }

    @Test
    void testConnectionCapRejectionsLeaveRequestChannelRegistrationsEmpty() throws Exception {
        byte[] frame = produceFrame(5, 1024, (byte) 0x45);
        KafkaRequestAdmissionController admission = admission(2, frame.length * 2L, 2);

        try (RunningServer server =
                new RunningServer(
                        admission,
                        frame.length,
                        NORMAL_STAGE_TIMEOUT,
                        NORMAL_STAGE_TIMEOUT,
                        false)) {
            List<ClientConnection> clients = new ArrayList<>();
            try {
                clients.add(server.connect());
                clients.add(server.connect());
                waitUntil(
                        () -> admission.connections() == 2,
                        WAIT_TIMEOUT,
                        "connection admission did not reach its exact limit");

                clients.add(server.connect());
                clients.add(server.connect());
                clients.get(2).awaitServerClosed();
                clients.get(3).awaitServerClosed();
                waitUntil(
                        () -> admission.connectionRejections() == 2,
                        WAIT_TIMEOUT,
                        "connection admission did not reject both excess connections");

                assertThat(server.requestChannel.requestsCount()).isZero();
                server.awaitRequestChannelUnregistered(clients.get(2).serverChannel);
                server.awaitRequestChannelUnregistered(clients.get(3).serverChannel);
            } finally {
                for (ClientConnection client : clients) {
                    client.close();
                }
            }

            server.awaitNoConnections();
            for (ClientConnection client : clients) {
                server.awaitRequestChannelUnregistered(client.serverChannel);
            }
            server.awaitEmptyLanes();
            assertThat(server.requestChannel.requestsCount()).isZero();
        }
    }

    private static KafkaRequestAdmissionController admission(
            long maxLiveRequests, long maxRawBytes, int maxConnections) {
        KafkaProduceAdmissionController produce =
                new KafkaProduceAdmissionController(
                        maxLiveRequests, maxRawBytes, maxLiveRequests, maxRawBytes, 16);
        KafkaProduceAdmissionController control =
                new KafkaProduceAdmissionController(16, maxRawBytes, 16, maxRawBytes, 16);
        return new KafkaRequestAdmissionController(produce, control, maxConnections);
    }

    private static void complete(KafkaRequest request) {
        request.detachProducePayload();
        request.releaseBuffer();
        request.complete(new ProduceResponse(new ProduceResponseData()));
    }

    private static byte[] produceFrame(int correlationId, int valueBytes, byte valueByte) {
        byte[] value = new byte[valueBytes];
        Arrays.fill(value, valueByte);
        short version = ApiKeys.PRODUCE.latestVersion();
        MemoryRecords records =
                MemoryRecords.withRecords(
                        RecordBatch.MAGIC_VALUE_V2,
                        1L,
                        Compression.NONE,
                        new SimpleRecord(1L, new byte[] {1}, value));
        ProduceRequestData.PartitionProduceData partition =
                new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records);
        ProduceRequestData.TopicProduceData topic =
                new ProduceRequestData.TopicProduceData()
                        .setName("topic")
                        .setPartitionData(Collections.singletonList(partition));
        ProduceRequest request =
                new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks((short) -1)
                                .setTimeoutMs(30_000)
                                .setTopicData(
                                        new ProduceRequestData.TopicProduceDataCollection(
                                                Collections.singletonList(topic).iterator())),
                        version);
        RequestHeader header =
                new RequestHeader(ApiKeys.PRODUCE, version, "nio-test", correlationId);
        ByteBuffer payload =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + payload.remaining());
        frame.putInt(payload.remaining()).put(payload);
        return frame.array();
    }

    private enum TestPauseReason implements PauseReason {
        REGISTRATION_PROBE
    }

    private static final class RunningServer implements AutoCloseable {
        private final KafkaRequestAdmissionController admission;
        private final RequestChannel requestChannel = new RequestChannel(1_000);
        private final EventLoopGroup acceptGroup = new NioEventLoopGroup(1);
        private final EventLoopGroup workerGroup = new NioEventLoopGroup(2);
        private final List<Channel> acceptedChannels =
                Collections.synchronizedList(new ArrayList<>());
        private final List<ClientConnection> clients =
                Collections.synchronizedList(new ArrayList<>());
        private final List<DelayedWriteHandler> delayedWrites =
                Collections.synchronizedList(new ArrayList<>());
        private final boolean delayWrites;
        private final Channel serverChannel;

        private RunningServer(
                KafkaRequestAdmissionController admission,
                int maxFrameBytes,
                Duration preFrameWaitTimeout,
                Duration bodyReadTimeout,
                boolean delayWrites) {
            this.admission = admission;
            this.delayWrites = delayWrites;
            serverChannel =
                    new ServerBootstrap()
                            .group(acceptGroup, workerGroup)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(
                                    new KafkaChannelInitializer(
                                            new RequestChannel[] {requestChannel},
                                            "KAFKA",
                                            60,
                                            maxFrameBytes,
                                            true,
                                            null,
                                            KafkaProduceMetrics.noOp(),
                                            admission,
                                            preFrameWaitTimeout,
                                            bodyReadTimeout) {
                                        @Override
                                        protected void initChannel(SocketChannel channel)
                                                throws Exception {
                                            super.initChannel(channel);
                                            if (RunningServer.this.delayWrites) {
                                                DelayedWriteHandler delayedWrite =
                                                        new DelayedWriteHandler();
                                                delayedWrites.add(delayedWrite);
                                                channel.pipeline()
                                                        .addBefore(
                                                                "frameDecoder",
                                                                "testDelayedWrite",
                                                                delayedWrite);
                                            }
                                            acceptedChannels.add(channel);
                                        }
                                    })
                            .bind("127.0.0.1", 0)
                            .syncUninterruptibly()
                            .channel();
        }

        private ClientConnection connect() throws Exception {
            int channelIndex = acceptedChannels.size();
            Socket socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.connect((InetSocketAddress) serverChannel.localAddress());
            waitUntil(
                    () -> acceptedChannels.size() > channelIndex,
                    WAIT_TIMEOUT,
                    "server did not initialize accepted Kafka connection");
            ClientConnection client = new ClientConnection(socket, acceptedChannel(channelIndex));
            clients.add(client);
            return client;
        }

        private Channel acceptedChannel(int index) {
            synchronized (acceptedChannels) {
                return acceptedChannels.get(index);
            }
        }

        private KafkaRequest awaitRequest() throws Exception {
            AtomicReference<KafkaRequest> request = new AtomicReference<>();
            waitUntil(
                    () -> {
                        KafkaRequest polled = (KafkaRequest) requestChannel.pollRequest(0);
                        if (polled == null) {
                            return false;
                        }
                        request.set(polled);
                        return true;
                    },
                    WAIT_TIMEOUT,
                    "production Kafka pipeline did not enqueue request");
            return request.get();
        }

        private DelayedWriteHandler awaitDelayedWrite() throws Exception {
            waitUntil(
                    () -> {
                        synchronized (delayedWrites) {
                            return !delayedWrites.isEmpty() && delayedWrites.get(0).isPending();
                        }
                    },
                    WAIT_TIMEOUT,
                    "Kafka response write was not delayed");
            synchronized (delayedWrites) {
                return delayedWrites.get(0);
            }
        }

        private void awaitNoConnections() throws Exception {
            waitUntil(
                    () ->
                            admission.connections() == 0
                                    && admission
                                                    .produceAdmissionController()
                                                    .registeredConnections()
                                            == 0
                                    && admission
                                                    .controlAdmissionController()
                                                    .registeredConnections()
                                            == 0,
                    WAIT_TIMEOUT,
                    "Kafka admission retained connection registrations");
        }

        private void awaitEmptyLanes() throws Exception {
            waitUntil(
                    () ->
                            isEmpty(admission.produceAdmissionController())
                                    && isEmpty(admission.controlAdmissionController()),
                    WAIT_TIMEOUT,
                    "Kafka admission retained request resources");
        }

        private void awaitRequestChannelUnregistered(Channel channel) throws Exception {
            waitUntil(
                    () -> isRequestChannelUnregistered(channel),
                    WAIT_TIMEOUT,
                    "closed Kafka connection remained registered with RequestChannel");
        }

        private boolean isRequestChannelUnregistered(Channel channel) {
            try {
                PauseLease lease =
                        requestChannel.pauseChannel(channel, TestPauseReason.REGISTRATION_PROBE);
                lease.close();
                return false;
            } catch (IllegalStateException expected) {
                return true;
            }
        }

        private static boolean isEmpty(KafkaProduceAdmissionController lane) {
            return lane.pendingReservations() == 0
                    && lane.liveRequests() == 0
                    && lane.rawBytes() == 0;
        }

        @Override
        public void close() {
            synchronized (delayedWrites) {
                for (DelayedWriteHandler delayedWrite : delayedWrites) {
                    delayedWrite.release();
                }
            }
            synchronized (clients) {
                for (ClientConnection client : clients) {
                    client.close();
                }
            }
            synchronized (acceptedChannels) {
                for (Channel channel : acceptedChannels) {
                    channel.close().syncUninterruptibly();
                }
            }
            KafkaRequest request;
            while ((request = (KafkaRequest) requestChannel.pollRequest(0)) != null) {
                request.cancel();
                request.releaseBuffer();
                request.markProcessingCompleted();
                request.fail(new IllegalStateException("test cleanup"));
            }
            serverChannel.close().syncUninterruptibly();
            acceptGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
            workerGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
            admission.close();
        }
    }

    private static final class ClientConnection implements AutoCloseable {
        private final Socket socket;
        private final Channel serverChannel;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private ClientConnection(Socket socket, Channel serverChannel) {
            this.socket = socket;
            this.serverChannel = serverChannel;
        }

        private void send(byte[] frame) throws IOException {
            send(frame, 0, frame.length);
        }

        private void send(byte[] frame, int offset, int length) throws IOException {
            OutputStream output = socket.getOutputStream();
            output.write(frame, offset, length);
            output.flush();
        }

        private void awaitServerClosed() throws Exception {
            waitUntil(
                    () -> !serverChannel.isActive(),
                    WAIT_TIMEOUT,
                    "server did not close Kafka connection");
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // The server may have already reset a rejected test connection.
                }
            }
        }
    }

    private static final class DelayedWriteHandler extends ChannelOutboundHandlerAdapter {
        private final AtomicReference<PendingWrite> pendingWrite = new AtomicReference<>();
        private final AtomicBoolean flushPending = new AtomicBoolean(false);

        @Override
        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            PendingWrite delayed = new PendingWrite(ctx, message, promise);
            if (!pendingWrite.compareAndSet(null, delayed)) {
                ctx.write(message, promise);
            }
        }

        @Override
        public void flush(ChannelHandlerContext ctx) {
            if (pendingWrite.get() == null) {
                ctx.flush();
            } else {
                flushPending.set(true);
            }
        }

        private boolean isPending() {
            return pendingWrite.get() != null;
        }

        private void release() {
            PendingWrite delayed = pendingWrite.getAndSet(null);
            if (delayed == null) {
                return;
            }
            delayed.ctx
                    .executor()
                    .execute(
                            () -> {
                                delayed.ctx.write(delayed.message, delayed.promise);
                                if (flushPending.compareAndSet(true, false)) {
                                    delayed.ctx.flush();
                                }
                            });
        }
    }

    private static final class PendingWrite {
        private final ChannelHandlerContext ctx;
        private final Object message;
        private final ChannelPromise promise;

        private PendingWrite(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            this.ctx = ctx;
            this.message = message;
            this.promise = promise;
        }
    }
}
