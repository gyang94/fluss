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

import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaRequestAdmissionController;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
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

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Real-NIO regression tests for the complete Kafka admission and command-decoder pipeline. */
class KafkaAdmissionCommandPipelineNioTest {
    private static final int MAX_FRAME_BYTES = 1024 * 1024;
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(10);

    @Test
    void testTwoPipelinedLargeProduceRequestsRemainOnOneConnection() throws Exception {
        KafkaProduceAdmissionController produce =
                new KafkaProduceAdmissionController(64, 16L * 1024 * 1024, 64, 1024 * 1024, 32);
        KafkaProduceAdmissionController control =
                new KafkaProduceAdmissionController(
                        128, 128L * 1024 * 1024, 64, 16L * 1024 * 1024, 32);
        KafkaRequestAdmissionController admission =
                new KafkaRequestAdmissionController(produce, control, 10_000, MAX_FRAME_BYTES);

        try (RunningServer server = new RunningServer(admission);
                Socket client = server.connect()) {
            byte[] first = produceFrame(1, 655_360, (byte) 0x61);
            byte[] second = produceFrame(2, 655_360, (byte) 0x62);
            assertThat(first.length).isLessThan(MAX_FRAME_BYTES);
            assertThat(second.length).isLessThan(MAX_FRAME_BYTES);
            assertThat((long) first.length + second.length).isGreaterThan(1024 * 1024L);

            OutputStream output = client.getOutputStream();
            output.write(first);
            output.write(second);
            output.flush();

            KafkaRequest firstRequest = server.awaitRequest();
            waitUntil(
                    () -> produce.pendingReservations() == 1,
                    WAIT_TIMEOUT,
                    "second Produce request did not wait at the six-byte probe");
            server.awaitEventLoopBarrier();
            assertThat(server.acceptedChannel.isActive()).isTrue();
            assertThat(server.failure).hasValue(null);

            complete(firstRequest);
            KafkaRequest secondRequest = server.awaitRequest();
            assertThat(secondRequest.header().correlationId()).isEqualTo(2);
            assertThat(server.acceptedChannel.isActive()).isTrue();
            assertThat(server.failure).hasValue(null);

            complete(secondRequest);
            waitUntil(
                    () ->
                            produce.liveRequests() == 0
                                    && produce.rawBytes() == 0
                                    && produce.pendingReservations() == 0,
                    WAIT_TIMEOUT,
                    "Produce admission resources were not released");
        }
    }

    private static void complete(KafkaRequest request) {
        request.detachProducePayload();
        request.releaseBuffer();
        request.complete(new ProduceResponse(new ProduceResponseData()));
    }

    private static byte[] produceFrame(int correlationId, int valueBytes, byte valueByte) {
        byte[] value = new byte[valueBytes];
        java.util.Arrays.fill(value, valueByte);
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

    private static final class RunningServer implements AutoCloseable {
        private final KafkaRequestAdmissionController admission;
        private final RequestChannel requestChannel = new RequestChannel(1_000);
        private final EventLoopGroup acceptGroup = new NioEventLoopGroup(1);
        private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Channel serverChannel;
        private volatile Channel acceptedChannel;

        private RunningServer(KafkaRequestAdmissionController admission) {
            this.admission = admission;
            serverChannel =
                    new ServerBootstrap()
                            .group(acceptGroup, workerGroup)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(
                                    new KafkaChannelInitializer(
                                            new RequestChannel[] {requestChannel},
                                            "KAFKA",
                                            60,
                                            MAX_FRAME_BYTES,
                                            true,
                                            null,
                                            KafkaProduceMetrics.noOp(),
                                            admission,
                                            WAIT_TIMEOUT,
                                            WAIT_TIMEOUT) {
                                        @Override
                                        protected void initChannel(SocketChannel channel) {
                                            acceptedChannel = channel;
                                            try {
                                                super.initChannel(channel);
                                            } catch (Throwable t) {
                                                failure.compareAndSet(null, t);
                                                channel.close();
                                            }
                                        }
                                    })
                            .bind("127.0.0.1", 0)
                            .syncUninterruptibly()
                            .channel();
        }

        private Socket connect() throws Exception {
            Socket socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.connect((InetSocketAddress) serverChannel.localAddress());
            waitUntil(
                    () -> acceptedChannel != null && admission.connections() == 1,
                    WAIT_TIMEOUT,
                    "server did not initialize Kafka connection");
            return socket;
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
                    "Kafka command decoder did not enqueue request");
            return request.get();
        }

        private void awaitEventLoopBarrier() {
            acceptedChannel.eventLoop().submit((Runnable) () -> {}).syncUninterruptibly();
        }

        @Override
        public void close() {
            if (acceptedChannel != null) {
                acceptedChannel.close().syncUninterruptibly();
            }
            serverChannel.close().syncUninterruptibly();
            acceptGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
            workerGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
            KafkaRequest request;
            while ((request = (KafkaRequest) requestChannel.pollRequest(0)) != null) {
                request.releaseBuffer();
                request.fail(new IllegalStateException("test cleanup"));
            }
        }
    }
}
