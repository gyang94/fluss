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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests Kafka listener configuration and plugin-owned conversion resources. */
class KafkaProtocolPluginTest {

    @Test
    void testZeroDisablesNettyIdleTimeout() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, Duration.ZERO);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            // Adapt the socket type while installing the real pipeline on the embedded event loop.
            SocketChannel socket = mock(SocketChannel.class);
            when(socket.pipeline()).thenReturn(channel.pipeline());
            KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
            plugin.setup(conf);
            KafkaChannelInitializer initializer =
                    (KafkaChannelInitializer)
                            plugin.createChannelHandler(
                                    new RequestChannel[] {new RequestChannel(1)}, "KAFKA");
            initializer.initChannel(socket);
            IdleStateHandler idleHandler = channel.pipeline().get(IdleStateHandler.class);
            assertThat(idleHandler.getAllIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getReaderIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getWriterIdleTimeInMillis()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testClosesAllTranscodersOnce(boolean failFirstClose) throws Exception {
        List<TrackingTranscoder> transcoders = new ArrayList<>();
        KafkaProtocolPlugin plugin =
                new KafkaProtocolPlugin(
                        () -> {
                            TrackingTranscoder transcoder = new TrackingTranscoder();
                            transcoder.failClose = failFirstClose && transcoders.isEmpty();
                            transcoders.add(transcoder);
                            return transcoder;
                        });
        plugin.setup(new Configuration());
        plugin.createRequestHandler(new TestingTabletGatewayService());
        plugin.createRequestHandler(new TestingTabletGatewayService());
        CompletableFuture<Void> closed = plugin.closeAsync();
        if (failFirstClose) {
            assertThatThrownBy(() -> closed.get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IOException.class);
        } else {
            closed.get(10, TimeUnit.SECONDS);
        }
        assertThat(transcoders).hasSize(2).allSatisfy(t -> assertThat(t.closeCalls).isEqualTo(1));
        assertThat(plugin.closeAsync()).isSameAs(closed);
        assertThatThrownBy(() -> plugin.createRequestHandler(new TestingTabletGatewayService()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(transcoders).hasSize(2);
    }

    @Test
    void testClosesTranscoderAfterConversionWorkerStops() throws Exception {
        CountDownLatch converting = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch releaseConversion = new CountDownLatch(1);
        TrackingTranscoder transcoder =
                new TrackingTranscoder() {
                    @Override
                    public BytesView transcode(
                            int partitionId, List<Record> records, TableInfo table) {
                        converting.countDown();
                        boolean done = false;
                        while (!done) {
                            try {
                                assertThat(releaseConversion.await(10, TimeUnit.SECONDS)).isTrue();
                                done = true;
                            } catch (InterruptedException e) {
                                interrupted.countDown();
                            }
                        }
                        assertThat(closeCalls).isZero();
                        return new MemorySegmentBytesView(MemorySegment.wrap(new byte[] {1}), 0, 1);
                    }
                };
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin(() -> transcoder);
        plugin.setup(new Configuration());
        KafkaRequestHandler handler =
                (KafkaRequestHandler) plugin.createRequestHandler(new ProduceMetadataService());
        KafkaRequest request = produceRequest();
        try {
            handler.processRequest(request);
            assertThat(converting.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> closed = plugin.closeAsync();
            assertThat(interrupted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(closed).isNotDone();
            assertThat(transcoder.closeCalls).isZero();
            releaseConversion.countDown();
            closed.get(10, TimeUnit.SECONDS);
            request.future().get(10, TimeUnit.SECONDS);
            assertThat(transcoder.closeCalls).isEqualTo(1);
        } finally {
            releaseConversion.countDown();
            plugin.closeAsync().get(10, TimeUnit.SECONDS);
            request.releaseBuffer();
        }
    }

    private static KafkaRequest produceRequest() {
        ProduceRequestData data = new ProduceRequestData().setAcks((short) 1).setTimeoutMs(10000);
        data.topicData()
                .add(
                        new ProduceRequestData.TopicProduceData()
                                .setName("kafka.topic")
                                .setPartitionData(
                                        Collections.singletonList(
                                                new ProduceRequestData.PartitionProduceData()
                                                        .setIndex(0)
                                                        .setRecords(
                                                                MemoryRecords.withRecords(
                                                                        Compression.NONE,
                                                                        new SimpleRecord(
                                                                                new byte[] {
                                                                                    65
                                                                                }))))));
        ByteBuf buffer = Unpooled.buffer(1);
        try {
            return new KafkaRequest(
                    ApiKeys.PRODUCE,
                    (short) 9,
                    new RequestHeader(ApiKeys.PRODUCE, (short) 9, "test", 1),
                    new ProduceRequest(data, (short) 9),
                    "KAFKA",
                    buffer,
                    new TestingChannelHandlerContext(),
                    new CompletableFuture<>());
        } finally {
            buffer.release();
        }
    }

    private static class TrackingTranscoder implements KafkaRecordTranscoder {
        protected volatile int closeCalls;
        private boolean failClose;

        @Override
        public BytesView transcode(int partitionId, List<Record> records, TableInfo table) {
            throw new AssertionError("Unexpected conversion");
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            if (failClose) {
                throw new IOException("test close failure");
            }
        }
    }

    private static final class ProduceMetadataService extends TestingTabletGatewayService {
        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("_kafka_value", DataTypes.BYTES())
                                            .build())
                            .distributedBy(1)
                            .logFormat(LogFormat.ARROW)
                            .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                            .property(
                                    ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                    KafkaValueConversionType.BINARY)
                            .build();
            return CompletableFuture.completedFuture(
                    new GetTableInfoResponse()
                            .setTableId(42L)
                            .setSchemaId(1)
                            .setTableJson(descriptor.toJsonBytes())
                            .setCreatedTime(1L)
                            .setModifiedTime(1L));
        }
    }
}
