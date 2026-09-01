/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.transcode.KafkaOutputMemoryBudget;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaTopicWritePlan;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.util.TestMetricGroup;
import org.apache.fluss.record.bytesview.ByteBufBytesView;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RpcRequest;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;
import org.apache.fluss.shaded.netty4.io.netty.handler.flow.FlowControlHandler;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests response ordering and ownership in {@link KafkaCommandDecoder}. */
public class KafkaCommandDecoderTest {

    private static final long RECORD_TIMESTAMP = 123456L;

    @Test
    public void testConnectionRegistersAndUsesSingleRequestChannel() {
        RecordingRequestChannel firstRequestChannel = new RecordingRequestChannel(100);
        RecordingRequestChannel secondRequestChannel = new RecordingRequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {firstRequestChannel, secondRequestChannel},
                                "KAFKA"));
        ByteBuf firstBuffer = createApiVersionsRequest(1);
        ByteBuf secondBuffer = createApiVersionsRequest(2);

        RecordingRequestChannel selectedRequestChannel =
                firstRequestChannel.registerCount == 1 ? firstRequestChannel : secondRequestChannel;
        RecordingRequestChannel otherRequestChannel =
                selectedRequestChannel == firstRequestChannel
                        ? secondRequestChannel
                        : firstRequestChannel;

        try {
            assertThat(selectedRequestChannel.registerCount).isOne();
            assertThat(otherRequestChannel.registerCount).isZero();

            channel.writeInbound(firstBuffer);
            channel.writeInbound(secondBuffer);

            assertThat(selectedRequestChannel.putCount).isEqualTo(2);
            assertThat(otherRequestChannel.putCount).isZero();

            releaseNextRequest(selectedRequestChannel);
            releaseNextRequest(selectedRequestChannel);
            channel.close();
            channel.close();
            channel.runPendingTasks();

            assertThat(selectedRequestChannel.unregisterCount).isOne();
            assertThat(otherRequestChannel.unregisterCount).isZero();
            assertThat(firstBuffer.refCnt()).isZero();
            assertThat(secondBuffer.refCnt()).isZero();
        } finally {
            channel.close();
            releaseQueuedRequests(firstRequestChannel);
            releaseQueuedRequests(secondRequestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(firstBuffer);
            releaseIfRetained(secondBuffer);
        }
    }

    @Test
    public void testConnectionOwnsNativeAdmissionHandleUntilInactive() {
        KafkaNativeProduceAdmissionController nativeController =
                new KafkaNativeProduceAdmissionController(1, 10, 1, 10, 10);
        RequestChannel requestChannel = new RequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                null,
                                KafkaProduceMetrics.noOp(),
                                null,
                                nativeController));
        ByteBuf requestBuffer = createApiVersionsRequest(1);
        KafkaRequest request = null;
        KafkaNativeProduceAdmissionController.RequestLease grantedLease = null;

        try {
            assertThat(nativeController.registeredConnections()).isOne();
            channel.writeInbound(requestBuffer);
            request = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(request).isNotNull();
            assertThat(request.nativeAdmissionConnection()).isNotNull();
            assertThat(request.admissionScheduler()).isSameAs(channel.eventLoop());

            KafkaNativeProduceAdmissionController.ConnectionHandle connection =
                    request.nativeAdmissionConnection();
            grantedLease = connection.reserve(1).getFuture().join();
            KafkaNativeProduceAdmissionController.Reservation waiting = connection.reserve(1);
            assertThat(waiting.getFuture()).isNotDone();

            channel.close();
            channel.runPendingTasks();

            assertThat(connection.isClosed()).isTrue();
            assertThat(nativeController.registeredConnections()).isZero();
            assertThat(nativeController.inFlightRequests()).isOne();
            assertThatThrownBy(waiting.getFuture()::join).isInstanceOf(CancellationException.class);

            grantedLease.close();
            grantedLease = null;
            assertThat(nativeController.inFlightRequests()).isZero();
            assertThat(nativeController.convertedBytes()).isZero();
        } finally {
            if (grantedLease != null) {
                grantedLease.close();
            }
            if (request != null) {
                request.releaseBuffer();
            }
            channel.close();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(requestBuffer);
            nativeController.close();
        }
    }

    @Test
    public void testRegisteredKafkaChannelIsPausedAndResumedByRequestThreshold() {
        RequestChannel requestChannel = new RequestChannel(2);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(new RequestChannel[] {requestChannel}, "KAFKA"));
        ByteBuf firstBuffer = createApiVersionsRequest(1);
        ByteBuf secondBuffer = createApiVersionsRequest(2);

        try {
            assertThat(channel.config().isAutoRead()).isTrue();

            channel.writeInbound(firstBuffer);
            assertThat(requestChannel.requestsCount()).isOne();
            assertThat(channel.config().isAutoRead()).isTrue();

            channel.writeInbound(secondBuffer);
            channel.runPendingTasks();
            assertThat(requestChannel.requestsCount()).isEqualTo(2);
            assertThat(channel.config().isAutoRead()).isFalse();

            releaseNextRequest(requestChannel);
            channel.runPendingTasks();
            assertThat(requestChannel.requestsCount()).isOne();
            assertThat(channel.config().isAutoRead()).isTrue();

            releaseNextRequest(requestChannel);
            channel.close();
            channel.runPendingTasks();
            assertThat(firstBuffer.refCnt()).isZero();
            assertThat(secondBuffer.refCnt()).isZero();
        } finally {
            channel.close();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(firstBuffer);
            releaseIfRetained(secondBuffer);
        }
    }

    @Test
    public void testApiVersionsDuringAuthenticationIsFlushedBeforeChannelCloses() throws Exception {
        RequestChannel requestChannel = new RequestChannel(100);
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        KafkaRequestHandler requestHandler = new KafkaRequestHandler(service, service, "kafka");
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new LengthFieldPrepender(4),
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                () -> mock(ServerAuthenticator.class)));

        short handshakeVersion = 1;
        RequestHeader handshakeHeader =
                new RequestHeader(ApiKeys.SASL_HANDSHAKE, handshakeVersion, "client", 16);
        SaslHandshakeRequest handshakeRequest =
                new SaslHandshakeRequest(
                        new SaslHandshakeRequestData().setMechanism("PLAIN"), handshakeVersion);
        ByteBuf handshakeBuffer = serialize(handshakeHeader, handshakeRequest);

        short unsupportedVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() + 1);
        RequestHeader apiVersionsHeader =
                new RequestHeader(ApiKeys.API_VERSIONS, unsupportedVersion, "client", 17);
        ByteBuf apiVersionsBuffer = serializeHeaderOnly(apiVersionsHeader);

        try {
            channel.writeInbound(handshakeBuffer);
            processNextRequest(requestChannel, requestHandler);
            channel.runPendingTasks();

            ByteBuf handshakeResponseLength = channel.readOutbound();
            ByteBuf handshakeResponseBuffer = channel.readOutbound();
            try {
                assertThat(handshakeResponseLength).isNotNull();
                assertThat(handshakeResponseBuffer).isNotNull();
                assertThat(handshakeResponseLength.readInt())
                        .isEqualTo(handshakeResponseBuffer.readableBytes());
                SaslHandshakeResponse handshakeResponse =
                        (SaslHandshakeResponse)
                                AbstractResponse.parseResponse(
                                        handshakeResponseBuffer.nioBuffer(), handshakeHeader);
                assertThat(handshakeResponse.errorCounts())
                        .containsExactlyEntriesOf(Collections.singletonMap(Errors.NONE, 1));
            } finally {
                if (handshakeResponseLength != null) {
                    handshakeResponseLength.release();
                }
                if (handshakeResponseBuffer != null) {
                    handshakeResponseBuffer.release();
                }
            }
            assertThat(channel.isActive()).isTrue();

            channel.writeInbound(apiVersionsBuffer);
            processNextRequest(requestChannel, requestHandler);
            channel.runPendingTasks();

            ByteBuf apiVersionsResponseLength = channel.readOutbound();
            ByteBuf apiVersionsResponseBuffer = channel.readOutbound();
            try {
                assertThat(apiVersionsResponseLength).isNotNull();
                assertThat(apiVersionsResponseBuffer).isNotNull();
                assertThat(apiVersionsResponseLength.readInt())
                        .isEqualTo(apiVersionsResponseBuffer.readableBytes());
                ByteBuffer responsePayload = apiVersionsResponseBuffer.nioBuffer();
                ResponseHeader responseHeader =
                        ResponseHeader.parse(
                                responsePayload,
                                apiVersionsHeader.toResponseHeader().headerVersion());
                ApiVersionsResponse apiVersionsResponse =
                        ApiVersionsResponse.parse(
                                responsePayload, ApiKeys.API_VERSIONS.oldestVersion());

                assertThat(responseHeader.correlationId()).isEqualTo(17);
                assertThat(apiVersionsResponse.errorCounts())
                        .containsExactlyEntriesOf(
                                Collections.singletonMap(Errors.ILLEGAL_SASL_STATE, 1));
                assertThat(apiVersionsResponse.data().apiKeys()).isEmpty();
            } finally {
                if (apiVersionsResponseLength != null) {
                    apiVersionsResponseLength.release();
                }
                if (apiVersionsResponseBuffer != null) {
                    apiVersionsResponseBuffer.release();
                }
            }
            assertThat(channel.isActive()).isFalse();
            Object additionalResponse = channel.readOutbound();
            assertThat(additionalResponse).isNull();
            assertThat(handshakeBuffer.refCnt()).isZero();
            assertThat(apiVersionsBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testUnauthenticatedProduceIsRejectedBeforeBodyParsing() {
        RequestChannel requestChannel = new RequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                () -> {
                                    throw new AssertionError(
                                            "Authenticator must not be created for a Produce request.");
                                }));
        short produceVersion = ApiKeys.PRODUCE.latestVersion();
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, produceVersion, "client", 1);
        ByteBuf headerOnlyBuffer = serializeHeader(header);

        try {
            channel.writeInbound(headerOnlyBuffer);
            channel.runPendingTasks();

            assertThat(requestChannel.requestsCount()).isZero();
            assertThat(channel.isActive()).isFalse();
            assertThat(headerOnlyBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testDisconnectDoesNotReleaseQueuedRequestBuffer() {
        RequestChannel requestChannel = new RequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(new RequestChannel[] {requestChannel}, "KAFKA"));
        short produceVersion = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest produceRequest =
                new ProduceRequest(
                        new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000),
                        produceVersion);
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, produceVersion, "client", 1);
        ByteBuf requestBuffer = serialize(header, produceRequest);

        try {
            channel.writeInbound(requestBuffer);
            KafkaRequest queuedRequest = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(queuedRequest).isNotNull();
            assertThat(requestBuffer.refCnt()).isEqualTo(2);

            channel.close();
            channel.runPendingTasks();

            // The response-queue reference is released on disconnect, while the independent
            // RequestProcessor ownership remains valid until the worker finishes the request.
            assertThat(requestBuffer.refCnt()).isOne();
            assertThat(queuedRequest.<ProduceRequest>request().acks()).isEqualTo((short) 1);
            queuedRequest.releaseBuffer();
            assertThat(requestBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testAcksZeroSuppressesResponseAndUnblocksFollowingResponse() {
        RequestChannel requestChannel = new RequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(new RequestChannel[] {requestChannel}, "KAFKA"));
        short produceVersion = ApiKeys.PRODUCE.latestVersion();
        ProduceRequest produceRequest =
                new ProduceRequest(
                        new ProduceRequestData().setAcks((short) 0).setTimeoutMs(1000),
                        produceVersion);
        RequestHeader produceHeader =
                new RequestHeader(ApiKeys.PRODUCE, produceVersion, "client", 1);
        ByteBuf produceBuffer = serialize(produceHeader, produceRequest);

        short apiVersionsVersion = ApiKeys.API_VERSIONS.latestVersion();
        ApiVersionsRequest apiVersionsRequest =
                new ApiVersionsRequest.Builder(
                                new ApiVersionsRequestData(),
                                apiVersionsVersion,
                                apiVersionsVersion)
                        .build();
        RequestHeader apiVersionsHeader =
                new RequestHeader(ApiKeys.API_VERSIONS, apiVersionsVersion, "client", 2);
        ByteBuf apiVersionsBuffer = serialize(apiVersionsHeader, apiVersionsRequest);

        try {
            channel.writeInbound(produceBuffer);
            channel.writeInbound(apiVersionsBuffer);
            KafkaRequest first = (KafkaRequest) requestChannel.pollRequest(1000);
            KafkaRequest second = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();

            // Polling the requests above stands in for RequestProcessor. In production its finally
            // block releases the processor-owned reference after dispatching each request, while
            // the ordered-response queue keeps its independent reference until completion.
            first.releaseBuffer();
            second.releaseBuffer();

            second.complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
            channel.runPendingTasks();
            Object blockedResponse = channel.readOutbound();
            assertThat(blockedResponse).isNull();

            first.complete(new ProduceResponse(new ProduceResponseData()));
            channel.runPendingTasks();

            ByteBuf response = channel.readOutbound();
            try {
                assertThat(response).isNotNull();
                ResponseHeader responseHeader =
                        ResponseHeader.parse(
                                response.nioBuffer(),
                                apiVersionsHeader.toResponseHeader().headerVersion());
                assertThat(responseHeader.correlationId()).isEqualTo(2);
                Object additionalResponse = channel.readOutbound();
                assertThat(additionalResponse).isNull();
            } finally {
                if (response != null) {
                    response.release();
                }
            }

            assertThat(produceBuffer.refCnt()).isZero();
            assertThat(apiVersionsBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testProduceRawBytesRemainChargedUntilPendingBackendCompletes() throws Exception {
        ProduceLifecycleFixture fixture = new ProduceLifecycleFixture();
        ByteBuf requestBuffer = createProduceRequest(1);
        int requestBytes = requestBuffer.readableBytes();

        try {
            fixture.channel.writeInbound(requestBuffer);
            KafkaRequest request = fixture.pollRequest();

            assertThat(fixture.admissionController.liveRequests()).isOne();
            assertThat(fixture.admissionController.liveBytes()).isEqualTo(requestBytes);
            assertThat(fixture.admissionController.rawBytes()).isEqualTo(requestBytes);

            fixture.requestHandler.processRequest(request);
            request.releaseBuffer();

            // Copying records grows PF raw ownership beyond the wire frame. Copied records can
            // remain reachable from any topic stage until the aggregate backend future ends, so
            // that grown accounting stays charged through the same boundary.
            assertThat(fixture.service.produceFuture).isNotDone();
            long retainedRawBytes = fixture.admissionController.rawBytes();
            assertThat(retainedRawBytes).isGreaterThan(requestBytes);
            assertThat(fixture.admissionController.liveRequests()).isOne();
            assertThat(fixture.admissionController.liveBytes()).isEqualTo(requestBytes);
            assertThat(requestBuffer.refCnt()).isZero();

            fixture.completeProduce();
            fixture.channel.runPendingTasks();

            ByteBuf response = fixture.channel.readOutbound();
            assertThat(response).isNotNull();
            response.release();
            assertThat(fixture.admissionController.liveRequests()).isZero();
            assertThat(fixture.admissionController.liveBytes()).isZero();
            assertThat(fixture.admissionController.rawBytes()).isZero();
        } finally {
            fixture.close();
            releaseIfRetained(requestBuffer);
        }
    }

    @Test
    public void testRawAdmissionTransfersWhileNativeReservationWaits() throws Exception {
        RequestChannel requestChannel = new RequestChannel(100);
        KafkaProduceAdmissionController rawController = admissionController();
        KafkaNativeProduceAdmissionController nativeController =
                new KafkaNativeProduceAdmissionController(1, 1 << 20, 1, 1 << 20, 10);
        KafkaNativeProduceAdmissionController.ConnectionHandle blockerConnection =
                nativeController.registerConnection();
        KafkaNativeProduceAdmissionController.RequestLease blocker =
                blockerConnection.reserve(1).getFuture().join();
        PendingProduceGatewayService service = new PendingProduceGatewayService();
        KafkaRequestHandler requestHandler =
                new KafkaRequestHandler(
                        service,
                        service,
                        "kafka",
                        KafkaProduceMetrics.noOp(),
                        new TestingKafkaRecordTranscoder());
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                null,
                                KafkaProduceMetrics.noOp(),
                                rawController,
                                nativeController));
        ByteBuf requestBuffer = createProduceRequest(1);
        int requestBytes = requestBuffer.readableBytes();

        try {
            channel.writeInbound(requestBuffer);
            KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(request).isNotNull();
            assertThat(rawController.rawBytes()).isEqualTo(requestBytes);
            requestHandler.processRequest(request);
            request.releaseBuffer();

            assertThat(nativeController.pendingReservations()).isOne();
            assertThat(nativeController.pendingReservedBytes()).isPositive();
            assertThat(rawController.rawBytes()).isZero();
            assertThat(service.produceFuture).isNotDone();

            blocker.close();
            assertThat(nativeController.pendingReservations()).isZero();
            assertThat(nativeController.inFlightRequests()).isOne();
            assertThat(nativeController.pendingReservedBytes()).isZero();
            assertThat(rawController.rawBytes()).isZero();
            assertThat(service.produceFuture).isNotDone();

            service.produceFuture.complete(
                    new ProduceLogResponse()
                            .addAllBucketsResps(
                                    Collections.singletonList(
                                            new PbProduceLogRespForBucket()
                                                    .setBucketId(0)
                                                    .setBaseOffset(42L))));
            channel.runPendingTasks();
            ByteBuf response = channel.readOutbound();
            assertThat(response).isNotNull();
            response.release();

            assertThat(nativeController.inFlightRequests()).isZero();
            assertThat(nativeController.totalReservedBytes()).isZero();
            assertThat(rawController.rawBytes()).isZero();
            assertThat(rawController.liveRequests()).isZero();
        } finally {
            blocker.close();
            blockerConnection.close();
            channel.close();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(requestBuffer);
            nativeController.close();
        }
    }

    @Test
    public void testDisconnectKeepsPendingProduceLiveUntilProcessingCompletes() throws Exception {
        ProduceLifecycleFixture fixture = new ProduceLifecycleFixture();
        ByteBuf requestBuffer = createProduceRequest(1);
        int requestBytes = requestBuffer.readableBytes();

        try {
            fixture.channel.writeInbound(requestBuffer);
            KafkaRequest request = fixture.pollRequest();
            fixture.requestHandler.processRequest(request);
            request.releaseBuffer();

            long retainedRawBytes = fixture.admissionController.rawBytes();
            assertThat(retainedRawBytes).isGreaterThan(requestBytes);
            assertThat(fixture.admissionController.liveRequests()).isOne();
            assertThat(fixture.admissionController.liveBytes()).isEqualTo(requestBytes);

            fixture.channel.close();
            fixture.channel.runPendingTasks();

            assertThat(fixture.admissionController.registeredConnections()).isZero();
            assertThat(fixture.admissionController.liveRequests()).isOne();
            assertThat(fixture.admissionController.liveBytes()).isEqualTo(requestBytes);
            assertThat(fixture.admissionController.rawBytes()).isEqualTo(retainedRawBytes);

            fixture.completeProduce();
            fixture.channel.runPendingTasks();

            assertThat(fixture.admissionController.liveRequests()).isZero();
            assertThat(fixture.admissionController.liveBytes()).isZero();
            assertThat(fixture.admissionController.rawBytes()).isZero();
            Object response = fixture.channel.readOutbound();
            assertThat(response).isNull();
            assertThat(requestBuffer.refCnt()).isZero();
        } finally {
            fixture.close();
            releaseIfRetained(requestBuffer);
        }
    }

    @Test
    public void testDelayedProduceResponseWriteIsVisibleUntilDisconnect() {
        RequestChannel requestChannel = new RequestChannel(100);
        ManualClock clock = new ManualClock();
        KafkaProduceMetrics metrics =
                new KafkaProduceMetrics(TestMetricGroup.newBuilder().build(), clock);
        DelayedWriteHandler delayedWriteHandler = new DelayedWriteHandler();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        delayedWriteHandler,
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel}, "KAFKA", null, metrics));
        ByteBuf requestBuffer = createProduceRequest(1);

        try {
            channel.writeInbound(requestBuffer);
            KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(request).isNotNull();
            request.releaseBuffer();
            request.complete(new ProduceResponse(new ProduceResponseData()));
            channel.runPendingTasks();

            assertThat(delayedWriteHandler.hasPendingWrite()).isTrue();
            assertThat(metrics.pendingResponseWrites()).isOne();
            assertThat(metrics.pendingResponseWriteBytes()).isPositive();
            clock.advanceTime(25, TimeUnit.MILLISECONDS);
            assertThat(metrics.oldestPendingResponseWriteAgeMillis()).isEqualTo(25L);

            channel.close();
            channel.runPendingTasks();
            assertThat(metrics.pendingResponseWrites()).isZero();
            assertThat(metrics.pendingResponseWriteBytes()).isZero();
            assertThat(metrics.oldestPendingResponseWriteAgeMillis()).isZero();
        } finally {
            delayedWriteHandler.releasePendingWrite();
            channel.close();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(requestBuffer);
        }
    }

    @Test
    public void testMetricClockFailuresDoNotDropProduceResponseOrLeakRequest() {
        RequestChannel requestChannel = new RequestChannel(100);
        KafkaProduceMetrics metrics =
                new KafkaProduceMetrics(TestMetricGroup.newBuilder().build(), new ThrowingClock());
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel}, "KAFKA", null, metrics));
        ByteBuf requestBuffer = createProduceRequest(1);
        ByteBuf responseBuffer = null;

        try {
            channel.writeInbound(requestBuffer);
            KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(request).isNotNull();
            request.releaseBuffer();
            request.complete(new ProduceResponse(new ProduceResponseData()));
            channel.runPendingTasks();

            responseBuffer = channel.readOutbound();
            assertThat(responseBuffer).isNotNull();
            assertThat(channel.isActive()).isTrue();
            assertThat(metrics.pendingResponseWrites()).isZero();
            assertThat(requestBuffer.refCnt()).isZero();
        } finally {
            ReferenceCountUtil.safeRelease(responseBuffer);
            channel.close();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(requestBuffer);
        }
    }

    @Test
    public void testFailedProduceResponseWriteIsRemovedExactlyOnce() {
        RequestChannel requestChannel = new RequestChannel(100);
        KafkaProduceMetrics metrics = new KafkaProduceMetrics(TestMetricGroup.newBuilder().build());
        DelayedWriteHandler delayedWriteHandler = new DelayedWriteHandler();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        delayedWriteHandler,
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel}, "KAFKA", null, metrics));
        ByteBuf requestBuffer = createProduceRequest(1);

        try {
            channel.writeInbound(requestBuffer);
            KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(request).isNotNull();
            request.releaseBuffer();
            request.complete(new ProduceResponse(new ProduceResponseData()));
            channel.runPendingTasks();
            assertThat(metrics.pendingResponseWrites()).isOne();

            delayedWriteHandler.failPendingWrite(new IllegalStateException("test write failure"));
            channel.runPendingTasks();
            assertThat(metrics.pendingResponseWrites()).isZero();
            assertThat(metrics.pendingResponseWriteBytes()).isZero();

            channel.close();
            channel.runPendingTasks();
            assertThat(metrics.pendingResponseWrites()).isZero();
        } finally {
            delayedWriteHandler.releasePendingWrite();
            channel.close();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(requestBuffer);
        }
    }

    @Test
    public void testDelayedWriteKeepsLiveLeaseUntilSuccessfulListenerCompletion() {
        ResponseWriteLifecycleFixture fixture = new ResponseWriteLifecycleFixture();
        ResponseWriteAttempt attempt = fixture.submitProduce(1);

        try {
            assertThat(fixture.handler.pendingCount()).isOne();
            assertThat(fixture.metrics.pendingResponseWrites()).isOne();
            assertThat(fixture.metrics.pendingResponseWriteBytes()).isPositive();
            assertThat(fixture.admissionController.liveRequests()).isOne();
            assertThat(fixture.admissionController.rawBytes()).isZero();
            assertThat(attempt.requestBuffer.refCnt()).isZero();
            assertThat(attempt.responseWrite.buffer().refCnt()).isOne();

            fixture.handler.succeedNext();
            fixture.channel.runPendingTasks();

            assertResponseWriteLifecycleReleased(fixture, Collections.singletonList(attempt), 1);
            assertThat(fixture.channel.isActive()).isTrue();
        } finally {
            fixture.close();
        }
    }

    @Test
    public void testFailedWriteListenerReleasesLiveLeaseAndMetricsExactlyOnce() {
        ResponseWriteLifecycleFixture fixture = new ResponseWriteLifecycleFixture();
        ResponseWriteAttempt attempt = fixture.submitProduce(1);

        try {
            fixture.handler.failNext(new IllegalStateException("test write failure"));
            fixture.channel.runPendingTasks();

            assertResponseWriteLifecycleReleased(fixture, Collections.singletonList(attempt), 1);
            assertThat(fixture.channel.isActive()).isFalse();

            fixture.channel.close();
            fixture.channel.runPendingTasks();
            assertThat(attempt.responseWrite.trySucceedAgain()).isFalse();
            assertResponseWriteLifecycleReleased(fixture, Collections.singletonList(attempt), 1);
        } finally {
            fixture.close();
        }
    }

    @Test
    public void testDisconnectBeforeMultipleWriteListenersReleasesEverythingExactlyOnce() {
        ResponseWriteLifecycleFixture fixture = new ResponseWriteLifecycleFixture();
        ResponseWriteAttempt first = fixture.submitProduce(1);
        ResponseWriteAttempt second = fixture.submitProduce(2);
        List<ResponseWriteAttempt> attempts = java.util.Arrays.asList(first, second);

        try {
            assertThat(fixture.handler.pendingCount()).isEqualTo(2);
            assertThat(fixture.metrics.pendingResponseWrites()).isEqualTo(2);
            assertThat(fixture.admissionController.liveRequests()).isEqualTo(2);
            assertThat(fixture.admissionController.rawBytes()).isZero();

            // Channel deactivation closes response metrics first, while the deliberately delayed
            // transport promises still own the response buffers and the PF live leases.
            fixture.channel.close();
            fixture.channel.runPendingTasks();
            assertThat(fixture.metrics.pendingResponseWrites()).isZero();
            assertThat(fixture.metrics.pendingResponseWriteBytes()).isZero();
            assertThat(fixture.metricGroup.responseWriteCompletionCount()).isEqualTo(2);
            assertThat(fixture.admissionController.liveRequests()).isEqualTo(2);

            fixture.handler.failAll(new IllegalStateException("connection closed"));
            fixture.channel.runPendingTasks();

            assertResponseWriteLifecycleReleased(fixture, attempts, 2);
            assertThat(first.responseWrite.trySucceedAgain()).isFalse();
            assertThat(second.responseWrite.trySucceedAgain()).isFalse();
            fixture.channel.close();
            fixture.channel.runPendingTasks();
            assertResponseWriteLifecycleReleased(fixture, attempts, 2);
        } finally {
            fixture.close();
        }
    }

    @Test
    public void testEnqueueFailureReleasesProduceAdmissionAndAllBufferOwners() {
        ThrowingRequestChannel requestChannel = new ThrowingRequestChannel();
        KafkaProduceAdmissionController admissionController = admissionController();
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                null,
                                KafkaProduceMetrics.noOp(),
                                admissionController));
        ByteBuf requestBuffer = createProduceRequest(1);

        try {
            channel.writeInbound(requestBuffer);
            channel.runPendingTasks();

            assertThat(channel.isActive()).isFalse();
            assertThat(requestChannel.requestsCount()).isZero();
            assertThat(admissionController.registeredConnections()).isZero();
            assertThat(admissionController.liveRequests()).isZero();
            assertThat(admissionController.liveBytes()).isZero();
            assertThat(admissionController.rawBytes()).isZero();
            assertThat(requestBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
            releaseIfRetained(requestBuffer);
        }
    }

    @Test
    public void testAdmissionPauseStopsRemainingFramesFromSameSocketRead() {
        RequestChannel requestChannel = new RequestChannel(100);
        KafkaProduceAdmissionController admissionController =
                new KafkaProduceAdmissionController(100, 1 << 20, 1, 1 << 20);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new LengthFieldBasedFrameDecoder(1 << 20, 0, 4, 0, 4),
                        new FlowControlHandler(),
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                null,
                                KafkaProduceMetrics.noOp(),
                                admissionController));
        ByteBuf firstPayload = createProduceRequest(1);
        ByteBuf secondPayload = createProduceRequest(2);
        ByteBuf combinedFrames = combineLengthPrefixed(firstPayload, secondPayload);
        firstPayload.release();
        secondPayload.release();

        KafkaRequest firstRequest = null;
        try {
            channel.writeInbound(combinedFrames);
            channel.runPendingTasks();

            // LengthFieldBasedFrameDecoder can produce both frames from one socket read. The
            // synchronous auto-read transition must be visible to FlowControlHandler before it
            // forwards the second frame to KafkaCommandDecoder.
            assertThat(requestChannel.requestsCount()).isOne();
            assertThat(admissionController.liveRequests()).isOne();
            assertThat(channel.config().isAutoRead()).isFalse();
            assertThat(requestChannel.activePauseReasons(channel))
                    .contains(
                            KafkaProduceAdmissionController.AdmissionPauseReason
                                    .LIVE_REQUEST_COUNT);

            firstRequest = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(firstRequest).isNotNull();
            firstRequest.releaseBuffer();
            channel.close();
            firstRequest.fail(new IllegalStateException("test cleanup"));
            channel.runPendingTasks();

            assertThat(requestChannel.requestsCount()).isZero();
            assertThat(admissionController.liveRequests()).isZero();
            assertThat(admissionController.rawBytes()).isZero();
        } finally {
            channel.close();
            if (firstRequest != null) {
                firstRequest.releaseBuffer();
                firstRequest.fail(new IllegalStateException("test cleanup"));
            }
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(combinedFrames);
        }
    }

    @Test
    public void testPerConnectionLivePauseIsReestablishedDuringSynchronousResume() {
        RequestChannel requestChannel = new RequestChannel(100);
        KafkaProduceAdmissionController admissionController =
                new KafkaProduceAdmissionController(100, 1 << 20, 1, 1 << 20);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new LengthFieldBasedFrameDecoder(1 << 20, 0, 4, 0, 4),
                        new FlowControlHandler(),
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                null,
                                KafkaProduceMetrics.noOp(),
                                admissionController));
        ByteBuf combinedFrames = createLengthPrefixedProduceRequests(1, 2, 3, 4);

        try {
            channel.writeInbound(combinedFrames);

            for (int correlationId = 1; correlationId <= 4; correlationId++) {
                assertThat(requestChannel.requestsCount())
                        .as("queued before completing correlation %s", correlationId)
                        .isOne();
                assertThat(admissionController.liveRequests())
                        .as("live before completing correlation %s", correlationId)
                        .isOne();
                assertThat(admissionController.maxConnectionLiveRequests()).isOne();
                assertThat(admissionController.maxLiveRequestOvershoot()).isZero();
                assertThat(channel.config().isAutoRead()).isFalse();

                KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
                assertThat(request).isNotNull();
                assertThat(request.header().correlationId()).isEqualTo(correlationId);
                completeProduceRequest(channel, request);
            }

            assertThat(requestChannel.requestsCount()).isZero();
            assertThat(admissionController.liveRequests()).isZero();
            assertThat(admissionController.maxConnectionLiveRequests()).isZero();
            assertThat(admissionController.maxLiveRequestOvershoot()).isZero();
            assertThat(channel.config().isAutoRead()).isTrue();
        } finally {
            channel.close();
            channel.runPendingTasks();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(combinedFrames);
        }
    }

    @Test
    public void testLegacyPerConnectionRawPauseIsReestablishedAfterLiveCompletion() {
        ByteBuf sampleRequest = createProduceRequest(0);
        int requestBytes = sampleRequest.readableBytes();
        sampleRequest.release();
        RequestChannel requestChannel = new RequestChannel(100);
        KafkaProduceAdmissionController admissionController =
                new KafkaProduceAdmissionController(100, 1 << 20, 100, requestBytes);
        EmbeddedChannel channel = createFramedAdmissionChannel(requestChannel, admissionController);
        ByteBuf combinedFrames = createLengthPrefixedProduceRequests(1, 2, 3, 4);

        try {
            channel.writeInbound(combinedFrames);

            for (int correlationId = 1; correlationId <= 4; correlationId++) {
                assertThat(requestChannel.requestsCount()).isOne();
                assertThat(admissionController.rawBytes()).isEqualTo(requestBytes);
                assertThat(admissionController.maxConnectionRawBytes()).isEqualTo(requestBytes);
                assertThat(admissionController.maxRawBytesOvershoot()).isZero();
                assertThat(channel.config().isAutoRead()).isFalse();

                KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
                assertThat(request).isNotNull();
                assertThat(request.header().correlationId()).isEqualTo(correlationId);
                // The production pre-frame path queues a frame when the live-envelope limit is
                // still occupied. This legacy post-frame fixture has no pending reservation, so
                // finish the response/live lifetime first and then release raw ownership. That
                // isolates the raw-pause resume/reentrancy behavior this test is intended to cover.
                request.complete(new ProduceResponse(new ProduceResponseData()));
                channel.runPendingTasks();
                ByteBuf response = channel.readOutbound();
                assertThat(response).isNotNull();
                response.release();
                request.releaseBuffer();
                channel.runPendingTasks();
            }

            assertThat(requestChannel.requestsCount()).isZero();
            assertThat(admissionController.rawBytes()).isZero();
            assertThat(admissionController.maxConnectionRawBytes()).isZero();
            assertThat(admissionController.maxRawBytesOvershoot()).isZero();
            assertThat(channel.config().isAutoRead()).isTrue();
        } finally {
            channel.close();
            channel.runPendingTasks();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            releaseIfRetained(combinedFrames);
        }
    }

    @Test
    public void testGlobalProducePressureDoesNotPauseApiOnlyConnection() {
        KafkaProduceAdmissionController admissionController =
                new KafkaProduceAdmissionController(1, 1 << 20, 1, 1 << 20);
        RequestChannel producerRequestChannel = new RequestChannel(100);
        EmbeddedChannel producerChannel =
                createAdmissionChannel(producerRequestChannel, admissionController);
        ByteBuf produceBuffer = createProduceRequest(1);
        RequestChannel apiRequestChannel = new RequestChannel(100);
        EmbeddedChannel apiChannel = null;
        ByteBuf apiVersionsBuffer = createApiVersionsRequest(2);
        KafkaRequest producerRequest = null;

        try {
            producerChannel.writeInbound(produceBuffer);
            producerRequest = (KafkaRequest) producerRequestChannel.pollRequest(1000);
            assertThat(producerRequest).isNotNull();
            assertThat(admissionController.liveRequests()).isOne();
            assertThat(admissionController.registeredConnections()).isOne();

            // Merely opening a Kafka connection must not register it with Produce admission, even
            // while a producer has activated the TabletServer-wide live-request watermark.
            apiChannel = createAdmissionChannel(apiRequestChannel, admissionController);
            assertThat(apiChannel.config().isAutoRead()).isTrue();
            assertThat(admissionController.registeredConnections()).isOne();

            apiChannel.writeInbound(apiVersionsBuffer);
            KafkaRequest apiRequest = (KafkaRequest) apiRequestChannel.pollRequest(1000);
            assertThat(apiRequest).isNotNull();
            assertThat(apiChannel.config().isAutoRead()).isTrue();
            assertThat(admissionController.registeredConnections()).isOne();

            apiRequest.releaseBuffer();
            apiRequest.complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
            apiChannel.runPendingTasks();
            ByteBuf response = apiChannel.readOutbound();
            assertThat(response).isNotNull();
            response.release();
        } finally {
            if (apiChannel != null) {
                apiChannel.close();
            }
            releaseQueuedRequests(apiRequestChannel);
            if (apiChannel != null) {
                apiChannel.finishAndReleaseAll();
            }
            if (producerRequest != null) {
                producerRequest.releaseBuffer();
            }
            producerChannel.close();
            if (producerRequest != null) {
                producerRequest.fail(new IllegalStateException("test cleanup"));
            }
            producerChannel.runPendingTasks();
            releaseQueuedRequests(producerRequestChannel);
            producerChannel.finishAndReleaseAll();
            releaseIfRetained(produceBuffer);
            releaseIfRetained(apiVersionsBuffer);
        }
    }

    @Test
    public void testLegacyPostFrameAdmissionRejectsWhenCapacityIsFull() {
        KafkaProduceAdmissionController admissionController =
                new KafkaProduceAdmissionController(1, 1 << 20, 1, 1 << 20);
        RequestChannel pressureRequestChannel = new RequestChannel(100);
        EmbeddedChannel pressureChannel =
                createAdmissionChannel(pressureRequestChannel, admissionController);
        ByteBuf pressureBuffer = createProduceRequest(1);
        RequestChannel newProducerRequestChannel = new RequestChannel(100);
        EmbeddedChannel newProducerChannel = null;
        ByteBuf newProduceBuffer = createProduceRequest(2);
        KafkaRequest pressureRequest = null;
        KafkaRequest newProduceRequest = null;

        try {
            pressureChannel.writeInbound(pressureBuffer);
            pressureRequest = (KafkaRequest) pressureRequestChannel.pollRequest(1000);
            assertThat(pressureRequest).isNotNull();
            assertThat(admissionController.liveRequests()).isOne();
            assertThat(admissionController.registeredConnections()).isOne();
            newProducerChannel =
                    createAdmissionChannel(newProducerRequestChannel, admissionController);
            assertThat(newProducerChannel.config().isAutoRead()).isTrue();

            newProducerChannel.writeInbound(newProduceBuffer);
            newProduceRequest = (KafkaRequest) newProducerRequestChannel.pollRequest(1000);
            newProducerChannel.runPendingTasks();

            assertThat(newProduceRequest).isNull();
            assertThat(newProducerChannel.isActive()).isFalse();
            assertThat(admissionController.registeredConnections()).isOne();
            assertThat(admissionController.liveRequests()).isOne();
            assertThat(admissionController.liveRequestOvershootEvents()).isZero();
            assertThat(admissionController.maxLiveRequestOvershoot()).isZero();
        } finally {
            if (newProduceRequest != null) {
                newProduceRequest.releaseBuffer();
            }
            if (newProducerChannel != null) {
                newProducerChannel.close();
            }
            if (newProduceRequest != null) {
                newProduceRequest.fail(new IllegalStateException("test cleanup"));
            }
            if (newProducerChannel != null) {
                newProducerChannel.runPendingTasks();
            }
            releaseQueuedRequests(newProducerRequestChannel);
            if (newProducerChannel != null) {
                newProducerChannel.finishAndReleaseAll();
            }
            if (pressureRequest != null) {
                pressureRequest.releaseBuffer();
            }
            pressureChannel.close();
            if (pressureRequest != null) {
                pressureRequest.fail(new IllegalStateException("test cleanup"));
            }
            pressureChannel.runPendingTasks();
            releaseQueuedRequests(pressureRequestChannel);
            pressureChannel.finishAndReleaseAll();
            releaseIfRetained(pressureBuffer);
            releaseIfRetained(newProduceBuffer);
        }
    }

    @Test
    public void testLegacyPostFrameAdmissionPreventsConnectionStormOvershoot() {
        final int connectionCount = 4;
        KafkaProduceAdmissionController admissionController =
                new KafkaProduceAdmissionController(1, 1 << 20, 1, 1 << 20);
        RequestChannel[] requestChannels = new RequestChannel[connectionCount];
        EmbeddedChannel[] channels = new EmbeddedChannel[connectionCount];
        ByteBuf[] requestBuffers = new ByteBuf[connectionCount];
        KafkaRequest[] requests = new KafkaRequest[connectionCount];
        int requestBytes = 0;

        try {
            for (int i = 0; i < connectionCount; i++) {
                requestChannels[i] = new RequestChannel(100);
                channels[i] = createAdmissionChannel(requestChannels[i], admissionController);
                requestBuffers[i] = createProduceRequest(i + 1);
                requestBytes = requestBuffers[i].readableBytes();
            }
            assertThat(admissionController.registeredConnections()).isZero();

            for (int i = 0; i < connectionCount; i++) {
                channels[i].writeInbound(requestBuffers[i]);
                requests[i] = (KafkaRequest) requestChannels[i].pollRequest(1000);
                channels[i].runPendingTasks();
                if (i == 0) {
                    assertThat(requests[i]).isNotNull();
                    assertThat(requests[i].header().correlationId()).isEqualTo(1);
                } else {
                    assertThat(requests[i]).isNull();
                    assertThat(channels[i].isActive()).isFalse();
                }
            }

            // Direct decoder users do not get the production pre-frame allocation boundary, but
            // the strict fallback still refuses capacity instead of recreating BP1's unbounded
            // first-frame overshoot.
            assertThat(admissionController.liveRequests()).isOne();
            assertThat(admissionController.liveBytes()).isEqualTo(requestBytes);
            assertThat(admissionController.rawBytes()).isEqualTo(requestBytes);
            assertThat(admissionController.maxConnectionLiveRequests()).isOne();
            assertThat(admissionController.liveRequestOvershootEvents()).isZero();
            assertThat(admissionController.maxLiveRequestOvershoot()).isZero();
            assertThat(admissionController.registeredConnections()).isOne();

            completeProduceRequest(channels[0], requests[0]);
            channels[0].runPendingTasks();

            assertThat(admissionController.liveRequests()).isZero();
            assertThat(admissionController.liveBytes()).isZero();
            assertThat(admissionController.rawBytes()).isZero();
            assertThat(admissionController.pausedConnections()).isZero();
            assertThat(channels[0].config().isAutoRead()).isTrue();
            assertThat(requestChannels[0].activePauseReasons(channels[0])).isEmpty();
        } finally {
            for (int i = 0; i < connectionCount; i++) {
                if (requests[i] != null) {
                    requests[i].releaseBuffer();
                    requests[i].fail(new IllegalStateException("test cleanup"));
                }
                if (channels[i] != null) {
                    channels[i].close();
                    channels[i].runPendingTasks();
                }
                if (requestChannels[i] != null) {
                    releaseQueuedRequests(requestChannels[i]);
                }
                if (channels[i] != null) {
                    channels[i].finishAndReleaseAll();
                }
                if (requestBuffers[i] != null) {
                    releaseIfRetained(requestBuffers[i]);
                }
            }
        }
        assertThat(admissionController.registeredConnections()).isZero();
        assertThat(admissionController.liveRequests()).isZero();
        assertThat(admissionController.liveBytes()).isZero();
        assertThat(admissionController.rawBytes()).isZero();
        assertThat(admissionController.pausedConnections()).isZero();
    }

    @Test
    public void testConnectionWithoutProduceNeverRegistersWithAdmission() {
        KafkaProduceAdmissionController admissionController = admissionController();
        RequestChannel requestChannel = new RequestChannel(100);
        EmbeddedChannel channel = createAdmissionChannel(requestChannel, admissionController);

        try {
            assertThat(admissionController.registeredConnections()).isZero();
            channel.close();
            channel.runPendingTasks();
            assertThat(admissionController.registeredConnections()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static ByteBuf serialize(RequestHeader header, AbstractRequest request) {
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        return Unpooled.wrappedBuffer(serialized);
    }

    private static ByteBuf createApiVersionsRequest(int correlationId) {
        short apiVersion = ApiKeys.API_VERSIONS.latestVersion();
        ApiVersionsRequest request =
                new ApiVersionsRequest.Builder(new ApiVersionsRequestData(), apiVersion, apiVersion)
                        .build();
        RequestHeader header =
                new RequestHeader(ApiKeys.API_VERSIONS, apiVersion, "client", correlationId);
        return serialize(header, request);
    }

    private static ByteBuf createProduceRequest(int correlationId) {
        short version = ApiKeys.PRODUCE.latestVersion();
        MemoryRecords records =
                MemoryRecords.withRecords(
                        org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2,
                        RECORD_TIMESTAMP,
                        org.apache.kafka.common.compress.Compression.NONE,
                        new SimpleRecord(
                                RECORD_TIMESTAMP,
                                "key".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                "value".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        ProduceRequestData.PartitionProduceData partition =
                new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records);
        ProduceRequestData.TopicProduceData topic =
                new ProduceRequestData.TopicProduceData()
                        .setName("topic")
                        .setPartitionData(Collections.singletonList(partition));
        ProduceRequest request =
                new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks((short) 1)
                                .setTimeoutMs(1000)
                                .setTopicData(
                                        new ProduceRequestData.TopicProduceDataCollection(
                                                Collections.singletonList(topic).iterator())),
                        version);
        return serialize(
                new RequestHeader(ApiKeys.PRODUCE, version, "client", correlationId), request);
    }

    private static KafkaProduceAdmissionController admissionController() {
        return new KafkaProduceAdmissionController(100, 1 << 20, 100, 1 << 20);
    }

    private static EmbeddedChannel createAdmissionChannel(
            RequestChannel requestChannel, KafkaProduceAdmissionController admissionController) {
        return new EmbeddedChannel(
                new KafkaCommandDecoder(
                        new RequestChannel[] {requestChannel},
                        "KAFKA",
                        null,
                        KafkaProduceMetrics.noOp(),
                        admissionController));
    }

    private static EmbeddedChannel createFramedAdmissionChannel(
            RequestChannel requestChannel, KafkaProduceAdmissionController admissionController) {
        return new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(1 << 20, 0, 4, 0, 4),
                new FlowControlHandler(),
                new KafkaCommandDecoder(
                        new RequestChannel[] {requestChannel},
                        "KAFKA",
                        null,
                        KafkaProduceMetrics.noOp(),
                        admissionController));
    }

    private static ByteBuf createLengthPrefixedProduceRequests(int... correlationIds) {
        ByteBuf[] payloads = new ByteBuf[correlationIds.length];
        try {
            for (int i = 0; i < correlationIds.length; i++) {
                payloads[i] = createProduceRequest(correlationIds[i]);
            }
            return combineLengthPrefixed(payloads);
        } finally {
            for (ByteBuf payload : payloads) {
                if (payload != null) {
                    payload.release();
                }
            }
        }
    }

    private static void completeProduceRequest(EmbeddedChannel channel, KafkaRequest request) {
        request.releaseBuffer();
        request.complete(new ProduceResponse(new ProduceResponseData()));
        channel.runPendingTasks();
        ByteBuf response = channel.readOutbound();
        assertThat(response).isNotNull();
        response.release();
    }

    private static ByteBuf combineLengthPrefixed(ByteBuf... payloads) {
        int totalBytes = 0;
        for (ByteBuf payload : payloads) {
            totalBytes += Integer.BYTES + payload.readableBytes();
        }
        ByteBuf combined = Unpooled.buffer(totalBytes);
        for (ByteBuf payload : payloads) {
            combined.writeInt(payload.readableBytes());
            combined.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
        }
        return combined;
    }

    private static void releaseNextRequest(RequestChannel requestChannel) {
        KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
        assertThat(request).isNotNull();
        request.releaseBuffer();
    }

    private static void releaseQueuedRequests(RequestChannel requestChannel) {
        RpcRequest request;
        while ((request = requestChannel.pollRequest(0)) != null) {
            request.releaseBuffer();
        }
    }

    private static void releaseIfRetained(ByteBuf buffer) {
        int referenceCount = buffer.refCnt();
        if (referenceCount > 0) {
            buffer.release(referenceCount);
        }
    }

    private static void assertResponseWriteLifecycleReleased(
            ResponseWriteLifecycleFixture fixture,
            List<ResponseWriteAttempt> attempts,
            long expectedCompletions) {
        assertThat(fixture.metrics.pendingResponseWrites()).isZero();
        assertThat(fixture.metrics.pendingResponseWriteBytes()).isZero();
        assertThat(fixture.metrics.oldestPendingResponseWriteAgeMillis()).isZero();
        assertThat(fixture.metricGroup.responseWriteCompletionCount())
                .isEqualTo(expectedCompletions);
        assertThat(fixture.admissionController.liveRequests()).isZero();
        assertThat(fixture.admissionController.liveBytes()).isZero();
        assertThat(fixture.admissionController.rawBytes()).isZero();
        for (ResponseWriteAttempt attempt : attempts) {
            assertThat(attempt.requestBuffer.refCnt()).isZero();
            assertThat(attempt.responseWrite.buffer().refCnt()).isZero();
        }
    }

    private static ByteBuf serializeHeaderOnly(RequestHeader header) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int headerSize = header.data().size(cache, header.headerVersion());
        ByteBuffer serialized = ByteBuffer.allocate(headerSize);
        header.data().write(new ByteBufferAccessor(serialized), cache, header.headerVersion());
        serialized.flip();
        return Unpooled.wrappedBuffer(serialized);
    }

    private static void processNextRequest(
            RequestChannel requestChannel, KafkaRequestHandler requestHandler) throws Exception {
        KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
        assertThat(request).isNotNull();
        try {
            requestHandler.processRequest(request);
        } finally {
            request.releaseBuffer();
        }
    }

    private static ByteBuf serializeHeader(RequestHeader header) {
        ProduceRequest emptyProduceRequest =
                new ProduceRequest(
                        new ProduceRequestData().setAcks((short) 1).setTimeoutMs(1000),
                        header.apiVersion());
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(),
                        header.headerVersion(),
                        emptyProduceRequest.data(),
                        emptyProduceRequest.version());
        int headerSize = header.data().size(new ObjectSerializationCache(), header.headerVersion());
        serialized.limit(headerSize);
        return Unpooled.wrappedBuffer(serialized);
    }

    private static final class RecordingRequestChannel extends RequestChannel {
        private int registerCount;
        private int unregisterCount;
        private int putCount;

        private RecordingRequestChannel(int backpressureThreshold) {
            super(backpressureThreshold);
        }

        @Override
        public void registerChannel(Channel channel) {
            registerCount++;
            super.registerChannel(channel);
        }

        @Override
        public void unregisterChannel(Channel channel) {
            unregisterCount++;
            super.unregisterChannel(channel);
        }

        @Override
        public void putRequest(RpcRequest request) {
            putCount++;
            super.putRequest(request);
        }
    }

    private static final class ThrowingRequestChannel extends RequestChannel {

        private ThrowingRequestChannel() {
            super(100);
        }

        @Override
        public void putRequest(RpcRequest request) {
            throw new IllegalStateException("test enqueue failure");
        }
    }

    private static final class DelayedWriteHandler extends ChannelOutboundHandlerAdapter {
        private Object pendingMessage;
        private ChannelPromise pendingPromise;

        @Override
        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            if (pendingPromise != null) {
                ReferenceCountUtil.safeRelease(message);
                promise.setFailure(
                        new IllegalStateException("Only one pending write is supported"));
                return;
            }
            pendingMessage = message;
            pendingPromise = promise;
        }

        private boolean hasPendingWrite() {
            return pendingPromise != null;
        }

        private void failPendingWrite(Throwable failure) {
            ChannelPromise promise = pendingPromise;
            Object message = pendingMessage;
            pendingPromise = null;
            pendingMessage = null;
            ReferenceCountUtil.safeRelease(message);
            assertThat(promise).isNotNull();
            promise.setFailure(failure);
        }

        private void releasePendingWrite() {
            ChannelPromise promise = pendingPromise;
            Object message = pendingMessage;
            pendingPromise = null;
            pendingMessage = null;
            ReferenceCountUtil.safeRelease(message);
            if (promise != null && !promise.isDone()) {
                promise.cancel(false);
            }
        }
    }

    private static final class ResponseWriteLifecycleFixture {
        private final RequestChannel requestChannel = new RequestChannel(100);
        private final KafkaProduceAdmissionController admissionController = admissionController();
        private final ResponseWriteMetricGroup metricGroup = new ResponseWriteMetricGroup();
        private final KafkaProduceMetrics metrics = new KafkaProduceMetrics(metricGroup);
        private final MultipleDelayedWriteHandler handler = new MultipleDelayedWriteHandler();
        private final List<ResponseWriteAttempt> attempts = new ArrayList<>();
        private final EmbeddedChannel channel =
                new EmbeddedChannel(
                        handler,
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                null,
                                metrics,
                                admissionController));

        private ResponseWriteAttempt submitProduce(int correlationId) {
            ByteBuf requestBuffer = createProduceRequest(correlationId);
            int writeCountBefore = handler.allWrites().size();
            channel.writeInbound(requestBuffer);
            KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(request).isNotNull();
            request.releaseBuffer();
            request.complete(new ProduceResponse(new ProduceResponseData()));
            channel.runPendingTasks();
            assertThat(handler.allWrites()).hasSize(writeCountBefore + 1);
            ResponseWriteAttempt attempt =
                    new ResponseWriteAttempt(
                            requestBuffer, handler.allWrites().get(writeCountBefore));
            attempts.add(attempt);
            return attempt;
        }

        private void close() {
            handler.cancelAll();
            channel.close();
            channel.runPendingTasks();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
            for (PendingResponseWrite write : handler.allWrites()) {
                write.releaseMessage();
            }
            for (ResponseWriteAttempt attempt : attempts) {
                releaseIfRetained(attempt.requestBuffer);
            }
        }
    }

    private static final class ResponseWriteAttempt {
        private final ByteBuf requestBuffer;
        private final PendingResponseWrite responseWrite;

        private ResponseWriteAttempt(ByteBuf requestBuffer, PendingResponseWrite responseWrite) {
            this.requestBuffer = requestBuffer;
            this.responseWrite = responseWrite;
        }
    }

    private static final class MultipleDelayedWriteHandler extends ChannelOutboundHandlerAdapter {
        private final List<PendingResponseWrite> pendingWrites = new ArrayList<>();
        private final List<PendingResponseWrite> allWrites = new ArrayList<>();

        @Override
        public synchronized void write(
                ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            PendingResponseWrite write = new PendingResponseWrite(message, promise);
            pendingWrites.add(write);
            allWrites.add(write);
        }

        private synchronized int pendingCount() {
            return pendingWrites.size();
        }

        private synchronized List<PendingResponseWrite> allWrites() {
            return new ArrayList<>(allWrites);
        }

        private void succeedNext() {
            takeNext().succeed();
        }

        private void failNext(Throwable failure) {
            takeNext().fail(failure);
        }

        private void failAll(Throwable failure) {
            for (PendingResponseWrite write : drainPending()) {
                write.fail(failure);
            }
        }

        private void cancelAll() {
            for (PendingResponseWrite write : drainPending()) {
                write.cancel();
            }
        }

        private synchronized PendingResponseWrite takeNext() {
            assertThat(pendingWrites).isNotEmpty();
            return pendingWrites.remove(0);
        }

        private synchronized List<PendingResponseWrite> drainPending() {
            List<PendingResponseWrite> writes = new ArrayList<>(pendingWrites);
            pendingWrites.clear();
            return writes;
        }
    }

    private static final class PendingResponseWrite {
        private final ByteBuf buffer;
        private final ChannelPromise promise;
        private boolean messageReleased;

        private PendingResponseWrite(Object message, ChannelPromise promise) {
            assertThat(message).isInstanceOf(ByteBuf.class);
            this.buffer = (ByteBuf) message;
            this.promise = promise;
        }

        private ByteBuf buffer() {
            return buffer;
        }

        private void succeed() {
            releaseMessage();
            promise.trySuccess();
        }

        private void fail(Throwable failure) {
            releaseMessage();
            promise.tryFailure(failure);
        }

        private void cancel() {
            releaseMessage();
            promise.cancel(false);
        }

        private boolean trySucceedAgain() {
            releaseMessage();
            return promise.trySuccess();
        }

        private synchronized void releaseMessage() {
            if (!messageReleased) {
                messageReleased = true;
                ReferenceCountUtil.safeRelease(buffer);
            }
        }
    }

    private static final class ResponseWriteMetricGroup extends TestMetricGroup {
        private Histogram responseWriteCompletionHistogram;

        private ResponseWriteMetricGroup() {
            super(
                    new String[0],
                    Collections.emptyMap(),
                    (name, filter) -> name,
                    (filter, delimiter) -> "test");
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            if ("responseWriteCompletionTimeMicros".equals(name)) {
                responseWriteCompletionHistogram = histogram;
            }
            return histogram;
        }

        private long responseWriteCompletionCount() {
            assertThat(responseWriteCompletionHistogram).isNotNull();
            return responseWriteCompletionHistogram.getCount();
        }
    }

    private static final class ThrowingClock implements Clock {
        @Override
        public long milliseconds() {
            throw new IllegalStateException("test metrics clock failure");
        }

        @Override
        public long nanoseconds() {
            throw new IllegalStateException("test metrics clock failure");
        }
    }

    private static final class ProduceLifecycleFixture {
        private final RequestChannel requestChannel = new RequestChannel(100);
        private final KafkaProduceAdmissionController admissionController = admissionController();
        private final PendingProduceGatewayService service = new PendingProduceGatewayService();
        private final KafkaRequestHandler requestHandler =
                new KafkaRequestHandler(
                        service,
                        service,
                        "kafka",
                        KafkaProduceMetrics.noOp(),
                        new TestingKafkaRecordTranscoder());
        private final EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                null,
                                KafkaProduceMetrics.noOp(),
                                admissionController));

        private KafkaRequest pollRequest() {
            KafkaRequest request = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(request).isNotNull();
            return request;
        }

        private void completeProduce() {
            service.produceFuture.complete(
                    new ProduceLogResponse()
                            .addAllBucketsResps(
                                    Collections.singletonList(
                                            new PbProduceLogRespForBucket()
                                                    .setBucketId(0)
                                                    .setBaseOffset(42L))));
        }

        private void close() {
            channel.close();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
        }
    }

    private static final class PendingProduceGatewayService extends TestingTabletGatewayService {
        private final CompletableFuture<ProduceLogResponse> produceFuture =
                new CompletableFuture<>();
        private final TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("payload", org.apache.fluss.types.DataTypes.BYTES())
                                        .build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .build();

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            return CompletableFuture.completedFuture(
                    new GetTableInfoResponse()
                            .setTableId(123L)
                            .setSchemaId(1)
                            .setTableJson(tableDescriptor.toJsonBytes())
                            .setCreatedTime(1L)
                            .setModifiedTime(1L));
        }

        @Override
        public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
            return produceFuture;
        }
    }

    private static final class TestingKafkaRecordTranscoder implements KafkaRecordTranscoder {

        @Override
        public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
            return null;
        }

        @Override
        public BytesView transcode(
                List<Record> records,
                KafkaTopicWritePlan writePlan,
                KafkaOutputMemoryBudget outputMemoryBudget) {
            return new ByteBufBytesView(new byte[0]);
        }
    }
}
