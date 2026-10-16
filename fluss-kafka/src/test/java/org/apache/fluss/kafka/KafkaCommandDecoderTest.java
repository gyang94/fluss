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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RpcRequest;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests response ordering and ownership in {@link KafkaCommandDecoder}. */
public class KafkaCommandDecoderTest {

    @Test
    public void testUnsupportedApiVersionsRequestReturnsVersionZeroErrorResponse() {
        RequestChannel requestChannel = new RequestChannel(1);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel}, "KAFKA_EXTERNAL"));
        short unsupportedVersion = (short) (ApiKeys.API_VERSIONS.latestVersion() + 1);
        RequestHeader header =
                new RequestHeader(ApiKeys.API_VERSIONS, unsupportedVersion, "client", 42);
        // The unsupported request body must not be decoded. A version-zero body is empty.
        ByteBuf requestBuffer =
                serialize(header, new ApiVersionsRequest.Builder().build((short) 0));
        KafkaRequest request = null;
        ByteBuf responseBuffer = null;
        try {
            channel.writeInbound(requestBuffer);
            request = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(request).isNotNull();
            assertThat(request.apiVersion()).isEqualTo(unsupportedVersion);
            assertThat(request.listenerName()).isEqualTo("KAFKA_EXTERNAL");
            ApiVersionsRequest parsedRequest = request.request();
            assertThat(parsedRequest.hasUnsupportedRequestVersion()).isTrue();
            assertThat(parsedRequest.version()).isZero();

            TestingTabletGatewayService service = new TestingTabletGatewayService();
            new KafkaRequestHandler(new Configuration(), service).processRequest(request);
            request.releaseBuffer();
            channel.runPendingTasks();

            responseBuffer = channel.readOutbound();
            assertThat(responseBuffer).isNotNull();
            ByteBuffer responseBytes = responseBuffer.nioBuffer();
            assertThat(ResponseHeader.parse(responseBytes, (short) 0).correlationId())
                    .isEqualTo(42);
            ApiVersionsResponse response = ApiVersionsResponse.parse(responseBytes, (short) 0);
            assertThat(response.data().errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
            assertThat(responseBytes.hasRemaining()).isFalse();
            assertThat(requestBuffer.refCnt()).isZero();
            assertThat(channel.isActive()).isTrue();
        } finally {
            if (responseBuffer != null) {
                responseBuffer.release();
            }
            if (request != null) {
                request.releaseBuffer();
            }
            channel.finishAndReleaseAll();
            if (requestBuffer.refCnt() > 0) {
                requestBuffer.release();
            }
        }
    }

    @Test
    public void testPostEnqueueFailurePreservesWorkerReference() {
        RequestChannel requestChannel =
                new RequestChannel(100) {
                    @Override
                    public void putRequest(RpcRequest request) {
                        super.putRequest(request);
                        throw new IllegalStateException("backpressure notification failed");
                    }
                };
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(new RequestChannel[] {requestChannel}, "KAFKA"));
        short version = ApiKeys.API_VERSIONS.oldestVersion();
        ByteBuf buffer =
                serialize(
                        new RequestHeader(ApiKeys.API_VERSIONS, version, "client", 1),
                        new ApiVersionsRequest.Builder().build(version));
        try {
            channel.writeInbound(buffer);
            channel.runPendingTasks();
            assertThat(channel.isActive()).isFalse();
            KafkaRequest queued = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(queued).isNotNull();
            assertThat(buffer.refCnt()).isOne();
            queued.releaseBuffer();
            assertThat(buffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testApiVersionsDuringAuthenticationIsFlushedBeforeChannelCloses() throws Exception {
        RequestChannel requestChannel = new RequestChannel(100);
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        KafkaRequestHandler requestHandler = new KafkaRequestHandler(new Configuration(), service);
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

        short supportedVersion = ApiKeys.API_VERSIONS.oldestVersion();
        RequestHeader apiVersionsHeader =
                new RequestHeader(ApiKeys.API_VERSIONS, supportedVersion, "client", 17);
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testResponseOrderingAndWriteFailureCleanup(boolean failWrite) {
        RequestChannel requests = new RequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new ChannelOutboundHandlerAdapter() {
                            @Override
                            public void write(
                                    ChannelHandlerContext ctx,
                                    Object message,
                                    ChannelPromise promise) {
                                if (failWrite) {
                                    ReferenceCountUtil.release(message);
                                    promise.setFailure(
                                            new IOException("simulated transport write failure"));
                                } else {
                                    ctx.write(message, promise);
                                }
                            }
                        },
                        new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
        ApiVersionsRequest body = new ApiVersionsRequest.Builder().build((short) 0);
        ByteBuf firstBuffer =
                serialize(new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "client", 71), body);
        ByteBuf secondBuffer =
                serialize(new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "client", 72), body);
        KafkaRequest first = null;
        KafkaRequest second = null;
        try {
            channel.writeInbound(firstBuffer, secondBuffer);
            first = (KafkaRequest) requests.pollRequest(1000);
            second = (KafkaRequest) requests.pollRequest(1000);
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();
            first.releaseBuffer();
            second.releaseBuffer();
            if (!failWrite) {
                second.complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
                channel.runPendingTasks();
                assertThat((Object) channel.readOutbound()).isNull();
            }
            first.complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
            channel.runPendingTasks();
            if (failWrite) {
                assertThat(channel.isActive()).isFalse();
                assertThat(second.cancelled()).isTrue();
                assertThat(secondBuffer.refCnt()).isZero();
                second.complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
                channel.runPendingTasks();
            } else {
                for (int id : new int[] {71, 72}) {
                    ByteBuf response = channel.readOutbound();
                    try {
                        assertThat(response).isNotNull();
                        assertThat(
                                        ResponseHeader.parse(response.nioBuffer(), (short) 0)
                                                .correlationId())
                                .isEqualTo(id);
                    } finally {
                        ReferenceCountUtil.release(response);
                    }
                }
                assertThat(channel.isActive()).isTrue();
            }
            assertThat((Object) channel.readOutbound()).isNull();
            assertThat(firstBuffer.refCnt()).isZero();
            assertThat(secondBuffer.refCnt()).isZero();
        } finally {
            channel.finishAndReleaseAll();
            if (first != null) {
                first.releaseBuffer();
            }
            if (second != null) {
                second.releaseBuffer();
            }
        }
    }

    @Test
    void testResponseEncodingFailureClosesAndPreservesQueuedWorkerReference() {
        RequestChannel requests = new RequestChannel(100);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new KafkaCommandDecoder(new RequestChannel[] {requests}, "KAFKA"));
        ApiVersionsRequest body = new ApiVersionsRequest.Builder().build((short) 0);
        ByteBuf firstBuffer =
                serialize(new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "client", 81), body);
        ByteBuf secondBuffer =
                serialize(new RequestHeader(ApiKeys.API_VERSIONS, (short) 0, "client", 82), body);
        KafkaRequest first = null;
        KafkaRequest second = null;
        try {
            channel.writeInbound(firstBuffer, secondBuffer);
            first = (KafkaRequest) requests.pollRequest(1000);
            second = (KafkaRequest) requests.pollRequest(1000);
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();
            first.releaseBuffer();
            ByteBufAllocator allocator = mock(ByteBufAllocator.class);
            when(allocator.buffer(anyInt()))
                    .thenThrow(new IllegalStateException("simulated response allocation failure"));
            channel.config().setAllocator(allocator);
            first.complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
            channel.runPendingTasks();
            assertThat(channel.isActive()).isFalse();
            assertThat(firstBuffer.refCnt()).isZero();
            assertThat(second.cancelled()).isTrue();
            assertThat(secondBuffer.refCnt()).isOne();
            second.releaseBuffer();
            second.complete(new ApiVersionsResponse(new ApiVersionsResponseData()));
            channel.runPendingTasks();
            assertThat(secondBuffer.refCnt()).isZero();
            assertThat((Object) channel.readOutbound()).isNull();
        } finally {
            channel.finishAndReleaseAll();
            if (first != null) {
                first.releaseBuffer();
            }
            if (second != null) {
                second.releaseBuffer();
            }
        }
    }

    private static ByteBuf serialize(RequestHeader header, AbstractRequest request) {
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        return Unpooled.wrappedBuffer(serialized);
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
}
