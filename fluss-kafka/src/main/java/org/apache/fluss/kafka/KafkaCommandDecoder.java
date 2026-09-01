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

import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController.RequestLease;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics.ResponseWrite;
import org.apache.fluss.kafka.network.KafkaFrame;
import org.apache.fluss.kafka.network.KafkaFrameAdmissionLease;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.CorruptedFrameException;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleState;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.fluss.utils.MathUtils;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;

/**
 * A decoder that decodes admitted Kafka frames into requests and sends them to the corresponding
 * RequestChannel.
 */
public class KafkaCommandDecoder extends SimpleChannelInboundHandler<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommandDecoder.class);

    private final RequestChannel[] requestChannels;
    private final int numChannels;
    private final String listenerName;
    private final KafkaSaslConnection saslConnection;
    private final KafkaProduceMetrics produceMetrics;
    @Nullable private final KafkaProduceAdmissionController admissionController;
    @Nullable private final KafkaNativeProduceAdmissionController nativeAdmissionController;
    private final AtomicBoolean requestChannelRegistered = new AtomicBoolean(false);

    @Nullable private RequestChannel requestChannel;
    @Nullable private ConnectionHandle admissionConnection;

    @Nullable
    private KafkaNativeProduceAdmissionController.ConnectionHandle nativeAdmissionConnection;

    // Need to use a Queue to store the inflight responses, because Kafka clients require the
    // responses to be sent in order.
    // See: org.apache.kafka.clients.InFlightRequests#completeNext
    private final ConcurrentLinkedDeque<KafkaRequest> inflightResponses =
            new ConcurrentLinkedDeque<>();
    private final Set<ResponseWrite> pendingResponseWrites = ConcurrentHashMap.newKeySet();
    protected final AtomicBoolean isActive = new AtomicBoolean(true);
    protected volatile ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;

    /** Creates a decoder for a PLAINTEXT Kafka connection. */
    public KafkaCommandDecoder(RequestChannel[] requestChannels, String listenerName) {
        this(requestChannels, listenerName, null, KafkaProduceMetrics.noOp());
    }

    /** Creates a decoder that requires SASL when an authenticator supplier is provided. */
    public KafkaCommandDecoder(
            RequestChannel[] requestChannels,
            String listenerName,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier) {
        this(requestChannels, listenerName, authenticatorSupplier, KafkaProduceMetrics.noOp());
    }

    /** Creates a decoder with optional authentication and Produce runtime metrics. */
    public KafkaCommandDecoder(
            RequestChannel[] requestChannels,
            String listenerName,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier,
            KafkaProduceMetrics produceMetrics) {
        this(requestChannels, listenerName, authenticatorSupplier, produceMetrics, null);
    }

    /** Creates a decoder with optional authentication, metrics, and Produce admission control. */
    public KafkaCommandDecoder(
            RequestChannel[] requestChannels,
            String listenerName,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier,
            KafkaProduceMetrics produceMetrics,
            @Nullable KafkaProduceAdmissionController admissionController) {
        this(
                requestChannels,
                listenerName,
                authenticatorSupplier,
                produceMetrics,
                admissionController,
                null);
    }

    /** Creates a decoder with frame admission and connection-scoped native Produce admission. */
    public KafkaCommandDecoder(
            RequestChannel[] requestChannels,
            String listenerName,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier,
            KafkaProduceMetrics produceMetrics,
            @Nullable KafkaProduceAdmissionController admissionController,
            @Nullable KafkaNativeProduceAdmissionController nativeAdmissionController) {
        super(false);
        this.requestChannels = requestChannels;
        this.numChannels = requestChannels.length;
        this.listenerName = listenerName;
        this.produceMetrics = produceMetrics;
        this.admissionController = admissionController;
        this.nativeAdmissionController = nativeAdmissionController;
        this.saslConnection =
                authenticatorSupplier == null
                        ? KafkaSaslConnection.plaintext()
                        : KafkaSaslConnection.sasl(authenticatorSupplier);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object message) throws Exception {
        KafkaFrame frame = message instanceof KafkaFrame ? (KafkaFrame) message : null;
        KafkaFrameAdmissionLease frameAdmissionLease = null;
        KafkaRequest request = null;
        boolean responseQueued = false;
        boolean processorOwnershipRetained = false;
        try {
            if (frame == null && !(message instanceof ByteBuf)) {
                throw new IllegalArgumentException(
                        "Kafka decoder requires KafkaFrame or legacy ByteBuf input");
            }
            ByteBuf buffer = frame == null ? (ByteBuf) message : frame.content();
            if (frame != null) {
                frameAdmissionLease = frame.takeAdmissionLease();
            }

            long receivedTimeNanos = metricsNowNanos();
            int requestBytes = buffer.readableBytes();
            CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
            ByteBuffer nioBuffer = buffer.nioBuffer();
            RequestHeader header = RequestHeader.parse(nioBuffer);
            if (frame != null && frame.apiKey() != header.apiKey().id) {
                throw new CorruptedFrameException(
                        "Kafka probe API key "
                                + frame.apiKey()
                                + " does not match parsed request API key "
                                + header.apiKey().id);
            }
            if (!saslConnection.isRequestAllowed(header.apiKey())) {
                LOG.warn(
                        "Rejecting Kafka API {} before authentication completes on listener {}",
                        header.apiKey(),
                        listenerName);
                close();
                return;
            }
            request =
                    parseRequest(
                            ctx,
                            future,
                            buffer,
                            listenerName,
                            saslConnection,
                            header,
                            nioBuffer,
                            receivedTimeNanos,
                            requestBytes,
                            nativeAdmissionConnection,
                            ctx.executor());
            if (frameAdmissionLease != null) {
                request.attachAdmissionLease(frameAdmissionLease);
                frameAdmissionLease = null;
            } else if (header.apiKey() == PRODUCE) {
                // Keep post-frame admission only for direct decoder users that do not install the
                // production pre-frame decoder.
                ConnectionHandle connection = produceAdmissionConnection(ctx);
                if (connection != null) {
                    RequestLease requestLease = connection.acquire(requestBytes);
                    try {
                        request.attachAdmissionLease(requestLease);
                    } catch (Throwable attachFailure) {
                        requestLease.close();
                        throw attachFailure;
                    }
                }
            }
            if (header.apiKey() == PRODUCE) {
                recordMetric(() -> produceMetrics.recordRequestDecode(receivedTimeNanos));
            }
            inflightResponses.addLast(request);
            responseQueued = true;
            KafkaRequest admittedRequest = request;
            future.whenComplete(
                    (r, t) -> {
                        try {
                            admittedRequest.markResponseReady(metricsNowNanos());
                        } finally {
                            admittedRequest.markProcessingCompleted();
                        }
                    });
            future.whenCompleteAsync((r, t) -> sendResponse(ctx), ctx.executor());
            // The worker and the ordered-response queue own independent references. This lets a
            // disconnect release response-side ownership without invalidating a request that is
            // still waiting in the shared RequestChannel.
            RequestChannel selectedRequestChannel = requestChannel();
            request.retainBufferForProcessor();
            processorOwnershipRetained = true;
            try {
                selectedRequestChannel.putRequest(request);
                processorOwnershipRetained = false;
            } catch (Throwable enqueueFailure) {
                request.releaseBuffer();
                processorOwnershipRetained = false;
                throw enqueueFailure;
            }

            if (!isActive.get()) {
                LOG.warn("Received a request on an inactive channel: {}", remoteAddress);
                request.cancel();
            }
        } catch (Throwable t) {
            LOG.error("Error handling request", t);
            if (request != null) {
                if (processorOwnershipRetained) {
                    request.releaseBuffer();
                }
                request.fail(t);
                request.markProcessingCompleted();
                if (!responseQueued) {
                    request.releaseOrderedBuffer();
                    request.markNetworkCompleted();
                }
            }
            close();
        } finally {
            if (frameAdmissionLease != null) {
                frameAdmissionLease.close();
            }
            // KafkaRequest retains the content because Kafka record sets can reference its memory
            // asynchronously. Release the decoder's ownership on every path; the request releases
            // its retained reference after response handling or cancellation.
            ReferenceCountUtil.release(message);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        isActive.set(true);

        if (requestChannel == null) {
            int channelIndex =
                    MathUtils.murmurHash(ctx.channel().id().asLongText().hashCode()) % numChannels;
            requestChannel = requestChannels[channelIndex];
        }
        if (requestChannelRegistered.compareAndSet(false, true)) {
            RequestChannel currentRequestChannel = requestChannel();
            try {
                currentRequestChannel.registerChannel(ctx.channel());
            } catch (RuntimeException | Error t) {
                try {
                    currentRequestChannel.unregisterChannel(ctx.channel());
                } catch (RuntimeException | Error rollbackError) {
                    t.addSuppressed(rollbackError);
                } finally {
                    requestChannelRegistered.set(false);
                }
                throw t;
            }
        }
        if (nativeAdmissionController != null && nativeAdmissionConnection == null) {
            nativeAdmissionConnection = nativeAdmissionController.registerConnection();
        }

        LOG.info("New connection from {}", ctx.channel().remoteAddress());
        // TODO Channel metrics
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Connection closed from {}", ctx.channel().remoteAddress());
        try {
            deactivate();
        } finally {
            try {
                closeNativeAdmissionConnection();
            } finally {
                try {
                    closeAdmissionConnection();
                } finally {
                    try {
                        unregisterRequestChannel(ctx);
                    } finally {
                        super.channelInactive(ctx);
                        // TODO Channel metrics
                    }
                }
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                LOG.warn("Connection {} is idle, closing...", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
    }

    private void sendResponse(ChannelHandlerContext ctx) {
        KafkaRequest request;
        while ((request = inflightResponses.peekFirst()) != null) {
            CompletableFuture<AbstractResponse> f = request.future();
            ApiKeys apiKey = request.apiKey();
            boolean isDone = f.isDone();
            boolean cancelled = request.cancelled();

            if (apiKey.equals(PRODUCE)) {
                ProduceRequest produceRequest = request.request();
                if (produceRequest.acks() == 0 && isDone) {
                    // if acks=0, we don't need to wait for the response to be sent
                    inflightResponses.pollFirst();
                    request.releaseOrderedBuffer();
                    request.markNetworkCompleted();
                    continue;
                }
            }

            if (cancelled) {
                inflightResponses.pollFirst();
                request.releaseOrderedBuffer();
                request.markNetworkCompleted();
                continue;
            }

            if (!isDone) {
                break;
            }

            inflightResponses.pollFirst();
            if (apiKey.equals(PRODUCE) && request.responseReadyTimeNanos() >= 0) {
                long responseReadyTimeNanos = request.responseReadyTimeNanos();
                recordMetric(() -> produceMetrics.recordResponseHeadOfLine(responseReadyTimeNanos));
            }
            if (isActive.get()) {
                ByteBuf responseBuffer = null;
                ResponseWrite responseWrite = null;
                try {
                    responseBuffer = request.responseBuffer();
                    if (apiKey.equals(PRODUCE)) {
                        responseWrite =
                                responseWriteStarted(
                                        (long) responseBuffer.readableBytes() + Integer.BYTES);
                        if (responseWrite != null) {
                            pendingResponseWrites.add(responseWrite);
                        }
                    }
                    ChannelFuture responseFuture = ctx.writeAndFlush(responseBuffer);
                    responseBuffer = null;
                    KafkaRequest writtenRequest = request;
                    ResponseWrite submittedResponseWrite = responseWrite;
                    boolean closeAfterResponse = request.shouldCloseConnectionAfterResponse();
                    if (closeAfterResponse) {
                        isActive.set(false);
                        saslConnection.close();
                    }
                    responseFuture.addListener(
                            completedWrite -> {
                                try {
                                    completeResponseWrite(submittedResponseWrite);
                                } finally {
                                    writtenRequest.markNetworkCompleted();
                                }
                                if (!completedWrite.isSuccess()) {
                                    LOG.warn(
                                            "Failed to write Kafka response for request {}.",
                                            writtenRequest.requestId(),
                                            completedWrite.cause());
                                    close();
                                } else if (closeAfterResponse) {
                                    releasePendingRequests();
                                    ctx.close();
                                }
                            });
                    if (closeAfterResponse) {
                        break;
                    }
                } catch (Throwable writeFailure) {
                    try {
                        completeResponseWrite(responseWrite);
                    } finally {
                        ReferenceCountUtil.safeRelease(responseBuffer);
                        request.releaseOrderedBuffer();
                        request.markNetworkCompleted();
                    }
                    LOG.warn(
                            "Failed to serialize or submit Kafka response for request {}.",
                            request.requestId(),
                            writeFailure);
                    close();
                    break;
                }
            } else {
                request.releaseOrderedBuffer();
                request.markNetworkCompleted();
            }
        }
    }

    protected void close() {
        deactivate();
        if (ctx != null) {
            ctx.close();
        }
        LOG.warn(
                "Close channel {} with {} pending requests.",
                remoteAddress,
                inflightResponses.size());
    }

    private void deactivate() {
        isActive.set(false);
        saslConnection.close();
        try {
            releasePendingResponseWrites();
        } finally {
            releasePendingRequests();
        }
    }

    private void completeResponseWrite(@Nullable ResponseWrite responseWrite) {
        if (responseWrite != null) {
            pendingResponseWrites.remove(responseWrite);
            try {
                responseWrite.close();
            } catch (Throwable metricFailure) {
                LOG.warn("Failed to complete Kafka response-write metrics.", metricFailure);
            }
        }
    }

    private void releasePendingResponseWrites() {
        for (ResponseWrite responseWrite : pendingResponseWrites) {
            completeResponseWrite(responseWrite);
        }
    }

    private void releasePendingRequests() {
        KafkaRequest request;
        while ((request = inflightResponses.pollFirst()) != null) {
            request.cancel();
            request.releaseOrderedBuffer();
            request.markNetworkCompleted();
        }
    }

    private RequestChannel requestChannel() {
        RequestChannel currentRequestChannel = requestChannel;
        if (currentRequestChannel == null) {
            throw new IllegalStateException("Kafka channel has not been registered");
        }
        return currentRequestChannel;
    }

    private void unregisterRequestChannel(ChannelHandlerContext ctx) {
        RequestChannel currentRequestChannel = requestChannel;
        if (currentRequestChannel != null && requestChannelRegistered.compareAndSet(true, false)) {
            try {
                currentRequestChannel.unregisterChannel(ctx.channel());
            } catch (RuntimeException | Error t) {
                requestChannelRegistered.set(true);
                throw t;
            }
        }
    }

    private void closeAdmissionConnection() {
        ConnectionHandle connection = admissionConnection;
        admissionConnection = null;
        if (connection != null) {
            connection.close();
        }
    }

    private void closeNativeAdmissionConnection() {
        KafkaNativeProduceAdmissionController.ConnectionHandle connection =
                nativeAdmissionConnection;
        nativeAdmissionConnection = null;
        if (connection != null && connection.closeAndGetHadInFlightRequests()) {
            recordMetric(produceMetrics::recordNativeDisconnectWithInflight);
        }
    }

    private long metricsNowNanos() {
        try {
            return produceMetrics.nowNanos();
        } catch (Throwable ignored) {
            // Metrics are observational and must not affect request ownership or completion.
            return System.nanoTime();
        }
    }

    private void recordMetric(Runnable recorder) {
        try {
            recorder.run();
        } catch (Throwable ignored) {
            // Metrics are observational and must not affect request ownership or completion.
        }
    }

    @Nullable
    private ResponseWrite responseWriteStarted(long responseBytes) {
        try {
            return produceMetrics.responseWriteStarted(responseBytes);
        } catch (Throwable ignored) {
            // A metrics failure must not prevent a valid response from reaching the network.
            return null;
        }
    }

    @Nullable
    private ConnectionHandle produceAdmissionConnection(ChannelHandlerContext ctx) {
        if (admissionController == null) {
            return null;
        }
        ConnectionHandle connection = admissionConnection;
        if (connection == null) {
            connection = admissionController.registerConnection(ctx.channel(), requestChannel());
            admissionConnection = connection;
        }
        return connection;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Exception caught on channel {}", remoteAddress, cause);
        close();
    }

    private static KafkaRequest parseRequest(
            ChannelHandlerContext ctx,
            CompletableFuture<AbstractResponse> future,
            ByteBuf buffer,
            String listenerName,
            KafkaSaslConnection saslConnection,
            RequestHeader header,
            ByteBuffer nioBuffer,
            long receivedTimeNanos,
            int requestBytes,
            @Nullable
                    KafkaNativeProduceAdmissionController.ConnectionHandle
                            nativeAdmissionConnection,
            @Nullable java.util.concurrent.ScheduledExecutorService admissionScheduler) {
        if (isUnsupportedApiVersionRequest(header)) {
            ApiVersionsRequest request =
                    new ApiVersionsRequest(
                            new ApiVersionsRequestData(),
                            API_VERSIONS.oldestVersion(),
                            header.apiVersion());
            return new KafkaRequest(
                    API_VERSIONS,
                    header.apiVersion(),
                    header,
                    request,
                    listenerName,
                    saslConnection,
                    buffer,
                    ctx,
                    future,
                    receivedTimeNanos,
                    requestBytes,
                    nativeAdmissionConnection,
                    admissionScheduler);
        }
        RequestAndSize request =
                AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), nioBuffer);
        return new KafkaRequest(
                header.apiKey(),
                header.apiVersion(),
                header,
                request.request,
                listenerName,
                saslConnection,
                buffer,
                ctx,
                future,
                receivedTimeNanos,
                requestBytes,
                nativeAdmissionConnection,
                admissionScheduler);
    }

    private static boolean isUnsupportedApiVersionRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS
                && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }
}
