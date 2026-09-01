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

import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.network.KafkaFrameAdmissionLease;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.rpc.netty.server.RpcRequest;
import org.apache.fluss.rpc.protocol.RequestType;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Represents a request received from Kafka protocol channel. */
public class KafkaRequest implements RpcRequest {
    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

    private final ApiKeys apiKey;
    private final short apiVersion;
    private final long requestId = ID_GENERATOR.getAndIncrement();
    private final RequestHeader header;
    private final AbstractRequest request;
    private final String listenerName;
    private final KafkaSaslConnection saslConnection;
    private final FlussPrincipal principal;
    private final ByteBuf buffer;
    private final ChannelHandlerContext ctx;
    private final long startTimeMs;
    private final long receivedTimeNanos;
    private final int requestBytes;
    private final @Nullable ConnectionHandle nativeAdmissionConnection;
    private final @Nullable ScheduledExecutorService admissionScheduler;
    private final CompletableFuture<AbstractResponse> future;
    private final Object rawAdmissionLock = new Object();
    private final AtomicBoolean orderedBufferOwned = new AtomicBoolean(true);
    private final AtomicBoolean processorBufferOwned = new AtomicBoolean(false);
    private final AtomicBoolean processingCompleted = new AtomicBoolean(false);
    private final AtomicBoolean networkCompleted = new AtomicBoolean(false);
    private final AtomicBoolean rawLeaseReleased = new AtomicBoolean(false);
    private final AtomicBoolean liveLeaseReleased = new AtomicBoolean(false);
    @Nullable private volatile KafkaFrameAdmissionLease admissionLease;
    private final AtomicReference<CompletableFuture<Void>> nativeAdmissionTransfer =
            new AtomicReference<>();
    private volatile long responseReadyTimeNanos = -1L;
    private volatile boolean cancelled = false;
    private volatile boolean closeConnectionAfterResponse;

    /** Creates an anonymous request with an unknown listener name. */
    protected KafkaRequest(
            ApiKeys apiKey,
            short apiVersion,
            RequestHeader header,
            AbstractRequest request,
            ByteBuf buffer,
            ChannelHandlerContext ctx,
            CompletableFuture<AbstractResponse> future) {
        this(
                apiKey,
                apiVersion,
                header,
                request,
                "UNKNOWN",
                KafkaSaslConnection.plaintext(),
                buffer,
                ctx,
                future);
    }

    /** Creates an anonymous request for the supplied listener. */
    protected KafkaRequest(
            ApiKeys apiKey,
            short apiVersion,
            RequestHeader header,
            AbstractRequest request,
            String listenerName,
            ByteBuf buffer,
            ChannelHandlerContext ctx,
            CompletableFuture<AbstractResponse> future) {
        this(
                apiKey,
                apiVersion,
                header,
                request,
                listenerName,
                KafkaSaslConnection.plaintext(),
                buffer,
                ctx,
                future);
    }

    /** Creates a request that snapshots identity from the supplied connection security state. */
    protected KafkaRequest(
            ApiKeys apiKey,
            short apiVersion,
            RequestHeader header,
            AbstractRequest request,
            String listenerName,
            KafkaSaslConnection saslConnection,
            ByteBuf buffer,
            ChannelHandlerContext ctx,
            CompletableFuture<AbstractResponse> future) {
        this(
                apiKey,
                apiVersion,
                header,
                request,
                listenerName,
                saslConnection,
                buffer,
                ctx,
                future,
                System.nanoTime(),
                buffer.readableBytes(),
                null,
                null);
    }

    KafkaRequest(
            ApiKeys apiKey,
            short apiVersion,
            RequestHeader header,
            AbstractRequest request,
            String listenerName,
            KafkaSaslConnection saslConnection,
            ByteBuf buffer,
            ChannelHandlerContext ctx,
            CompletableFuture<AbstractResponse> future,
            long receivedTimeNanos,
            int requestBytes) {
        this(
                apiKey,
                apiVersion,
                header,
                request,
                listenerName,
                saslConnection,
                buffer,
                ctx,
                future,
                receivedTimeNanos,
                requestBytes,
                null,
                null);
    }

    KafkaRequest(
            ApiKeys apiKey,
            short apiVersion,
            RequestHeader header,
            AbstractRequest request,
            String listenerName,
            KafkaSaslConnection saslConnection,
            ByteBuf buffer,
            ChannelHandlerContext ctx,
            CompletableFuture<AbstractResponse> future,
            long receivedTimeNanos,
            int requestBytes,
            @Nullable ConnectionHandle nativeAdmissionConnection,
            @Nullable ScheduledExecutorService admissionScheduler) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.header = header;
        this.request = request;
        this.listenerName = listenerName;
        this.saslConnection = saslConnection;
        this.principal = saslConnection.principal();
        this.buffer = buffer.retain();
        this.ctx = ctx;
        this.startTimeMs = System.currentTimeMillis();
        this.receivedTimeNanos = receivedTimeNanos;
        this.requestBytes = requestBytes;
        this.nativeAdmissionConnection = nativeAdmissionConnection;
        this.admissionScheduler = admissionScheduler;
        this.future = future;
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.KAFKA;
    }

    @Override
    public void releaseBuffer() {
        if (processorBufferOwned.compareAndSet(true, false)) {
            ReferenceCountUtil.safeRelease(buffer);
            tryReleaseRawLease();
        }
    }

    /** Retains the request buffer for ownership by the RequestProcessor queue. */
    void retainBufferForProcessor() {
        if (!processorBufferOwned.compareAndSet(false, true)) {
            throw new IllegalStateException("RequestProcessor buffer ownership is already held");
        }
        buffer.retain();
    }

    /** Releases ordered-response ownership of the raw request buffer. */
    void releaseOrderedBuffer() {
        if (orderedBufferOwned.compareAndSet(true, false)) {
            ReferenceCountUtil.safeRelease(buffer);
            tryReleaseRawLease();
        }
    }

    /** Attaches the pre-frame admission ownership acquired for this request. */
    void attachAdmissionLease(KafkaFrameAdmissionLease requestLease) {
        if (admissionLease != null) {
            throw new IllegalStateException("Kafka frame admission lease is already attached");
        }
        admissionLease = requestLease;
        tryReleaseRawLease();
        tryReleaseLiveLease();
    }

    /**
     * Clears copied Produce records and releases ordered raw-buffer ownership.
     *
     * <p>{@link ProduceRequest#clearPartitionRecords()} caches the partition metadata needed for a
     * later error response before dropping references to the Kafka record buffers.
     */
    void detachProducePayload() {
        if (apiKey != ApiKeys.PRODUCE) {
            return;
        }
        ((ProduceRequest) request).clearPartitionRecords();
        releaseOrderedBuffer();
    }

    /**
     * Keeps PF raw-byte ownership until copied Produce data has transferred into native admission.
     */
    void registerNativeAdmissionTransfer(CompletableFuture<Void> transferFuture) {
        if (transferFuture == null) {
            throw new NullPointerException("transferFuture must not be null");
        }
        if (apiKey != ApiKeys.PRODUCE) {
            throw new IllegalStateException(
                    "Native admission transfer is only valid for Kafka Produce requests.");
        }
        if (!nativeAdmissionTransfer.compareAndSet(null, transferFuture)) {
            throw new IllegalStateException(
                    "Native admission transfer is already registered for this request.");
        }
        transferFuture.whenComplete((ignored, failure) -> tryReleaseRawLease());
        tryReleaseRawLease();
    }

    /** Adds copied/decompressed Produce bytes to this request's PF raw admission ownership. */
    void growRawAdmissionBytes(long additionalBytes) {
        synchronized (rawAdmissionLock) {
            KafkaFrameAdmissionLease requestLease = admissionLease;
            if (requestLease != null) {
                if (rawLeaseReleased.get()) {
                    throw new IllegalStateException(
                            "Kafka frame raw-byte ownership is already released");
                }
                requestLease.growFrameBytes(additionalBytes);
            }
        }
    }

    /** Rolls back copied/decompressed Produce bytes previously added to PF raw ownership. */
    void releaseGrownRawAdmissionBytes(long additionalBytes) {
        synchronized (rawAdmissionLock) {
            KafkaFrameAdmissionLease requestLease = admissionLease;
            if (requestLease != null) {
                requestLease.releaseGrownFrameBytes(additionalBytes);
            }
        }
    }

    /** Marks the final Kafka response future as complete. */
    void markProcessingCompleted() {
        processingCompleted.set(true);
        tryReleaseLiveLease();
    }

    /** Marks response write, no-op response, or connection cancellation as complete. */
    void markNetworkCompleted() {
        networkCompleted.set(true);
        tryReleaseLiveLease();
    }

    public ApiKeys apiKey() {
        return apiKey;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public long requestId() {
        return requestId;
    }

    public RequestHeader header() {
        return header;
    }

    public <T> T request() {
        return (T) request;
    }

    public String listenerName() {
        return listenerName;
    }

    /** Returns the connection-level SASL state associated with this request. */
    public KafkaSaslConnection saslConnection() {
        return saslConnection;
    }

    /** Returns the principal captured when this request was parsed. */
    public FlussPrincipal principal() {
        return principal;
    }

    public ChannelHandlerContext ctx() {
        return ctx;
    }

    public long startTimeMs() {
        return startTimeMs;
    }

    long receivedTimeNanos() {
        return receivedTimeNanos;
    }

    int requestBytes() {
        return requestBytes;
    }

    @Nullable
    ConnectionHandle nativeAdmissionConnection() {
        return nativeAdmissionConnection;
    }

    @Nullable
    ScheduledExecutorService admissionScheduler() {
        return admissionScheduler;
    }

    void markResponseReady(long responseReadyTimeNanos) {
        this.responseReadyTimeNanos = responseReadyTimeNanos;
    }

    long responseReadyTimeNanos() {
        return responseReadyTimeNanos;
    }

    public CompletableFuture<AbstractResponse> future() {
        return future;
    }

    public void complete(AbstractResponse response) {
        future.complete(response);
    }

    public void fail(Throwable t) {
        future.completeExceptionally(t);
    }

    public void cancel() {
        cancelled = true;
    }

    public boolean cancelled() {
        return cancelled;
    }

    /** Marks this request so the channel closes only after its response has been flushed. */
    public void closeConnectionAfterResponse() {
        closeConnectionAfterResponse = true;
    }

    /** Returns whether the channel must close after this request's response is flushed. */
    public boolean shouldCloseConnectionAfterResponse() {
        return closeConnectionAfterResponse;
    }

    public ByteBuf responseBuffer() {
        try {
            AbstractResponse response = future.join();
            return serialize(response);
        } catch (Throwable t) {
            AbstractResponse response = request.getErrorResponse(t);
            return serialize(response);
        } finally {
            releaseOrderedBuffer();
        }
    }

    private void tryReleaseRawLease() {
        synchronized (rawAdmissionLock) {
            KafkaFrameAdmissionLease requestLease = admissionLease;
            CompletableFuture<Void> transferFuture = nativeAdmissionTransfer.get();
            if (requestLease != null
                    && !orderedBufferOwned.get()
                    && !processorBufferOwned.get()
                    && (transferFuture == null || transferFuture.isDone())
                    && rawLeaseReleased.compareAndSet(false, true)) {
                requestLease.releaseFrameBytes();
            }
        }
    }

    private void tryReleaseLiveLease() {
        KafkaFrameAdmissionLease requestLease = admissionLease;
        if (requestLease != null
                && processingCompleted.get()
                && networkCompleted.get()
                && liveLeaseReleased.compareAndSet(false, true)) {
            requestLease.releaseRequest();
        }
    }

    private ByteBuf serialize(AbstractResponse response) {
        limitProduceErrorMessages(response);
        final ObjectSerializationCache cache = new ObjectSerializationCache();
        ResponseHeader responseHeader = header.toResponseHeader();
        short headerVersion = responseHeader.headerVersion();
        short apiVersion = request.version();
        Message headerData = responseHeader.data();
        int headerSize = headerData.size(cache, headerVersion);
        ApiMessage apiMessage = response.data();
        int messageSize = apiMessage.size(cache, apiVersion);
        final ByteBuf responseBuffer = ctx.alloc().buffer(headerSize + messageSize);
        try {
            responseBuffer.writerIndex(headerSize + messageSize);
            final ByteBuffer nioBuffer = responseBuffer.nioBuffer();
            final ByteBufferAccessor writable = new ByteBufferAccessor(nioBuffer);
            headerData.write(writable, cache, headerVersion);
            apiMessage.write(writable, cache, apiVersion);
            return responseBuffer;
        } catch (Throwable serializationFailure) {
            ReferenceCountUtil.safeRelease(responseBuffer);
            throw serializationFailure;
        }
    }

    private void limitProduceErrorMessages(AbstractResponse response) {
        if (apiKey != ApiKeys.PRODUCE || !(response instanceof ProduceResponse)) {
            return;
        }
        int remainingBytes =
                Math.min(
                        Math.max(requestBytes, 0),
                        KafkaProduceResult.MAX_TOTAL_ERROR_MESSAGE_BYTES);
        ProduceResponseData responseData = ((ProduceResponse) response).data();
        for (ProduceResponseData.TopicProduceResponse topic : responseData.responses()) {
            for (ProduceResponseData.PartitionProduceResponse partition :
                    topic.partitionResponses()) {
                String errorMessage =
                        KafkaProduceResult.limitErrorMessage(
                                partition.errorMessage(),
                                Math.min(
                                        remainingBytes,
                                        KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES));
                partition.setErrorMessage(errorMessage);
                remainingBytes -= KafkaProduceResult.errorMessageBytes(errorMessage);
            }
        }
    }
}
