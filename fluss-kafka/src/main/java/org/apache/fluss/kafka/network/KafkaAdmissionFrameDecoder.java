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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.network.KafkaFrameAdmission.Reservation;
import org.apache.fluss.kafka.network.KafkaFrameReadPauser.PauseLease;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandler;
import org.apache.fluss.shaded.netty4.io.netty.channel.RecvByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.CorruptedFrameException;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.fluss.shaded.netty4.io.netty.util.concurrent.ScheduledFuture;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Decodes Kafka frames only after admission has been granted for their complete wire size.
 *
 * <p>This decoder initially retains only a six-byte probe containing the length field and API key.
 * Its companion {@link KafkaProbeAwareRecvByteBufAllocator} prevents the socket transport from
 * reading any body bytes while the admission future is incomplete. After a grant, the decoder reads
 * exactly the remaining bytes, strips the length field, and emits a {@link KafkaFrame} that owns
 * both the content and admission lease.
 */
@Internal
public final class KafkaAdmissionFrameDecoder extends ByteToMessageDecoder {
    private static final int LENGTH_FIELD_BYTES = 4;
    private static final int API_KEY_BYTES = 2;
    // apiKey + apiVersion + correlationId + nullable clientId length.
    private static final int MIN_REQUEST_HEADER_BYTES = 2 + 2 + 4 + 2;
    private static final long TIMEOUT_DISABLED = -1L;
    private static final long NO_STAGE_STARTED = -1L;

    private static final KafkaFrameAdmissionMetrics NO_OP_METRICS =
            new KafkaFrameAdmissionMetrics() {
                @Override
                public long nowNanos() {
                    return System.nanoTime();
                }

                @Override
                public void recordReservationRejected() {}

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
            };

    private final int maxFrameLength;
    private final KafkaFrameAdmission admission;
    private final KafkaFrameReadPauser readPauser;
    private final long preFrameWaitTimeoutMillis;
    private final long bodyReadTimeoutMillis;
    private final KafkaFrameAdmissionMetrics metrics;
    private final KafkaFrameReadState readState = new KafkaFrameReadState();

    private @Nullable Reservation pendingReservation;
    private @Nullable KafkaFrameAdmissionLease admissionLease;
    private @Nullable PauseLease waitPauseLease;
    private @Nullable ScheduledFuture<?> preFrameWaitTimeoutTask;
    private @Nullable ScheduledFuture<?> bodyReadTimeoutTask;
    private long preFrameWaitStartedNanos = NO_STAGE_STARTED;
    private long bodyReadStartedNanos = NO_STAGE_STARTED;
    private int wireFrameBytes = -1;
    private int frameBodyBytes = -1;
    private short apiKey;
    private volatile boolean terminal;

    /**
     * Creates a standalone decoder that pauses by changing auto-read directly.
     *
     * <p>Production pipelines must use the constructor that accepts {@link KafkaFrameReadPauser} so
     * pre-frame waiting participates in shared pause arbitration.
     *
     * @param maxFrameLength maximum complete wire frame size, including the length field
     * @param preferHeap whether frame cumulation should use heap buffers
     * @param admission pre-frame admission policy
     */
    KafkaAdmissionFrameDecoder(
            int maxFrameLength, boolean preferHeap, KafkaFrameAdmission admission) {
        this(
                maxFrameLength,
                preferHeap,
                admission,
                new DirectAutoReadPauser(),
                TIMEOUT_DISABLED,
                TIMEOUT_DISABLED,
                NO_OP_METRICS);
    }

    /**
     * Creates an admission-aware Kafka frame decoder with shared channel-pause arbitration.
     *
     * @param maxFrameLength maximum complete wire frame size, including the length field
     * @param preferHeap whether frame cumulation should use heap buffers
     * @param admission pre-frame admission policy
     * @param readPauser owner of the pre-frame wait pause reason
     */
    public KafkaAdmissionFrameDecoder(
            int maxFrameLength,
            boolean preferHeap,
            KafkaFrameAdmission admission,
            KafkaFrameReadPauser readPauser) {
        this(
                maxFrameLength,
                preferHeap,
                admission,
                readPauser,
                TIMEOUT_DISABLED,
                TIMEOUT_DISABLED,
                NO_OP_METRICS);
    }

    /**
     * Creates an admission-aware decoder with bounded pre-frame and body-read stages.
     *
     * <p>A pre-frame timeout cancels a reservation only if cancellation wins before its grant. A
     * body-read timeout closes the channel and releases the granted lease without emitting a
     * request. Both timeout tasks are cancelled after their respective stage completes.
     *
     * @param maxFrameLength maximum complete wire frame size, including the length field
     * @param preferHeap whether frame cumulation should use heap buffers
     * @param admission pre-frame admission policy
     * @param readPauser owner of the pre-frame wait pause reason
     * @param preFrameWaitTimeout maximum time to wait for a frame reservation
     * @param bodyReadTimeout maximum time to read the body after a reservation is granted
     * @param metrics lifecycle metrics callback
     */
    public KafkaAdmissionFrameDecoder(
            int maxFrameLength,
            boolean preferHeap,
            KafkaFrameAdmission admission,
            KafkaFrameReadPauser readPauser,
            Duration preFrameWaitTimeout,
            Duration bodyReadTimeout,
            KafkaFrameAdmissionMetrics metrics) {
        this(
                maxFrameLength,
                preferHeap,
                admission,
                readPauser,
                positiveTimeoutMillis(preFrameWaitTimeout, "preFrameWaitTimeout"),
                positiveTimeoutMillis(bodyReadTimeout, "bodyReadTimeout"),
                metrics);
    }

    private KafkaAdmissionFrameDecoder(
            int maxFrameLength,
            boolean preferHeap,
            KafkaFrameAdmission admission,
            KafkaFrameReadPauser readPauser,
            long preFrameWaitTimeoutMillis,
            long bodyReadTimeoutMillis,
            KafkaFrameAdmissionMetrics metrics) {
        checkArgument(
                maxFrameLength >= KafkaFrameReadState.PROBE_BYTES,
                "maxFrameLength must be at least %s",
                KafkaFrameReadState.PROBE_BYTES);
        this.maxFrameLength = maxFrameLength;
        this.admission = checkNotNull(admission, "admission");
        this.readPauser = checkNotNull(readPauser, "readPauser");
        this.preFrameWaitTimeoutMillis = preFrameWaitTimeoutMillis;
        this.bodyReadTimeoutMillis = bodyReadTimeoutMillis;
        this.metrics = checkNotNull(metrics, "metrics");
        setCumulator(new ProbeAwareCumulator(readState, preferHeap));
    }

    /**
     * Creates the receive allocator that must be installed on the same channel before its first
     * read.
     *
     * @param delegate transport's normal receive allocation policy
     * @return probe-aware receive allocator coupled to this decoder
     */
    public RecvByteBufAllocator newRecvByteBufAllocator(RecvByteBufAllocator delegate) {
        return new KafkaProbeAwareRecvByteBufAllocator(delegate, readState);
    }

    /**
     * Creates the outbound read gate that must be installed upstream of this decoder and every
     * other Kafka pipeline handler that may issue a read.
     *
     * <p>The gate shares this decoder's frame state and suppresses explicit reads while admission
     * is waiting or the decoder has stopped. This includes the compensating read issued by {@link
     * ByteToMessageDecoder} when auto-read is disabled before a frame has been emitted.
     *
     * @return read gate coupled to this decoder
     */
    public ChannelOutboundHandler newReadGate() {
        return new KafkaFrameReadGate(readState);
    }

    KafkaProbeAwareRecvByteBufAllocator newRecvByteBufAllocator(
            RecvByteBufAllocator delegate,
            KafkaProbeAwareRecvByteBufAllocator.ReadObserver observer) {
        return new KafkaProbeAwareRecvByteBufAllocator(delegate, readState, observer);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        try {
            decodeFrame(ctx, in, out);
        } catch (RuntimeException | Error failure) {
            terminal = true;
            ctx.close();
            cleanupAdmission();
            throw failure;
        }
    }

    private void decodeFrame(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (terminal || pendingReservation != null) {
            return;
        }

        if (admissionLease == null) {
            int readableBytes = in.readableBytes();
            if (readableBytes < LENGTH_FIELD_BYTES) {
                readState.updateProbeBytes(readableBytes);
                return;
            }

            int frameBodyLength = in.getInt(in.readerIndex());
            if (frameBodyLength < MIN_REQUEST_HEADER_BYTES) {
                throw new CorruptedFrameException(
                        "Kafka frame length must include the minimum request header of "
                                + MIN_REQUEST_HEADER_BYTES
                                + " bytes: "
                                + frameBodyLength);
            }
            long completeFrameBytes = (long) LENGTH_FIELD_BYTES + frameBodyLength;
            if (completeFrameBytes > maxFrameLength) {
                throw new TooLongFrameException(
                        "Adjusted frame length exceeds "
                                + maxFrameLength
                                + ": "
                                + completeFrameBytes);
            }
            if (readableBytes < KafkaFrameReadState.PROBE_BYTES) {
                readState.updateProbeBytes(readableBytes);
                return;
            }

            wireFrameBytes = (int) completeFrameBytes;
            frameBodyBytes = frameBodyLength;
            apiKey = in.getShort(in.readerIndex() + LENGTH_FIELD_BYTES);
            beginReservation(ctx);
            return;
        }

        int readableBytes = in.readableBytes();
        if (readableBytes < wireFrameBytes) {
            readState.readBody(wireFrameBytes - readableBytes);
            return;
        }
        if (readableBytes > wireFrameBytes) {
            throw new CorruptedFrameException(
                    "Kafka transport read beyond the admitted frame boundary: admitted="
                            + wireFrameBytes
                            + ", buffered="
                            + readableBytes);
        }

        in.skipBytes(LENGTH_FIELD_BYTES);
        ByteBuf content = in.readRetainedSlice(frameBodyBytes);
        KafkaFrameAdmissionLease frameLease = admissionLease;
        admissionLease = null;
        finishBodyRead();
        KafkaFrame frame = null;
        boolean transferred = false;
        try {
            frame = new KafkaFrame(content, apiKey, wireFrameBytes, frameLease);
            out.add(frame);
            transferred = true;
            frame = null;
            resetFrameState();
        } finally {
            if (frame != null) {
                frame.release();
            } else if (!transferred) {
                ReferenceCountUtil.safeRelease(content);
                frameLease.close();
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Propagate first so the downstream Kafka handler can register this channel with its
        // selected RequestChannel before admission tries to acquire shared pause leases.
        super.channelActive(ctx);
        admission.channelActive(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        terminal = true;
        try {
            cleanupAdmission();
        } finally {
            try {
                admission.close();
            } finally {
                super.channelInactive(ctx);
            }
        }
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        terminal = true;
        try {
            cleanupAdmission();
        } finally {
            try {
                admission.close();
            } finally {
                super.handlerRemoved0(ctx);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        terminal = true;
        cleanupAdmission();
        ctx.close();
        ctx.fireExceptionCaught(cause);
    }

    private void beginReservation(ChannelHandlerContext ctx) {
        readState.waitForGrant(wireFrameBytes);
        preFrameWaitStartedNanos = metricsNowNanos();
        Reservation reservation;
        try {
            reservation =
                    checkNotNull(
                            admission.reserve(ctx.channel(), wireFrameBytes, apiKey),
                            "admission reservation");
        } catch (RuntimeException rejection) {
            recordMetric(metrics::recordReservationRejected);
            throw rejection;
        }
        pendingReservation = reservation;
        CompletableFuture<? extends KafkaFrameAdmissionLease> grantFuture;
        try {
            grantFuture = checkNotNull(reservation.getFuture(), "admission grant future");
        } catch (RuntimeException rejection) {
            recordMetric(metrics::recordReservationRejected);
            throw rejection;
        }

        // The exact-capacity controller completes the common, uncontended reservation before
        // reserve() returns. Accept that grant in the current event-loop turn: the body gate is
        // already closed by readState.waitForGrant(), and completeGrant() installs the lease before
        // reopening it. Avoiding a pause/resume pair and an extra executor hop is important on the
        // normal Produce path. An incomplete future takes the bounded asynchronous wait path below.
        if (grantFuture.isDone() && ctx.channel().config().isAutoRead()) {
            KafkaFrameAdmissionLease lease = null;
            Throwable error = null;
            try {
                lease = grantFuture.join();
            } catch (CancellationException cancellation) {
                error = cancellation;
            } catch (CompletionException completion) {
                error = completion.getCause() == null ? completion : completion.getCause();
            }
            completeGrant(ctx, reservation, lease, error);
            return;
        }

        try {
            waitPauseLease =
                    checkNotNull(readPauser.pause(ctx.channel()), "pre-frame wait pause lease");
        } catch (RuntimeException | Error pauseFailure) {
            // The grant may win after the fast-path check but before pause acquisition fails.
            // Observe it before outer cleanup cancels the reservation so a losing cancellation
            // cannot orphan an already-created lease.
            grantFuture.whenComplete(
                    (lease, ignored) -> {
                        if (lease != null) {
                            lease.close();
                        }
                    });
            throw pauseFailure;
        }
        grantFuture.whenComplete(
                (lease, error) -> scheduleGrantCompletion(ctx, reservation, lease, error));
        schedulePreFrameWaitTimeout(ctx, reservation, preFrameWaitStartedNanos);
    }

    private void scheduleGrantCompletion(
            ChannelHandlerContext ctx,
            Reservation reservation,
            @Nullable KafkaFrameAdmissionLease lease,
            @Nullable Throwable error) {
        try {
            ctx.executor().execute(() -> completeGrant(ctx, reservation, lease, error));
        } catch (RuntimeException schedulingFailure) {
            terminal = true;
            try {
                cancelReservation(reservation);
            } finally {
                if (pendingReservation == reservation) {
                    pendingReservation = null;
                }
                try {
                    if (lease != null) {
                        lease.close();
                    }
                } finally {
                    cancelPreFrameWaitTimeout();
                    cancelBodyReadTimeout();
                    preFrameWaitStartedNanos = NO_STAGE_STARTED;
                    bodyReadStartedNanos = NO_STAGE_STARTED;
                    stopFrameReads();
                    try {
                        releaseWaitPause();
                    } finally {
                        ctx.close();
                    }
                }
            }
        }
    }

    private void completeGrant(
            ChannelHandlerContext ctx,
            Reservation reservation,
            @Nullable KafkaFrameAdmissionLease lease,
            @Nullable Throwable error) {
        if (pendingReservation != reservation) {
            if (lease != null) {
                lease.close();
            }
            return;
        }
        pendingReservation = null;
        cancelPreFrameWaitTimeout();

        if (error != null || lease == null || !ctx.channel().isActive()) {
            preFrameWaitStartedNanos = NO_STAGE_STARTED;
            terminal = true;
            try {
                if (error != null || lease == null) {
                    recordMetric(metrics::recordReservationRejected);
                }
                if (lease != null) {
                    lease.close();
                }
            } finally {
                stopFrameReads();
                try {
                    releaseWaitPause();
                } finally {
                    try {
                        if (error != null) {
                            ctx.fireExceptionCaught(error);
                        }
                    } finally {
                        ctx.close();
                    }
                }
            }
            return;
        }

        long completedPreFrameWaitStartedNanos = preFrameWaitStartedNanos;
        preFrameWaitStartedNanos = NO_STAGE_STARTED;
        admissionLease = lease;
        bodyReadStartedNanos = metricsNowNanos();
        readState.readBody(wireFrameBytes - KafkaFrameReadState.PROBE_BYTES);
        scheduleBodyReadTimeout(ctx, lease, bodyReadStartedNanos);
        recordMetric(() -> metrics.recordPreFrameWait(completedPreFrameWaitStartedNanos));
        releaseWaitPause();
    }

    private void cleanupAdmission() {
        cancelPreFrameWaitTimeout();
        cancelBodyReadTimeout();
        preFrameWaitStartedNanos = NO_STAGE_STARTED;
        bodyReadStartedNanos = NO_STAGE_STARTED;
        Reservation reservation = pendingReservation;
        pendingReservation = null;
        if (reservation != null) {
            cancelReservation(reservation);
        }
        KafkaFrameAdmissionLease lease = admissionLease;
        admissionLease = null;
        if (lease != null) {
            lease.close();
        }
        stopFrameReads();
        releaseWaitPause();
    }

    private void schedulePreFrameWaitTimeout(
            ChannelHandlerContext ctx, Reservation reservation, long startedNanos) {
        if (preFrameWaitTimeoutMillis == TIMEOUT_DISABLED) {
            return;
        }
        preFrameWaitTimeoutTask =
                ctx.executor()
                        .schedule(
                                () -> preFrameWaitTimedOut(ctx, reservation, startedNanos),
                                preFrameWaitTimeoutMillis,
                                TimeUnit.MILLISECONDS);
    }

    private void preFrameWaitTimedOut(
            ChannelHandlerContext ctx, Reservation reservation, long startedNanos) {
        if (terminal || pendingReservation != reservation) {
            return;
        }
        preFrameWaitTimeoutTask = null;
        if (!cancelReservation(reservation)) {
            return;
        }

        pendingReservation = null;
        preFrameWaitStartedNanos = NO_STAGE_STARTED;
        terminal = true;
        recordMetric(() -> metrics.recordPreFrameWaitTimeout(startedNanos));
        stopFrameReads();
        try {
            releaseWaitPause();
        } finally {
            ctx.close();
        }
    }

    private void scheduleBodyReadTimeout(
            ChannelHandlerContext ctx, KafkaFrameAdmissionLease lease, long startedNanos) {
        if (bodyReadTimeoutMillis == TIMEOUT_DISABLED) {
            return;
        }
        bodyReadTimeoutTask =
                ctx.executor()
                        .schedule(
                                () -> bodyReadTimedOut(ctx, lease, startedNanos),
                                bodyReadTimeoutMillis,
                                TimeUnit.MILLISECONDS);
    }

    private void bodyReadTimedOut(
            ChannelHandlerContext ctx, KafkaFrameAdmissionLease lease, long startedNanos) {
        if (terminal || admissionLease != lease) {
            return;
        }
        bodyReadTimeoutTask = null;
        admissionLease = null;
        bodyReadStartedNanos = NO_STAGE_STARTED;
        terminal = true;
        recordMetric(() -> metrics.recordBodyReadTimeout(startedNanos));
        try {
            lease.close();
        } finally {
            stopFrameReads();
            ctx.close();
        }
    }

    private boolean cancelReservation(Reservation reservation) {
        boolean cancelled = reservation.cancel();
        if (cancelled) {
            recordMetric(metrics::recordReservationCancelled);
        }
        return cancelled;
    }

    private void finishBodyRead() {
        cancelBodyReadTimeout();
        if (bodyReadStartedNanos != NO_STAGE_STARTED) {
            long completedBodyReadStartedNanos = bodyReadStartedNanos;
            bodyReadStartedNanos = NO_STAGE_STARTED;
            recordMetric(() -> metrics.recordBodyRead(completedBodyReadStartedNanos));
        }
    }

    private long metricsNowNanos() {
        try {
            return metrics.nowNanos();
        } catch (Throwable ignored) {
            // Metrics are observational and must not affect frame ownership or admission.
            return System.nanoTime();
        }
    }

    private void recordMetric(Runnable recorder) {
        try {
            recorder.run();
        } catch (Throwable ignored) {
            // Metrics are observational and must not affect frame ownership or admission.
        }
    }

    private void cancelPreFrameWaitTimeout() {
        ScheduledFuture<?> timeoutTask = preFrameWaitTimeoutTask;
        preFrameWaitTimeoutTask = null;
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
    }

    private void cancelBodyReadTimeout() {
        ScheduledFuture<?> timeoutTask = bodyReadTimeoutTask;
        bodyReadTimeoutTask = null;
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
    }

    private void releaseWaitPause() {
        PauseLease pauseLease = waitPauseLease;
        waitPauseLease = null;
        if (pauseLease != null) {
            pauseLease.close();
        }
    }

    private void resetFrameState() {
        wireFrameBytes = -1;
        frameBodyBytes = -1;
        readState.reset();
    }

    private static long positiveTimeoutMillis(Duration timeout, String name) {
        long timeoutMillis = checkNotNull(timeout, name).toMillis();
        checkArgument(timeoutMillis > 0, "%s must be at least one millisecond", name);
        return timeoutMillis;
    }

    private void stopFrameReads() {
        wireFrameBytes = -1;
        frameBodyBytes = -1;
        readState.stop();
    }

    /** Cumulator that never allocates a complete Kafka frame before admission is granted. */
    private static final class ProbeAwareCumulator implements Cumulator {
        private final KafkaFrameReadState readState;
        private final boolean preferHeap;

        private ProbeAwareCumulator(KafkaFrameReadState readState, boolean preferHeap) {
            this.readState = readState;
            this.preferHeap = preferHeap;
        }

        @Override
        public ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in) {
            if (cumulation == in) {
                in.release();
                return cumulation;
            }
            int oldBytes = cumulation.readableBytes();
            int incomingBytes = in.readableBytes();
            int totalBytes = Math.addExact(oldBytes, incomingBytes);
            int targetCapacity = readState.cumulationCapacity(totalBytes);
            if (totalBytes > targetCapacity) {
                in.release();
                throw new CorruptedFrameException(
                        "Kafka transport delivered bytes beyond the current read boundary: "
                                + totalBytes
                                + " > "
                                + targetCapacity);
            }

            if (oldBytes == 0 || !canAppend(cumulation, incomingBytes)) {
                return replaceCumulation(alloc, cumulation, in, targetCapacity);
            }
            try {
                cumulation.writeBytes(in, in.readerIndex(), incomingBytes);
                in.readerIndex(in.writerIndex());
                return cumulation;
            } finally {
                in.release();
            }
        }

        private boolean canAppend(ByteBuf cumulation, int incomingBytes) {
            return !cumulation.isReadOnly()
                    && cumulation.refCnt() == 1
                    && incomingBytes <= cumulation.maxWritableBytes()
                    && (preferHeap ? !cumulation.isDirect() : true);
        }

        private ByteBuf replaceCumulation(
                ByteBufAllocator alloc, ByteBuf oldCumulation, ByteBuf in, int capacity) {
            ByteBuf replacement =
                    preferHeap
                            ? alloc.heapBuffer(capacity, capacity)
                            : alloc.buffer(capacity, capacity);
            ByteBuf toRelease = replacement;
            try {
                replacement.writeBytes(
                        oldCumulation, oldCumulation.readerIndex(), oldCumulation.readableBytes());
                replacement.writeBytes(in, in.readerIndex(), in.readableBytes());
                in.readerIndex(in.writerIndex());
                toRelease = oldCumulation;
                return replacement;
            } finally {
                toRelease.release();
                in.release();
            }
        }
    }

    /** Standalone fallback used by tests until the decoder is wired to shared pause arbitration. */
    private static final class DirectAutoReadPauser implements KafkaFrameReadPauser {

        @Override
        public PauseLease pause(org.apache.fluss.shaded.netty4.io.netty.channel.Channel channel) {
            boolean restoreAutoRead = channel.config().isAutoRead();
            channel.config().setAutoRead(false);
            AtomicBoolean closed = new AtomicBoolean(false);
            return () -> {
                if (!closed.compareAndSet(false, true) || !channel.isActive()) {
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
}
