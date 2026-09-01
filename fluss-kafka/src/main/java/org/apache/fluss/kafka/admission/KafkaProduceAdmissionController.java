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

package org.apache.fluss.kafka.admission;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.network.KafkaFrameAdmissionLease;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseLease;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseReason;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * TabletServer-local admission accounting for Kafka Produce requests.
 *
 * <p>Pre-frame callers use {@link ConnectionHandle#reserve(long)} to atomically reserve one live
 * request and the full Kafka wire-frame bytes before reading the body. Capacity never overshoots a
 * configured limit. Waiters use a bounded arrival-ordered queue; global pressure preserves FIFO,
 * while a waiter blocked only by its own connection may be skipped to avoid cross-connection
 * head-of-line blocking.
 *
 * <p>The transitional {@link ConnectionHandle#acquire(long)} API remains for the old fully-framed
 * decoder. It now rejects requests that cannot fit instead of overshooting. Reaching a high
 * watermark continues to pause subsequent legacy channel reads, which resume at the fixed 50
 * percent low watermark.
 *
 * <p>Connection closure stops new admission and removes channel pause leases, but deliberately
 * keeps outstanding request accounting until each {@link RequestLease} releases its raw and live
 * ownership.
 */
@Internal
@ThreadSafe
public final class KafkaProduceAdmissionController {

    private static final int DEFAULT_MAX_PENDING_RESERVATIONS = 1024;

    /** Kafka Produce resources that can independently pause a connection. */
    public enum AdmissionPauseReason implements PauseReason {
        LIVE_REQUEST_COUNT,
        RAW_BYTES
    }

    /**
     * Ownership of one admitted Produce request.
     *
     * <p>Raw bytes and the live request have different lifetimes. Both release methods and {@link
     * #close()} are thread-safe and idempotent.
     */
    public interface RequestLease extends KafkaFrameAdmissionLease {

        /** Releases this request's raw Kafka frame bytes. */
        void releaseRaw();

        /** Releases this request's encoded Kafka wire-frame bytes. */
        @Override
        default void releaseFrameBytes() {
            releaseRaw();
        }

        /** Releases this request from the live request count. */
        void releaseLive();

        /** Releases this request from the live request count. */
        @Override
        default void releaseRequest() {
            releaseLive();
        }

        /** Releases both raw and live ownership. */
        @Override
        void close();
    }

    /**
     * A cancellable request for pre-frame Produce capacity.
     *
     * <p>The future is completed only after the controller lock has been released. Cancellation
     * wins only while the reservation is waiting in the FIFO queue. Once capacity has been granted,
     * the future owns the returned {@link RequestLease}; a decoder that no longer needs the grant
     * must close that lease.
     */
    public interface Reservation {

        /** Returns the future that receives exactly one raw/live request lease. */
        CompletableFuture<RequestLease> getFuture();

        /**
         * Cancels this reservation if it is still waiting.
         *
         * @return whether cancellation won before capacity was granted
         */
        boolean cancel();
    }

    private final Object lock = new Object();
    private final Clock clock;
    private final int maxPendingReservations;
    private final Map<Channel, ConnectionState> connections = new HashMap<>();
    private final Deque<ReservationImpl> pendingReservations = new ArrayDeque<>();

    private long maxLiveRequests;
    private long maxRawBytes;
    private long maxLiveRequestsPerConnection;
    private long maxRawBytesPerConnection;
    private long liveRequestResumeThreshold;
    private long rawBytesResumeThreshold;
    private long connectionLiveRequestResumeThreshold;
    private long connectionRawBytesResumeThreshold;

    private long liveRequests;
    private long liveBytes;
    private long rawBytes;
    private boolean globalLiveRequestPauseActive;
    private boolean globalRawBytesPauseActive;
    private long globalLiveRequestPauseStartedNanos;
    private long globalRawBytesPauseStartedNanos;

    private long liveRequestPauseEvents;
    private long rawBytesPauseEvents;
    private long cumulativeLiveRequestPauseNanos;
    private long cumulativeRawBytesPauseNanos;
    private long liveRequestOvershootEvents;
    private long rawBytesOvershootEvents;
    private long maxLiveRequestOvershoot;
    private long maxRawBytesOvershoot;

    /**
     * Creates one controller shared by every Kafka connection on a TabletServer.
     *
     * @param maxLiveRequests TabletServer-wide live request high watermark
     * @param maxRawBytes TabletServer-wide raw frame byte high watermark
     * @param maxLiveRequestsPerConnection per-connection live request high watermark
     * @param maxRawBytesPerConnection per-connection raw frame byte high watermark
     */
    public KafkaProduceAdmissionController(
            long maxLiveRequests,
            long maxRawBytes,
            long maxLiveRequestsPerConnection,
            long maxRawBytesPerConnection) {
        this(
                maxLiveRequests,
                maxRawBytes,
                maxLiveRequestsPerConnection,
                maxRawBytesPerConnection,
                SystemClock.getInstance(),
                DEFAULT_MAX_PENDING_RESERVATIONS);
    }

    /**
     * Creates one controller with an explicit bound on pending pre-frame reservations.
     *
     * @param maxLiveRequests TabletServer-wide live request limit
     * @param maxRawBytes TabletServer-wide raw wire-byte limit
     * @param maxLiveRequestsPerConnection per-connection live request limit
     * @param maxRawBytesPerConnection per-connection raw wire-byte limit
     * @param maxPendingReservations maximum queued pre-frame reservations
     */
    public KafkaProduceAdmissionController(
            long maxLiveRequests,
            long maxRawBytes,
            long maxLiveRequestsPerConnection,
            long maxRawBytesPerConnection,
            int maxPendingReservations) {
        this(
                maxLiveRequests,
                maxRawBytes,
                maxLiveRequestsPerConnection,
                maxRawBytesPerConnection,
                SystemClock.getInstance(),
                maxPendingReservations);
    }

    KafkaProduceAdmissionController(
            long maxLiveRequests,
            long maxRawBytes,
            long maxLiveRequestsPerConnection,
            long maxRawBytesPerConnection,
            Clock clock) {
        this(
                maxLiveRequests,
                maxRawBytes,
                maxLiveRequestsPerConnection,
                maxRawBytesPerConnection,
                clock,
                DEFAULT_MAX_PENDING_RESERVATIONS);
    }

    KafkaProduceAdmissionController(
            long maxLiveRequests,
            long maxRawBytes,
            long maxLiveRequestsPerConnection,
            long maxRawBytesPerConnection,
            Clock clock,
            int maxPendingReservations) {
        validateLimits(
                maxLiveRequests,
                maxRawBytes,
                maxLiveRequestsPerConnection,
                maxRawBytesPerConnection);
        checkArgument(maxPendingReservations > 0, "maxPendingReservations must be greater than 0");
        this.clock = checkNotNull(clock, "clock");
        this.maxPendingReservations = maxPendingReservations;
        setLimitsLocked(
                maxLiveRequests,
                maxRawBytes,
                maxLiveRequestsPerConnection,
                maxRawBytesPerConnection);
    }

    private static void validateLimits(
            long maxLiveRequests,
            long maxRawBytes,
            long maxLiveRequestsPerConnection,
            long maxRawBytesPerConnection) {
        checkArgument(maxLiveRequests > 0, "maxLiveRequests must be greater than 0");
        checkArgument(maxRawBytes > 0, "maxRawBytes must be greater than 0");
        checkArgument(
                maxLiveRequestsPerConnection > 0,
                "maxLiveRequestsPerConnection must be greater than 0");
        checkArgument(
                maxRawBytesPerConnection > 0, "maxRawBytesPerConnection must be greater than 0");
        checkArgument(
                maxLiveRequestsPerConnection <= maxLiveRequests,
                "maxLiveRequestsPerConnection must not exceed maxLiveRequests");
        checkArgument(
                maxRawBytesPerConnection <= maxRawBytes,
                "maxRawBytesPerConnection must not exceed maxRawBytes");
    }

    private void setLimitsLocked(
            long maxLiveRequests,
            long maxRawBytes,
            long maxLiveRequestsPerConnection,
            long maxRawBytesPerConnection) {
        this.maxLiveRequests = maxLiveRequests;
        this.maxRawBytes = maxRawBytes;
        this.maxLiveRequestsPerConnection = maxLiveRequestsPerConnection;
        this.maxRawBytesPerConnection = maxRawBytesPerConnection;
        this.liveRequestResumeThreshold = maxLiveRequests / 2;
        this.rawBytesResumeThreshold = maxRawBytes / 2;
        this.connectionLiveRequestResumeThreshold = maxLiveRequestsPerConnection / 2;
        this.connectionRawBytesResumeThreshold = maxRawBytesPerConnection / 2;
    }

    /**
     * Registers a Kafka connection and returns its admission handle.
     *
     * <p>The channel must already be registered with the supplied {@link RequestChannel}. A new
     * connection immediately inherits active TabletServer-wide pressure.
     */
    public ConnectionHandle registerConnection(Channel channel, RequestChannel requestChannel) {
        checkNotNull(channel, "channel");
        checkNotNull(requestChannel, "requestChannel");
        List<PauseLease> pauseLeases = new ArrayList<>(2);
        RuntimeException runtimeFailure = null;
        Error errorFailure = null;
        ConnectionHandle handle = null;
        synchronized (lock) {
            checkState(
                    !connections.containsKey(channel),
                    "Kafka Produce admission connection is already registered");
            ConnectionState state = new ConnectionState(channel, requestChannel);
            try {
                if (globalLiveRequestPauseActive) {
                    state.globalLiveRequestPauseLease =
                            requestChannel.pauseChannel(
                                    channel, AdmissionPauseReason.LIVE_REQUEST_COUNT);
                }
                if (globalRawBytesPauseActive) {
                    state.globalRawBytesPauseLease =
                            requestChannel.pauseChannel(channel, AdmissionPauseReason.RAW_BYTES);
                }
            } catch (RuntimeException | Error failure) {
                pauseLeases.add(state.globalLiveRequestPauseLease);
                pauseLeases.add(state.globalRawBytesPauseLease);
                state.globalLiveRequestPauseLease = null;
                state.globalRawBytesPauseLease = null;
                if (failure instanceof RuntimeException) {
                    runtimeFailure = (RuntimeException) failure;
                } else {
                    errorFailure = (Error) failure;
                }
            }
            if (runtimeFailure == null && errorFailure == null) {
                connections.put(channel, state);
                handle = new ConnectionHandle(state);
            }
        }
        closePauseLeasesAfterOperation(pauseLeases, runtimeFailure, errorFailure);
        return handle;
    }

    /**
     * Atomically replaces all admission limits and immediately reconciles high/low watermark state
     * against current usage.
     *
     * <p>Increasing a limit releases an existing pause only when usage is at or below the new 50
     * percent low watermark. Decreasing a limit activates a pause when usage is at or above the new
     * high watermark.
     */
    public void updateLimits(
            long maxLiveRequests,
            long maxRawBytes,
            long maxLiveRequestsPerConnection,
            long maxRawBytesPerConnection) {
        validateLimits(
                maxLiveRequests,
                maxRawBytes,
                maxLiveRequestsPerConnection,
                maxRawBytesPerConnection);
        List<ReservationCompletion> completions = new ArrayList<>();
        List<PauseLease> pauseLeases = new ArrayList<>();
        RuntimeException runtimeFailure = null;
        Error errorFailure = null;
        synchronized (lock) {
            try {
                setLimitsLocked(
                        maxLiveRequests,
                        maxRawBytes,
                        maxLiveRequestsPerConnection,
                        maxRawBytesPerConnection);
                reconcileGlobalLiveRequestPauseLocked(pauseLeases);
                reconcileGlobalRawBytesPauseLocked(pauseLeases);
                for (ConnectionState state : connections.values()) {
                    reconcileConnectionLiveRequestPauseLocked(state, pauseLeases);
                    reconcileConnectionRawBytesPauseLocked(state, pauseLeases);
                    recordConnectionOvershootLocked(state);
                }
                recordGlobalOvershootLocked();
                drainPendingReservationsLocked(completions);
            } catch (RuntimeException | Error failure) {
                if (failure instanceof RuntimeException) {
                    runtimeFailure = (RuntimeException) failure;
                } else {
                    errorFailure = (Error) failure;
                }
            }
        }
        completeReservations(completions);
        closePauseLeasesAfterOperation(pauseLeases, runtimeFailure, errorFailure);
    }

    /** Returns the TabletServer-wide live request high watermark. */
    public long maxLiveRequestsLimit() {
        synchronized (lock) {
            return maxLiveRequests;
        }
    }

    /** Returns the TabletServer-wide raw byte high watermark. */
    public long maxRawBytesLimit() {
        synchronized (lock) {
            return maxRawBytes;
        }
    }

    /** Returns the per-connection live request high watermark. */
    public long maxLiveRequestsPerConnectionLimit() {
        synchronized (lock) {
            return maxLiveRequestsPerConnection;
        }
    }

    /** Returns the per-connection raw byte high watermark. */
    public long maxRawBytesPerConnectionLimit() {
        synchronized (lock) {
            return maxRawBytesPerConnection;
        }
    }

    /** Returns the current TabletServer-wide live Produce request count. */
    public long liveRequests() {
        synchronized (lock) {
            return liveRequests;
        }
    }

    /** Returns bytes retained by live requests, independent of raw frame ownership. */
    public long liveBytes() {
        synchronized (lock) {
            return liveBytes;
        }
    }

    /** Returns the current TabletServer-wide admitted raw Kafka frame bytes. */
    public long rawBytes() {
        synchronized (lock) {
            return rawBytes;
        }
    }

    /** Returns the number of connections currently accepting new Produce requests. */
    public int registeredConnections() {
        synchronized (lock) {
            return connections.size();
        }
    }

    /** Returns the number of registered connections paused by this admission controller. */
    public int pausedConnections() {
        synchronized (lock) {
            int paused = 0;
            for (ConnectionState state : connections.values()) {
                if (hasAdmissionPauseLease(state)) {
                    paused++;
                }
            }
            return paused;
        }
    }

    /** Returns the number of Produce frame probes waiting for an all-or-nothing grant. */
    public int pendingReservations() {
        synchronized (lock) {
            return pendingReservations.size();
        }
    }

    /** Returns the largest current live request count among registered connections. */
    public long maxConnectionLiveRequests() {
        synchronized (lock) {
            long maximum = 0;
            for (ConnectionState state : connections.values()) {
                maximum = Math.max(maximum, state.liveRequests);
            }
            return maximum;
        }
    }

    /** Returns the largest current raw byte count among registered connections. */
    public long maxConnectionRawBytes() {
        synchronized (lock) {
            long maximum = 0;
            for (ConnectionState state : connections.values()) {
                maximum = Math.max(maximum, state.rawBytes);
            }
            return maximum;
        }
    }

    /** Returns cumulative global and per-connection live request pause activations. */
    public long liveRequestPauseEvents() {
        synchronized (lock) {
            return liveRequestPauseEvents;
        }
    }

    /** Returns cumulative global and per-connection raw byte pause activations. */
    public long rawBytesPauseEvents() {
        synchronized (lock) {
            return rawBytesPauseEvents;
        }
    }

    /** Returns cumulative global and per-connection live request overshoot observations. */
    public long liveRequestOvershootEvents() {
        synchronized (lock) {
            return liveRequestOvershootEvents;
        }
    }

    /** Returns cumulative global and per-connection raw byte overshoot observations. */
    public long rawBytesOvershootEvents() {
        synchronized (lock) {
            return rawBytesOvershootEvents;
        }
    }

    /** Returns the largest observed global or per-connection live request overshoot. */
    public long maxLiveRequestOvershoot() {
        synchronized (lock) {
            return maxLiveRequestOvershoot;
        }
    }

    /** Returns the largest observed global or per-connection raw byte overshoot. */
    public long maxRawBytesOvershoot() {
        synchronized (lock) {
            return maxRawBytesOvershoot;
        }
    }

    /**
     * Returns cumulative live-request pause time in microseconds.
     *
     * <p>Global and per-connection pressure episodes are summed and may overlap.
     */
    public long cumulativeLiveRequestPauseTimeMicros() {
        synchronized (lock) {
            return TimeUnit.NANOSECONDS.toMicros(currentLiveRequestPauseNanosLocked());
        }
    }

    /**
     * Returns cumulative raw-byte pause time in microseconds.
     *
     * <p>Global and per-connection pressure episodes are summed and may overlap.
     */
    public long cumulativeRawBytesPauseTimeMicros() {
        synchronized (lock) {
            return TimeUnit.NANOSECONDS.toMicros(currentRawBytesPauseNanosLocked());
        }
    }

    /** Returns the duration in milliseconds of the longest active live-request pause episode. */
    public long longestActiveLiveRequestPauseTimeMillis() {
        synchronized (lock) {
            long nowNanos = clock.nanoseconds();
            long longest =
                    globalLiveRequestPauseActive
                            ? elapsedNanos(globalLiveRequestPauseStartedNanos, nowNanos)
                            : 0;
            for (ConnectionState state : connections.values()) {
                if (state.connectionLiveRequestPauseLease != null) {
                    longest =
                            Math.max(
                                    longest,
                                    elapsedNanos(
                                            state.connectionLiveRequestPauseStartedNanos,
                                            nowNanos));
                }
            }
            return TimeUnit.NANOSECONDS.toMillis(longest);
        }
    }

    /** Returns the duration in milliseconds of the longest active raw-byte pause episode. */
    public long longestActiveRawBytesPauseTimeMillis() {
        synchronized (lock) {
            long nowNanos = clock.nanoseconds();
            long longest =
                    globalRawBytesPauseActive
                            ? elapsedNanos(globalRawBytesPauseStartedNanos, nowNanos)
                            : 0;
            for (ConnectionState state : connections.values()) {
                if (state.connectionRawBytesPauseLease != null) {
                    longest =
                            Math.max(
                                    longest,
                                    elapsedNanos(
                                            state.connectionRawBytesPauseStartedNanos, nowNanos));
                }
            }
            return TimeUnit.NANOSECONDS.toMillis(longest);
        }
    }

    /** A per-connection handle used to reserve and admit Produce requests. */
    public final class ConnectionHandle implements AutoCloseable {
        private final ConnectionState state;

        private ConnectionHandle(ConnectionState state) {
            this.state = state;
        }

        /**
         * Immediately admits one fully framed Produce request.
         *
         * <p>This transitional API never waits. It rejects the request if any global or
         * per-connection capacity is unavailable and never overshoots a limit. New frame decoders
         * must use {@link #reserve(long)} before reading the request body.
         *
         * @param requestBytes bytes in the decoded Kafka frame, excluding the length prefix
         * @return independent raw/live ownership for the admitted request
         */
        public RequestLease acquire(long requestBytes) {
            checkArgument(requestBytes >= 0, "requestBytes must not be negative");
            List<PauseLease> pauseLeases = new ArrayList<>();
            RuntimeException runtimeFailure = null;
            Error errorFailure = null;
            RequestLease requestLease = null;
            synchronized (lock) {
                try {
                    checkState(state.registered, "Kafka Produce admission connection is closed");
                    checkFrameCanEverFitLocked(requestBytes);
                    if (!pendingReservations.isEmpty()
                            || !hasReservationCapacityLocked(state, requestBytes)) {
                        throw new RejectedExecutionException(
                                "Kafka Produce admission capacity is currently unavailable");
                    }
                    requestLease = acquireLocked(state, requestBytes, pauseLeases);
                } catch (RuntimeException | Error failure) {
                    if (failure instanceof RuntimeException) {
                        runtimeFailure = (RuntimeException) failure;
                    } else {
                        errorFailure = (Error) failure;
                    }
                }
            }
            closePauseLeasesAfterOperation(pauseLeases, runtimeFailure, errorFailure);
            return requestLease;
        }

        /**
         * Reserves one live request and all raw frame bytes before the frame body is read.
         *
         * <p>Capacity is granted atomically across the TabletServer-wide and per-connection live
         * and raw limits. If capacity is unavailable, the reservation joins one bounded strict FIFO
         * queue. A candidate blocked by TabletServer-wide capacity stops the scan, preventing
         * smaller frames from starving it. A candidate blocked only by its connection limit may be
         * skipped so one slow connection cannot stall unrelated connections. Each connection may
         * have at most one waiting reservation. Impossible requests, duplicate waiters, and queue
         * overflow complete the returned future exceptionally.
         *
         * @param frameBytes full Kafka wire frame bytes, including the four-byte length prefix
         * @return a cancellable reservation
         */
        public Reservation reserve(long frameBytes) {
            checkArgument(frameBytes >= 0, "frameBytes must not be negative");
            ReservationImpl reservation = new ReservationImpl(state, frameBytes);
            List<ReservationCompletion> completions = new ArrayList<>(1);
            synchronized (lock) {
                checkState(state.registered, "Kafka Produce admission connection is closed");
                if (!frameCanEverFitLocked(frameBytes)) {
                    rejectLocked(
                            reservation,
                            new IllegalArgumentException(
                                    "Kafka Produce frame exceeds an admission raw-byte limit"),
                            completions);
                } else if (state.pendingReservation != null) {
                    rejectLocked(
                            reservation,
                            new RejectedExecutionException(
                                    "Kafka Produce connection already has a waiting reservation"),
                            completions);
                } else if (pendingReservations.size() >= maxPendingReservations) {
                    rejectLocked(
                            reservation,
                            new RejectedExecutionException(
                                    "Kafka Produce admission waiter queue is full"),
                            completions);
                } else if (pendingReservations.isEmpty()
                        && hasReservationCapacityLocked(state, frameBytes)) {
                    grantLocked(reservation, completions);
                } else {
                    state.pendingReservation = reservation;
                    pendingReservations.addLast(reservation);
                    drainPendingReservationsLocked(completions);
                }
            }
            completeReservations(completions);
            return reservation;
        }

        /** Returns the current live request count retained by this connection. */
        public long liveRequests() {
            synchronized (lock) {
                return state.liveRequests;
            }
        }

        /** Returns bytes retained until this connection's live requests finish. */
        public long liveBytes() {
            synchronized (lock) {
                return state.liveBytes;
            }
        }

        /** Returns the current raw frame bytes retained by this connection. */
        public long rawBytes() {
            synchronized (lock) {
                return state.rawBytes;
            }
        }

        /** Returns whether this handle still accepts new requests. */
        public boolean isRegistered() {
            synchronized (lock) {
                return state.registered;
            }
        }

        /**
         * Stops new request admission and removes this connection's admission pause leases.
         * Outstanding request leases remain accounted until they release themselves.
         */
        @Override
        public void close() {
            List<PauseLease> pauseLeases;
            List<ReservationCompletion> completions = new ArrayList<>();
            synchronized (lock) {
                if (!state.registered) {
                    return;
                }
                state.registered = false;
                connections.remove(state.channel, state);
                cancelWaitingReservationsLocked(state, completions);
                drainPendingReservationsLocked(completions);
                pauseLeases = detachAllPauseLeasesLocked(state);
            }
            completeReservations(completions);
            closePauseLeases(pauseLeases);
        }
    }

    private void checkFrameCanEverFitLocked(long frameBytes) {
        checkArgument(
                frameCanEverFitLocked(frameBytes),
                "Kafka Produce frame exceeds an admission raw-byte limit");
    }

    private boolean frameCanEverFitLocked(long frameBytes) {
        return frameBytes <= maxRawBytes && frameBytes <= maxRawBytesPerConnection;
    }

    private boolean hasReservationCapacityLocked(ConnectionState state, long frameBytes) {
        return hasGlobalReservationCapacityLocked(frameBytes)
                && hasConnectionReservationCapacityLocked(state, frameBytes);
    }

    private boolean hasGlobalReservationCapacityLocked(long frameBytes) {
        return liveRequests < maxLiveRequests
                && canAddWithinLimit(rawBytes, frameBytes, maxRawBytes)
                && canAddWithinLimit(liveBytes, frameBytes, maxRawBytes);
    }

    private boolean hasConnectionReservationCapacityLocked(ConnectionState state, long frameBytes) {
        return state.liveRequests < maxLiveRequestsPerConnection
                && canAddWithinLimit(state.rawBytes, frameBytes, maxRawBytesPerConnection)
                && canAddWithinLimit(state.liveBytes, frameBytes, maxRawBytesPerConnection);
    }

    private static boolean canAddWithinLimit(long current, long addition, long limit) {
        return current <= limit && addition <= limit - current;
    }

    private void grantLocked(ReservationImpl reservation, List<ReservationCompletion> completions) {
        checkState(
                reservation.state.compareAndSet(ReservationState.WAITING, ReservationState.GRANTED),
                "Only a waiting Kafka Produce reservation can be granted");
        ConnectionState state = reservation.connectionState;
        long frameBytes = reservation.frameBytes;
        liveRequests++;
        liveBytes += frameBytes;
        rawBytes += frameBytes;
        state.liveRequests++;
        state.liveBytes += frameBytes;
        state.rawBytes += frameBytes;
        RequestLease requestLease = new RequestLeaseImpl(state, frameBytes, true);
        completions.add(ReservationCompletion.granted(reservation, requestLease));
    }

    private static void rejectLocked(
            ReservationImpl reservation,
            Throwable failure,
            List<ReservationCompletion> completions) {
        checkState(
                reservation.state.compareAndSet(
                        ReservationState.WAITING, ReservationState.REJECTED),
                "Only a waiting Kafka Produce reservation can be rejected");
        completions.add(ReservationCompletion.failed(reservation, failure));
    }

    private void cancelWaitingReservationsLocked(
            ConnectionState state, List<ReservationCompletion> completions) {
        Iterator<ReservationImpl> iterator = pendingReservations.iterator();
        while (iterator.hasNext()) {
            ReservationImpl reservation = iterator.next();
            if (reservation.connectionState == state
                    && reservation.state.compareAndSet(
                            ReservationState.WAITING, ReservationState.CANCELLED)) {
                iterator.remove();
                state.pendingReservation = null;
                completions.add(
                        ReservationCompletion.failed(
                                reservation,
                                new CancellationException(
                                        "Kafka Produce admission connection is closed")));
            }
        }
    }

    private void drainPendingReservationsLocked(List<ReservationCompletion> completions) {
        Iterator<ReservationImpl> iterator = pendingReservations.iterator();
        while (iterator.hasNext()) {
            ReservationImpl reservation = iterator.next();
            if (reservation.state.get() != ReservationState.WAITING) {
                iterator.remove();
                clearPendingReservationLocked(reservation);
                continue;
            }
            if (!reservation.connectionState.registered) {
                iterator.remove();
                clearPendingReservationLocked(reservation);
                if (reservation.state.compareAndSet(
                        ReservationState.WAITING, ReservationState.CANCELLED)) {
                    completions.add(
                            ReservationCompletion.failed(
                                    reservation,
                                    new CancellationException(
                                            "Kafka Produce admission connection is closed")));
                }
                continue;
            }
            if (!frameCanEverFitLocked(reservation.frameBytes)) {
                iterator.remove();
                clearPendingReservationLocked(reservation);
                rejectLocked(
                        reservation,
                        new IllegalArgumentException(
                                "Kafka Produce frame exceeds an admission raw-byte limit"),
                        completions);
                continue;
            }
            if (!hasGlobalReservationCapacityLocked(reservation.frameBytes)) {
                // Do not let smaller followers starve an older request on global capacity.
                return;
            }
            if (!hasConnectionReservationCapacityLocked(
                    reservation.connectionState, reservation.frameBytes)) {
                // A slow connection must not head-of-line block unrelated connections.
                continue;
            }
            iterator.remove();
            clearPendingReservationLocked(reservation);
            grantLocked(reservation, completions);
        }
    }

    private static void clearPendingReservationLocked(ReservationImpl reservation) {
        if (reservation.connectionState.pendingReservation == reservation) {
            reservation.connectionState.pendingReservation = null;
        }
    }

    private boolean cancelReservation(ReservationImpl reservation) {
        List<ReservationCompletion> completions = new ArrayList<>();
        boolean cancelled;
        synchronized (lock) {
            cancelled =
                    reservation.state.compareAndSet(
                            ReservationState.WAITING, ReservationState.CANCELLED);
            if (cancelled) {
                pendingReservations.remove(reservation);
                clearPendingReservationLocked(reservation);
                completions.add(
                        ReservationCompletion.failed(
                                reservation,
                                new CancellationException(
                                        "Kafka Produce admission reservation was cancelled")));
                drainPendingReservationsLocked(completions);
            }
        }
        completeReservations(completions);
        return cancelled;
    }

    private static void completeReservations(List<ReservationCompletion> completions) {
        for (ReservationCompletion completion : completions) {
            if (completion.failure == null) {
                if (!completion.reservation.future.complete(completion.requestLease)) {
                    // A caller may have cancelled the exposed future directly. Capacity was
                    // already granted, so return it immediately if ownership cannot be delivered.
                    completion.requestLease.close();
                }
            } else {
                completion.reservation.future.completeExceptionally(completion.failure);
            }
        }
    }

    private RequestLease acquireLocked(
            ConnectionState state, long requestBytes, List<PauseLease> pauseLeases) {
        long newGlobalLiveRequests = Math.addExact(liveRequests, 1L);
        long newGlobalLiveBytes = Math.addExact(liveBytes, requestBytes);
        long newGlobalRawBytes = Math.addExact(rawBytes, requestBytes);
        long newConnectionLiveRequests = Math.addExact(state.liveRequests, 1L);
        long newConnectionLiveBytes = Math.addExact(state.liveBytes, requestBytes);
        long newConnectionRawBytes = Math.addExact(state.rawBytes, requestBytes);

        long previousLiveRequests = liveRequests;
        long previousLiveBytes = liveBytes;
        long previousRawBytes = rawBytes;
        long previousConnectionLiveRequests = state.liveRequests;
        long previousConnectionLiveBytes = state.liveBytes;
        long previousConnectionRawBytes = state.rawBytes;
        long previousLiveRequestPauseEvents = liveRequestPauseEvents;
        long previousRawBytesPauseEvents = rawBytesPauseEvents;
        boolean activatedGlobalLive = false;
        boolean activatedGlobalRaw = false;
        boolean activatedConnectionLive = false;
        boolean activatedConnectionRaw = false;

        liveRequests = newGlobalLiveRequests;
        liveBytes = newGlobalLiveBytes;
        rawBytes = newGlobalRawBytes;
        state.liveRequests = newConnectionLiveRequests;
        state.liveBytes = newConnectionLiveBytes;
        state.rawBytes = newConnectionRawBytes;
        try {
            if (!globalLiveRequestPauseActive && liveRequests >= maxLiveRequests) {
                activateGlobalPauseLocked(AdmissionPauseReason.LIVE_REQUEST_COUNT, pauseLeases);
                activatedGlobalLive = true;
            }
            if (!globalRawBytesPauseActive && rawBytes >= maxRawBytes) {
                activateGlobalPauseLocked(AdmissionPauseReason.RAW_BYTES, pauseLeases);
                activatedGlobalRaw = true;
            }
            if (state.connectionLiveRequestPauseLease == null
                    && state.liveRequests >= maxLiveRequestsPerConnection) {
                activateConnectionPauseLocked(state, AdmissionPauseReason.LIVE_REQUEST_COUNT);
                activatedConnectionLive = true;
            }
            if (state.connectionRawBytesPauseLease == null
                    && state.rawBytes >= maxRawBytesPerConnection) {
                activateConnectionPauseLocked(state, AdmissionPauseReason.RAW_BYTES);
                activatedConnectionRaw = true;
            }
        } catch (RuntimeException | Error failure) {
            if (activatedConnectionRaw) {
                rollbackConnectionPauseLocked(state, AdmissionPauseReason.RAW_BYTES, pauseLeases);
            }
            if (activatedConnectionLive) {
                rollbackConnectionPauseLocked(
                        state, AdmissionPauseReason.LIVE_REQUEST_COUNT, pauseLeases);
            }
            if (activatedGlobalRaw) {
                rollbackGlobalPauseLocked(AdmissionPauseReason.RAW_BYTES, pauseLeases);
            }
            if (activatedGlobalLive) {
                rollbackGlobalPauseLocked(AdmissionPauseReason.LIVE_REQUEST_COUNT, pauseLeases);
            }
            liveRequestPauseEvents = previousLiveRequestPauseEvents;
            rawBytesPauseEvents = previousRawBytesPauseEvents;
            liveRequests = previousLiveRequests;
            liveBytes = previousLiveBytes;
            rawBytes = previousRawBytes;
            state.liveRequests = previousConnectionLiveRequests;
            state.liveBytes = previousConnectionLiveBytes;
            state.rawBytes = previousConnectionRawBytes;
            throw failure;
        }

        recordGlobalOvershootLocked();
        recordConnectionOvershootLocked(state);
        return new RequestLeaseImpl(state, requestBytes, false);
    }

    private void growRawLocked(
            RequestLeaseImpl lease, long additionalBytes, List<PauseLease> pauseLeases) {
        checkState(!lease.rawReleased, "Kafka Produce raw-byte ownership is already released");
        if (!canAddWithinLimit(lease.rawBytes, additionalBytes, maxRawBytes)
                || !canAddWithinLimit(lease.rawBytes, additionalBytes, maxRawBytesPerConnection)) {
            throw new IllegalArgumentException(
                    "Kafka Produce frame and copied payload exceed an admission raw-byte limit");
        }
        if (!canAddWithinLimit(rawBytes, additionalBytes, maxRawBytes)
                || !canAddWithinLimit(
                        lease.state.rawBytes, additionalBytes, maxRawBytesPerConnection)) {
            throw new RejectedExecutionException(
                    "Kafka Produce copied-payload admission capacity is currently unavailable");
        }

        long previousRawBytes = rawBytes;
        long previousConnectionRawBytes = lease.state.rawBytes;
        long previousLeaseRawBytes = lease.rawBytes;
        long previousLeaseGrownRawBytes = lease.grownRawBytes;
        long previousRawBytesPauseEvents = rawBytesPauseEvents;
        boolean activatedGlobalRaw = false;
        boolean activatedConnectionRaw = false;

        rawBytes += additionalBytes;
        lease.state.rawBytes += additionalBytes;
        lease.rawBytes += additionalBytes;
        lease.grownRawBytes += additionalBytes;
        try {
            if (!lease.hardReservation) {
                if (!globalRawBytesPauseActive && rawBytes >= maxRawBytes) {
                    activateGlobalPauseLocked(AdmissionPauseReason.RAW_BYTES, pauseLeases);
                    activatedGlobalRaw = true;
                }
                if (lease.state.registered
                        && lease.state.connectionRawBytesPauseLease == null
                        && lease.state.rawBytes >= maxRawBytesPerConnection) {
                    activateConnectionPauseLocked(lease.state, AdmissionPauseReason.RAW_BYTES);
                    activatedConnectionRaw = true;
                }
            }
        } catch (RuntimeException | Error failure) {
            if (activatedConnectionRaw) {
                rollbackConnectionPauseLocked(
                        lease.state, AdmissionPauseReason.RAW_BYTES, pauseLeases);
            }
            if (activatedGlobalRaw) {
                rollbackGlobalPauseLocked(AdmissionPauseReason.RAW_BYTES, pauseLeases);
            }
            rawBytesPauseEvents = previousRawBytesPauseEvents;
            rawBytes = previousRawBytes;
            lease.state.rawBytes = previousConnectionRawBytes;
            lease.rawBytes = previousLeaseRawBytes;
            lease.grownRawBytes = previousLeaseGrownRawBytes;
            throw failure;
        }

        recordGlobalOvershootLocked();
        recordConnectionOvershootLocked(lease.state);
    }

    private void releaseGrownRawLocked(
            RequestLeaseImpl lease,
            long additionalBytes,
            List<PauseLease> pauseLeases,
            List<ReservationCompletion> completions) {
        if (lease.rawReleased) {
            return;
        }
        checkArgument(
                additionalBytes <= lease.grownRawBytes,
                "Cannot release more Kafka Produce copied-payload bytes than were reserved");
        checkState(rawBytes >= additionalBytes, "Global raw byte accounting underflow");
        checkState(
                lease.state.rawBytes >= additionalBytes,
                "Connection raw byte accounting underflow");
        checkState(lease.rawBytes >= additionalBytes, "Lease raw byte accounting underflow");
        rawBytes -= additionalBytes;
        lease.state.rawBytes -= additionalBytes;
        lease.rawBytes -= additionalBytes;
        lease.grownRawBytes -= additionalBytes;

        if (!lease.hardReservation) {
            reconcileGlobalRawBytesPauseLocked(pauseLeases);
            if (lease.state.registered) {
                reconcileConnectionRawBytesPauseLocked(lease.state, pauseLeases);
            }
        }
        drainPendingReservationsLocked(completions);
    }

    private void releaseRawLocked(RequestLeaseImpl lease, List<PauseLease> pauseLeases) {
        if (lease.rawReleased) {
            return;
        }
        checkState(rawBytes >= lease.rawBytes, "Global raw byte accounting underflow");
        checkState(
                lease.state.rawBytes >= lease.rawBytes, "Connection raw byte accounting underflow");
        rawBytes -= lease.rawBytes;
        lease.state.rawBytes -= lease.rawBytes;
        lease.rawBytes = 0;
        lease.grownRawBytes = 0;
        lease.rawReleased = true;

        reconcileGlobalRawBytesPauseLocked(pauseLeases);
        if (lease.state.registered) {
            reconcileConnectionRawBytesPauseLocked(lease.state, pauseLeases);
        }
    }

    private void releaseLiveLocked(RequestLeaseImpl lease, List<PauseLease> pauseLeases) {
        if (lease.liveReleased) {
            return;
        }
        checkState(liveRequests > 0, "Global live request accounting underflow");
        checkState(lease.state.liveRequests > 0, "Connection live request accounting underflow");
        checkState(liveBytes >= lease.requestBytes, "Global live byte accounting underflow");
        checkState(
                lease.state.liveBytes >= lease.requestBytes,
                "Connection live byte accounting underflow");
        liveRequests--;
        liveBytes -= lease.requestBytes;
        lease.state.liveRequests--;
        lease.state.liveBytes -= lease.requestBytes;
        lease.liveReleased = true;

        reconcileGlobalLiveRequestPauseLocked(pauseLeases);
        if (lease.state.registered) {
            reconcileConnectionLiveRequestPauseLocked(lease.state, pauseLeases);
        }
    }

    private void releaseHardRawLocked(
            RequestLeaseImpl lease, List<ReservationCompletion> completions) {
        if (lease.rawReleased) {
            return;
        }
        checkState(rawBytes >= lease.rawBytes, "Global raw byte accounting underflow");
        checkState(
                lease.state.rawBytes >= lease.rawBytes, "Connection raw byte accounting underflow");
        rawBytes -= lease.rawBytes;
        lease.state.rawBytes -= lease.rawBytes;
        lease.rawBytes = 0;
        lease.grownRawBytes = 0;
        lease.rawReleased = true;
        drainPendingReservationsLocked(completions);
    }

    private void releaseHardLiveLocked(
            RequestLeaseImpl lease, List<ReservationCompletion> completions) {
        if (lease.liveReleased) {
            return;
        }
        checkState(liveRequests > 0, "Global live request accounting underflow");
        checkState(lease.state.liveRequests > 0, "Connection live request accounting underflow");
        checkState(liveBytes >= lease.requestBytes, "Global live byte accounting underflow");
        checkState(
                lease.state.liveBytes >= lease.requestBytes,
                "Connection live byte accounting underflow");
        liveRequests--;
        liveBytes -= lease.requestBytes;
        lease.state.liveRequests--;
        lease.state.liveBytes -= lease.requestBytes;
        lease.liveReleased = true;
        drainPendingReservationsLocked(completions);
    }

    private void activateGlobalPauseLocked(
            AdmissionPauseReason reason, List<PauseLease> pauseLeasesToClose) {
        long startedNanos = clock.nanoseconds();
        List<ConnectionState> acquired = new ArrayList<>();
        try {
            for (ConnectionState state : connections.values()) {
                PauseLease pauseLease = state.requestChannel.pauseChannel(state.channel, reason);
                setGlobalPauseLease(state, reason, pauseLease);
                acquired.add(state);
            }
        } catch (RuntimeException | Error failure) {
            for (ConnectionState state : acquired) {
                pauseLeasesToClose.add(globalPauseLease(state, reason));
                setGlobalPauseLease(state, reason, null);
            }
            throw failure;
        }

        if (reason == AdmissionPauseReason.LIVE_REQUEST_COUNT) {
            globalLiveRequestPauseActive = true;
            globalLiveRequestPauseStartedNanos = startedNanos;
            liveRequestPauseEvents = saturatingIncrement(liveRequestPauseEvents);
        } else {
            globalRawBytesPauseActive = true;
            globalRawBytesPauseStartedNanos = startedNanos;
            rawBytesPauseEvents = saturatingIncrement(rawBytesPauseEvents);
        }
    }

    private void deactivateGlobalPauseLocked(
            AdmissionPauseReason reason, List<PauseLease> pauseLeasesToClose) {
        long stoppedNanos = clock.nanoseconds();
        for (ConnectionState state : connections.values()) {
            pauseLeasesToClose.add(globalPauseLease(state, reason));
            setGlobalPauseLease(state, reason, null);
        }
        if (reason == AdmissionPauseReason.LIVE_REQUEST_COUNT) {
            cumulativeLiveRequestPauseNanos =
                    saturatingAdd(
                            cumulativeLiveRequestPauseNanos,
                            elapsedNanos(globalLiveRequestPauseStartedNanos, stoppedNanos));
            globalLiveRequestPauseActive = false;
            globalLiveRequestPauseStartedNanos = 0;
        } else {
            cumulativeRawBytesPauseNanos =
                    saturatingAdd(
                            cumulativeRawBytesPauseNanos,
                            elapsedNanos(globalRawBytesPauseStartedNanos, stoppedNanos));
            globalRawBytesPauseActive = false;
            globalRawBytesPauseStartedNanos = 0;
        }
        // The caller closes detached leases after releasing the controller lock. Closing the last
        // lease can synchronously resume the channel and re-enter request admission.
    }

    private void rollbackGlobalPauseLocked(
            AdmissionPauseReason reason, List<PauseLease> pauseLeasesToClose) {
        for (ConnectionState state : connections.values()) {
            pauseLeasesToClose.add(globalPauseLease(state, reason));
            setGlobalPauseLease(state, reason, null);
        }
        if (reason == AdmissionPauseReason.LIVE_REQUEST_COUNT) {
            globalLiveRequestPauseActive = false;
            globalLiveRequestPauseStartedNanos = 0;
        } else {
            globalRawBytesPauseActive = false;
            globalRawBytesPauseStartedNanos = 0;
        }
    }

    private void activateConnectionPauseLocked(ConnectionState state, AdmissionPauseReason reason) {
        long startedNanos = clock.nanoseconds();
        PauseLease pauseLease = state.requestChannel.pauseChannel(state.channel, reason);
        if (reason == AdmissionPauseReason.LIVE_REQUEST_COUNT) {
            state.connectionLiveRequestPauseLease = pauseLease;
            state.connectionLiveRequestPauseStartedNanos = startedNanos;
            liveRequestPauseEvents = saturatingIncrement(liveRequestPauseEvents);
        } else {
            state.connectionRawBytesPauseLease = pauseLease;
            state.connectionRawBytesPauseStartedNanos = startedNanos;
            rawBytesPauseEvents = saturatingIncrement(rawBytesPauseEvents);
        }
    }

    private void deactivateConnectionPauseLocked(
            ConnectionState state,
            AdmissionPauseReason reason,
            List<PauseLease> pauseLeasesToClose) {
        long stoppedNanos = clock.nanoseconds();
        if (reason == AdmissionPauseReason.LIVE_REQUEST_COUNT) {
            PauseLease pauseLease = state.connectionLiveRequestPauseLease;
            if (pauseLease == null) {
                return;
            }
            state.connectionLiveRequestPauseLease = null;
            cumulativeLiveRequestPauseNanos =
                    saturatingAdd(
                            cumulativeLiveRequestPauseNanos,
                            elapsedNanos(
                                    state.connectionLiveRequestPauseStartedNanos, stoppedNanos));
            state.connectionLiveRequestPauseStartedNanos = 0;
            pauseLeasesToClose.add(pauseLease);
        } else {
            PauseLease pauseLease = state.connectionRawBytesPauseLease;
            if (pauseLease == null) {
                return;
            }
            state.connectionRawBytesPauseLease = null;
            cumulativeRawBytesPauseNanos =
                    saturatingAdd(
                            cumulativeRawBytesPauseNanos,
                            elapsedNanos(state.connectionRawBytesPauseStartedNanos, stoppedNanos));
            state.connectionRawBytesPauseStartedNanos = 0;
            pauseLeasesToClose.add(pauseLease);
        }
    }

    private static void rollbackConnectionPauseLocked(
            ConnectionState state,
            AdmissionPauseReason reason,
            List<PauseLease> pauseLeasesToClose) {
        if (reason == AdmissionPauseReason.LIVE_REQUEST_COUNT) {
            PauseLease pauseLease = state.connectionLiveRequestPauseLease;
            state.connectionLiveRequestPauseLease = null;
            state.connectionLiveRequestPauseStartedNanos = 0;
            pauseLeasesToClose.add(pauseLease);
        } else {
            PauseLease pauseLease = state.connectionRawBytesPauseLease;
            state.connectionRawBytesPauseLease = null;
            state.connectionRawBytesPauseStartedNanos = 0;
            pauseLeasesToClose.add(pauseLease);
        }
    }

    private void reconcileGlobalLiveRequestPauseLocked(List<PauseLease> pauseLeasesToClose) {
        if (globalLiveRequestPauseActive) {
            if (liveRequests <= liveRequestResumeThreshold) {
                deactivateGlobalPauseLocked(
                        AdmissionPauseReason.LIVE_REQUEST_COUNT, pauseLeasesToClose);
            }
        } else if (liveRequests >= maxLiveRequests) {
            activateGlobalPauseLocked(AdmissionPauseReason.LIVE_REQUEST_COUNT, pauseLeasesToClose);
        }
    }

    private void reconcileGlobalRawBytesPauseLocked(List<PauseLease> pauseLeasesToClose) {
        if (globalRawBytesPauseActive) {
            if (rawBytes <= rawBytesResumeThreshold) {
                deactivateGlobalPauseLocked(AdmissionPauseReason.RAW_BYTES, pauseLeasesToClose);
            }
        } else if (rawBytes >= maxRawBytes) {
            activateGlobalPauseLocked(AdmissionPauseReason.RAW_BYTES, pauseLeasesToClose);
        }
    }

    private void reconcileConnectionLiveRequestPauseLocked(
            ConnectionState state, List<PauseLease> pauseLeasesToClose) {
        if (state.connectionLiveRequestPauseLease != null) {
            if (state.liveRequests <= connectionLiveRequestResumeThreshold) {
                deactivateConnectionPauseLocked(
                        state, AdmissionPauseReason.LIVE_REQUEST_COUNT, pauseLeasesToClose);
            }
        } else if (state.liveRequests >= maxLiveRequestsPerConnection) {
            activateConnectionPauseLocked(state, AdmissionPauseReason.LIVE_REQUEST_COUNT);
        }
    }

    private void reconcileConnectionRawBytesPauseLocked(
            ConnectionState state, List<PauseLease> pauseLeasesToClose) {
        if (state.connectionRawBytesPauseLease != null) {
            if (state.rawBytes <= connectionRawBytesResumeThreshold) {
                deactivateConnectionPauseLocked(
                        state, AdmissionPauseReason.RAW_BYTES, pauseLeasesToClose);
            }
        } else if (state.rawBytes >= maxRawBytesPerConnection) {
            activateConnectionPauseLocked(state, AdmissionPauseReason.RAW_BYTES);
        }
    }

    private void recordGlobalOvershootLocked() {
        long globalLiveOvershoot = overshoot(liveRequests, maxLiveRequests);
        if (globalLiveOvershoot > 0) {
            liveRequestOvershootEvents = saturatingIncrement(liveRequestOvershootEvents);
        }
        maxLiveRequestOvershoot = Math.max(maxLiveRequestOvershoot, globalLiveOvershoot);

        long globalRawOvershoot = overshoot(rawBytes, maxRawBytes);
        if (globalRawOvershoot > 0) {
            rawBytesOvershootEvents = saturatingIncrement(rawBytesOvershootEvents);
        }
        maxRawBytesOvershoot = Math.max(maxRawBytesOvershoot, globalRawOvershoot);
    }

    private void recordConnectionOvershootLocked(ConnectionState state) {
        long connectionLiveOvershoot = overshoot(state.liveRequests, maxLiveRequestsPerConnection);
        if (connectionLiveOvershoot > 0) {
            liveRequestOvershootEvents = saturatingIncrement(liveRequestOvershootEvents);
        }
        maxLiveRequestOvershoot = Math.max(maxLiveRequestOvershoot, connectionLiveOvershoot);

        long connectionRawOvershoot = overshoot(state.rawBytes, maxRawBytesPerConnection);
        if (connectionRawOvershoot > 0) {
            rawBytesOvershootEvents = saturatingIncrement(rawBytesOvershootEvents);
        }
        maxRawBytesOvershoot = Math.max(maxRawBytesOvershoot, connectionRawOvershoot);
    }

    private List<PauseLease> detachAllPauseLeasesLocked(ConnectionState state) {
        long stoppedNanos = clock.nanoseconds();
        List<PauseLease> pauseLeases = new ArrayList<>(4);
        pauseLeases.add(state.globalLiveRequestPauseLease);
        pauseLeases.add(state.globalRawBytesPauseLease);
        pauseLeases.add(state.connectionLiveRequestPauseLease);
        pauseLeases.add(state.connectionRawBytesPauseLease);
        if (state.connectionLiveRequestPauseLease != null) {
            cumulativeLiveRequestPauseNanos =
                    saturatingAdd(
                            cumulativeLiveRequestPauseNanos,
                            elapsedNanos(
                                    state.connectionLiveRequestPauseStartedNanos, stoppedNanos));
        }
        if (state.connectionRawBytesPauseLease != null) {
            cumulativeRawBytesPauseNanos =
                    saturatingAdd(
                            cumulativeRawBytesPauseNanos,
                            elapsedNanos(state.connectionRawBytesPauseStartedNanos, stoppedNanos));
        }
        state.globalLiveRequestPauseLease = null;
        state.globalRawBytesPauseLease = null;
        state.connectionLiveRequestPauseLease = null;
        state.connectionRawBytesPauseLease = null;
        state.connectionLiveRequestPauseStartedNanos = 0;
        state.connectionRawBytesPauseStartedNanos = 0;
        return pauseLeases;
    }

    private long currentLiveRequestPauseNanosLocked() {
        long nowNanos = clock.nanoseconds();
        long total = cumulativeLiveRequestPauseNanos;
        if (globalLiveRequestPauseActive) {
            total =
                    saturatingAdd(
                            total, elapsedNanos(globalLiveRequestPauseStartedNanos, nowNanos));
        }
        for (ConnectionState state : connections.values()) {
            if (state.connectionLiveRequestPauseLease != null) {
                total =
                        saturatingAdd(
                                total,
                                elapsedNanos(
                                        state.connectionLiveRequestPauseStartedNanos, nowNanos));
            }
        }
        return total;
    }

    private long currentRawBytesPauseNanosLocked() {
        long nowNanos = clock.nanoseconds();
        long total = cumulativeRawBytesPauseNanos;
        if (globalRawBytesPauseActive) {
            total = saturatingAdd(total, elapsedNanos(globalRawBytesPauseStartedNanos, nowNanos));
        }
        for (ConnectionState state : connections.values()) {
            if (state.connectionRawBytesPauseLease != null) {
                total =
                        saturatingAdd(
                                total,
                                elapsedNanos(state.connectionRawBytesPauseStartedNanos, nowNanos));
            }
        }
        return total;
    }

    private static boolean hasAdmissionPauseLease(ConnectionState state) {
        return state.globalLiveRequestPauseLease != null
                || state.globalRawBytesPauseLease != null
                || state.connectionLiveRequestPauseLease != null
                || state.connectionRawBytesPauseLease != null;
    }

    private static PauseLease globalPauseLease(ConnectionState state, AdmissionPauseReason reason) {
        return reason == AdmissionPauseReason.LIVE_REQUEST_COUNT
                ? state.globalLiveRequestPauseLease
                : state.globalRawBytesPauseLease;
    }

    private static void setGlobalPauseLease(
            ConnectionState state, AdmissionPauseReason reason, PauseLease pauseLease) {
        if (reason == AdmissionPauseReason.LIVE_REQUEST_COUNT) {
            state.globalLiveRequestPauseLease = pauseLease;
        } else {
            state.globalRawBytesPauseLease = pauseLease;
        }
    }

    private static void closePauseLease(PauseLease pauseLease) {
        if (pauseLease != null) {
            pauseLease.close();
        }
    }

    private static void closePauseLeasesAfterOperation(
            List<PauseLease> pauseLeases, RuntimeException runtimeFailure, Error errorFailure) {
        try {
            closePauseLeases(pauseLeases);
        } catch (RuntimeException | Error cleanupFailure) {
            if (runtimeFailure != null) {
                runtimeFailure.addSuppressed(cleanupFailure);
            } else if (errorFailure != null) {
                errorFailure.addSuppressed(cleanupFailure);
            } else {
                throw cleanupFailure;
            }
        }
        if (runtimeFailure != null) {
            throw runtimeFailure;
        }
        if (errorFailure != null) {
            throw errorFailure;
        }
    }

    private static void closePauseLeases(List<PauseLease> pauseLeases) {
        RuntimeException runtimeFailure = null;
        Error errorFailure = null;
        for (PauseLease pauseLease : pauseLeases) {
            try {
                closePauseLease(pauseLease);
            } catch (RuntimeException failure) {
                if (runtimeFailure == null && errorFailure == null) {
                    runtimeFailure = failure;
                } else if (runtimeFailure != null) {
                    runtimeFailure.addSuppressed(failure);
                } else {
                    errorFailure.addSuppressed(failure);
                }
            } catch (Error failure) {
                if (runtimeFailure == null && errorFailure == null) {
                    errorFailure = failure;
                } else if (runtimeFailure != null) {
                    runtimeFailure.addSuppressed(failure);
                } else {
                    errorFailure.addSuppressed(failure);
                }
            }
        }
        if (runtimeFailure != null) {
            throw runtimeFailure;
        }
        if (errorFailure != null) {
            throw errorFailure;
        }
    }

    private static long overshoot(long value, long limit) {
        return value > limit ? value - limit : 0;
    }

    private static long saturatingIncrement(long value) {
        return value == Long.MAX_VALUE ? Long.MAX_VALUE : value + 1;
    }

    private static long saturatingAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    private static long elapsedNanos(long startedNanos, long currentNanos) {
        long elapsed = currentNanos - startedNanos;
        return elapsed < 0 ? 0 : elapsed;
    }

    private static final class ConnectionState {
        private final Channel channel;
        private final RequestChannel requestChannel;
        private boolean registered = true;
        private long liveRequests;
        private long liveBytes;
        private long rawBytes;
        private ReservationImpl pendingReservation;
        private long connectionLiveRequestPauseStartedNanos;
        private long connectionRawBytesPauseStartedNanos;
        private PauseLease globalLiveRequestPauseLease;
        private PauseLease globalRawBytesPauseLease;
        private PauseLease connectionLiveRequestPauseLease;
        private PauseLease connectionRawBytesPauseLease;

        private ConnectionState(Channel channel, RequestChannel requestChannel) {
            this.channel = channel;
            this.requestChannel = requestChannel;
        }
    }

    private enum ReservationState {
        WAITING,
        GRANTED,
        CANCELLED,
        REJECTED
    }

    private final class ReservationImpl implements Reservation {
        private final ConnectionState connectionState;
        private final long frameBytes;
        private final CompletableFuture<RequestLease> future = new CompletableFuture<>();
        private final AtomicReference<ReservationState> state =
                new AtomicReference<>(ReservationState.WAITING);

        private ReservationImpl(ConnectionState connectionState, long frameBytes) {
            this.connectionState = connectionState;
            this.frameBytes = frameBytes;
            future.whenComplete(
                    (ignored, failure) -> {
                        if (future.isCancelled()) {
                            cancelReservation(this);
                        }
                    });
        }

        @Override
        public CompletableFuture<RequestLease> getFuture() {
            return future;
        }

        @Override
        public boolean cancel() {
            return cancelReservation(this);
        }
    }

    private static final class ReservationCompletion {
        private final ReservationImpl reservation;
        private final RequestLease requestLease;
        private final Throwable failure;

        private ReservationCompletion(
                ReservationImpl reservation, RequestLease requestLease, Throwable failure) {
            this.reservation = reservation;
            this.requestLease = requestLease;
            this.failure = failure;
        }

        private static ReservationCompletion granted(
                ReservationImpl reservation, RequestLease requestLease) {
            return new ReservationCompletion(reservation, requestLease, null);
        }

        private static ReservationCompletion failed(
                ReservationImpl reservation, Throwable failure) {
            return new ReservationCompletion(reservation, null, failure);
        }
    }

    private final class RequestLeaseImpl implements RequestLease {
        private final ConnectionState state;
        private final long requestBytes;
        private final boolean hardReservation;
        private long rawBytes;
        private long grownRawBytes;
        private boolean rawReleased;
        private boolean liveReleased;

        private RequestLeaseImpl(
                ConnectionState state, long requestBytes, boolean hardReservation) {
            this.state = state;
            this.requestBytes = requestBytes;
            this.hardReservation = hardReservation;
            this.rawBytes = requestBytes;
        }

        @Override
        public void growFrameBytes(long additionalBytes) {
            checkArgument(additionalBytes > 0, "additionalBytes must be positive");
            List<PauseLease> pauseLeases = new ArrayList<>();
            RuntimeException runtimeFailure = null;
            Error errorFailure = null;
            synchronized (lock) {
                try {
                    growRawLocked(this, additionalBytes, pauseLeases);
                } catch (RuntimeException | Error failure) {
                    if (failure instanceof RuntimeException) {
                        runtimeFailure = (RuntimeException) failure;
                    } else {
                        errorFailure = (Error) failure;
                    }
                }
            }
            closePauseLeasesAfterOperation(pauseLeases, runtimeFailure, errorFailure);
        }

        @Override
        public void releaseGrownFrameBytes(long additionalBytes) {
            checkArgument(additionalBytes >= 0, "additionalBytes must not be negative");
            if (additionalBytes == 0) {
                return;
            }
            List<ReservationCompletion> completions = new ArrayList<>();
            List<PauseLease> pauseLeases = new ArrayList<>();
            RuntimeException runtimeFailure = null;
            Error errorFailure = null;
            synchronized (lock) {
                try {
                    releaseGrownRawLocked(this, additionalBytes, pauseLeases, completions);
                } catch (RuntimeException | Error failure) {
                    if (failure instanceof RuntimeException) {
                        runtimeFailure = (RuntimeException) failure;
                    } else {
                        errorFailure = (Error) failure;
                    }
                }
            }
            completeReservations(completions);
            closePauseLeasesAfterOperation(pauseLeases, runtimeFailure, errorFailure);
        }

        @Override
        public void releaseRaw() {
            List<ReservationCompletion> completions = new ArrayList<>();
            List<PauseLease> pauseLeases = new ArrayList<>();
            RuntimeException runtimeFailure = null;
            Error errorFailure = null;
            synchronized (lock) {
                try {
                    if (hardReservation) {
                        releaseHardRawLocked(this, completions);
                    } else {
                        releaseRawLocked(this, pauseLeases);
                    }
                } catch (RuntimeException | Error failure) {
                    if (failure instanceof RuntimeException) {
                        runtimeFailure = (RuntimeException) failure;
                    } else {
                        errorFailure = (Error) failure;
                    }
                }
            }
            completeReservations(completions);
            closePauseLeasesAfterOperation(pauseLeases, runtimeFailure, errorFailure);
        }

        @Override
        public void releaseLive() {
            List<ReservationCompletion> completions = new ArrayList<>();
            List<PauseLease> pauseLeases = new ArrayList<>();
            RuntimeException runtimeFailure = null;
            Error errorFailure = null;
            synchronized (lock) {
                try {
                    if (hardReservation) {
                        releaseHardLiveLocked(this, completions);
                    } else {
                        releaseLiveLocked(this, pauseLeases);
                    }
                } catch (RuntimeException | Error failure) {
                    if (failure instanceof RuntimeException) {
                        runtimeFailure = (RuntimeException) failure;
                    } else {
                        errorFailure = (Error) failure;
                    }
                }
            }
            completeReservations(completions);
            closePauseLeasesAfterOperation(pauseLeases, runtimeFailure, errorFailure);
        }

        @Override
        public void close() {
            List<ReservationCompletion> completions = new ArrayList<>();
            List<PauseLease> pauseLeases = new ArrayList<>();
            RuntimeException runtimeFailure = null;
            Error errorFailure = null;
            synchronized (lock) {
                try {
                    if (hardReservation) {
                        releaseHardRawLocked(this, completions);
                        releaseHardLiveLocked(this, completions);
                    } else {
                        releaseRawLocked(this, pauseLeases);
                        releaseLiveLocked(this, pauseLeases);
                    }
                } catch (RuntimeException | Error failure) {
                    if (failure instanceof RuntimeException) {
                        runtimeFailure = (RuntimeException) failure;
                    } else {
                        errorFailure = (Error) failure;
                    }
                }
            }
            completeReservations(completions);
            closePauseLeasesAfterOperation(pauseLeases, runtimeFailure, errorFailure);
        }
    }
}
