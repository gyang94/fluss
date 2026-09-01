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
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * TabletServer-local admission for converted Kafka Produce requests awaiting native completion.
 *
 * <p>Every waiting reservation owns its estimated-byte token before it enters the bounded queue.
 * Granting atomically transfers that token from pending bytes to converted bytes without changing
 * the total reserved bytes. Consequently, already materialized Produce commands cannot accumulate
 * outside the byte budget while waiting for request-count capacity. The caller must acquire the
 * lease before conversion, resize it to the exact sum of the produced native buffers, call {@link
 * RequestLease#tryMarkSubmitted()} immediately before submission, and retain it until the original
 * native Produce future reaches a terminal state.
 *
 * <p>Global pressure preserves FIFO ordering. A queue entry blocked only by its own connection may
 * be skipped so one saturated connection does not prevent other connections from progressing.
 * Capacity updates and lease releases are exact and never overshoot a configured limit.
 *
 * <p>The controller does not create threads or schedule deadlines. Callers schedule their own
 * admission deadline and invoke {@link Reservation#timeout()} if it expires. All reservation
 * futures are completed after the controller lock has been released.
 */
@Internal
@ThreadSafe
public final class KafkaNativeProduceAdmissionController implements AutoCloseable {

    /** A converted request cannot ever fit within the configured admission limits. */
    public static final class RequestTooLargeException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private final long requestedBytes;
        private final long byteLimit;

        private RequestTooLargeException(long requestedBytes, long byteLimit) {
            super(
                    String.format(
                            "Native Produce request requires %s bytes but its admission limit is %s bytes",
                            requestedBytes, byteLimit));
            this.requestedBytes = requestedBytes;
            this.byteLimit = byteLimit;
        }

        /** Returns the number of bytes requested by the operation. */
        public long requestedBytes() {
            return requestedBytes;
        }

        /** Returns the effective global or per-connection limit that was exceeded. */
        public long byteLimit() {
            return byteLimit;
        }
    }

    /** Capacity is not currently available, or the bounded admission queue cannot accept work. */
    public static class AdmissionUnavailableException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private AdmissionUnavailableException(String message) {
            super(message);
        }
    }

    /** A caller-owned admission deadline expired before a reservation was granted. */
    public static final class AdmissionTimeoutException extends AdmissionUnavailableException {

        private static final long serialVersionUID = 1L;

        private AdmissionTimeoutException() {
            super("Timed out waiting for native Produce admission");
        }
    }

    /** Ownership of one native request count and its converted bytes. */
    public interface RequestLease extends AutoCloseable {

        /** Returns the estimate used to acquire this lease. */
        long estimatedBytes();

        /** Returns the bytes currently charged to this lease. */
        long reservedBytes();

        /**
         * Atomically adjusts the byte reservation to the exact converted buffer size.
         *
         * <p>Shrinking always succeeds and may grant queued reservations. Growing succeeds only
         * when the additional bytes are immediately available. A request larger than either hard
         * limit throws {@link RequestTooLargeException}; temporary capacity pressure throws {@link
         * AdmissionUnavailableException}. A failed resize leaves the existing reservation
         * unchanged.
         */
        void resize(long actualBytes);

        /**
         * Atomically marks the boundary after which disconnect must not release this lease early.
         *
         * <p>The caller invokes this immediately before native submission. A {@code false} result
         * means connection closure won the race; the caller must discard converted buffers and
         * close the lease without submitting. A {@code true} result commits the caller to either
         * submit or handle a synchronous submit failure, and to close the lease only after that
         * operation reaches its real terminal state.
         */
        boolean tryMarkSubmitted();

        /** Releases the native request count and all currently reserved bytes. */
        @Override
        void close();
    }

    /** A cancellable request for one native request count and estimated bytes. */
    public interface Reservation {

        /** Returns the future that receives the admission lease. */
        CompletableFuture<RequestLease> getFuture();

        /**
         * Returns whether this reservation currently owns its estimated-byte token.
         *
         * <p>The value is fixed to {@code true} before an accepted reservation is returned to its
         * caller. It remains true through the pending-to-granted migration and becomes false only
         * after a pending reservation terminates and its cleanup has run.
         */
        boolean ownsByteReservation();

        /**
         * Cancels this reservation if it is still waiting.
         *
         * @return whether cancellation won before capacity was granted
         */
        boolean cancel();

        /**
         * Marks this reservation as timed out if it is still waiting.
         *
         * @return whether timeout won before capacity was granted
         */
        boolean timeout();
    }

    /** A stable connection-scoped view of native Produce admission. */
    public interface ConnectionHandle extends AutoCloseable {

        /** Reserves one native request and the supplied estimated converted bytes. */
        Reservation reserve(long estimatedBytes);

        /**
         * Reserves one native request and installs cleanup for copied payload protected by a
         * waiting byte token.
         *
         * <p>The cleanup runs synchronously under the admission lock immediately before a waiting
         * token is released because of cancellation, timeout, connection close, controller close,
         * or queue overflow. It must be constant-time, idempotent, and must not throw. It is not
         * invoked after the reservation is granted; the lease owner then releases its payload
         * before closing the granted lease.
         */
        Reservation reserve(long estimatedBytes, Runnable pendingTokenCleanup);

        /** Returns this connection's currently granted native request count. */
        long inFlightRequests();

        /** Returns this connection's currently granted converted bytes. */
        long convertedBytes();

        /** Returns this connection's estimated bytes reserved by waiting requests. */
        long pendingReservedBytes();

        /** Returns this connection's granted plus waiting reserved bytes. */
        long totalReservedBytes();

        /** Returns whether this connection no longer accepts reservations. */
        boolean isClosed();

        /**
         * Registers a callback that runs when this connection is closed.
         *
         * <p>The callback runs outside the admission lock and must not throw. A {@code false}
         * result means the connection was already closed and the caller must cancel its work.
         */
        boolean addCloseListener(Runnable closeListener);

        /** Removes a previously registered connection-close callback. */
        void removeCloseListener(Runnable closeListener);

        /**
         * Closes this handle and atomically reports whether it owned granted requests.
         *
         * <p>Only the invocation that transitions an open handle to closed can return {@code true}.
         * This allows the channel owner to record a disconnect-with-inflight event exactly once.
         *
         * @return whether this invocation closed an open handle with granted requests
         */
        boolean closeAndGetHadInFlightRequests();

        /**
         * Stops new reservations and cancels queued reservations.
         *
         * <p>Already granted leases retain their accounting until the caller closes them.
         */
        @Override
        void close();
    }

    private enum ReservationState {
        WAITING,
        GRANTED,
        TERMINAL
    }

    private final Object lock = new Object();
    private final long maxInFlightRequests;
    private final long maxConvertedBytes;
    private final long maxInFlightRequestsPerConnection;
    private final long maxConvertedBytesPerConnection;
    private final int maxPendingReservations;
    private final Runnable admissionMutationHookForTesting;
    private final Set<ConnectionState> connections = new HashSet<>();
    private final Deque<ReservationImpl> pendingReservations = new ArrayDeque<>();

    private long inFlightRequests;
    private long convertedBytes;
    private long pendingReservedBytes;
    private int registeredConnections;
    private boolean closed;

    /** Creates one native admission controller shared by all Kafka connections on a server. */
    public KafkaNativeProduceAdmissionController(
            long maxInFlightRequests,
            long maxConvertedBytes,
            long maxInFlightRequestsPerConnection,
            long maxConvertedBytesPerConnection,
            int maxPendingReservations) {
        this(
                maxInFlightRequests,
                maxConvertedBytes,
                maxInFlightRequestsPerConnection,
                maxConvertedBytesPerConnection,
                maxPendingReservations,
                null);
    }

    KafkaNativeProduceAdmissionController(
            long maxInFlightRequests,
            long maxConvertedBytes,
            long maxInFlightRequestsPerConnection,
            long maxConvertedBytesPerConnection,
            int maxPendingReservations,
            Runnable admissionMutationHookForTesting) {
        checkArgument(maxInFlightRequests > 0, "maxInFlightRequests must be greater than 0");
        checkArgument(maxConvertedBytes > 0, "maxConvertedBytes must be greater than 0");
        checkArgument(
                maxInFlightRequestsPerConnection > 0,
                "maxInFlightRequestsPerConnection must be greater than 0");
        checkArgument(
                maxConvertedBytesPerConnection > 0,
                "maxConvertedBytesPerConnection must be greater than 0");
        checkArgument(
                maxInFlightRequestsPerConnection <= maxInFlightRequests,
                "maxInFlightRequestsPerConnection must not exceed maxInFlightRequests");
        checkArgument(
                maxConvertedBytesPerConnection <= maxConvertedBytes,
                "maxConvertedBytesPerConnection must not exceed maxConvertedBytes");
        checkArgument(maxPendingReservations > 0, "maxPendingReservations must be greater than 0");
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxConvertedBytes = maxConvertedBytes;
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
        this.maxConvertedBytesPerConnection = maxConvertedBytesPerConnection;
        this.maxPendingReservations = maxPendingReservations;
        this.admissionMutationHookForTesting = admissionMutationHookForTesting;
    }

    /** Registers and returns a new connection-scoped admission handle. */
    public ConnectionHandle registerConnection() {
        synchronized (lock) {
            checkState(!closed, "Native Produce admission controller is closed");
            ConnectionState connection = new ConnectionState();
            connections.add(connection);
            registeredConnections++;
            return connection.handle;
        }
    }

    /** Returns the currently granted native request count. */
    public long inFlightRequests() {
        synchronized (lock) {
            return inFlightRequests;
        }
    }

    /** Returns the bytes currently owned by granted leases. */
    public long convertedBytes() {
        synchronized (lock) {
            return convertedBytes;
        }
    }

    /** Returns estimated bytes reserved by requests waiting for count capacity or FIFO order. */
    public long pendingReservedBytes() {
        synchronized (lock) {
            return pendingReservedBytes;
        }
    }

    /** Returns the bytes owned by granted leases and waiting reservations. */
    public long totalReservedBytes() {
        synchronized (lock) {
            return totalReservedBytesLocked();
        }
    }

    /** Returns the number of reservations waiting for count and byte capacity. */
    public int pendingReservations() {
        synchronized (lock) {
            return pendingReservations.size();
        }
    }

    /** Returns the number of open registered connections. */
    public int registeredConnections() {
        synchronized (lock) {
            return registeredConnections;
        }
    }

    /** Returns the largest current in-flight request count owned by one connection. */
    public long maxConnectionInFlightRequests() {
        synchronized (lock) {
            long maximum = 0;
            for (ConnectionState connection : connections) {
                maximum = Math.max(maximum, connection.inFlightRequests);
            }
            return maximum;
        }
    }

    /** Returns the largest current converted-byte reservation owned by one connection. */
    public long maxConnectionConvertedBytes() {
        synchronized (lock) {
            long maximum = 0;
            for (ConnectionState connection : connections) {
                maximum = Math.max(maximum, connection.convertedBytes);
            }
            return maximum;
        }
    }

    /** Returns the largest current pending-byte reservation owned by one connection. */
    public long maxConnectionPendingReservedBytes() {
        synchronized (lock) {
            long maximum = 0;
            for (ConnectionState connection : connections) {
                maximum = Math.max(maximum, connection.pendingReservedBytes);
            }
            return maximum;
        }
    }

    /** Returns the largest current total byte reservation owned by one connection. */
    public long maxConnectionTotalReservedBytes() {
        synchronized (lock) {
            long maximum = 0;
            for (ConnectionState connection : connections) {
                maximum = Math.max(maximum, totalReservedBytesLocked(connection));
            }
            return maximum;
        }
    }

    /** Returns the TabletServer-local native request limit. */
    public long maxInFlightRequestsLimit() {
        return maxInFlightRequests;
    }

    /** Returns the TabletServer-local converted-byte limit. */
    public long maxConvertedBytesLimit() {
        return maxConvertedBytes;
    }

    /** Returns the per-connection native request limit. */
    public long maxInFlightRequestsPerConnectionLimit() {
        return maxInFlightRequestsPerConnection;
    }

    /** Returns the per-connection converted-byte limit. */
    public long maxConvertedBytesPerConnectionLimit() {
        return maxConvertedBytesPerConnection;
    }

    /** Returns the configured upper bound on queued reservations. */
    public int maxPendingReservationsLimit() {
        return maxPendingReservations;
    }

    /** Returns whether this controller no longer accepts connections or reservations. */
    public boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    /**
     * Stops admission and cancels every queued reservation.
     *
     * <p>Granted leases are not released by controller shutdown. Their callers must retain them
     * until the original native Produce future reaches a terminal state and then close them.
     */
    @Override
    public void close() {
        List<FutureCompletion> completions = new ArrayList<>();
        List<Runnable> closeListeners = new ArrayList<>();
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            for (ConnectionState connection : connections) {
                if (!connection.closed) {
                    connection.closed = true;
                    registeredConnections--;
                }
                closeListeners.addAll(connection.closeListeners);
                connection.closeListeners.clear();
            }
            for (ReservationImpl reservation : pendingReservations) {
                reservation.state = ReservationState.TERMINAL;
                reservation.connection.pendingReservations--;
                releaseTerminatedPendingBytesLocked(reservation);
                completions.add(cancellationCompletion(reservation.future));
            }
            pendingReservations.clear();
            checkState(
                    pendingReservedBytes == 0,
                    "Closed native Produce controller retained pending reserved bytes");
            Iterator<ConnectionState> iterator = connections.iterator();
            while (iterator.hasNext()) {
                ConnectionState connection = iterator.next();
                if (connection.inFlightRequests == 0) {
                    checkState(
                            connection.convertedBytes == 0,
                            "Closed connection retained converted bytes without native requests");
                    checkState(
                            connection.pendingReservedBytes == 0,
                            "Closed connection retained bytes without pending reservations");
                    iterator.remove();
                }
            }
            checkState(registeredConnections == 0, "Native Produce connections remained open");
        }
        completeOutsideLock(completions);
        runCloseListeners(closeListeners);
    }

    private Reservation reserve(
            ConnectionState connection, long estimatedBytes, Runnable pendingTokenCleanup) {
        checkArgument(estimatedBytes >= 0, "estimatedBytes must not be negative");
        checkArgument(pendingTokenCleanup != null, "pendingTokenCleanup must not be null");
        List<FutureCompletion> completions = new ArrayList<>();
        ReservationImpl reservation =
                new ReservationImpl(connection, estimatedBytes, pendingTokenCleanup);
        synchronized (lock) {
            if (connection.closed) {
                rejectLocked(
                        reservation,
                        new AdmissionUnavailableException(
                                "Kafka connection is closed for native Produce admission"),
                        completions);
            } else {
                long effectiveLimit = Math.min(maxConvertedBytes, maxConvertedBytesPerConnection);
                if (estimatedBytes > effectiveLimit) {
                    rejectLocked(
                            reservation,
                            new RequestTooLargeException(estimatedBytes, effectiveLimit),
                            completions);
                } else if (!fits(totalReservedBytesLocked(), estimatedBytes, maxConvertedBytes)
                        || !fits(
                                totalReservedBytesLocked(connection),
                                estimatedBytes,
                                maxConvertedBytesPerConnection)) {
                    rejectLocked(
                            reservation,
                            new AdmissionUnavailableException(
                                    "Native Produce pending byte reservation is not currently available"),
                            completions);
                } else {
                    pendingReservations.addLast(reservation);
                    connection.pendingReservations++;
                    pendingReservedBytes += estimatedBytes;
                    connection.pendingReservedBytes += estimatedBytes;
                    drainPendingLocked(completions);
                    if (reservation.state == ReservationState.WAITING
                            && pendingReservations.size() > maxPendingReservations) {
                        checkState(
                                pendingReservations.remove(reservation),
                                "Overflowing native Produce reservation is missing from the queue");
                        reservation.state = ReservationState.TERMINAL;
                        connection.pendingReservations--;
                        releaseTerminatedPendingBytesLocked(reservation);
                        completions.add(
                                failureCompletion(
                                        reservation.future,
                                        new AdmissionUnavailableException(
                                                "Native Produce admission queue is full")));
                    }
                }
            }
        }
        completeOutsideLock(completions);
        return reservation;
    }

    private boolean terminateWaiting(ReservationImpl reservation, boolean timeout) {
        List<FutureCompletion> completions = new ArrayList<>();
        synchronized (lock) {
            if (reservation.state != ReservationState.WAITING) {
                return false;
            }
            checkState(
                    pendingReservations.remove(reservation),
                    "Waiting native Produce reservation is missing from the queue");
            reservation.state = ReservationState.TERMINAL;
            reservation.connection.pendingReservations--;
            releaseTerminatedPendingBytesLocked(reservation);
            if (timeout) {
                completions.add(
                        failureCompletion(reservation.future, new AdmissionTimeoutException()));
            } else {
                completions.add(cancellationCompletion(reservation.future));
            }
            drainPendingLocked(completions);
            cleanupConnectionLocked(reservation.connection);
        }
        completeOutsideLock(completions);
        return true;
    }

    private void resize(RequestLeaseImpl lease, long actualBytes) {
        checkArgument(actualBytes >= 0, "actualBytes must not be negative");
        List<FutureCompletion> completions = new ArrayList<>();
        synchronized (lock) {
            runAdmissionMutationHookForTesting();
            checkState(!lease.closed, "Native Produce admission lease is already closed");
            checkState(!lease.submitted, "Native Produce admission lease is already submitted");
            long currentBytes = lease.reservedBytes;
            if (actualBytes == currentBytes) {
                return;
            }
            if (actualBytes > currentBytes) {
                if (lease.connection.closed) {
                    throw new AdmissionUnavailableException(
                            "Kafka connection closed before native Produce conversion completed");
                }
                long effectiveLimit = Math.min(maxConvertedBytes, maxConvertedBytesPerConnection);
                if (actualBytes > effectiveLimit) {
                    throw new RequestTooLargeException(actualBytes, effectiveLimit);
                }
                long additionalBytes = actualBytes - currentBytes;
                if (!fits(totalReservedBytesLocked(), additionalBytes, maxConvertedBytes)
                        || !fits(
                                totalReservedBytesLocked(lease.connection),
                                additionalBytes,
                                maxConvertedBytesPerConnection)) {
                    throw new AdmissionUnavailableException(
                            "Additional native Produce bytes are not currently available");
                }
                convertedBytes += additionalBytes;
                lease.connection.convertedBytes += additionalBytes;
                lease.reservedBytes = actualBytes;
            } else {
                long releasedBytes = currentBytes - actualBytes;
                convertedBytes -= releasedBytes;
                lease.connection.convertedBytes -= releasedBytes;
                lease.reservedBytes = actualBytes;
                drainPendingLocked(completions);
            }
        }
        completeOutsideLock(completions);
    }

    private void closeLease(RequestLeaseImpl lease) {
        List<FutureCompletion> completions = new ArrayList<>();
        synchronized (lock) {
            runAdmissionMutationHookForTesting();
            if (lease.closed) {
                return;
            }
            lease.closed = true;
            releaseLocked(lease, completions);
        }
        completeOutsideLock(completions);
    }

    private void releaseLocked(RequestLeaseImpl lease, List<FutureCompletion> completions) {
        convertedBytes -= lease.reservedBytes;
        inFlightRequests--;
        lease.connection.convertedBytes -= lease.reservedBytes;
        lease.connection.inFlightRequests--;
        lease.reservedBytes = 0;
        checkState(convertedBytes >= 0, "Native Produce converted bytes became negative");
        checkState(inFlightRequests >= 0, "Native Produce request count became negative");
        checkState(
                lease.connection.convertedBytes >= 0,
                "Connection native Produce converted bytes became negative");
        checkState(
                lease.connection.inFlightRequests >= 0,
                "Connection native Produce request count became negative");
        drainPendingLocked(completions);
        cleanupConnectionLocked(lease.connection);
    }

    private boolean closeConnection(ConnectionState connection) {
        List<FutureCompletion> completions = new ArrayList<>();
        List<Runnable> closeListeners = new ArrayList<>();
        boolean hadInFlightRequests;
        synchronized (lock) {
            runAdmissionMutationHookForTesting();
            if (connection.closed) {
                return false;
            }
            hadInFlightRequests = connection.inFlightRequests > 0;
            connection.closed = true;
            registeredConnections--;
            closeListeners.addAll(connection.closeListeners);
            connection.closeListeners.clear();
            Iterator<ReservationImpl> iterator = pendingReservations.iterator();
            while (iterator.hasNext()) {
                ReservationImpl reservation = iterator.next();
                if (reservation.connection == connection) {
                    iterator.remove();
                    reservation.state = ReservationState.TERMINAL;
                    connection.pendingReservations--;
                    releaseTerminatedPendingBytesLocked(reservation);
                    completions.add(cancellationCompletion(reservation.future));
                }
            }
            drainPendingLocked(completions);
            cleanupConnectionLocked(connection);
        }
        completeOutsideLock(completions);
        runCloseListeners(closeListeners);
        return hadInFlightRequests;
    }

    private void drainPendingLocked(List<FutureCompletion> completions) {
        Set<ConnectionState> blockedConnections = new HashSet<>();
        Iterator<ReservationImpl> iterator = pendingReservations.iterator();
        while (iterator.hasNext()) {
            ReservationImpl reservation = iterator.next();
            ConnectionState connection = reservation.connection;
            if (connection.closed) {
                iterator.remove();
                reservation.state = ReservationState.TERMINAL;
                connection.pendingReservations--;
                releaseTerminatedPendingBytesLocked(reservation);
                completions.add(cancellationCompletion(reservation.future));
                continue;
            }

            if (blockedConnections.contains(connection)) {
                continue;
            }

            boolean blockedGlobally = !fits(inFlightRequests, 1, maxInFlightRequests);
            if (blockedGlobally) {
                return;
            }

            boolean blockedByConnection =
                    !fits(connection.inFlightRequests, 1, maxInFlightRequestsPerConnection);
            if (blockedByConnection) {
                blockedConnections.add(connection);
                continue;
            }

            iterator.remove();
            connection.pendingReservations--;
            releasePendingBytesLocked(reservation);
            inFlightRequests++;
            convertedBytes += reservation.estimatedBytes;
            connection.inFlightRequests++;
            connection.convertedBytes += reservation.estimatedBytes;
            reservation.state = ReservationState.GRANTED;
            RequestLeaseImpl lease = new RequestLeaseImpl(connection, reservation.estimatedBytes);
            completions.add(successCompletion(reservation.future, lease));
        }
    }

    private void rejectLocked(
            ReservationImpl reservation,
            RuntimeException failure,
            List<FutureCompletion> completions) {
        reservation.state = ReservationState.TERMINAL;
        completions.add(failureCompletion(reservation.future, failure));
    }

    private void cleanupConnectionLocked(ConnectionState connection) {
        if (connection.closed
                && connection.pendingReservations == 0
                && connection.inFlightRequests == 0) {
            checkState(
                    connection.convertedBytes == 0,
                    "Closed connection retained converted bytes without native requests");
            checkState(
                    connection.pendingReservedBytes == 0,
                    "Closed connection retained bytes without pending reservations");
            connections.remove(connection);
        }
    }

    private void releasePendingBytesLocked(ReservationImpl reservation) {
        pendingReservedBytes -= reservation.estimatedBytes;
        reservation.connection.pendingReservedBytes -= reservation.estimatedBytes;
        checkState(
                pendingReservedBytes >= 0, "Native Produce pending reserved bytes became negative");
        checkState(
                reservation.connection.pendingReservedBytes >= 0,
                "Connection native Produce pending reserved bytes became negative");
    }

    private void releaseTerminatedPendingBytesLocked(ReservationImpl reservation) {
        reservation.pendingTokenCleanup.run();
        releasePendingBytesLocked(reservation);
    }

    private long totalReservedBytesLocked() {
        checkState(
                convertedBytes <= maxConvertedBytes - pendingReservedBytes,
                "Native Produce total reserved bytes exceeded its limit");
        return convertedBytes + pendingReservedBytes;
    }

    private long totalReservedBytesLocked(ConnectionState connection) {
        checkState(
                connection.convertedBytes
                        <= maxConvertedBytesPerConnection - connection.pendingReservedBytes,
                "Connection native Produce total reserved bytes exceeded its limit");
        return connection.convertedBytes + connection.pendingReservedBytes;
    }

    private static boolean fits(long current, long additional, long limit) {
        return current >= 0 && additional >= 0 && current <= limit && additional <= limit - current;
    }

    private void runAdmissionMutationHookForTesting() {
        if (admissionMutationHookForTesting != null) {
            admissionMutationHookForTesting.run();
        }
    }

    private void completeOutsideLock(List<FutureCompletion> completions) {
        for (FutureCompletion completion : completions) {
            completion.complete();
        }
    }

    private static void runCloseListeners(List<Runnable> closeListeners) {
        Throwable failure = null;
        for (Runnable closeListener : closeListeners) {
            try {
                closeListener.run();
            } catch (Throwable closeFailure) {
                failure = ExceptionUtils.firstOrSuppressed(closeFailure, failure);
            }
        }
        if (failure != null) {
            ExceptionUtils.rethrow(failure);
        }
    }

    private final class ConnectionState {
        private final ConnectionHandle handle = new ConnectionHandleImpl(this);
        private final Set<Runnable> closeListeners = new HashSet<>();
        private boolean closed;
        private int pendingReservations;
        private long inFlightRequests;
        private long convertedBytes;
        private long pendingReservedBytes;
    }

    private final class ConnectionHandleImpl implements ConnectionHandle {
        private final ConnectionState connection;

        private ConnectionHandleImpl(ConnectionState connection) {
            this.connection = connection;
        }

        @Override
        public Reservation reserve(long estimatedBytes) {
            return reserve(estimatedBytes, () -> {});
        }

        @Override
        public Reservation reserve(long estimatedBytes, Runnable pendingTokenCleanup) {
            return KafkaNativeProduceAdmissionController.this.reserve(
                    connection, estimatedBytes, pendingTokenCleanup);
        }

        @Override
        public long inFlightRequests() {
            synchronized (lock) {
                return connection.inFlightRequests;
            }
        }

        @Override
        public long convertedBytes() {
            synchronized (lock) {
                return connection.convertedBytes;
            }
        }

        @Override
        public long pendingReservedBytes() {
            synchronized (lock) {
                return connection.pendingReservedBytes;
            }
        }

        @Override
        public long totalReservedBytes() {
            synchronized (lock) {
                return totalReservedBytesLocked(connection);
            }
        }

        @Override
        public boolean isClosed() {
            synchronized (lock) {
                return connection.closed;
            }
        }

        @Override
        public boolean addCloseListener(Runnable closeListener) {
            checkArgument(closeListener != null, "closeListener must not be null");
            synchronized (lock) {
                return !connection.closed && connection.closeListeners.add(closeListener);
            }
        }

        @Override
        public void removeCloseListener(Runnable closeListener) {
            checkArgument(closeListener != null, "closeListener must not be null");
            synchronized (lock) {
                connection.closeListeners.remove(closeListener);
            }
        }

        @Override
        public void close() {
            closeConnection(connection);
        }

        @Override
        public boolean closeAndGetHadInFlightRequests() {
            return closeConnection(connection);
        }
    }

    private final class ReservationImpl implements Reservation {
        private final ConnectionState connection;
        private final long estimatedBytes;
        private final Runnable pendingTokenCleanup;
        private final ReservationFuture future;
        private ReservationState state = ReservationState.WAITING;

        private ReservationImpl(
                ConnectionState connection, long estimatedBytes, Runnable pendingTokenCleanup) {
            this.connection = connection;
            this.estimatedBytes = estimatedBytes;
            this.pendingTokenCleanup = pendingTokenCleanup;
            this.future = new ReservationFuture(this);
        }

        @Override
        public CompletableFuture<RequestLease> getFuture() {
            return future;
        }

        @Override
        public boolean ownsByteReservation() {
            synchronized (lock) {
                return state == ReservationState.WAITING || state == ReservationState.GRANTED;
            }
        }

        @Override
        public boolean cancel() {
            return terminateWaiting(this, false);
        }

        @Override
        public boolean timeout() {
            return terminateWaiting(this, true);
        }
    }

    private final class ReservationFuture extends CompletableFuture<RequestLease> {
        private final ReservationImpl reservation;

        private ReservationFuture(ReservationImpl reservation) {
            this.reservation = reservation;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return terminateWaiting(reservation, false);
        }

        @Override
        public boolean complete(RequestLease value) {
            return false;
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            return false;
        }

        @Override
        public void obtrudeValue(RequestLease value) {
            throw new UnsupportedOperationException(
                    "Native Produce admission futures are controller-owned");
        }

        @Override
        public void obtrudeException(Throwable failure) {
            throw new UnsupportedOperationException(
                    "Native Produce admission futures are controller-owned");
        }

        private boolean completeGrant(RequestLease lease) {
            return super.complete(lease);
        }

        private boolean completeFailure(Throwable failure) {
            return super.completeExceptionally(failure);
        }

        private boolean completeCancellation() {
            return super.cancel(false);
        }
    }

    private final class RequestLeaseImpl implements RequestLease {
        private final ConnectionState connection;
        private final long estimatedBytes;
        private long reservedBytes;
        private boolean submitted;
        private boolean closed;

        private RequestLeaseImpl(ConnectionState connection, long estimatedBytes) {
            this.connection = connection;
            this.estimatedBytes = estimatedBytes;
            this.reservedBytes = estimatedBytes;
        }

        @Override
        public long estimatedBytes() {
            return estimatedBytes;
        }

        @Override
        public long reservedBytes() {
            synchronized (lock) {
                return reservedBytes;
            }
        }

        @Override
        public void resize(long actualBytes) {
            KafkaNativeProduceAdmissionController.this.resize(this, actualBytes);
        }

        @Override
        public boolean tryMarkSubmitted() {
            synchronized (lock) {
                runAdmissionMutationHookForTesting();
                if (closed) {
                    return false;
                }
                if (submitted) {
                    return true;
                }
                if (connection.closed) {
                    return false;
                }
                submitted = true;
                return true;
            }
        }

        @Override
        public void close() {
            closeLease(this);
        }
    }

    private FutureCompletion successCompletion(ReservationFuture future, RequestLease lease) {
        return new FutureCompletion() {
            @Override
            void complete() {
                if (!future.completeGrant(lease)) {
                    lease.close();
                }
            }
        };
    }

    private FutureCompletion failureCompletion(ReservationFuture future, Throwable failure) {
        return new FutureCompletion() {
            @Override
            void complete() {
                future.completeFailure(failure);
            }
        };
    }

    private FutureCompletion cancellationCompletion(ReservationFuture future) {
        return new FutureCompletion() {
            @Override
            void complete() {
                future.completeCancellation();
            }
        };
    }

    private abstract class FutureCompletion {
        abstract void complete();
    }
}
