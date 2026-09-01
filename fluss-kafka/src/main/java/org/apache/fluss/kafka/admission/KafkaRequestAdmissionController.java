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
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.kafka.network.KafkaFrameAdmission;
import org.apache.fluss.kafka.network.KafkaFrameAdmissionLease;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.kafka.common.protocol.ApiKeys;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * TabletServer-local admission coordinator for all Kafka request frames.
 *
 * <p>Produce and control-plane frames have independent exact-capacity lanes so Produce pressure
 * cannot consume the resources reserved for metadata and administrative requests. Every accepted
 * Kafka connection is registered lazily with a lane when its first frame for that lane is probed.
 * This keeps lane connection metrics accurate and prevents pressure in an unused lane from pausing
 * the connection. A control-plane frame-size limit rejects oversized non-Produce frames before a
 * control-lane handle is created or the body is read. A separate global connection limit protects
 * the six-byte pre-frame probe and connection-scoped Netty resources, which are not represented by
 * either lane's request accounting.
 *
 * <p>Global connection admission and each lazy lane registration are all-or-nothing. Closing a
 * connection releases every lane handle it actually created and the global slot exactly once;
 * already granted request leases retain their accounting until their owners release them.
 */
@Internal
@ThreadSafe
public final class KafkaRequestAdmissionController implements AutoCloseable {

    private static final long MIN_KAFKA_FRAME_BYTES = 6L;
    private static final Runnable NO_OP_REJECTION_LISTENER = () -> {};

    private final Object lock = new Object();
    private final KafkaProduceAdmissionController produceAdmissionController;
    private final KafkaProduceAdmissionController controlAdmissionController;
    private final int maxConnections;
    private final long maxControlFrameBytes;
    private final Runnable connectionRejectedListener;
    private final Map<Channel, ConnectionRegistration> connections = new HashMap<>();

    private long connectionRejections;
    private boolean closed;

    /**
     * Creates an admission coordinator without a connection-rejection callback.
     *
     * @param produceAdmissionController exact-capacity lane for Produce frames
     * @param controlAdmissionController exact-capacity lane for all non-Produce frames
     * @param maxConnections maximum number of concurrently registered Kafka connections
     */
    public KafkaRequestAdmissionController(
            KafkaProduceAdmissionController produceAdmissionController,
            KafkaProduceAdmissionController controlAdmissionController,
            int maxConnections) {
        this(
                produceAdmissionController,
                controlAdmissionController,
                maxConnections,
                Long.MAX_VALUE,
                NO_OP_REJECTION_LISTENER);
    }

    /**
     * Creates an admission coordinator with an independent control-plane frame-size limit.
     *
     * @param produceAdmissionController exact-capacity lane for Produce frames
     * @param controlAdmissionController exact-capacity lane for all non-Produce frames
     * @param maxConnections maximum number of concurrently registered Kafka connections
     * @param maxControlFrameBytes maximum complete wire size of a non-Produce frame
     */
    public KafkaRequestAdmissionController(
            KafkaProduceAdmissionController produceAdmissionController,
            KafkaProduceAdmissionController controlAdmissionController,
            int maxConnections,
            long maxControlFrameBytes) {
        this(
                produceAdmissionController,
                controlAdmissionController,
                maxConnections,
                maxControlFrameBytes,
                NO_OP_REJECTION_LISTENER);
    }

    /**
     * Creates an admission coordinator with a callback for connection-limit rejections.
     *
     * <p>The callback is invoked outside the coordinator lock. It is intended for metric accounting
     * and must not perform connection registration itself.
     *
     * @param produceAdmissionController exact-capacity lane for Produce frames
     * @param controlAdmissionController exact-capacity lane for all non-Produce frames
     * @param maxConnections maximum number of concurrently registered Kafka connections
     * @param connectionRejectedListener callback invoked for each connection-limit rejection
     */
    public KafkaRequestAdmissionController(
            KafkaProduceAdmissionController produceAdmissionController,
            KafkaProduceAdmissionController controlAdmissionController,
            int maxConnections,
            Runnable connectionRejectedListener) {
        this(
                produceAdmissionController,
                controlAdmissionController,
                maxConnections,
                Long.MAX_VALUE,
                connectionRejectedListener);
    }

    /**
     * Creates an admission coordinator with control-plane frame and connection limits.
     *
     * <p>The callback is invoked outside the coordinator lock. It is intended for metric accounting
     * and must not perform connection registration itself.
     *
     * @param produceAdmissionController exact-capacity lane for Produce frames
     * @param controlAdmissionController exact-capacity lane for all non-Produce frames
     * @param maxConnections maximum number of concurrently registered Kafka connections
     * @param maxControlFrameBytes maximum complete wire size of a non-Produce frame
     * @param connectionRejectedListener callback invoked for each connection-limit rejection
     */
    public KafkaRequestAdmissionController(
            KafkaProduceAdmissionController produceAdmissionController,
            KafkaProduceAdmissionController controlAdmissionController,
            int maxConnections,
            long maxControlFrameBytes,
            Runnable connectionRejectedListener) {
        checkArgument(maxConnections > 0, "maxConnections must be greater than 0");
        checkArgument(
                maxControlFrameBytes >= MIN_KAFKA_FRAME_BYTES,
                "maxControlFrameBytes must be at least %s",
                MIN_KAFKA_FRAME_BYTES);
        this.produceAdmissionController =
                checkNotNull(produceAdmissionController, "produceAdmissionController");
        this.controlAdmissionController =
                checkNotNull(controlAdmissionController, "controlAdmissionController");
        checkArgument(
                produceAdmissionController != controlAdmissionController,
                "Produce and control admission must use independent controllers");
        this.maxConnections = maxConnections;
        this.maxControlFrameBytes = maxControlFrameBytes;
        this.connectionRejectedListener =
                checkNotNull(connectionRejectedListener, "connectionRejectedListener");
    }

    /**
     * Creates the admission endpoint for one Kafka connection.
     *
     * <p>The returned endpoint acquires the global connection slot when its {@link
     * KafkaFrameAdmission#channelActive(Channel)} callback runs. Each lane is registered lazily
     * when the endpoint sees its first frame for that lane.
     */
    public KafkaFrameAdmission createConnectionAdmission(RequestChannel requestChannel) {
        return new ConnectionAdmission(checkNotNull(requestChannel, "requestChannel"));
    }

    /** Returns the exact-capacity Produce request lane. */
    public KafkaProduceAdmissionController produceAdmissionController() {
        return produceAdmissionController;
    }

    /** Returns the independent exact-capacity control-plane request lane. */
    public KafkaProduceAdmissionController controlAdmissionController() {
        return controlAdmissionController;
    }

    /** Returns the number of accepted Kafka connections, including registrations in progress. */
    public int connections() {
        synchronized (lock) {
            return connections.size();
        }
    }

    /** Returns the configured TabletServer-local Kafka connection limit. */
    public int connectionLimit() {
        return maxConnections;
    }

    /** Returns the maximum complete wire size accepted for a non-Produce frame. */
    public long controlFrameLimit() {
        return maxControlFrameBytes;
    }

    /** Returns the cumulative number of connections rejected by the connection limit. */
    public long connectionRejections() {
        synchronized (lock) {
            return connectionRejections;
        }
    }

    /**
     * Stops connection admission and closes every registered connection handle.
     *
     * <p>The registration map is atomically detached before handles are closed so no new connection
     * can enter while shutdown is in progress. Closing a handle cancels its waiting reservations,
     * but already granted request leases deliberately retain their raw/live accounting until their
     * actual owners release them. Every detached registration is given a best-effort close even if
     * another registration fails.
     */
    @Override
    public void close() {
        List<ConnectionRegistration> registrations;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            registrations = new ArrayList<>(connections.values());
            connections.clear();
        }

        Throwable failure = null;
        for (ConnectionRegistration registration : registrations) {
            try {
                registration.close();
            } catch (Throwable closeFailure) {
                failure = ExceptionUtils.firstOrSuppressed(closeFailure, failure);
            }
        }
        if (failure != null) {
            ExceptionUtils.rethrow(failure);
        }
    }

    private ConnectionRegistration registerConnection(Channel channel) {
        ConnectionRegistration registration = new ConnectionRegistration(channel);
        boolean rejected = false;
        synchronized (lock) {
            if (closed) {
                throw new RejectedExecutionException(
                        "Kafka request admission controller is closed");
            }
            checkState(
                    !connections.containsKey(channel),
                    "Kafka request admission connection is already registered");
            if (connections.size() >= maxConnections) {
                connectionRejections = saturatingIncrement(connectionRejections);
                rejected = true;
            } else {
                connections.put(channel, registration);
            }
        }

        if (rejected) {
            RejectedExecutionException rejection =
                    new RejectedExecutionException(
                            "Kafka connection limit is reached on this TabletServer");
            notifyConnectionRejected(rejection);
            throw rejection;
        }

        return registration;
    }

    private void unregisterConnection(ConnectionRegistration registration) {
        synchronized (lock) {
            if (!connections.remove(registration.channel, registration)) {
                return;
            }
        }
        registration.close();
    }

    private void notifyConnectionRejected(RejectedExecutionException rejection) {
        try {
            connectionRejectedListener.run();
        } catch (RuntimeException | Error callbackFailure) {
            rejection.addSuppressed(callbackFailure);
        }
    }

    private static void closeHandles(
            @Nullable ConnectionHandle first, @Nullable ConnectionHandle second) {
        RuntimeException runtimeFailure = null;
        Error errorFailure = null;
        try {
            closeHandle(first);
        } catch (RuntimeException failure) {
            runtimeFailure = failure;
        } catch (Error failure) {
            errorFailure = failure;
        }
        try {
            closeHandle(second);
        } catch (RuntimeException failure) {
            if (runtimeFailure != null) {
                runtimeFailure.addSuppressed(failure);
            } else if (errorFailure != null) {
                errorFailure.addSuppressed(failure);
            } else {
                runtimeFailure = failure;
            }
        } catch (Error failure) {
            if (runtimeFailure != null) {
                runtimeFailure.addSuppressed(failure);
            } else if (errorFailure != null) {
                errorFailure.addSuppressed(failure);
            } else {
                errorFailure = failure;
            }
        }
        if (runtimeFailure != null) {
            throw runtimeFailure;
        }
        if (errorFailure != null) {
            throw errorFailure;
        }
    }

    private static void closeHandle(@Nullable ConnectionHandle handle) {
        if (handle != null) {
            handle.close();
        }
    }

    private static long saturatingIncrement(long value) {
        return value == Long.MAX_VALUE ? Long.MAX_VALUE : value + 1L;
    }

    private final class ConnectionAdmission implements KafkaFrameAdmission {
        private final RequestChannel requestChannel;

        private @Nullable Channel channel;
        private @Nullable ConnectionRegistration registration;
        private boolean closed;

        private ConnectionAdmission(RequestChannel requestChannel) {
            this.requestChannel = requestChannel;
        }

        @Override
        public synchronized void channelActive(Channel activeChannel) {
            checkState(!closed, "Kafka request admission is closed");
            checkState(channel == null, "Kafka request admission is already active");
            channel = checkNotNull(activeChannel, "channel");
            try {
                registration = registerConnection(activeChannel);
            } catch (RuntimeException | Error failure) {
                channel = null;
                closed = true;
                throw failure;
            }
        }

        @Override
        public synchronized Reservation reserve(
                Channel activeChannel, long frameBytes, short apiKey) {
            checkState(!closed, "Kafka request admission is closed");
            checkState(channel == activeChannel, "Kafka request admission channel does not match");
            ConnectionRegistration activeRegistration =
                    checkNotNull(registration, "Kafka request admission is not active");
            KafkaProduceAdmissionController.Reservation reservation =
                    activeRegistration.reserve(requestChannel, frameBytes, apiKey);
            return new FrameReservation(reservation);
        }

        @Override
        public void close() {
            ConnectionRegistration registrationToClose;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                channel = null;
                registrationToClose = registration;
                registration = null;
            }
            if (registrationToClose != null) {
                unregisterConnection(registrationToClose);
            }
        }
    }

    private final class ConnectionRegistration {
        private final Channel channel;

        private @Nullable ConnectionHandle produceHandle;
        private @Nullable ConnectionHandle controlHandle;
        private boolean closed;

        private ConnectionRegistration(Channel channel) {
            this.channel = channel;
        }

        private synchronized KafkaProduceAdmissionController.Reservation reserve(
                RequestChannel requestChannel, long frameBytes, short apiKey) {
            checkState(!closed, "Kafka connection registration is closed");
            ConnectionHandle handle;
            if (apiKey == ApiKeys.PRODUCE.id) {
                if (produceHandle == null) {
                    produceHandle =
                            produceAdmissionController.registerConnection(channel, requestChannel);
                }
                handle = produceHandle;
            } else {
                if (frameBytes > maxControlFrameBytes) {
                    throw new RejectedExecutionException(
                            "Kafka control-plane frame size "
                                    + frameBytes
                                    + " exceeds the TabletServer limit "
                                    + maxControlFrameBytes);
                }
                if (controlHandle == null) {
                    controlHandle =
                            controlAdmissionController.registerConnection(channel, requestChannel);
                }
                handle = controlHandle;
            }
            return handle.reserve(frameBytes);
        }

        private void close() {
            ConnectionHandle produceHandleToClose;
            ConnectionHandle controlHandleToClose;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                produceHandleToClose = produceHandle;
                controlHandleToClose = controlHandle;
                produceHandle = null;
                controlHandle = null;
            }
            closeHandles(controlHandleToClose, produceHandleToClose);
        }
    }

    private static final class FrameReservation implements KafkaFrameAdmission.Reservation {
        private final KafkaProduceAdmissionController.Reservation delegate;

        private FrameReservation(KafkaProduceAdmissionController.Reservation delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<? extends KafkaFrameAdmissionLease> getFuture() {
            return delegate.getFuture();
        }

        @Override
        public boolean cancel() {
            return delegate.cancel();
        }
    }
}
