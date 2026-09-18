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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A queue channel that can receive requests and send responses.
 *
 * <p>Uses an unbounded LinkedBlockingQueue to ensure that putRequest() never blocks, preventing
 * EventLoop threads from being blocked. Backpressure is applied at the TCP level by pausing channel
 * reads when the queue size exceeds the backpressure threshold.
 *
 * <p>Each RequestChannel instance manages its own associated Netty channels (those hashed to this
 * RequestChannel). Each associated channel has a pause controller that arbitrates queue pressure
 * and other resource-specific pause reasons, so one component cannot resume reads while another
 * reason is still active.
 */
@ThreadSafe
public class RequestChannel {
    private static final Logger LOG = LoggerFactory.getLogger(RequestChannel.class);

    /**
     * A stable token that identifies why a channel's inbound reads are paused.
     *
     * <p>Implementations must keep {@link #equals(Object)} and {@link #hashCode()} stable while a
     * lease is active. Enums are recommended.
     */
    public interface PauseReason {

        /** Returns the stable, human-readable name of this reason. */
        String name();
    }

    /** Pause reasons owned by the protocol-independent request channel. */
    public enum BuiltInPauseReason implements PauseReason {
        QUEUE_COUNT
    }

    /** A reference-counted pause lease. Closing the lease more than once has no effect. */
    public interface PauseLease extends AutoCloseable {

        @Override
        void close();
    }

    /** Unbounded blocking queue to hold incoming requests. Never blocks on put. */
    protected final BlockingQueue<RpcRequest> requestQueue;

    /**
     * The threshold at which backpressure should be applied (pausing channel reads). When queue
     * size exceeds this value, channels should be paused to prevent memory exhaustion.
     */
    private final int backpressureThreshold;

    /**
     * The threshold at which to resume paused channels. Set to 50% of backpressureThreshold to
     * provide hysteresis and avoid thrashing.
     */
    private final int resumeThreshold;

    /**
     * All Netty channels that are hashed to this RequestChannel. Channels are registered when they
     * become active and unregistered when they become inactive.
     *
     * <p>Queue pressure applies one pause lease to every controller. Other pause reasons remain
     * isolated to their own channel.
     */
    private final Map<Channel, ChannelPauseController> associatedChannels = new HashMap<>();

    /** The queue-pressure lease owned for each currently registered channel. */
    private final Map<Channel, PauseLease> queuePauseLeases = new HashMap<>();

    /**
     * Indicates whether queue-count backpressure is currently active. Other reasons are tracked by
     * each channel's pause controller.
     *
     * <p>Volatile ensures visibility for fast-path reads (outside the lock). All modifications are
     * protected by pauseLock, so atomicity is guaranteed by the lock, not by atomic operations.
     */
    private volatile boolean isQueueBackpressureActive = false;

    /**
     * Lock protecting channel registration, pause reason reference counts, queue-pressure state,
     * and task submissions.
     *
     * <p>The lock eliminates the need for CAS operations - simple boolean checks and assignments
     * under the lock are sufficient for correctness.
     */
    private final ReentrantLock pauseLock = new ReentrantLock();

    public RequestChannel(int backpressureThreshold) {
        this.requestQueue = new LinkedBlockingQueue<>();
        this.backpressureThreshold = Math.max(1, backpressureThreshold);
        this.resumeThreshold = this.backpressureThreshold / 2;
    }

    /**
     * Send a request to be handled. Since this uses an unbounded queue, this method never blocks,
     * ensuring EventLoop threads are never blocked by queue operations.
     *
     * <p>After adding the request, automatically checks if backpressure should be applied. If the
     * queue size exceeds the backpressure threshold, ALL channels associated with this
     * RequestChannel will be paused to prevent further memory growth.
     *
     * <p>The high-watermark check is performed after every enqueue. This closes the race where a
     * concurrent low-watermark transition could release queue pressure after an enqueue observed
     * the old active state.
     */
    public void putRequest(RpcRequest request) {
        requestQueue.add(request);
        reconcileBackpressure();
    }

    /**
     * Sends a shutdown request to the channel. This can allow request processor gracefully
     * shutdown.
     */
    public void putShutdownRequest() {
        putRequest(ShutdownRequest.INSTANCE);
    }

    /**
     * Get the next request, waiting up to the specified timeout if the queue is empty. After
     * successfully polling a request, attempts to resume paused channels if the queue size has
     * dropped below the resume threshold.
     *
     * @return the head of this queue, or null if the specified waiting time elapses before an
     *     element is available.
     */
    public RpcRequest pollRequest(long timeoutMs) {
        try {
            RpcRequest request = requestQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (request != null) {
                reconcileBackpressure();
            }
            return request;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Get the number of requests in the queue. */
    public int requestsCount() {
        return requestQueue.size();
    }

    /**
     * Registers a Netty channel as being associated with this RequestChannel. This is called when a
     * channel becomes active and is hashed to this RequestChannel.
     *
     * <p>If queue pressure is already active, the newly registered channel immediately inherits a
     * queue pause lease. Re-registering the same channel is idempotent.
     *
     * @param channel the channel to register
     */
    public void registerChannel(Channel channel) {
        checkNotNull(channel, "channel");
        pauseLock.lock();
        try {
            if (associatedChannels.containsKey(channel)) {
                return;
            }

            ChannelPauseController pauseController = new ChannelPauseController(channel);
            associatedChannels.put(channel, pauseController);
            if (isQueueBackpressureActive) {
                queuePauseLeases.put(
                        channel,
                        acquirePauseLeaseLocked(pauseController, BuiltInPauseReason.QUEUE_COUNT));
            } else {
                scheduleChannelStateReconciliation(pauseController);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Registered channel {} to RequestChannel (backpressure threshold: {}, associated channels: {}, queue backpressure active: {})",
                        channel.remoteAddress(),
                        backpressureThreshold,
                        associatedChannels.size(),
                        isQueueBackpressureActive);
            }
        } finally {
            pauseLock.unlock();
        }
    }

    /**
     * Unregisters a Netty channel from this RequestChannel. This is called when a channel becomes
     * inactive.
     *
     * @param channel the channel to unregister
     */
    public void unregisterChannel(Channel channel) {
        checkNotNull(channel, "channel");
        pauseLock.lock();
        try {
            ChannelPauseController pauseController = associatedChannels.remove(channel);
            queuePauseLeases.remove(channel);
            if (pauseController == null) {
                return;
            }
            pauseController.registered = false;
            pauseController.activeReasonRefCounts.clear();

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Unregistered channel {} from RequestChannel (associated channels: {}, queue backpressure active: {})",
                        channel.remoteAddress(),
                        associatedChannels.size(),
                        isQueueBackpressureActive);
            }
        } finally {
            pauseLock.unlock();
        }
    }

    /**
     * Pauses reads for one registered channel for the supplied reason.
     *
     * <p>Each call owns one reference. The channel resumes only after every lease for every reason
     * has been closed. The returned lease is thread-safe and idempotent.
     *
     * @param channel the registered channel to pause
     * @param reason the reason token owned by the caller
     * @return a lease that releases exactly this pause reference
     * @throws IllegalStateException if the channel is not registered
     */
    public PauseLease pauseChannel(Channel channel, PauseReason reason) {
        checkNotNull(channel, "channel");
        checkNotNull(reason, "reason");
        pauseLock.lock();
        try {
            ChannelPauseController pauseController = associatedChannels.get(channel);
            if (pauseController == null) {
                throw new IllegalStateException(
                        "Channel is not registered with this RequestChannel");
            }
            return acquirePauseLeaseLocked(pauseController, reason);
        } finally {
            pauseLock.unlock();
        }
    }

    /** Returns whether the registered channel has at least one active pause reason. */
    public boolean isChannelPaused(Channel channel) {
        checkNotNull(channel, "channel");
        pauseLock.lock();
        try {
            ChannelPauseController pauseController = associatedChannels.get(channel);
            return pauseController != null && !pauseController.activeReasonRefCounts.isEmpty();
        } finally {
            pauseLock.unlock();
        }
    }

    /** Returns a snapshot of the registered channel's active pause reasons. */
    public Set<PauseReason> activePauseReasons(Channel channel) {
        checkNotNull(channel, "channel");
        pauseLock.lock();
        try {
            ChannelPauseController pauseController = associatedChannels.get(channel);
            if (pauseController == null) {
                return Collections.emptySet();
            }
            return Collections.unmodifiableSet(
                    new HashSet<>(pauseController.activeReasonRefCounts.keySet()));
        } finally {
            pauseLock.unlock();
        }
    }

    /** Reconciles queue pressure while preserving independent channel pause reasons. */
    private void reconcileBackpressure() {
        int queueSize = requestQueue.size();
        boolean backpressureActive = isQueueBackpressureActive;
        if (backpressureActive ? queueSize > resumeThreshold : queueSize < backpressureThreshold) {
            return;
        }

        pauseLock.lock();
        try {
            while (true) {
                queueSize = requestQueue.size();
                if (isQueueBackpressureActive) {
                    if (queueSize > resumeThreshold) {
                        return;
                    }
                    isQueueBackpressureActive = false;
                    for (PauseLease queuePauseLease : queuePauseLeases.values()) {
                        queuePauseLease.close();
                    }
                    queuePauseLeases.clear();
                    LOG.info(
                            "Queue size ({}) dropped to resume threshold ({}), released queue pause leases.",
                            queueSize,
                            resumeThreshold);
                } else {
                    if (queueSize < backpressureThreshold) {
                        return;
                    }
                    isQueueBackpressureActive = true;
                    for (ChannelPauseController pauseController : associatedChannels.values()) {
                        queuePauseLeases.put(
                                pauseController.channel,
                                acquirePauseLeaseLocked(
                                        pauseController, BuiltInPauseReason.QUEUE_COUNT));
                    }
                    LOG.warn(
                            "Queue size ({}) reached backpressure threshold ({}), activated queue pause for {} channels.",
                            queueSize,
                            backpressureThreshold,
                            associatedChannels.size());
                }
            }
        } finally {
            pauseLock.unlock();
        }
    }

    private PauseLease acquirePauseLeaseLocked(
            ChannelPauseController pauseController, PauseReason reason) {
        boolean wasPaused = !pauseController.activeReasonRefCounts.isEmpty();
        Integer currentRefCount = pauseController.activeReasonRefCounts.get(reason);
        if (currentRefCount == null) {
            pauseController.activeReasonRefCounts.put(reason, 1);
        } else {
            if (currentRefCount == Integer.MAX_VALUE) {
                throw new IllegalStateException(
                        "Pause reason reference count overflow: " + reason.name());
            }
            pauseController.activeReasonRefCounts.put(reason, currentRefCount + 1);
        }
        if (!wasPaused) {
            applyPauseImmediatelyOrSchedule(pauseController);
        }
        return new RefCountedPauseLease(pauseController, reason);
    }

    private void releasePauseLease(RefCountedPauseLease pauseLease) {
        if (!pauseLease.closed.compareAndSet(false, true)) {
            return;
        }

        pauseLock.lock();
        try {
            ChannelPauseController pauseController = pauseLease.pauseController;
            if (!pauseController.registered) {
                return;
            }
            Integer currentRefCount = pauseController.activeReasonRefCounts.get(pauseLease.reason);
            if (currentRefCount == null) {
                return;
            }
            if (currentRefCount == 1) {
                pauseController.activeReasonRefCounts.remove(pauseLease.reason);
            } else {
                pauseController.activeReasonRefCounts.put(pauseLease.reason, currentRefCount - 1);
            }
            if (pauseController.activeReasonRefCounts.isEmpty()) {
                // Enabling auto-read can synchronously issue a channel read and re-enter protocol
                // handlers. Always defer resume until the lease owner's state transition has
                // returned; pause acquisition remains synchronous on the event loop so additional
                // frames from the current socket read cannot pass the flow controller.
                scheduleChannelStateReconciliation(pauseController);
            }
        } finally {
            pauseLock.unlock();
        }
    }

    private void applyPauseImmediatelyOrSchedule(ChannelPauseController pauseController) {
        Channel channel = pauseController.channel;
        if (channel.eventLoop().inEventLoop()) {
            // Admission is commonly activated while decoding a request on this event loop. Apply
            // the pause before channelRead returns so FlowControlHandler cannot deliver additional
            // frames from the same socket read after the high watermark has been reached.
            try {
                reconcileChannelState(pauseController);
            } catch (RuntimeException e) {
                // putRequest has already transferred the request to the worker queue. A channel
                // configuration failure must not turn that successful transfer into an apparent
                // enqueue failure and let the decoder release the worker-owned buffer.
                LOG.debug("Unable to reconcile auto-read for channel {}.", channel, e);
            }
            return;
        }
        scheduleChannelStateReconciliation(pauseController);
    }

    private void scheduleChannelStateReconciliation(ChannelPauseController pauseController) {
        Channel channel = pauseController.channel;
        try {
            channel.eventLoop().execute(() -> reconcileChannelState(pauseController));
        } catch (RuntimeException e) {
            LOG.debug("Unable to schedule auto-read reconciliation for channel {}.", channel, e);
        }
    }

    private void reconcileChannelState(ChannelPauseController pauseController) {
        pauseLock.lock();
        try {
            Channel channel = pauseController.channel;
            if (!pauseController.registered
                    || associatedChannels.get(channel) != pauseController
                    || !channel.isActive()) {
                return;
            }

            boolean shouldAutoRead = pauseController.activeReasonRefCounts.isEmpty();
            if (channel.config().isAutoRead() == shouldAutoRead) {
                return;
            }
            channel.config().setAutoRead(shouldAutoRead);
            if (shouldAutoRead) {
                LOG.info("Resumed channel {} after all pause reasons were released.", channel);
            } else {
                LOG.warn(
                        "Paused channel {} for reasons {}.",
                        channel,
                        pauseController.activeReasonRefCounts.keySet());
            }
        } finally {
            pauseLock.unlock();
        }
    }

    private static final class ChannelPauseController {
        private final Channel channel;
        private final Map<PauseReason, Integer> activeReasonRefCounts = new HashMap<>();
        private boolean registered = true;

        private ChannelPauseController(Channel channel) {
            this.channel = channel;
        }
    }

    private final class RefCountedPauseLease implements PauseLease {
        private final ChannelPauseController pauseController;
        private final PauseReason reason;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private RefCountedPauseLease(ChannelPauseController pauseController, PauseReason reason) {
            this.pauseController = pauseController;
            this.reason = reason;
        }

        @Override
        public void close() {
            releasePauseLease(this);
        }
    }
}
