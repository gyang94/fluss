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

import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.util.AttributeKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Controls Kafka service admission without changing listeners or shared transport resources. */
@ThreadSafe
final class KafkaServiceController implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaServiceController.class);
    private static final AttributeKey<Connection> CONNECTION =
            AttributeKey.valueOf("fluss.kafka.service.connection");

    private final Object lock = new Object();
    private final Set<Connection> connections = new HashSet<>();
    private final Duration drainTimeout;
    private volatile boolean enabled;
    private boolean closed;

    KafkaServiceController(boolean enabled, Duration drainTimeout) {
        this.enabled = enabled;
        this.drainTimeout = drainTimeout;
    }

    boolean isEnabled() {
        return enabled;
    }

    int retiredConnections() {
        synchronized (lock) {
            return (int) connections.stream().filter(connection -> !connection.accepting).count();
        }
    }

    void setEnabled(boolean newEnabled) {
        int retired;
        synchronized (lock) {
            if (closed || enabled == newEnabled) {
                return;
            }
            enabled = newEnabled;
            retired = newEnabled ? 0 : connections.size();
            if (!newEnabled) {
                connections.forEach(connection -> connection.accepting = false);
            }
        }
        LOG.info(
                "Kafka service enabled={}, retiring {} existing connections.", newEnabled, retired);
    }

    ChannelInboundHandlerAdapter newConnection() {
        return new Connection();
    }

    static @Nullable Connection connection(Channel channel) {
        return channel.attr(CONNECTION).get();
    }

    @Override
    public void close() {
        List<Connection> snapshot;
        synchronized (lock) {
            closed = true;
            enabled = false;
            snapshot = new ArrayList<>(connections);
            snapshot.forEach(connection -> connection.accepting = false);
        }
        snapshot.forEach(connection -> connection.channel.close());
    }

    /** One connection generation; once retired, it can never resume serving requests. */
    final class Connection extends ChannelInboundHandlerAdapter {
        private final AtomicInteger requests = new AtomicInteger();
        private volatile boolean accepting = true;
        private Channel channel;
        // Accessed only on the channel event loop.
        private @Nullable ScheduledFuture<?> drainTask;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            channel = ctx.channel();
            channel.attr(CONNECTION).set(this);
            synchronized (lock) {
                accepting = enabled && !closed;
                connections.add(this);
            }
            super.channelActive(ctx);
        }

        boolean isServing() {
            return enabled && accepting;
        }

        boolean startRequest() {
            synchronized (lock) {
                if (closed || !channel.isActive()) {
                    return false;
                }
                requests.incrementAndGet();
                return true;
            }
        }

        int pendingRequests() {
            return requests.get();
        }

        void finishRequest() {
            requests.decrementAndGet();
        }

        // A protocol request which has no legal error response closes in FIFO order. Bound the
        // wait for earlier responses without cancelling their native acknowledgement futures.
        void closeAfterPendingResponses() {
            channel.eventLoop()
                    .execute(
                            () -> {
                                if (drainTask == null && channel.isActive()) {
                                    drainTask =
                                            channel.eventLoop()
                                                    .schedule(
                                                            () -> channel.close(),
                                                            drainTimeout.toMillis(),
                                                            TimeUnit.MILLISECONDS);
                                }
                            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            accepting = false;
            synchronized (lock) {
                connections.remove(this);
            }
            if (drainTask != null) {
                drainTask.cancel(false);
            }
            super.channelInactive(ctx);
        }
    }
}
