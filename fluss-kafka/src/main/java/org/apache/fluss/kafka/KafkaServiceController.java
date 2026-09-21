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
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;

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

    int drainingConnections() {
        synchronized (lock) {
            return (int) connections.stream().filter(connection -> !connection.accepting).count();
        }
    }

    void setEnabled(boolean newEnabled) {
        List<Connection> draining;
        synchronized (lock) {
            if (closed || enabled == newEnabled) {
                return;
            }
            enabled = newEnabled;
            draining = newEnabled ? new ArrayList<>() : new ArrayList<>(connections);
            // Permanently retire these connections before reopening can race with this drain.
            draining.forEach(connection -> connection.accepting = false);
        }
        LOG.info(
                "Kafka service enabled={}, draining {} existing connections.",
                newEnabled,
                draining.size());
        draining.forEach(Connection::drain);
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

    /** One connection generation; once drained, it can never resume serving requests. */
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
                if (accepting) {
                    connections.add(this);
                }
            }
            if (accepting) {
                super.channelActive(ctx);
            } else {
                ctx.close();
            }
        }

        boolean isServing() {
            return enabled && accepting;
        }

        boolean startRequest() {
            synchronized (lock) {
                if (!isServing()) {
                    return false;
                }
                requests.incrementAndGet();
                return true;
            }
        }

        void finishRequest() {
            if (requests.decrementAndGet() == 0 && !accepting) {
                channel.close();
            }
        }

        private void drain() {
            try {
                channel.eventLoop()
                        .execute(
                                () -> {
                                    if (!channel.isActive()) {
                                        return;
                                    }
                                    channel.config().setAutoRead(false);
                                    if (requests.get() == 0) {
                                        channel.close();
                                    } else if (drainTask == null) {
                                        drainTask =
                                                channel.eventLoop()
                                                        .schedule(
                                                                () -> channel.close(),
                                                                drainTimeout.toMillis(),
                                                                TimeUnit.MILLISECONDS);
                                    }
                                });
            } catch (RuntimeException failure) {
                channel.close();
                LOG.warn(
                        "Unable to schedule Kafka connection drain; disconnected immediately.",
                        failure);
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
            if (isServing()) {
                super.channelRead(ctx, message);
            } else {
                ReferenceCountUtil.release(message);
            }
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
