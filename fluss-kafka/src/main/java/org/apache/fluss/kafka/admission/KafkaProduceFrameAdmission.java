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

import org.apache.kafka.common.protocol.ApiKeys;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Per-connection adapter from Kafka framing to strict Produce admission. */
@Internal
public final class KafkaProduceFrameAdmission implements KafkaFrameAdmission {
    private static final KafkaFrameAdmissionLease NO_OP_LEASE =
            new KafkaFrameAdmissionLease() {
                @Override
                public void releaseFrameBytes() {}

                @Override
                public void close() {}
            };

    private final RequestChannel requestChannel;
    private final @Nullable KafkaProduceAdmissionController controller;

    private @Nullable Channel channel;
    private @Nullable ConnectionHandle connectionHandle;
    private boolean closed;

    /** Creates a connection adapter with optional Produce admission. */
    public KafkaProduceFrameAdmission(
            RequestChannel requestChannel, @Nullable KafkaProduceAdmissionController controller) {
        this.requestChannel = checkNotNull(requestChannel, "requestChannel");
        this.controller = controller;
    }

    @Override
    public synchronized void channelActive(Channel activeChannel) {
        checkState(!closed, "Kafka frame admission is closed");
        checkState(channel == null, "Kafka frame admission is already active");
        channel = checkNotNull(activeChannel, "channel");
        if (controller != null) {
            connectionHandle = controller.registerConnection(activeChannel, requestChannel);
        }
    }

    @Override
    public synchronized Reservation reserve(Channel requestChannel, long frameBytes, short apiKey) {
        checkState(!closed, "Kafka frame admission is closed");
        checkState(channel == requestChannel, "Kafka frame admission channel does not match");
        ConnectionHandle handle = connectionHandle;
        if (apiKey != ApiKeys.PRODUCE.id || handle == null) {
            return completedReservation(NO_OP_LEASE);
        }
        return new ProduceReservation(handle.reserve(frameBytes));
    }

    @Override
    public void close() {
        ConnectionHandle handle;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            handle = connectionHandle;
            connectionHandle = null;
            channel = null;
        }
        if (handle != null) {
            handle.close();
        }
    }

    private static Reservation completedReservation(KafkaFrameAdmissionLease lease) {
        CompletableFuture<KafkaFrameAdmissionLease> future =
                CompletableFuture.completedFuture(lease);
        return new Reservation() {
            @Override
            public CompletableFuture<KafkaFrameAdmissionLease> getFuture() {
                return future;
            }

            @Override
            public boolean cancel() {
                return false;
            }
        };
    }

    private static final class ProduceReservation implements Reservation {
        private final KafkaProduceAdmissionController.Reservation delegate;

        private ProduceReservation(KafkaProduceAdmissionController.Reservation delegate) {
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
