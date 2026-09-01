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
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;

import java.util.concurrent.CompletableFuture;

/**
 * Reserves the resources required to read one Kafka frame from a socket.
 *
 * <p>The framing layer invokes this interface after it has read only the four-byte frame length and
 * the two-byte Kafka API key. Implementations must not complete a reservation successfully until
 * the complete frame can be admitted. A reservation that cannot be granted immediately may complete
 * its future asynchronously.
 */
@Internal
public interface KafkaFrameAdmission extends AutoCloseable {

    /** Activates this connection after downstream request-channel registration has completed. */
    default void channelActive(Channel channel) {}

    /**
     * Starts a reservation for a frame whose body has not yet been read.
     *
     * @param channel channel receiving the frame
     * @param frameBytes complete wire size, including the four-byte length field
     * @param apiKey Kafka API key read from the probe
     * @return cancellable asynchronous reservation
     */
    Reservation reserve(Channel channel, long frameBytes, short apiKey);

    /** Releases connection-scoped admission state. */
    @Override
    default void close() {}

    /** A pending or granted pre-frame reservation. */
    interface Reservation {

        /**
         * Returns the grant future. A successful completion transfers one admission lease to the
         * framing layer.
         */
        CompletableFuture<? extends KafkaFrameAdmissionLease> getFuture();

        /**
         * Attempts to cancel a reservation that is still waiting.
         *
         * @return whether cancellation won before the reservation was granted
         */
        boolean cancel();
    }
}
