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

/**
 * Ownership of a granted Kafka frame admission reservation.
 *
 * <p>Implementations must make {@link #close()} idempotent. The lease initially belongs to the
 * frame decoder, is transferred to {@link KafkaFrame}, and may subsequently be taken by the Kafka
 * request lifecycle. Closing the last owner releases all reservation resources.
 */
@Internal
public interface KafkaFrameAdmissionLease extends AutoCloseable {

    /**
     * Atomically adds memory retained while this frame's raw ownership is held.
     *
     * <p>The additional bytes protect allocations derived from the encoded frame, such as copied or
     * decompressed Produce records. Implementations with hard admission limits must either account
     * all {@code additionalBytes} globally and for the owning connection, or fail without changing
     * accounting. This operation must not change request-lifetime live-byte accounting.
     *
     * @throws IllegalArgumentException if this frame and the additional bytes can never fit its
     *     configured raw-byte limits
     * @throws java.util.concurrent.RejectedExecutionException if aggregate raw-byte capacity is
     *     currently unavailable
     * @throws IllegalStateException if raw ownership has already been released
     */
    default void growFrameBytes(long additionalBytes) {}

    /**
     * Rolls back bytes previously added by {@link #growFrameBytes(long)}.
     *
     * <p>This operation is idempotent after raw ownership has been released. Otherwise the caller
     * must not release more bytes than it previously grew.
     */
    default void releaseGrownFrameBytes(long additionalBytes) {}

    /**
     * Releases the reservation for the encoded network frame after no owner can reference its
     * bytes. Other request-lifetime resources remain reserved until {@link #close()}.
     */
    default void releaseFrameBytes() {}

    /** Releases request-lifetime capacity after processing and network ownership both end. */
    default void releaseRequest() {
        close();
    }

    /** Releases the admission reservation. */
    @Override
    void close();
}
