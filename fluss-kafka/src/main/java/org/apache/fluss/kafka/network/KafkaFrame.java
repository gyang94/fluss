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
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCounted;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A complete Kafka request frame and its pre-frame admission ownership.
 *
 * <p>The content starts at the Kafka API key and excludes the four-byte length prefix, matching the
 * input previously produced by Netty's length-field decoder. Releasing the final buffer reference
 * also closes an admission lease that has not been transferred. A consumer that attaches the lease
 * to a longer-lived request must call {@link #takeAdmissionLease()} exactly once.
 */
@Internal
public final class KafkaFrame implements ReferenceCounted {
    private final ByteBuf content;
    private final short apiKey;
    private final long wireBytes;
    private final AtomicReference<KafkaFrameAdmissionLease> admissionLease;

    KafkaFrame(
            ByteBuf content,
            short apiKey,
            long wireBytes,
            KafkaFrameAdmissionLease admissionLease) {
        this.content = checkNotNull(content, "content");
        this.apiKey = apiKey;
        this.wireBytes = wireBytes;
        this.admissionLease = new AtomicReference<>(checkNotNull(admissionLease, "admissionLease"));
    }

    /** Returns the frame content excluding the four-byte length prefix. */
    public ByteBuf content() {
        return content;
    }

    /** Returns the API key observed in the six-byte probe. */
    public short apiKey() {
        return apiKey;
    }

    /** Returns the complete wire size including the four-byte length prefix. */
    public long wireBytes() {
        return wireBytes;
    }

    /**
     * Transfers admission ownership out of this frame.
     *
     * @return the admission lease now owned by the caller
     * @throws IllegalStateException if ownership was already transferred or released
     */
    public KafkaFrameAdmissionLease takeAdmissionLease() {
        KafkaFrameAdmissionLease lease = admissionLease.getAndSet(null);
        if (lease == null) {
            throw new IllegalStateException("Kafka frame admission lease is no longer owned");
        }
        return lease;
    }

    @Override
    public int refCnt() {
        return content.refCnt();
    }

    @Override
    public KafkaFrame retain() {
        content.retain();
        return this;
    }

    @Override
    public KafkaFrame retain(int increment) {
        content.retain(increment);
        return this;
    }

    @Override
    public KafkaFrame touch() {
        content.touch();
        return this;
    }

    @Override
    public KafkaFrame touch(Object hint) {
        content.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        boolean released = content.release();
        if (released) {
            closeAdmissionLease();
        }
        return released;
    }

    @Override
    public boolean release(int decrement) {
        boolean released = content.release(decrement);
        if (released) {
            closeAdmissionLease();
        }
        return released;
    }

    private void closeAdmissionLease() {
        KafkaFrameAdmissionLease lease = admissionLease.getAndSet(null);
        if (lease != null) {
            lease.close();
        }
    }
}
