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

/** Event-loop-owned read state shared by the frame decoder and receive allocator. */
final class KafkaFrameReadState {
    static final int PROBE_BYTES = 6;

    private enum Phase {
        PROBE,
        WAITING,
        BODY
    }

    private volatile Phase phase = Phase.PROBE;
    private volatile int remainingBytes = PROBE_BYTES;
    private volatile int wireFrameBytes = -1;

    void updateProbeBytes(int bufferedBytes) {
        phase = Phase.PROBE;
        remainingBytes = Math.max(0, PROBE_BYTES - bufferedBytes);
    }

    void waitForGrant(int frameBytes) {
        phase = Phase.WAITING;
        remainingBytes = 0;
        wireFrameBytes = frameBytes;
    }

    void readBody(int bodyBytes) {
        phase = Phase.BODY;
        remainingBytes = Math.max(0, bodyBytes);
    }

    void reset() {
        phase = Phase.PROBE;
        remainingBytes = PROBE_BYTES;
        wireFrameBytes = -1;
    }

    void stop() {
        phase = Phase.WAITING;
        remainingBytes = 0;
        wireFrameBytes = -1;
    }

    int allocationSize(int suggestedBytes) {
        int remaining = remainingBytes;
        if (phase == Phase.WAITING || remaining == 0) {
            throw new IllegalStateException(
                    "Kafka transport requested an allocation while frame reads are stopped");
        }
        int positiveSuggestion = Math.max(1, suggestedBytes);
        return Math.min(remaining, positiveSuggestion);
    }

    int cumulationCapacity(int readableBytes) {
        if (phase == Phase.BODY && wireFrameBytes > 0) {
            return wireFrameBytes;
        }
        return Math.max(PROBE_BYTES, readableBytes);
    }

    boolean canRead() {
        return phase != Phase.WAITING && remainingBytes > 0;
    }
}
