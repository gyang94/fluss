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

/** Metrics callback for the Kafka pre-frame admission and bounded body-read lifecycle. */
@Internal
public interface KafkaFrameAdmissionMetrics {

    /** Returns a monotonic timestamp used as the start of an admission or read stage. */
    long nowNanos();

    /** Records a reservation rejected before the frame body was read. */
    void recordReservationRejected();

    /** Records a reservation cancelled while it was still waiting for admission. */
    void recordReservationCancelled();

    /** Records successful completion of pre-frame admission waiting. */
    void recordPreFrameWait(long startedNanos);

    /** Records a pre-frame wait timeout and the time spent waiting. */
    void recordPreFrameWaitTimeout(long startedNanos);

    /** Records successful completion of an admitted frame body read. */
    void recordBodyRead(long startedNanos);

    /** Records an admitted frame body read timeout and the time spent reading. */
    void recordBodyReadTimeout(long startedNanos);
}
