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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;

/**
 * Admission budget for heap pages retained by converted Kafka Produce output.
 *
 * <p>{@link #reserve(long)} is invoked before a physical page allocation. A successful call
 * transfers ownership of those bytes to the caller until either conversion aborts and invokes
 * {@link #release(long)}, or the request-level owner releases its admission lease after the native
 * Produce operation reaches its terminal state.
 */
@Internal
public interface KafkaOutputMemoryBudget {

    /** A budget that permits every allocation and does not perform request accounting. */
    KafkaOutputMemoryBudget UNBOUNDED =
            new KafkaOutputMemoryBudget() {
                @Override
                public void reserve(long bytes) {}

                @Override
                public void release(long bytes) {}
            };

    /** Reserves bytes before their corresponding output storage is physically allocated. */
    void reserve(long bytes);

    /** Releases bytes whose output storage was not retained by a successful conversion. */
    void release(long bytes);

    /** Fails when the owning conversion has been cancelled; otherwise returns immediately. */
    default void checkpoint() {}
}
