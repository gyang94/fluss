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

/**
 * Acquires an independently owned reason for pausing reads on one Kafka connection.
 *
 * <p>The production implementation must participate in the channel's shared pause arbitration so
 * releasing a pre-frame wait cannot override queue or resource pressure owned by another component.
 * The arbitration owner is also responsible for restarting reads after the last pause reason is
 * released: enabling auto-read triggers the normal transport read, while a channel kept in manual
 * read mode must receive exactly one explicit read. The frame read gate deliberately does not queue
 * or replay read attempts that it suppresses while admission is waiting.
 */
@Internal
public interface KafkaFrameReadPauser {

    /** Acquires a pause lease for the supplied channel. */
    PauseLease pause(Channel channel);

    /**
     * Idempotent ownership of one channel-read pause reason.
     *
     * <p>Closing the last active pause reason must restart the channel according to the resume
     * contract documented on {@link KafkaFrameReadPauser}.
     */
    interface PauseLease extends AutoCloseable {

        /** Releases only this pause reason. */
        @Override
        void close();
    }
}
