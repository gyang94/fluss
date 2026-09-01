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

import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Prevents explicit transport reads while Kafka frame admission has closed the read boundary. */
final class KafkaFrameReadGate extends ChannelOutboundHandlerAdapter {

    private final KafkaFrameReadState readState;

    KafkaFrameReadGate(KafkaFrameReadState readState) {
        this.readState = checkNotNull(readState, "readState");
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        if (readState.canRead()) {
            ctx.read();
        }
    }
}
