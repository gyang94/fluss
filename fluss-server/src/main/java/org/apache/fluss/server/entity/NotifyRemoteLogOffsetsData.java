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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;

/** The data for request {@link NotifyRemoteLogOffsetsRequest}. */
public class NotifyRemoteLogOffsetsData {
    private final TableBucket tableBucket;
    private final long remoteLogStartOffset;
    private final long remoteLogEndOffset;
    private final long tieredEndOffset;
    private final int coordinatorEpoch;

    public NotifyRemoteLogOffsetsData(
            TableBucket tableBucket,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            long tieredEndOffset,
            int coordinatorEpoch) {
        this.tableBucket = tableBucket;
        this.remoteLogStartOffset = remoteLogStartOffset;
        this.remoteLogEndOffset = remoteLogEndOffset;
        this.tieredEndOffset = tieredEndOffset;
        this.coordinatorEpoch = coordinatorEpoch;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public long getRemoteLogStartOffset() {
        return remoteLogStartOffset;
    }

    public long getRemoteLogEndOffset() {
        return remoteLogEndOffset;
    }

    public long getTieredEndOffset() {
        return tieredEndOffset;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    @Override
    public String toString() {
        return "NotifyRemoteLogOffsetsData{"
                + "tableBucket="
                + tableBucket
                + ", remoteLogEndOffset="
                + remoteLogEndOffset
                + ", tieredEndOffset="
                + tieredEndOffset
                + ", coordinatorEpoch="
                + coordinatorEpoch
                + '}';
    }
}
