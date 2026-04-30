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

package org.apache.fluss.server.coordinator.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;

import javax.annotation.Nullable;

/**
 * Per-bucket state machine for a group coordinator.
 *
 * <p>Each shard owns an {@link OffsetMetadataManager} for offset storage and a {@link
 * GroupMetadataManager} for membership validation. The {@code loaded} flag indicates whether the
 * shard has finished replaying its state from the underlying KV store.
 */
@Internal
public class GroupCoordinatorShard {

    private final int bucketId;
    private final int leaderEpoch;
    private final OffsetMetadataManager offsetMetadataManager;
    private final GroupMetadataManager groupMetadataManager;
    private volatile boolean loaded;

    public GroupCoordinatorShard(int bucketId, int leaderEpoch) {
        this.bucketId = bucketId;
        this.leaderEpoch = leaderEpoch;
        this.offsetMetadataManager = new OffsetMetadataManager();
        this.groupMetadataManager = new GroupMetadataManager();
        this.loaded = false;
    }

    /**
     * Commits offsets after validating group membership.
     *
     * @param request the commit offsets request
     * @return a result containing KV records and the response
     */
    public CoordinatorResult<CommitOffsetsResponse> commitOffset(CommitOffsetsRequest request) {
        if (request.hasMemberId()) {
            groupMetadataManager.validateOffsetCommit(
                    request.getGroupId(), request.getMemberId(), request.getGenerationId());
        }
        return offsetMetadataManager.commitOffset(request);
    }

    /**
     * Fetches committed offsets for a group from the in-memory cache.
     *
     * @param request the fetch offsets request
     * @return the response with offset information
     */
    public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest request) {
        return offsetMetadataManager.fetchOffsets(request.getGroupId(), request);
    }

    /**
     * Replays a KV record into the in-memory cache.
     *
     * @param key the encoded offset key
     * @param value the encoded offset value, or {@code null} for a tombstone
     */
    public void replay(byte[] key, @Nullable byte[] value) {
        offsetMetadataManager.replay(key, value);
    }

    /** Returns the bucket ID this shard is responsible for. */
    public int bucketId() {
        return bucketId;
    }

    /** Returns the leader epoch this shard was created with. */
    public int leaderEpoch() {
        return leaderEpoch;
    }

    /** Returns whether this shard has finished loading its state. */
    public boolean isLoaded() {
        return loaded;
    }

    /** Sets the loaded state of this shard. */
    public void setLoaded(boolean loaded) {
        this.loaded = loaded;
    }
}
