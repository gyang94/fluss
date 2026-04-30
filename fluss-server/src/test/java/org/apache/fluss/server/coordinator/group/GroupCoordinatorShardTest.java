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

import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.PbCommitOffsetEntry;
import org.apache.fluss.rpc.messages.PbFetchOffsetResultEntry;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GroupCoordinatorShard}. */
class GroupCoordinatorShardTest {

    private GroupCoordinatorShard shard;

    @BeforeEach
    void setUp() {
        shard = new GroupCoordinatorShard(0, 1);
    }

    @Test
    void testInitialState() {
        assertThat(shard.bucketId()).isEqualTo(0);
        assertThat(shard.leaderEpoch()).isEqualTo(1);
        assertThat(shard.isLoaded()).isFalse();
    }

    @Test
    void testSetLoaded() {
        shard.setLoaded(true);
        assertThat(shard.isLoaded()).isTrue();
        shard.setLoaded(false);
        assertThat(shard.isLoaded()).isFalse();
    }

    @Test
    void testCommitAndFetchOffsets() {
        String groupId = "test-group";

        // Commit an offset.
        CommitOffsetsRequest commitRequest = new CommitOffsetsRequest();
        commitRequest.setGroupId(groupId);
        commitRequest.setGenerationId(-1);
        PbCommitOffsetEntry entry = commitRequest.addOffset();
        entry.setTableId(100L);
        entry.setBucketId(0);
        entry.setOffset(42L);
        entry.setLeaderEpoch(5);

        CoordinatorResult<CommitOffsetsResponse> result = shard.commitOffset(commitRequest);
        assertThat(result.records()).isNotEmpty();
        assertThat(result.response().getResultsCount()).isEqualTo(1);
        assertThat(result.response().getResultAt(0).getErrorCode()).isEqualTo(Errors.NONE.code());

        // Replay the records into the cache.
        for (CoordinatorResult.KvRecord record : result.records()) {
            shard.replay(record.key(), record.value());
        }

        // Fetch the committed offset.
        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId(groupId);

        FetchOffsetsResponse fetchResponse = shard.fetchOffsets(fetchRequest);
        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);
        PbFetchOffsetResultEntry fetchResult = fetchResponse.getResultAt(0);
        assertThat(fetchResult.getOffset()).isEqualTo(42L);
        assertThat(fetchResult.getLeaderEpoch()).isEqualTo(5);
        assertThat(fetchResult.getErrorCode()).isEqualTo(Errors.NONE.code());
    }

    @Test
    void testReplayTombstoneRemovesOffset() {
        String groupId = "test-group";

        // Commit an offset and replay.
        CommitOffsetsRequest commitRequest = new CommitOffsetsRequest();
        commitRequest.setGroupId(groupId);
        commitRequest.setGenerationId(-1);
        PbCommitOffsetEntry entry = commitRequest.addOffset();
        entry.setTableId(200L);
        entry.setBucketId(1);
        entry.setOffset(10L);
        entry.setLeaderEpoch(2);

        CoordinatorResult<CommitOffsetsResponse> result = shard.commitOffset(commitRequest);
        for (CoordinatorResult.KvRecord record : result.records()) {
            shard.replay(record.key(), record.value());
        }

        // Replay a tombstone (null value) for the same key.
        shard.replay(result.records().get(0).key(), null);

        // Fetch should show empty offset.
        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId(groupId);
        FetchOffsetsResponse fetchResponse = shard.fetchOffsets(fetchRequest);
        assertThat(fetchResponse.getResultsCount()).isEqualTo(0);
    }
}
