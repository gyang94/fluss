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
import org.apache.fluss.rpc.messages.PbFetchOffsetResultEntry;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OffsetMetadataManager}. */
class OffsetMetadataManagerTest {

    private OffsetMetadataManager manager;

    @BeforeEach
    void setUp() {
        manager = new OffsetMetadataManager();
    }

    @Test
    void testCommitSingleOffset() {
        CommitOffsetsRequest request = buildCommitRequest("group-1", 100L, 1, 0, 42L, 5, "meta");

        CoordinatorResult<CommitOffsetsResponse> result = manager.commitOffset(request);

        assertThat(result.records()).hasSize(1);
        assertThat(result.response().getResultsCount()).isEqualTo(1);
        assertThat(result.response().getResultAt(0).getErrorCode()).isEqualTo(Errors.NONE.code());
        assertThat(result.response().getResultAt(0).getTableId()).isEqualTo(100L);
        assertThat(result.response().getResultAt(0).getBucketId()).isEqualTo(0);
    }

    @Test
    void testCommitNegativeOffsetReturnsError() {
        CommitOffsetsRequest request = buildCommitRequest("group-1", 100L, 1, 0, -1L, 0, "");

        CoordinatorResult<CommitOffsetsResponse> result = manager.commitOffset(request);

        assertThat(result.records()).isEmpty();
        assertThat(result.response().getResultsCount()).isEqualTo(1);
        assertThat(result.response().getResultAt(0).getErrorCode())
                .isEqualTo(Errors.INVALID_COMMIT_OFFSET_EXCEPTION.code());
    }

    @Test
    void testReplayAndFetchOffset() {
        CommitOffsetsRequest request = buildCommitRequest("group-1", 100L, 1, 0, 42L, 5, "meta");
        CoordinatorResult<CommitOffsetsResponse> result = manager.commitOffset(request);

        // Replay all records into cache
        for (CoordinatorResult.KvRecord record : result.records()) {
            manager.replay(record.key(), record.value());
        }

        // Fetch the committed offset
        FetchOffsetsRequest fetchRequest = buildFetchRequest("group-1", 100L, 1, 0);
        FetchOffsetsResponse fetchResponse = manager.fetchOffsets("group-1", fetchRequest);

        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);
        PbFetchOffsetResultEntry entry = fetchResponse.getResultAt(0);
        assertThat(entry.getOffset()).isEqualTo(42L);
        assertThat(entry.getLeaderEpoch()).isEqualTo(5);
        assertThat(entry.getMetadata()).isEqualTo("meta");
        assertThat(entry.getErrorCode()).isEqualTo(Errors.NONE.code());
    }

    @Test
    void testFetchUnknownOffsetReturnsMinusOne() {
        FetchOffsetsRequest fetchRequest = buildFetchRequest("unknown-group", 999L, null, 0);
        FetchOffsetsResponse fetchResponse = manager.fetchOffsets("unknown-group", fetchRequest);

        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);
        PbFetchOffsetResultEntry entry = fetchResponse.getResultAt(0);
        assertThat(entry.getOffset()).isEqualTo(-1L);
        assertThat(entry.getLeaderEpoch()).isEqualTo(-1);
        assertThat(entry.getErrorCode()).isEqualTo(Errors.NONE.code());
    }

    @Test
    void testFetchAllOffsetsForGroup() {
        // Commit two offsets for the same group
        CommitOffsetsRequest req1 = buildCommitRequest("group-1", 100L, 1, 0, 10L, 1, "");
        CommitOffsetsRequest req2 = buildCommitRequest("group-1", 200L, null, 1, 20L, 2, "m2");

        CoordinatorResult<CommitOffsetsResponse> result1 = manager.commitOffset(req1);
        CoordinatorResult<CommitOffsetsResponse> result2 = manager.commitOffset(req2);

        for (CoordinatorResult.KvRecord record : result1.records()) {
            manager.replay(record.key(), record.value());
        }
        for (CoordinatorResult.KvRecord record : result2.records()) {
            manager.replay(record.key(), record.value());
        }

        // Fetch with empty entries should return all
        FetchOffsetsRequest fetchAll = new FetchOffsetsRequest();
        fetchAll.setGroupId("group-1");
        FetchOffsetsResponse fetchResponse = manager.fetchOffsets("group-1", fetchAll);

        assertThat(fetchResponse.getResultsCount()).isEqualTo(2);
    }

    @Test
    void testReplayTombstoneRemovesOffset() {
        // Commit and replay
        CommitOffsetsRequest request = buildCommitRequest("group-1", 100L, 1, 0, 42L, 5, "meta");
        CoordinatorResult<CommitOffsetsResponse> result = manager.commitOffset(request);
        for (CoordinatorResult.KvRecord record : result.records()) {
            manager.replay(record.key(), record.value());
        }

        // Replay tombstone (null value)
        byte[] key = result.records().get(0).key();
        manager.replay(key, null);

        // Fetch should return -1
        FetchOffsetsRequest fetchRequest = buildFetchRequest("group-1", 100L, 1, 0);
        FetchOffsetsResponse fetchResponse = manager.fetchOffsets("group-1", fetchRequest);

        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);
        assertThat(fetchResponse.getResultAt(0).getOffset()).isEqualTo(-1L);
    }

    @Test
    void testOverwriteOffset() {
        // First commit
        CommitOffsetsRequest req1 = buildCommitRequest("group-1", 100L, 1, 0, 10L, 1, "first");
        CoordinatorResult<CommitOffsetsResponse> result1 = manager.commitOffset(req1);
        for (CoordinatorResult.KvRecord record : result1.records()) {
            manager.replay(record.key(), record.value());
        }

        // Second commit with higher offset
        CommitOffsetsRequest req2 = buildCommitRequest("group-1", 100L, 1, 0, 50L, 3, "second");
        CoordinatorResult<CommitOffsetsResponse> result2 = manager.commitOffset(req2);
        for (CoordinatorResult.KvRecord record : result2.records()) {
            manager.replay(record.key(), record.value());
        }

        // Fetch should return latest
        FetchOffsetsRequest fetchRequest = buildFetchRequest("group-1", 100L, 1, 0);
        FetchOffsetsResponse fetchResponse = manager.fetchOffsets("group-1", fetchRequest);

        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);
        assertThat(fetchResponse.getResultAt(0).getOffset()).isEqualTo(50L);
        assertThat(fetchResponse.getResultAt(0).getLeaderEpoch()).isEqualTo(3);
        assertThat(fetchResponse.getResultAt(0).getMetadata()).isEqualTo("second");
    }

    // ------------------------------------------------------------------------
    //  Helper methods
    // ------------------------------------------------------------------------

    private static CommitOffsetsRequest buildCommitRequest(
            String groupId,
            long tableId,
            Integer partitionId,
            int bucketId,
            long offset,
            int leaderEpoch,
            String metadata) {
        CommitOffsetsRequest request = new CommitOffsetsRequest();
        request.setGroupId(groupId);
        request.setGenerationId(-1);
        request.addOffset()
                .setTableId(tableId)
                .setBucketId(bucketId)
                .setOffset(offset)
                .setLeaderEpoch(leaderEpoch)
                .setMetadata(metadata);
        if (partitionId != null) {
            request.getOffsetAt(0).setPartitionId(partitionId);
        }
        return request;
    }

    private static FetchOffsetsRequest buildFetchRequest(
            String groupId, long tableId, Integer partitionId, int bucketId) {
        FetchOffsetsRequest request = new FetchOffsetsRequest();
        request.setGroupId(groupId);
        request.addEntry().setTableId(tableId).setBucketId(bucketId);
        if (partitionId != null) {
            request.getEntryAt(0).setPartitionId(partitionId);
        }
        return request;
    }
}
