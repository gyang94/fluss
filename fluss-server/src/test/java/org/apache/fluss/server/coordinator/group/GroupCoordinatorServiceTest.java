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

import org.apache.fluss.exception.InvalidGroupIdException;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.FindCoordinatorRequest;
import org.apache.fluss.rpc.messages.FindCoordinatorResponse;
import org.apache.fluss.rpc.messages.PbCommitOffsetEntry;
import org.apache.fluss.rpc.messages.PbFetchOffsetResultEntry;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GroupCoordinatorService}. */
class GroupCoordinatorServiceTest {

    private static final int NUM_BUCKETS = 8;

    private CoordinatorRuntime runtime;
    private GroupCoordinatorService service;

    @BeforeEach
    void setUp() {
        runtime = new CoordinatorRuntime();
        service = new GroupCoordinatorService(runtime, NUM_BUCKETS);
    }

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void testEmptyGroupIdFailsForFindCoordinator() {
        FindCoordinatorRequest request = new FindCoordinatorRequest();
        request.setGroupId("");

        assertThatThrownBy(() -> service.findCoordinator(request).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidGroupIdException.class);
    }

    @Test
    void testEmptyGroupIdFailsForCommitOffsets() {
        CommitOffsetsRequest request = new CommitOffsetsRequest();
        request.setGroupId("");

        assertThatThrownBy(() -> service.commitOffsets(request).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidGroupIdException.class);
    }

    @Test
    void testEmptyGroupIdFailsForFetchOffsets() {
        FetchOffsetsRequest request = new FetchOffsetsRequest();
        request.setGroupId("  ");

        assertThatThrownBy(() -> service.fetchOffsets(request).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidGroupIdException.class);
    }

    @Test
    void testComputeBucketIdConsistentAndInRange() {
        String groupId = "consistent-group";
        int bucket1 = GroupCoordinatorService.computeBucketId(groupId, NUM_BUCKETS);
        int bucket2 = GroupCoordinatorService.computeBucketId(groupId, NUM_BUCKETS);
        assertThat(bucket1).isEqualTo(bucket2);
        assertThat(bucket1).isGreaterThanOrEqualTo(0).isLessThan(NUM_BUCKETS);
    }

    @Test
    void testComputeBucketIdNonNegativeForNegativeHashCode() {
        // String with negative hashCode.
        String groupId = "aaaaaaaaa";
        int bucket = GroupCoordinatorService.computeBucketId(groupId, NUM_BUCKETS);
        assertThat(bucket).isGreaterThanOrEqualTo(0).isLessThan(NUM_BUCKETS);
    }

    @Test
    void testFindCoordinatorReturnsNotAvailable() throws Exception {
        FindCoordinatorRequest request = new FindCoordinatorRequest();
        request.setGroupId("my-group");

        FindCoordinatorResponse response = service.findCoordinator(request).get();
        assertThat(response.getErrorCode())
                .isEqualTo(Errors.COORDINATOR_NOT_AVAILABLE_EXCEPTION.code());
    }

    @Test
    void testEndToEndCommitAndFetch() throws Exception {
        String groupId = "e2e-group";
        int bucketId = GroupCoordinatorService.computeBucketId(groupId, NUM_BUCKETS);

        // Load the shard for the computed bucket.
        runtime.scheduleLoadOperation(bucketId, 1);
        runtime.markShardLoaded(bucketId);

        // Commit offsets.
        CommitOffsetsRequest commitRequest = new CommitOffsetsRequest();
        commitRequest.setGroupId(groupId);
        commitRequest.setGenerationId(-1);
        PbCommitOffsetEntry entry = commitRequest.addOffset();
        entry.setTableId(10L);
        entry.setBucketId(0);
        entry.setOffset(500L);
        entry.setLeaderEpoch(7);

        CommitOffsetsResponse commitResponse = service.commitOffsets(commitRequest).get();
        assertThat(commitResponse.getResultsCount()).isEqualTo(1);
        assertThat(commitResponse.getResultAt(0).getErrorCode()).isEqualTo(Errors.NONE.code());

        // Fetch offsets.
        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId(groupId);

        FetchOffsetsResponse fetchResponse = service.fetchOffsets(fetchRequest).get();
        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);
        PbFetchOffsetResultEntry fetchResult = fetchResponse.getResultAt(0);
        assertThat(fetchResult.getOffset()).isEqualTo(500L);
        assertThat(fetchResult.getLeaderEpoch()).isEqualTo(7);
    }
}
