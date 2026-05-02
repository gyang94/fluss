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

import org.apache.fluss.exception.CoordinatorLoadInProgressException;
import org.apache.fluss.exception.NotCoordinatorException;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.PbCommitOffsetEntry;
import org.apache.fluss.rpc.messages.PbFetchOffsetResultEntry;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.server.coordinator.group.CoordinatorRuntimeTest.TestRecordHelper.validRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CoordinatorRuntime}. */
class CoordinatorRuntimeTest {

    private CoordinatorRuntime runtime;

    @BeforeEach
    void setUp() {
        runtime = new CoordinatorRuntime();
    }

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void testWriteOnNonExistentShardThrowsNotCoordinator() {
        CompletableFuture<String> future =
                runtime.scheduleWriteOperation(
                        99,
                        shard ->
                                new CoordinatorResult<>(
                                        Collections.<CoordinatorResult.KvRecord>emptyList(), "ok"));

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(NotCoordinatorException.class);
    }

    @Test
    void testReadOnNonExistentShardThrowsNotCoordinator() {
        CompletableFuture<String> future = runtime.scheduleReadOperation(99, shard -> "result");

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(NotCoordinatorException.class);
    }

    @Test
    void testReadOnNotYetLoadedShardThrowsLoadInProgress() {
        runtime.scheduleLoadOperation(0, 1);

        CompletableFuture<String> future = runtime.scheduleReadOperation(0, shard -> "result");

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(CoordinatorLoadInProgressException.class);
    }

    @Test
    void testLoadMarkLoadedAndRead() throws Exception {
        runtime.scheduleLoadOperation(0, 1);
        runtime.markShardLoaded(0);

        CompletableFuture<String> future = runtime.scheduleReadOperation(0, shard -> "hello");

        assertThat(future.get()).isEqualTo("hello");
    }

    @Test
    void testWriteWithEmptyRecordsCompletesImmediately() throws Exception {
        runtime.scheduleLoadOperation(0, 1);
        runtime.markShardLoaded(0);

        CompletableFuture<String> future =
                runtime.scheduleWriteOperation(
                        0,
                        shard ->
                                new CoordinatorResult<>(
                                        Collections.<CoordinatorResult.KvRecord>emptyList(),
                                        "done"));

        assertThat(future.get()).isEqualTo("done");
    }

    @Test
    void testWriteWithRecordsAndNoKvWriterCompletesImmediately() throws Exception {
        runtime.scheduleLoadOperation(0, 1);
        runtime.markShardLoaded(0);

        List<CoordinatorResult.KvRecord> records = Collections.singletonList(validRecord());

        CompletableFuture<String> future =
                runtime.scheduleWriteOperation(
                        0, shard -> new CoordinatorResult<>(records, "persisted"));

        assertThat(future.get()).isEqualTo("persisted");
    }

    @Test
    void testWriteWithKvWriterChainsCompletion() throws Exception {
        runtime.scheduleLoadOperation(0, 1);
        runtime.markShardLoaded(0);

        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        runtime.setKvWriter((bucketId, records) -> writeFuture);

        List<CoordinatorResult.KvRecord> records = Collections.singletonList(validRecord());

        CompletableFuture<String> future =
                runtime.scheduleWriteOperation(0, shard -> new CoordinatorResult<>(records, "ack"));

        // Not yet completed since kvWriter hasn't completed.
        assertThat(future.isDone()).isFalse();

        // Complete the write.
        writeFuture.complete(null);
        assertThat(future.get()).isEqualTo("ack");
    }

    @Test
    void testReadWaitsForPendingWriteAckAndSeesCommittedState() throws Exception {
        runtime.scheduleLoadOperation(0, 1);
        runtime.markShardLoaded(0);

        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        runtime.setKvWriter((bucketId, records) -> writeFuture);

        String groupId = "pending-write-group";
        CommitOffsetsRequest commitRequest = buildCommitRequest(groupId, 100L, 3);
        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId(groupId);

        CompletableFuture<CommitOffsetsResponse> commitFuture =
                runtime.scheduleWriteOperation(0, shard -> shard.commitOffset(commitRequest));
        CompletableFuture<FetchOffsetsResponse> fetchFuture =
                runtime.scheduleReadOperation(0, shard -> shard.fetchOffsets(fetchRequest));

        assertThat(commitFuture.isDone()).isFalse();
        assertThat(fetchFuture.isDone()).isFalse();

        writeFuture.complete(null);

        assertThat(commitFuture.get().getResultAt(0).getErrorCode()).isEqualTo(Errors.NONE.code());
        FetchOffsetsResponse fetchResponse = fetchFuture.get();
        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);
        assertThat(fetchResponse.getResultAt(0).getOffset()).isEqualTo(100L);
        assertThat(fetchResponse.getResultAt(0).getLeaderEpoch()).isEqualTo(3);
    }

    @Test
    void testFailedWriteDoesNotReplayStateIntoCache() throws Exception {
        runtime.scheduleLoadOperation(0, 1);
        runtime.markShardLoaded(0);

        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        runtime.setKvWriter((bucketId, records) -> writeFuture);

        String groupId = "failed-write-group";
        CommitOffsetsRequest commitRequest = buildCommitRequest(groupId, 100L, 3);
        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId(groupId);

        CompletableFuture<CommitOffsetsResponse> commitFuture =
                runtime.scheduleWriteOperation(0, shard -> shard.commitOffset(commitRequest));
        CompletableFuture<FetchOffsetsResponse> fetchFuture =
                runtime.scheduleReadOperation(0, shard -> shard.fetchOffsets(fetchRequest));

        writeFuture.completeExceptionally(new RuntimeException("write failed"));

        assertThatThrownBy(commitFuture::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("write failed");
        assertThat(fetchFuture.get().getResultsCount()).isZero();
    }

    @Test
    void testCompletedWriteRemovesBucketFromOperationQueue() throws Exception {
        runtime.scheduleLoadOperation(0, 1);
        runtime.markShardLoaded(0);

        CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        runtime.setKvWriter((bucketId, records) -> writeFuture);

        CompletableFuture<String> future =
                runtime.scheduleWriteOperation(
                        0,
                        shard ->
                                new CoordinatorResult<>(
                                        Collections.singletonList(validRecord()), "done"));

        assertThat(runtime.getOperationQueueCount()).isEqualTo(1);

        writeFuture.complete(null);

        assertThat(future.get()).isEqualTo("done");
        assertThat(runtime.getOperationQueueCount()).isZero();
    }

    @Test
    void testLoadAndUnloadLifecycle() {
        runtime.scheduleLoadOperation(0, 1);
        assertThat(runtime.getShardCount()).isEqualTo(1);
        assertThat(runtime.getShard(0)).isNotNull();

        runtime.scheduleUnloadOperation(0);
        assertThat(runtime.getShardCount()).isEqualTo(0);
        assertThat(runtime.getShard(0)).isNull();
    }

    @Test
    void testEndToEndCommitAndFetch() throws Exception {
        runtime.scheduleLoadOperation(0, 1);
        runtime.markShardLoaded(0);

        String groupId = "my-group";

        // Commit offsets.
        CommitOffsetsRequest commitRequest = new CommitOffsetsRequest();
        commitRequest.setGroupId(groupId);
        commitRequest.setGenerationId(-1);
        PbCommitOffsetEntry entry = commitRequest.addOffset();
        entry.setTableId(1L);
        entry.setBucketId(0);
        entry.setOffset(100L);
        entry.setLeaderEpoch(3);

        CompletableFuture<CommitOffsetsResponse> commitFuture =
                runtime.scheduleWriteOperation(0, shard -> shard.commitOffset(commitRequest));

        CommitOffsetsResponse commitResponse = commitFuture.get();
        assertThat(commitResponse.getResultsCount()).isEqualTo(1);
        assertThat(commitResponse.getResultAt(0).getErrorCode()).isEqualTo(Errors.NONE.code());

        // Fetch offsets.
        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId(groupId);

        CompletableFuture<FetchOffsetsResponse> fetchFuture =
                runtime.scheduleReadOperation(0, shard -> shard.fetchOffsets(fetchRequest));

        FetchOffsetsResponse fetchResponse = fetchFuture.get();
        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);
        PbFetchOffsetResultEntry fetchResult = fetchResponse.getResultAt(0);
        assertThat(fetchResult.getOffset()).isEqualTo(100L);
        assertThat(fetchResult.getLeaderEpoch()).isEqualTo(3);
    }

    /** Helper for creating valid KV records that can be replayed by the shard. */
    static final class TestRecordHelper {
        private TestRecordHelper() {}

        static CoordinatorResult.KvRecord validRecord() {
            byte[] key = OffsetKeyValueCodec.encodeKey("test-group", 1L, null, 0);
            OffsetAndMetadata oam = new OffsetAndMetadata(0L, 0, "", 0L);
            byte[] value = OffsetKeyValueCodec.encodeValue(oam);
            return new CoordinatorResult.KvRecord(key, value);
        }
    }

    private static CommitOffsetsRequest buildCommitRequest(
            String groupId, long offset, int leaderEpoch) {
        CommitOffsetsRequest commitRequest = new CommitOffsetsRequest();
        commitRequest.setGroupId(groupId);
        commitRequest.setGenerationId(-1);
        PbCommitOffsetEntry entry = commitRequest.addOffset();
        entry.setTableId(1L);
        entry.setBucketId(0);
        entry.setOffset(offset);
        entry.setLeaderEpoch(leaderEpoch);
        return commitRequest;
    }
}
