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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.CoordinatorNotAvailableException;
import org.apache.fluss.exception.IllegalGenerationException;
import org.apache.fluss.exception.InvalidCommitOffsetException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FindCoordinatorRequest;
import org.apache.fluss.rpc.messages.FindCoordinatorResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.clock.Clock;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link OffsetCommitter}. */
class OffsetCommitterTest {

    private static final String GROUP_ID = "group-1";
    private static final ServerNode NODE1 =
            new ServerNode(1, "localhost", 9011, ServerType.TABLET_SERVER);
    private static final ServerNode NODE2 =
            new ServerNode(2, "localhost", 9012, ServerType.TABLET_SERVER);
    private static final TableBucket BUCKET0 = new TableBucket(DATA1_TABLE_ID, 0);
    private static final TableBucket PARTITION_BUCKET = new TableBucket(DATA1_TABLE_ID, 1001L, 1);

    @Test
    void testCommitOffsetsSyncBuildsAssignModeCommitRequest() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(2));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        Map<TableBucket, Long> offsets = new LinkedHashMap<>();
        offsets.put(BUCKET0, 12L);
        offsets.put(PARTITION_BUCKET, 34L);

        committer.commitOffsetsSync(offsets, 500L);

        assertThat(bootstrapGateway.findCoordinatorCalls()).isEqualTo(1);
        assertThat(coordinatorGateway.commitOffsetsCalls()).isEqualTo(1);
        assertThat(sleeper.sleepDurationsMs()).isEmpty();

        CommitOffsetsRequest request = coordinatorGateway.lastCommitRequest();
        assertThat(request.getGroupId()).isEqualTo(GROUP_ID);
        assertThat(request.getGenerationId()).isEqualTo(-1);
        assertThat(request.getMemberId()).isEqualTo("");
        assertThat(request.getOffsetsCount()).isEqualTo(2);

        assertThat(request.getOffsetAt(0).getTableId()).isEqualTo(DATA1_TABLE_ID);
        assertThat(request.getOffsetAt(0).hasPartitionId()).isFalse();
        assertThat(request.getOffsetAt(0).getBucketId()).isEqualTo(0);
        assertThat(request.getOffsetAt(0).getOffset()).isEqualTo(12L);
        assertThat(request.getOffsetAt(0).getLeaderEpoch()).isEqualTo(-1);
        assertThat(request.getOffsetAt(0).getMetadata()).isEmpty();

        assertThat(request.getOffsetAt(1).getTableId()).isEqualTo(DATA1_TABLE_ID);
        assertThat(request.getOffsetAt(1).hasPartitionId()).isTrue();
        assertThat(request.getOffsetAt(1).getPartitionId()).isEqualTo(1001L);
        assertThat(request.getOffsetAt(1).getBucketId()).isEqualTo(1);
        assertThat(request.getOffsetAt(1).getOffset()).isEqualTo(34L);
        assertThat(request.getOffsetAt(1).getLeaderEpoch()).isEqualTo(-1);
        assertThat(request.getOffsetAt(1).getMetadata()).isEmpty();
    }

    @Test
    void testCommitOffsetsSyncKeepsCoordinatorOnLoadInProgress() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(
                errorCommitResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS_EXCEPTION));
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 9L), 500L);

        assertThat(bootstrapGateway.findCoordinatorCalls()).isEqualTo(1);
        assertThat(coordinatorGateway.commitOffsetsCalls()).isEqualTo(2);
        assertThat(sleeper.sleepDurationsMs()).containsExactly(25L);
    }

    @Test
    void testCommitOffsetsSyncClearsCoordinatorAndRediscoverOnNotCoordinator() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE2));

        TestingOffsetGateway coordinatorGateway1 = new TestingOffsetGateway();
        coordinatorGateway1.enqueueCommitOffsetsResponse(
                errorCommitResponse(Errors.NOT_COORDINATOR_EXCEPTION));

        TestingOffsetGateway coordinatorGateway2 = new TestingOffsetGateway();
        coordinatorGateway2.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1, NODE2), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway1);
        metadataUpdater.registerGateway(NODE2.id(), coordinatorGateway2);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 9L), 500L);

        assertThat(bootstrapGateway.findCoordinatorCalls()).isEqualTo(2);
        assertThat(coordinatorGateway1.commitOffsetsCalls()).isEqualTo(1);
        assertThat(coordinatorGateway2.commitOffsetsCalls()).isEqualTo(1);
        assertThat(sleeper.sleepDurationsMs()).containsExactly(25L);
    }

    @Test
    void testCommitOffsetsSyncRefreshesMetadataWhenCoordinatorMissingInCluster() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE2));

        TestingOffsetGateway coordinatorGateway2 = new TestingOffsetGateway();
        coordinatorGateway2.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE2.id(), coordinatorGateway2);
        metadataUpdater.setUpdateAction(
                () -> metadataUpdater.setCluster(clusterWith(NODE1, NODE2)));

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 11L), 500L);

        assertThat(metadataUpdater.metadataUpdateTimeouts()).hasSize(1);
        assertThat(metadataUpdater.metadataUpdateTimeouts().get(0))
                .isPositive()
                .isLessThanOrEqualTo(500L);
        assertThat(bootstrapGateway.findCoordinatorCalls()).isEqualTo(1);
        assertThat(coordinatorGateway2.commitOffsetsCalls()).isEqualTo(1);
    }

    @Test
    void testCommitOffsetsSyncReusesCachedCoordinator() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(1));
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 1L), 500L);
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 2L), 500L);

        assertThat(bootstrapGateway.findCoordinatorCalls()).isEqualTo(1);
        assertThat(coordinatorGateway.commitOffsetsCalls()).isEqualTo(2);
        assertThat(sleeper.sleepDurationsMs()).isEmpty();
    }

    @Test
    void testCommitOffsetsSyncRetriesFindCoordinatorTopLevelError() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(
                errorFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE_EXCEPTION));
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 8L), 500L);

        assertThat(bootstrapGateway.findCoordinatorCalls()).isEqualTo(2);
        assertThat(coordinatorGateway.commitOffsetsCalls()).isEqualTo(1);
        assertThat(sleeper.sleepDurationsMs()).containsExactly(25L);
    }

    @Test
    void testCommitOffsetsSyncRetriesWhenFindCoordinatorFutureFails() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorFailure(
                new CoordinatorNotAvailableException("coordinator unavailable"));
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 8L), 500L);

        assertThat(bootstrapGateway.findCoordinatorCalls()).isEqualTo(2);
        assertThat(coordinatorGateway.commitOffsetsCalls()).isEqualTo(1);
        assertThat(sleeper.sleepDurationsMs()).containsExactly(25L);
    }

    @Test
    void testCommitOffsetsSyncClearsCachedCoordinatorOnCommitFutureFailure() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE2));

        TestingOffsetGateway coordinatorGateway1 = new TestingOffsetGateway();
        coordinatorGateway1.enqueueCommitOffsetsResponse(successCommitResponse(1));
        coordinatorGateway1.enqueueCommitOffsetsFailure(
                new NetworkException("disconnected from coordinator"));

        TestingOffsetGateway coordinatorGateway2 = new TestingOffsetGateway();
        coordinatorGateway2.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1, NODE2), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway1);
        metadataUpdater.registerGateway(NODE2.id(), coordinatorGateway2);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 1L), 500L);
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 2L), 500L);

        assertThat(bootstrapGateway.findCoordinatorCalls()).isEqualTo(2);
        assertThat(coordinatorGateway1.commitOffsetsCalls()).isEqualTo(2);
        assertThat(coordinatorGateway2.commitOffsetsCalls()).isEqualTo(1);
        assertThat(sleeper.sleepDurationsMs()).containsExactly(25L);
    }

    @Test
    void testCommitOffsetsSyncFailsOnCommitResponseTopLevelError() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(
                errorCommitResponse(Errors.ILLEGAL_GENERATION_EXCEPTION));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        assertThatThrownBy(
                        () ->
                                committer.commitOffsetsSync(
                                        Collections.singletonMap(BUCKET0, 8L), 500L))
                .isInstanceOf(IllegalGenerationException.class);
    }

    @Test
    void testCommitOffsetsSyncFailsOnCommitResponseEntryError() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(
                entryErrorCommitResponse(Errors.INVALID_COMMIT_OFFSET_EXCEPTION));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        assertThatThrownBy(
                        () ->
                                committer.commitOffsetsSync(
                                        Collections.singletonMap(BUCKET0, 8L), 500L))
                .isInstanceOf(InvalidCommitOffsetException.class);
    }

    @Test
    void testCommitOffsetsSyncTimesOutWhenCoordinatorStaysUnavailable() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(
                errorFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE_EXCEPTION));
        bootstrapGateway.enqueueFindCoordinatorResponse(
                errorFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE_EXCEPTION));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        assertThatThrownBy(
                        () ->
                                committer.commitOffsetsSync(
                                        Collections.singletonMap(BUCKET0, 8L), 40L))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("committing offsets");
    }

    @Test
    void testCommitOffsetsAsyncInvokesCallbackOnSuccess() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        ConcurrentLinkedQueue<OffsetCommitter.OffsetCommitCompletion> queue =
                new ConcurrentLinkedQueue<>();
        List<Map<TableBucket, Long>> callbackOffsets = new ArrayList<>();
        List<Exception> callbackExceptions = new ArrayList<>();

        OffsetCommitCallback callback =
                (offsets, exception) -> {
                    callbackOffsets.add(offsets);
                    callbackExceptions.add(exception);
                };

        Map<TableBucket, Long> offsets = Collections.singletonMap(BUCKET0, 5L);
        committer.commitOffsetsAsync(offsets, callback, queue);

        // Drain the queue — the callback should have been enqueued by now
        // (since CompletableFuture.completedFuture completes immediately)
        assertThat(queue).hasSize(1);
        queue.poll().invoke();

        assertThat(callbackOffsets).hasSize(1);
        assertThat(callbackOffsets.get(0)).containsEntry(BUCKET0, 5L);
        assertThat(callbackExceptions.get(0)).isNull();
    }

    @Test
    void testCommitOffsetsAsyncInvokesCallbackOnFailure() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsFailure(new NetworkException("connection reset"));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        ConcurrentLinkedQueue<OffsetCommitter.OffsetCommitCompletion> queue =
                new ConcurrentLinkedQueue<>();
        List<Exception> callbackExceptions = new ArrayList<>();

        OffsetCommitCallback callback = (offsets, exception) -> callbackExceptions.add(exception);

        committer.commitOffsetsAsync(Collections.singletonMap(BUCKET0, 5L), callback, queue);

        assertThat(queue).hasSize(1);
        queue.poll().invoke();

        assertThat(callbackExceptions).hasSize(1);
        assertThat(callbackExceptions.get(0)).isInstanceOf(NetworkException.class);
    }

    @Test
    void testCommitOffsetsAsyncNoCallbackDoesNotEnqueue() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        ConcurrentLinkedQueue<OffsetCommitter.OffsetCommitCompletion> queue =
                new ConcurrentLinkedQueue<>();

        committer.commitOffsetsAsync(Collections.singletonMap(BUCKET0, 5L), null, queue);

        // With null callback, nothing should be enqueued
        assertThat(queue).isEmpty();
        assertThat(coordinatorGateway.commitOffsetsCalls()).isEqualTo(1);
    }

    @Test
    void testCommitOffsetsAsyncWithNoTabletServerFailsImmediately() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();

        // Empty cluster — no tablet servers
        Cluster emptyCluster =
                new Cluster(
                        Collections.emptyMap(),
                        null,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(emptyCluster, bootstrapGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        ConcurrentLinkedQueue<OffsetCommitter.OffsetCommitCompletion> queue =
                new ConcurrentLinkedQueue<>();
        List<Exception> callbackExceptions = new ArrayList<>();

        OffsetCommitCallback callback = (offsets, exception) -> callbackExceptions.add(exception);

        committer.commitOffsetsAsync(Collections.singletonMap(BUCKET0, 5L), callback, queue);

        assertThat(queue).hasSize(1);
        queue.poll().invoke();

        assertThat(callbackExceptions).hasSize(1);
        assertThat(callbackExceptions.get(0)).isInstanceOf(CoordinatorNotAvailableException.class);
    }

    @Test
    void testCommitOffsetsAsyncEmptyOffsetsInvokesCallbackImmediately() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        ConcurrentLinkedQueue<OffsetCommitter.OffsetCommitCompletion> queue =
                new ConcurrentLinkedQueue<>();
        List<Exception> callbackExceptions = new ArrayList<>();

        OffsetCommitCallback callback = (offsets, exception) -> callbackExceptions.add(exception);

        committer.commitOffsetsAsync(Collections.emptyMap(), callback, queue);

        assertThat(queue).hasSize(1);
        queue.poll().invoke();

        assertThat(callbackExceptions).hasSize(1);
        assertThat(callbackExceptions.get(0)).isNull();
    }

    @Test
    void testCommitOffsetsAsyncReusesCoordinator() {
        ManualClock clock = new ManualClock();
        RecordingSleeper sleeper = new RecordingSleeper(clock);
        TestingOffsetGateway bootstrapGateway = new TestingOffsetGateway();
        bootstrapGateway.enqueueFindCoordinatorResponse(findCoordinatorResponse(NODE1));

        TestingOffsetGateway coordinatorGateway = new TestingOffsetGateway();
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(1));
        coordinatorGateway.enqueueCommitOffsetsResponse(successCommitResponse(1));

        TrackingMetadataUpdater metadataUpdater =
                new TrackingMetadataUpdater(clusterWith(NODE1), bootstrapGateway);
        metadataUpdater.registerGateway(NODE1.id(), coordinatorGateway);

        OffsetCommitter committer =
                new OffsetCommitter(GROUP_ID, metadataUpdater, 25L, clock, sleeper);

        ConcurrentLinkedQueue<OffsetCommitter.OffsetCommitCompletion> queue =
                new ConcurrentLinkedQueue<>();

        // First sync commit discovers coordinator
        committer.commitOffsetsSync(Collections.singletonMap(BUCKET0, 1L), 500L);
        // Second async commit reuses it
        committer.commitOffsetsAsync(Collections.singletonMap(BUCKET0, 2L), null, queue);

        assertThat(bootstrapGateway.findCoordinatorCalls()).isEqualTo(1);
        assertThat(coordinatorGateway.commitOffsetsCalls()).isEqualTo(2);
    }

    private static Cluster clusterWith(ServerNode... nodes) {
        Map<Integer, ServerNode> aliveTabletServers = new HashMap<>();
        for (ServerNode node : nodes) {
            aliveTabletServers.put(node.id(), node);
        }
        return new Cluster(
                aliveTabletServers,
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    private static FindCoordinatorResponse findCoordinatorResponse(ServerNode node) {
        FindCoordinatorResponse response = new FindCoordinatorResponse();
        response.setCoordinatorServerId(node.id());
        response.setHost(node.host());
        response.setPort(node.port());
        return response;
    }

    private static FindCoordinatorResponse errorFindCoordinatorResponse(Errors error) {
        FindCoordinatorResponse response = new FindCoordinatorResponse();
        response.setErrorCode(error.code());
        response.setErrorMessage(error.message());
        return response;
    }

    private static CommitOffsetsResponse successCommitResponse(int resultsCount) {
        CommitOffsetsResponse response = new CommitOffsetsResponse();
        for (int i = 0; i < resultsCount; i++) {
            response.addResult()
                    .setTableId(DATA1_TABLE_ID)
                    .setBucketId(i)
                    .setErrorCode(Errors.NONE.code());
        }
        return response;
    }

    private static CommitOffsetsResponse errorCommitResponse(Errors error) {
        CommitOffsetsResponse response = new CommitOffsetsResponse();
        response.setErrorCode(error.code());
        response.setErrorMessage(error.message());
        return response;
    }

    private static CommitOffsetsResponse entryErrorCommitResponse(Errors error) {
        CommitOffsetsResponse response = new CommitOffsetsResponse();
        response.addResult()
                .setTableId(DATA1_TABLE_ID)
                .setBucketId(0)
                .setErrorCode(error.code())
                .setErrorMessage(error.message());
        return response;
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable throwable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }

    private static final class TrackingMetadataUpdater extends MetadataUpdater {

        private final TabletServerGateway bootstrapGateway;
        private final Map<Integer, TabletServerGateway> gatewaysById = new HashMap<>();
        private final List<Long> metadataUpdateTimeouts = new ArrayList<>();

        private Runnable updateAction = () -> {};

        private TrackingMetadataUpdater(
                Cluster initialCluster, TabletServerGateway bootstrapGateway) {
            super(
                    RpcClient.create(new Configuration(), TestingClientMetricGroup.newInstance()),
                    new Configuration(),
                    initialCluster);
            this.bootstrapGateway = bootstrapGateway;
        }

        void registerGateway(int serverId, TabletServerGateway gateway) {
            gatewaysById.put(serverId, gateway);
        }

        void setUpdateAction(Runnable updateAction) {
            this.updateAction = updateAction;
        }

        void setCluster(Cluster cluster) {
            this.cluster = cluster;
        }

        List<Long> metadataUpdateTimeouts() {
            return metadataUpdateTimeouts;
        }

        @Override
        public TabletServerGateway newRandomTabletServerClient() {
            return cluster.getAliveTabletServers().isEmpty() ? null : bootstrapGateway;
        }

        @Override
        public @Nullable TabletServerGateway newTabletServerClientForNode(int serverId) {
            if (cluster.getTabletServer(serverId) == null) {
                return null;
            }
            return gatewaysById.get(serverId);
        }

        @Override
        public void updateMetadata(
                @Nullable Set<TablePath> tablePaths,
                @Nullable Collection<PhysicalTablePath> tablePartitionNames,
                @Nullable Collection<Long> tablePartitionIds,
                long timeoutMs) {
            metadataUpdateTimeouts.add(timeoutMs);
            updateAction.run();
        }
    }

    private static final class TestingOffsetGateway extends TestTabletServerGateway {

        private final Queue<CompletableFuture<FindCoordinatorResponse>> findCoordinatorResponses =
                new ArrayDeque<>();
        private final Queue<CompletableFuture<CommitOffsetsResponse>> commitOffsetsResponses =
                new ArrayDeque<>();
        private final List<CommitOffsetsRequest> commitRequests = new ArrayList<>();

        private int findCoordinatorCalls;
        private int commitOffsetsCalls;

        private TestingOffsetGateway() {
            super(false, Collections.emptySet());
        }

        void enqueueFindCoordinatorResponse(FindCoordinatorResponse response) {
            findCoordinatorResponses.add(CompletableFuture.completedFuture(response));
        }

        void enqueueFindCoordinatorFailure(Throwable throwable) {
            findCoordinatorResponses.add(failedFuture(throwable));
        }

        void enqueueCommitOffsetsResponse(CommitOffsetsResponse response) {
            commitOffsetsResponses.add(CompletableFuture.completedFuture(response));
        }

        void enqueueCommitOffsetsFailure(Throwable throwable) {
            commitOffsetsResponses.add(failedFuture(throwable));
        }

        int findCoordinatorCalls() {
            return findCoordinatorCalls;
        }

        int commitOffsetsCalls() {
            return commitOffsetsCalls;
        }

        CommitOffsetsRequest lastCommitRequest() {
            return commitRequests.get(commitRequests.size() - 1);
        }

        @Override
        public CompletableFuture<FindCoordinatorResponse> findCoordinator(
                FindCoordinatorRequest request) {
            findCoordinatorCalls++;
            return findCoordinatorResponses.remove();
        }

        @Override
        public CompletableFuture<CommitOffsetsResponse> commitOffsets(
                CommitOffsetsRequest request) {
            commitOffsetsCalls++;
            commitRequests.add(new CommitOffsetsRequest().copyFrom(request));
            return commitOffsetsResponses.remove();
        }
    }

    private static final class ManualClock implements Clock {
        private long milliseconds;
        private long nanoseconds;

        @Override
        public long milliseconds() {
            return milliseconds;
        }

        @Override
        public long nanoseconds() {
            return nanoseconds;
        }

        void advanceMs(long durationMs) {
            milliseconds += durationMs;
            nanoseconds += TimeUnit.MILLISECONDS.toNanos(durationMs);
        }
    }

    private static final class RecordingSleeper implements OffsetCommitter.Sleeper {
        private final ManualClock clock;
        private final List<Long> sleepDurationsMs = new ArrayList<>();

        private RecordingSleeper(ManualClock clock) {
            this.clock = clock;
        }

        List<Long> sleepDurationsMs() {
            return sleepDurationsMs;
        }

        @Override
        public void sleep(long sleepMs) {
            sleepDurationsMs.add(sleepMs);
            clock.advanceMs(sleepMs);
        }
    }
}
