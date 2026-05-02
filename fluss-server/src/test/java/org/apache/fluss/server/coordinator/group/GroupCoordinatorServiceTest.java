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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalGenerationException;
import org.apache.fluss.exception.InvalidGroupIdException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.FindCoordinatorRequest;
import org.apache.fluss.rpc.messages.FindCoordinatorResponse;
import org.apache.fluss.rpc.messages.PbCommitOffsetEntry;
import org.apache.fluss.rpc.messages.PbFetchOffsetResultEntry;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link GroupCoordinatorService}. */
class GroupCoordinatorServiceTest {

    private static final String DEFAULT_LISTENER = "INTERNAL";
    private static final String CLIENT_LISTENER = "CLIENT";
    private static final long CONSUMER_OFFSETS_TABLE_ID = 99L;
    private static final TablePath CONSUMER_OFFSETS_TABLE_PATH =
            TablePath.of("sys", "consumer_offsets");
    private static final Schema CONSUMER_OFFSETS_SCHEMA =
            Schema.newBuilder()
                    .column("offset_key", DataTypes.BYTES())
                    .column("offset_value", DataTypes.BYTES())
                    .primaryKey("offset_key")
                    .build();
    private static final int NUM_BUCKETS = 8;
    private static final TableInfo CONSUMER_OFFSETS_TABLE_INFO =
            new TableInfo(
                    CONSUMER_OFFSETS_TABLE_PATH,
                    CONSUMER_OFFSETS_TABLE_ID,
                    0,
                    CONSUMER_OFFSETS_SCHEMA,
                    Collections.singletonList("offset_key"),
                    Collections.<String>emptyList(),
                    NUM_BUCKETS,
                    new Configuration(),
                    new Configuration(),
                    null,
                    null,
                    1L,
                    1L);

    private CoordinatorRuntime runtime;
    private GroupCoordinatorService service;
    private TabletServerMetadataCache metadataCache;

    @BeforeEach
    void setUp() {
        runtime = new CoordinatorRuntime();
        MetadataManager metadataManager = mock(MetadataManager.class);
        when(metadataManager.tableExists(CONSUMER_OFFSETS_TABLE_PATH)).thenReturn(true);
        when(metadataManager.getTable(CONSUMER_OFFSETS_TABLE_PATH))
                .thenReturn(CONSUMER_OFFSETS_TABLE_INFO);
        metadataCache = new TabletServerMetadataCache(metadataManager);
        CoordinatorGateway coordinatorGateway = mock(CoordinatorGateway.class);
        service =
                new GroupCoordinatorService(
                        runtime,
                        metadataCache,
                        metadataManager,
                        DEFAULT_LISTENER,
                        coordinatorGateway,
                        new Configuration());
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
    void testFindCoordinatorReturnsLeaderEndpointForRequestListener() throws Exception {
        String groupId = "my-group";
        int bucketId = GroupCoordinatorService.computeBucketId(groupId, NUM_BUCKETS);
        updateConsumerOffsetsMetadata(
                bucketId,
                new ServerInfo(
                        3,
                        null,
                        Arrays.asList(
                                new Endpoint("internal-host", 9123, DEFAULT_LISTENER),
                                new Endpoint("client-host", 9124, CLIENT_LISTENER)),
                        ServerType.TABLET_SERVER));

        FindCoordinatorRequest request = new FindCoordinatorRequest();
        request.setGroupId(groupId);

        FindCoordinatorResponse response = service.findCoordinator(request, CLIENT_LISTENER).get();
        assertThat(response.getErrorCode()).isEqualTo(Errors.NONE.code());
        assertThat(response.getCoordinatorServerId()).isEqualTo(3);
        assertThat(response.getHost()).isEqualTo("client-host");
        assertThat(response.getPort()).isEqualTo(9124);
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

    @Test
    void testPositiveGenerationFailsWithoutMemberId() {
        String groupId = "generation-group";
        int bucketId = GroupCoordinatorService.computeBucketId(groupId, NUM_BUCKETS);
        runtime.scheduleLoadOperation(bucketId, 1);
        runtime.markShardLoaded(bucketId);

        CommitOffsetsRequest request = new CommitOffsetsRequest();
        request.setGroupId(groupId);
        request.setGenerationId(1);
        request.addOffset().setTableId(10L).setBucketId(0).setOffset(1L);

        assertThatThrownBy(() -> service.commitOffsets(request).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalGenerationException.class);
    }

    private void updateConsumerOffsetsMetadata(int bucketId, ServerInfo leaderServer) {
        Set<ServerInfo> aliveServers = new HashSet<>(Collections.singletonList(leaderServer));
        TableMetadata tableMetadata =
                new TableMetadata(
                        CONSUMER_OFFSETS_TABLE_INFO,
                        Collections.singletonList(
                                new BucketMetadata(
                                        bucketId,
                                        leaderServer.id(),
                                        1,
                                        Collections.singletonList(leaderServer.id()))));
        metadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        null,
                        aliveServers,
                        Collections.singletonList(tableMetadata),
                        Collections.emptyList()));
    }
}
