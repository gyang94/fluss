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
import org.apache.fluss.exception.NotCoordinatorException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbCommitOffsetEntry;
import org.apache.fluss.rpc.messages.PbFetchOffsetEntry;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for GroupCoordinator offset management.
 *
 * <p>Tests the RPC routing from TabletService through GroupCoordinatorService to
 * CoordinatorRuntime. Since the coordinator shard loading requires wiring the sys.consumer_offsets
 * table ID to ReplicaManager (not yet implemented), commit and fetch requests reach
 * CoordinatorRuntime but fail with NotCoordinatorException because no shard is loaded for the
 * target bucket. This test verifies:
 *
 * <ul>
 *   <li>System table creation at coordinator startup
 *   <li>RPC routing through TabletService to GroupCoordinatorService
 *   <li>Error handling for invalid group IDs
 * </ul>
 */
class GroupCoordinatorITCase {

    private static final String SYSTEM_DATABASE_NAME = "fluss_system";
    private static final String CONSUMER_OFFSETS_TABLE_NAME = "sys.consumer_offsets";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @Test
    void testSystemTableCreatedAtStartup() throws Exception {
        // The coordinator should have created sys.consumer_offsets during startup.
        CoordinatorGateway coordinator = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();

        TablePath consumerOffsetsPath =
                TablePath.of(SYSTEM_DATABASE_NAME, CONSUMER_OFFSETS_TABLE_NAME);

        GetTableInfoResponse response =
                coordinator
                        .getTableInfo(
                                RpcMessageTestUtils.newGetTableInfoRequest(consumerOffsetsPath))
                        .get();
        assertThat(response.getTableId()).isGreaterThanOrEqualTo(0);
        assertThat(response.getSchemaId()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testCommitOffsetsReachesGroupCoordinatorService() throws Exception {
        // CommitOffsets should reach GroupCoordinatorService -> CoordinatorRuntime.
        // Since no shard is loaded, we expect NotCoordinatorException.
        int serverId = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).id();
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(serverId);

        CommitOffsetsRequest commitRequest = new CommitOffsetsRequest();
        commitRequest.setGroupId("test-group-it");
        commitRequest.setGenerationId(-1);
        PbCommitOffsetEntry entry = commitRequest.addOffset();
        entry.setTableId(1L);
        entry.setBucketId(0);
        entry.setOffset(42L);
        entry.setLeaderEpoch(1);

        assertThatThrownBy(() -> gateway.commitOffsets(commitRequest).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(NotCoordinatorException.class);
    }

    @Test
    void testFetchOffsetsReachesGroupCoordinatorService() throws Exception {
        // FetchOffsets should reach GroupCoordinatorService -> CoordinatorRuntime.
        // Since no shard is loaded, we expect NotCoordinatorException.
        int serverId = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).id();
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(serverId);

        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId("test-group-it");
        PbFetchOffsetEntry entry = fetchRequest.addEntry();
        entry.setTableId(1L);
        entry.setBucketId(0);

        assertThatThrownBy(() -> gateway.fetchOffsets(fetchRequest).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(NotCoordinatorException.class);
    }

    @Test
    void testCommitOffsetsWithEmptyGroupIdReturnsError() throws Exception {
        int serverId = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).id();
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(serverId);

        CommitOffsetsRequest commitRequest = new CommitOffsetsRequest();
        commitRequest.setGroupId("");
        commitRequest.setGenerationId(-1);

        assertThatThrownBy(() -> gateway.commitOffsets(commitRequest).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidGroupIdException.class);
    }
}
