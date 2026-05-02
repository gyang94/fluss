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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.FindCoordinatorRequest;
import org.apache.fluss.rpc.messages.FindCoordinatorResponse;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbCommitOffsetEntry;
import org.apache.fluss.rpc.messages.PbFetchOffsetEntry;
import org.apache.fluss.rpc.messages.PbFetchOffsetResultEntry;
import org.apache.fluss.rpc.protocol.Errors;
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
 * <p>Validates dynamic system table creation, coordinator discovery, and commit/fetch offset RPCs
 * on a real Fluss cluster.
 */
class GroupCoordinatorITCase {

    private static final String SYSTEM_DATABASE_NAME = "sys";
    private static final String CONSUMER_OFFSETS_TABLE_NAME = "consumer_offsets";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @Test
    void testSystemTableCreatedDynamically() throws Exception {
        // The consumer_offsets table is created dynamically on the first FindCoordinator request.
        // Trigger creation by sending a FindCoordinator request, then wait for the table to appear.
        triggerConsumerOffsetsCreation();
        waitUntilConsumerOffsetsReady();

        GetTableInfoResponse response = getConsumerOffsetsTableInfo();
        assertThat(response.getTableId()).isGreaterThanOrEqualTo(0);
        assertThat(response.getSchemaId()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testFindCoordinatorReturnsLeaderEndpoint() throws Exception {
        triggerConsumerOffsetsCreation();
        waitUntilConsumerOffsetsReady();

        int serverId = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).id();
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(serverId);

        FindCoordinatorRequest request = new FindCoordinatorRequest();
        request.setGroupId("test-group-it");

        // Retry until the tablet server's metadata cache has the bucket leader info.
        FindCoordinatorResponse response = null;
        int maxRetries = 30;
        for (int i = 0; i < maxRetries; i++) {
            response = gateway.findCoordinator(request).get();
            if (response.getErrorCode() == Errors.NONE.code()) {
                break;
            }
            Thread.sleep(200);
        }
        assertThat(response).isNotNull();
        assertThat(response.getErrorCode()).isEqualTo(Errors.NONE.code());
        assertThat(response.getCoordinatorServerId()).isEqualTo(serverId);
        assertThat(response.getHost())
                .isEqualTo(FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).host());
        assertThat(response.getPort())
                .isEqualTo(FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).port());
    }

    @Test
    void testCommitAndFetchOffsetsSucceed() throws Exception {
        triggerConsumerOffsetsCreation();
        waitUntilConsumerOffsetsReady();

        String groupId = "test-group-it";
        TabletServerGateway gateway = coordinatorGatewayForGroup(groupId);

        CommitOffsetsRequest commitRequest = new CommitOffsetsRequest();
        commitRequest.setGroupId(groupId);
        commitRequest.setGenerationId(-1);
        PbCommitOffsetEntry entry = commitRequest.addOffset();
        entry.setTableId(1L);
        entry.setBucketId(0);
        entry.setOffset(42L);
        entry.setLeaderEpoch(1);

        // Retry commitOffsets until the coordinator shard is loaded on the tablet server.
        CommitOffsetsResponse commitResponse = null;
        int maxRetries = 30;
        for (int i = 0; i < maxRetries; i++) {
            try {
                commitResponse = gateway.commitOffsets(commitRequest).get();
                if (commitResponse.getResultAt(0).getErrorCode() == Errors.NONE.code()) {
                    break;
                }
            } catch (Exception e) {
                // NotCoordinatorException - shard not loaded yet, retry.
            }
            Thread.sleep(200);
        }
        assertThat(commitResponse).isNotNull();
        assertThat(commitResponse.getResultsCount()).isEqualTo(1);
        assertThat(commitResponse.getResultAt(0).getErrorCode()).isEqualTo(Errors.NONE.code());

        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId(groupId);
        PbFetchOffsetEntry fetchEntry = fetchRequest.addEntry();
        fetchEntry.setTableId(1L);
        fetchEntry.setBucketId(0);

        FetchOffsetsResponse fetchResponse = null;
        for (int i = 0; i < maxRetries; i++) {
            try {
                fetchResponse = gateway.fetchOffsets(fetchRequest).get();
                if (fetchResponse.getResultAt(0).getErrorCode() == Errors.NONE.code()) {
                    break;
                }
            } catch (Exception e) {
                // Coordinator shard still loading, retry.
            }
            Thread.sleep(200);
        }
        assertThat(fetchResponse).isNotNull();
        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);
        PbFetchOffsetResultEntry fetchResult = fetchResponse.getResultAt(0);
        assertThat(fetchResult.getErrorCode()).isEqualTo(Errors.NONE.code());
        assertThat(fetchResult.getOffset()).isEqualTo(42L);
        assertThat(fetchResult.getLeaderEpoch()).isEqualTo(1);
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

    /**
     * Triggers the dynamic creation of sys.consumer_offsets by sending a FindCoordinator request.
     * The first request triggers the table creation RPC; subsequent calls will find the table.
     */
    private void triggerConsumerOffsetsCreation() throws Exception {
        int serverId = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).id();
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(serverId);

        FindCoordinatorRequest request = new FindCoordinatorRequest();
        request.setGroupId("trigger-creation");

        // This call triggers table creation; the response will be COORDINATOR_NOT_AVAILABLE.
        FindCoordinatorResponse response = gateway.findCoordinator(request).get();
        // Accept either NONE (table already exists) or COORDINATOR_NOT_AVAILABLE (creation
        // triggered)
        assertThat(response.getErrorCode())
                .isIn(Errors.NONE.code(), Errors.COORDINATOR_NOT_AVAILABLE_EXCEPTION.code());
    }

    private GetTableInfoResponse getConsumerOffsetsTableInfo() throws Exception {
        CoordinatorGateway coordinator = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TablePath consumerOffsetsPath =
                TablePath.of(SYSTEM_DATABASE_NAME, CONSUMER_OFFSETS_TABLE_NAME);
        return coordinator
                .getTableInfo(RpcMessageTestUtils.newGetTableInfoRequest(consumerOffsetsPath))
                .get();
    }

    private void waitUntilConsumerOffsetsReady() throws Exception {
        // Retry until the table is created and metadata is propagated.
        int maxRetries = 30;
        for (int i = 0; i < maxRetries; i++) {
            try {
                GetTableInfoResponse response = getConsumerOffsetsTableInfo();
                if (response.getTableId() >= 0) {
                    FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(response.getTableId());
                    FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
                    return;
                }
            } catch (Exception e) {
                // Table not yet available, retry.
            }
            Thread.sleep(200);
        }
        throw new AssertionError("sys.consumer_offsets was not created within the expected time.");
    }

    private TabletServerGateway coordinatorGatewayForGroup(String groupId) throws Exception {
        int serverId = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).id();
        TabletServerGateway bootstrapGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(serverId);

        FindCoordinatorRequest request = new FindCoordinatorRequest();
        request.setGroupId(groupId);

        // Retry until the coordinator is available (metadata cache may need time to sync).
        int maxRetries = 30;
        for (int i = 0; i < maxRetries; i++) {
            FindCoordinatorResponse response = bootstrapGateway.findCoordinator(request).get();
            if (response.getErrorCode() == Errors.NONE.code()) {
                return FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                        response.getCoordinatorServerId());
            }
            Thread.sleep(200);
        }
        throw new AssertionError(
                "FindCoordinator did not return a valid coordinator within the expected time.");
    }
}
