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

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.exception.InvalidGroupIdException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.PbFetchOffsetResultEntry;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for {@link LogScannerImpl#commitSync()}. */
@Execution(ExecutionMode.SAME_THREAD)
class LogScannerCommitSyncITCase extends ClientToServerITCaseBase {

    private static final TablePath CONSUMER_OFFSETS_PATH = TablePath.of("sys", "consumer_offsets");

    @Test
    void testCommitSyncWithoutGroupIdFails() throws Exception {
        TablePath tablePath = TablePath.of("db", "commit_sync_no_group_" + System.nanoTime());
        createSingleBucketTable(tablePath);

        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.subscribe(0, 5L);

            assertThatThrownBy(logScanner::commitSync)
                    .isInstanceOf(InvalidGroupIdException.class)
                    .hasMessageContaining("setGroupId()");
        }
    }

    @Test
    void testCommitSyncBeforePollWithExplicitSubscriptionCommitsOffset() throws Exception {
        waitUntilConsumerOffsetsReady();

        TablePath tablePath = TablePath.of("db", "commit_sync_before_poll_" + System.nanoTime());
        long tableId = createSingleBucketTable(tablePath);

        String groupId = "commit-sync-before-poll-" + System.nanoTime();
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.setGroupId(groupId);
            logScanner.subscribe(0, 100L);
            logScanner.commitSync();
        }

        assertThat(fetchCommittedOffset(groupId, tableId, 0)).isEqualTo(100L);
    }

    @Test
    void testCommitSyncCommitsNextFetchOffsets() throws Exception {
        waitUntilConsumerOffsetsReady();

        TablePath tablePath = TablePath.of("db", "commit_sync_" + System.nanoTime());
        long tableId = createSingleBucketTable(tablePath);

        String groupId = "commit-sync-" + System.nanoTime();
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < 5; i++) {
                appendWriter.append(row(i, "v" + i)).get();
            }

            logScanner.setGroupId(groupId);
            logScanner.subscribeFromBeginning(0);

            int totalCount = 0;
            while (totalCount < 5) {
                totalCount += logScanner.poll(Duration.ofSeconds(1)).count();
            }

            logScanner.commitSync();
        }
        long committedOffset = fetchCommittedOffset(groupId, tableId, 0);
        assertThat(committedOffset).isEqualTo(5L);
    }

    @Test
    void testCommitSyncBeforePositionResolvedDoesNotCommitOffset() throws Exception {
        waitUntilConsumerOffsetsReady();

        TablePath tablePath = TablePath.of("db", "commit_sync_unresolved_" + System.nanoTime());
        long tableId = createSingleBucketTable(tablePath);

        String groupId = "commit-sync-unresolved-" + System.nanoTime();
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.setGroupId(groupId);
            logScanner.subscribeFromBeginning(0);
            logScanner.commitSync();
        }

        assertThat(fetchCommittedOffset(groupId, tableId, 0)).isEqualTo(-1L);
    }

    @Test
    void testCommitSyncWithExplicitOffsetsCommitsProvidedOffset() throws Exception {
        waitUntilConsumerOffsetsReady();

        TablePath tablePath = TablePath.of("db", "commit_sync_explicit_" + System.nanoTime());
        long tableId = createSingleBucketTable(tablePath);

        String groupId = "commit-sync-explicit-" + System.nanoTime();
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.setGroupId(groupId);
            logScanner.subscribeFromBeginning(0);
            logScanner.commitSync(Collections.singletonMap(new TableBucket(tableId, 0), 3L));
        }

        assertThat(fetchCommittedOffset(groupId, tableId, 0)).isEqualTo(3L);
    }

    @Test
    void testCommitSyncAfterResubscribeCommitsLatestExplicitOffset() throws Exception {
        waitUntilConsumerOffsetsReady();

        TablePath tablePath = TablePath.of("db", "commit_sync_resubscribe_" + System.nanoTime());
        long tableId = createSingleBucketTable(tablePath);

        String groupId = "commit-sync-resubscribe-" + System.nanoTime();
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.setGroupId(groupId);
            logScanner.subscribe(0, 100L);
            logScanner.subscribe(0, 200L);
            logScanner.commitSync();
        }

        assertThat(fetchCommittedOffset(groupId, tableId, 0)).isEqualTo(200L);
    }

    @Test
    void testCommitSyncAfterResubscribeToBeginningDoesNotCommitOffset() throws Exception {
        waitUntilConsumerOffsetsReady();

        TablePath tablePath =
                TablePath.of("db", "commit_sync_resubscribe_beginning_" + System.nanoTime());
        long tableId = createSingleBucketTable(tablePath);

        String groupId = "commit-sync-resubscribe-beginning-" + System.nanoTime();
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.setGroupId(groupId);
            logScanner.subscribe(0, 100L);
            logScanner.subscribeFromBeginning(0);
            logScanner.commitSync();
        }

        assertThat(fetchCommittedOffset(groupId, tableId, 0)).isEqualTo(-1L);
    }

    private long createSingleBucketTable(TablePath tablePath) throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build();
        long tableId = createTable(tablePath, descriptor, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        return tableId;
    }

    private void waitUntilConsumerOffsetsReady() throws Exception {
        // With dynamic creation, sys.consumer_offsets is created on first FindCoordinator.
        // In tests, we ensure the table exists by triggering creation and waiting.
        int maxRetries = 60;
        for (int i = 0; i < maxRetries; i++) {
            // Each findCoordinator call triggers creation if the table doesn't exist yet.
            triggerConsumerOffsetsCreation();
            try {
                TableInfo tableInfo = admin.getTableInfo(CONSUMER_OFFSETS_PATH).get();
                if (tableInfo.getTableId() != TableInfo.UNKNOWN_TABLE_ID) {
                    FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
                    FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableInfo.getTableId());
                    return;
                }
            } catch (Exception e) {
                // Table not yet available, retry.
            }
            Thread.sleep(500);
        }
        throw new AssertionError("sys.consumer_offsets was not created within the expected time.");
    }

    private void triggerConsumerOffsetsCreation() throws Exception {
        int serverId = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().get(0).id();
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(serverId);

        org.apache.fluss.rpc.messages.FindCoordinatorRequest request =
                new org.apache.fluss.rpc.messages.FindCoordinatorRequest();
        request.setGroupId("trigger-creation");
        // Fire-and-forget: triggers table creation RPC on the tablet server.
        gateway.findCoordinator(request).get();
    }

    private long fetchCommittedOffset(String groupId, long tableId, int bucketId) throws Exception {
        TabletServerGateway gateway = coordinatorGatewayForGroup(groupId);
        return fetchCommittedOffset(gateway, groupId, tableId, bucketId);
    }

    private long fetchCommittedOffset(
            TabletServerGateway gateway, String groupId, long tableId, int bucketId)
            throws Exception {
        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId(groupId);
        fetchRequest.addEntry().setTableId(tableId).setBucketId(bucketId);

        FetchOffsetsResponse fetchResponse = gateway.fetchOffsets(fetchRequest).get();
        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);

        PbFetchOffsetResultEntry result = fetchResponse.getResultAt(0);
        assertThat(result.getErrorCode()).isEqualTo(Errors.NONE.code());
        return result.getOffset();
    }

    private void commitOffsetForGroup(TablePath tablePath, String groupId, long offset)
            throws Exception {
        try (Table table = conn.getTable(tablePath);
                LogScanner logScanner = table.newScan().createLogScanner()) {
            logScanner.setGroupId(groupId);
            logScanner.subscribe(0, offset);
            logScanner.commitSync();
        }
    }

    private int coordinatorServerIdForGroup(String groupId) throws Exception {
        TableInfo consumerOffsetsTableInfo = admin.getTableInfo(CONSUMER_OFFSETS_PATH).get();
        int bucketId =
                (groupId.hashCode() & Integer.MAX_VALUE) % consumerOffsetsTableInfo.getNumBuckets();
        TableBucket coordinatorBucket =
                new TableBucket(consumerOffsetsTableInfo.getTableId(), bucketId);
        return waitValue(
                () -> {
                    Optional<LeaderAndIsr> leaderAndIsr =
                            FLUSS_CLUSTER_EXTENSION
                                    .getZooKeeperClient()
                                    .getLeaderAndIsr(coordinatorBucket);
                    if (!leaderAndIsr.isPresent() || leaderAndIsr.get().leader() < 0) {
                        return Optional.empty();
                    }
                    return Optional.of(leaderAndIsr.get().leader());
                },
                Duration.ofMinutes(1),
                "Fail to resolve coordinator leader for group " + groupId);
    }

    private TabletServerGateway coordinatorGatewayForGroup(String groupId) throws Exception {
        return FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                coordinatorServerIdForGroup(groupId));
    }
}
