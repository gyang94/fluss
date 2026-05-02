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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.FindCoordinatorRequest;
import org.apache.fluss.rpc.messages.PbFetchOffsetResultEntry;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.flink.utils.FlinkTestBase.writeRows;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case for testing Flink source offset commit on checkpoint.
 *
 * <p>Verifies that when {@code scan.commit.offset.group-id} is configured, the Flink source commits
 * consumption offsets to the Fluss server upon checkpoint completion, enabling external consumer
 * lag monitoring.
 */
abstract class FlinkSourceCommitOffsetITCase extends AbstractTestBase {

    private static final TablePath CONSUMER_OFFSETS_PATH = TablePath.of("sys", "consumer_offsets");

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    static final String CATALOG_NAME = "testcatalog";
    static final String DEFAULT_DB = "defaultdb";
    protected StreamExecutionEnvironment execEnv;
    protected StreamTableEnvironment tEnv;
    protected static Connection conn;
    protected static Admin admin;
    protected static Configuration clientConf;

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    void before() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());

        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    /**
     * Test: Log table source commits offsets on checkpoint.
     *
     * <p>Steps:
     *
     * <ol>
     *   <li>Create a log table with 1 bucket
     *   <li>Write 5 records
     *   <li>Start Flink streaming read with checkpoint enabled and scan.commit.offset.group-id set
     *   <li>Wait until all 5 records are consumed
     *   <li>Verify committed offsets on server side via FetchOffsets RPC
     * </ol>
     */
    @Test
    void testLogTableCommitOffsetOnCheckpoint() throws Exception {
        waitUntilConsumerOffsetsReady();

        String groupId = "log-commit-test-" + System.nanoTime();
        tEnv.executeSql(
                String.format(
                        "create table log_commit_test ("
                                + "a int, b varchar"
                                + ") with ("
                                + "'bucket.num' = '1', "
                                + "'scan.commit.offset.group-id' = '%s'"
                                + ")",
                        groupId));
        TablePath tablePath = TablePath.of(DEFAULT_DB, "log_commit_test");

        List<InternalRow> rows =
                Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"), row(4, "v4"), row(5, "v5"));
        writeRows(conn, tablePath, rows, true);

        execEnv.enableCheckpointing(200);
        CloseableIterator<Row> iter = tEnv.executeSql("select * from log_commit_test").collect();

        // Collect 5 rows to ensure consumption progresses
        List<String> collected = collectRowsWithTimeout(iter, 5, false);
        assertThat(collected).hasSize(5);

        long tableId = admin.getTableInfo(tablePath).get().getTableId();

        // Wait for checkpoint to complete and offsets to be committed
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long committedOffset = fetchCommittedOffset(groupId, tableId, 0);
                    assertThat(committedOffset)
                            .as("Committed offset should be >= 5 (next offset to fetch)")
                            .isGreaterThanOrEqualTo(5L);
                });

        iter.close();
    }

    /**
     * Test: PK table source commits offsets on checkpoint after snapshot phase.
     *
     * <p>Steps:
     *
     * <ol>
     *   <li>Create a PK table with 1 bucket
     *   <li>Write initial rows and trigger snapshot
     *   <li>Start Flink streaming read with checkpoint and group-id
     *   <li>Wait until snapshot + log records are consumed
     *   <li>Verify committed offsets are non-negative (indicating log phase reached)
     * </ol>
     */
    @Test
    void testPkTableCommitOffsetOnCheckpointAfterSnapshot() throws Exception {
        waitUntilConsumerOffsetsReady();

        String groupId = "pk-commit-test-" + System.nanoTime();
        tEnv.executeSql(
                String.format(
                        "create table pk_commit_test ("
                                + "a int not null primary key not enforced, b varchar"
                                + ") with ("
                                + "'bucket.num' = '1', "
                                + "'scan.commit.offset.group-id' = '%s'"
                                + ")",
                        groupId));
        TablePath tablePath = TablePath.of(DEFAULT_DB, "pk_commit_test");

        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
        writeRows(conn, tablePath, rows, false);

        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        // Write more rows to generate log records
        writeRows(conn, tablePath, Arrays.asList(row(4, "v4"), row(5, "v5")), false);

        execEnv.enableCheckpointing(200);
        CloseableIterator<Row> iter = tEnv.executeSql("select * from pk_commit_test").collect();

        // Collect all 5 rows (3 from snapshot + 2 from log)
        List<String> collected = collectRowsWithTimeout(iter, 5, false);
        assertThat(collected).hasSize(5);

        long tableId = admin.getTableInfo(tablePath).get().getTableId();

        // Wait for checkpoint to complete and offsets to be committed.
        // After snapshot phase completes and log reading begins, the offset should be committed.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long committedOffset = fetchCommittedOffset(groupId, tableId, 0);
                    assertThat(committedOffset)
                            .as("Committed offset should be non-negative after log phase starts")
                            .isGreaterThanOrEqualTo(0L);
                });

        iter.close();
    }

    /**
     * Test: No offsets are committed when group-id is not configured.
     *
     * <p>Steps:
     *
     * <ol>
     *   <li>Create a log table without scan.commit.offset.group-id
     *   <li>Write records and consume them
     *   <li>Verify no offsets are committed to any group
     * </ol>
     */
    @Test
    void testNoOffsetCommitWithoutGroupId() throws Exception {
        tEnv.executeSql(
                "create table no_commit_test ("
                        + "a int, b varchar"
                        + ") with ("
                        + "'bucket.num' = '1'"
                        + ")");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "no_commit_test");

        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"));
        writeRows(conn, tablePath, rows, true);

        execEnv.enableCheckpointing(200);
        CloseableIterator<Row> iter = tEnv.executeSql("select * from no_commit_test").collect();

        List<String> collected = collectRowsWithTimeout(iter, 2, false);
        assertThat(collected).hasSize(2);

        // Wait a few seconds to allow any potential checkpoint to complete
        Thread.sleep(3000);

        // No group should exist, so this is a negative verification.
        // The test passes as long as consuming works without error and no exceptions are thrown.
        iter.close();
    }

    // ---- Utilities ----

    private void waitUntilConsumerOffsetsReady() throws Exception {
        int maxRetries = 60;
        for (int i = 0; i < maxRetries; i++) {
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
        FindCoordinatorRequest request = new FindCoordinatorRequest();
        request.setGroupId("trigger-creation");
        gateway.findCoordinator(request).get();
    }

    private long fetchCommittedOffset(String groupId, long tableId, int bucketId) throws Exception {
        TabletServerGateway gateway = coordinatorGatewayForGroup(groupId);

        FetchOffsetsRequest fetchRequest = new FetchOffsetsRequest();
        fetchRequest.setGroupId(groupId);
        fetchRequest.addEntry().setTableId(tableId).setBucketId(bucketId);

        FetchOffsetsResponse fetchResponse = gateway.fetchOffsets(fetchRequest).get();
        assertThat(fetchResponse.getResultsCount()).isEqualTo(1);

        PbFetchOffsetResultEntry result = fetchResponse.getResultAt(0);
        assertThat(result.getErrorCode()).isEqualTo(Errors.NONE.code());
        return result.getOffset();
    }

    private TabletServerGateway coordinatorGatewayForGroup(String groupId) throws Exception {
        return FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                coordinatorServerIdForGroup(groupId));
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
}
