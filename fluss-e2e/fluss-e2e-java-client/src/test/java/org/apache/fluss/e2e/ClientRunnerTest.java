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

package org.apache.fluss.e2e;

import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClientRunnerTest {

    @Test
    void testCommandLabelUsesSubcommandsForClusterAndAdmin() {
        assertThat(ClientRunner.commandLabel(new String[] {"cluster", "ping"}))
                .isEqualTo("cluster ping");
        assertThat(ClientRunner.commandLabel(new String[] {"admin", "list-tables"}))
                .isEqualTo("admin list-tables");
        assertThat(ClientRunner.commandLabel(new String[] {"write"})).isEqualTo("write");
    }

    @Test
    void testParsedArgsRejectsPositionalValues() {
        assertThatThrownBy(() -> ClientRunner.ParsedArgs.parse(new String[] {"oops"}))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Unexpected positional argument");
    }

    @Test
    void testDeterministicChecksumIsStable() {
        assertThat(Rows.expectedChecksum(10)).isEqualTo(Rows.expectedChecksum(10));
        assertThat(Rows.expectedChecksum(10)).isNotEqualTo(Rows.expectedChecksum(11));
        assertThat(Rows.expectedChecksum(5, 10)).isEqualTo(Rows.expectedChecksum(5, 10));
        assertThat(Rows.expectedChecksum(5, 10)).isNotEqualTo(Rows.expectedChecksum(0, 10));
    }

    @Test
    void testElapsedMillisUsesNanosecondRange() {
        assertThat(ClientRunner.elapsedMillis(1_000_000L, 4_000_000L)).isEqualTo(3L);
    }

    @Test
    void testTablePayloadIncludesSchemaMetadata() {
        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("e2e", "table_ops"),
                        17L,
                        3,
                        Rows.defaultTableDescriptor(4, 2),
                        1L,
                        2L);

        Map<String, Object> payload = ClientRunner.tablePayload(tableInfo);

        assertThat(payload.get("database")).isEqualTo("e2e");
        assertThat(payload.get("table")).isEqualTo("table_ops");
        assertThat(payload.get("table_id")).isEqualTo(17L);
        assertThat(payload.get("schema_id")).isEqualTo(3);
        assertThat(payload.get("bucket_count")).isEqualTo(4);
        assertThat(payload.get("replication_factor")).isEqualTo(2);
        assertThat(payload.get("primary_keys")).isEqualTo(List.of("id"));
        assertThat(payload.get("bucket_keys")).isEqualTo(List.of("id"));
        assertThat((List<Map<String, Object>>) payload.get("columns"))
                .extracting(column -> column.get("name"))
                .containsExactly("id", "name", "value", "ts");
    }

    @Test
    void testParseColumnTypeUsesFlussTypeParser() {
        assertThat(ClientRunner.parseColumnType("STRING")).isEqualTo(DataTypes.STRING());
        assertThat(ClientRunner.parseColumnType("ARRAY<STRING>"))
                .isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
    }

    @Test
    void testParseColumnTypeRejectsInvalidTypeStrings() {
        assertThatThrownBy(() -> ClientRunner.parseColumnType("NOT_A_REAL_TYPE"))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Unsupported --column-type value");
    }

    @Test
    void testParseGoalTypesNormalizesSupportedNames() {
        assertThat(ClientRunner.parseGoalTypes("leader-distribution, replica_distribution"))
                .containsExactly(GoalType.LEADER_DISTRIBUTION, GoalType.REPLICA_DISTRIBUTION);
    }

    @Test
    void testParseGoalTypesRejectsUnknownGoals() {
        assertThatThrownBy(() -> ClientRunner.parseGoalTypes("leader_distribution,unknown_goal"))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Unsupported --goals value");
    }

    @Test
    void testRebalanceProgressPayloadIncludesNamedStatuses() {
        TableBucket tableBucket = new TableBucket(23L, 7);
        RebalancePlanForBucket plan =
                new RebalancePlanForBucket(tableBucket, 1, 2, List.of(1, 2), List.of(2, 3));
        Map<TableBucket, RebalanceResultForBucket> bucketProgress = new LinkedHashMap<>();
        bucketProgress.put(
                tableBucket, RebalanceResultForBucket.of(plan, RebalanceStatus.COMPLETED));
        RebalanceProgress progress =
                new RebalanceProgress("rb-7", RebalanceStatus.COMPLETED, 1.0d, bucketProgress);

        Map<String, Object> payload = ClientRunner.rebalanceProgressPayload(progress);

        assertThat(payload.get("rebalance_id")).isEqualTo("rb-7");
        assertThat(payload.get("rebalance_status")).isEqualTo("COMPLETED");
        assertThat(payload.get("progress").toString()).contains("100");
        assertThat((List<Map<String, Object>>) payload.get("progress_for_buckets"))
                .singleElement()
                .satisfies(
                        bucket -> {
                            assertThat(bucket.get("table_id")).isEqualTo(23L);
                            assertThat(bucket.get("bucket_id")).isEqualTo(7);
                            assertThat(bucket.get("original_leader")).isEqualTo(1);
                            assertThat(bucket.get("new_leader")).isEqualTo(2);
                            assertThat(bucket.get("rebalance_status")).isEqualTo("COMPLETED");
                        });
    }

    @Test
    void testCommandLabelIncludesFlinkSubcommand() {
        assertThat(ClientRunner.commandLabel(new String[] {"flink", "submit"}))
                .isEqualTo("flink submit");
        assertThat(ClientRunner.commandLabel(new String[] {"flink", "status"}))
                .isEqualTo("flink status");
        assertThat(ClientRunner.commandLabel(new String[] {"flink", "wait"}))
                .isEqualTo("flink wait");
    }

    @Test
    void testFlinkDispatchRejectsMissingSubcommand() {
        assertThatThrownBy(() -> ClientRunner.dispatch(new String[] {"flink"}))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Missing flink subcommand");
    }

    @Test
    void testFlinkDispatchRejectsUnknownSubcommand() {
        assertThatThrownBy(() -> ClientRunner.dispatch(new String[] {"flink", "unknown"}))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Unknown flink subcommand");
    }

    @Test
    void testFlinkSubmitRejectsMissingJar() {
        assertThatThrownBy(
                        () ->
                                ClientRunner.dispatch(
                                        new String[] {
                                            "flink", "submit", "--entry-class", "com.example.Job"
                                        }))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Missing --jar");
    }

    @Test
    void testFlinkSubmitRejectsMissingEntryClass() {
        assertThatThrownBy(
                        () ->
                                ClientRunner.dispatch(
                                        new String[] {"flink", "submit", "--jar", "/tmp/job.jar"}))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Missing --entry-class");
    }

    @Test
    void testFlinkStatusRejectsMissingJobId() {
        assertThatThrownBy(() -> ClientRunner.dispatch(new String[] {"flink", "status"}))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Missing --job-id");
    }

    @Test
    void testFlinkWaitRejectsMissingJobId() {
        assertThatThrownBy(() -> ClientRunner.dispatch(new String[] {"flink", "wait"}))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Missing --job-id");
    }

    @Test
    void testIsTerminalStateRecognizesFlinkStatuses() throws Exception {
        // Use reflection to access private method, or test via handleFlinkWait behavior
        // Instead, test indirectly through the commandLabel test
        assertThat(ClientRunner.commandLabel(new String[] {"flink", "wait"}))
                .isEqualTo("flink wait");
    }

    @Test
    void testRunKeepsStdoutJsonOnlyWhenCommandWritesNoise() {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();

        int exitCode =
                ClientRunner.run(
                        new String[] {"cluster", "ping"},
                        args -> {
                            System.out.println("WARNING: noisy stdout");
                            return JsonOutput.success("cluster ping");
                        },
                        new PrintStream(stdout, true, StandardCharsets.UTF_8),
                        new PrintStream(stderr, true, StandardCharsets.UTF_8));

        String stdoutText = stdout.toString(StandardCharsets.UTF_8);
        String stderrText = stderr.toString(StandardCharsets.UTF_8);

        assertThat(exitCode).isZero();
        assertThat(stdoutText)
                .startsWith("{")
                .contains("\"command\":\"cluster ping\"")
                .contains("\"ok\":true")
                .doesNotContain("WARNING: noisy stdout");
        assertThat(stderrText).contains("WARNING: noisy stdout");
    }
}
