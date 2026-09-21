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

package org.apache.fluss.kafka;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Dynamic service switching preserves native access and configured listener ownership. */
class KafkaServiceSwitchITCase {
    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @Test
    void testToggleReconnectAndDisabledRestartPreserveNativeAccess() throws Exception {
        TablePath path = TablePath.of("kafka_switch", "records");
        List<ServerNode> endpoints = CLUSTER.getTabletServerNodes("KAFKA");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            assertDisabledSockets();
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("id", DataTypes.INT())
                                                    .build())
                                    .distributedBy(1)
                                    .logFormat(LogFormat.ARROW)
                                    .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                                    .build(),
                            false)
                    .get();
            TableInfo info = admin.getTableInfo(path).get();
            CLUSTER.waitUntilAllReplicaReady(new TableBucket(info.getTableId(), 0));
            try (Table table = connection.getTable(path)) {
                table.newAppend().createWriter().append(GenericRow.of(1)).get(10, TimeUnit.SECONDS);
                assertRows(table, 1);
                setEnabled(admin, true);
                try (KafkaProducer<String, String> producer = producer()) {
                    send(producer, path, 2);
                    assertRows(table, 1, 2);
                    setEnabled(admin, false);
                    assertDisabledSockets();
                    table.newAppend()
                            .createWriter()
                            .append(GenericRow.of(3))
                            .get(10, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> send(producer, path, 4))
                            .hasRootCauseInstanceOf(
                                    org.apache.kafka.common.errors.TimeoutException.class);
                    assertRows(table, 1, 2, 3);
                    setEnabled(admin, true);
                    send(producer, path, 5);
                    assertRows(table, 1, 2, 3, 5);
                }
                assertThat(CLUSTER.getTabletServerNodes("KAFKA"))
                        .containsExactlyInAnyOrderElementsOf(endpoints);
                setEnabled(admin, false);
                Configuration startupEnabled = new Configuration();
                startupEnabled.set(ConfigOptions.KAFKA_ENABLED, true);
                CLUSTER.restartTabletServer(0, startupEnabled);
                CLUSTER.waitUntilAllReplicaReady(new TableBucket(info.getTableId(), 0));
                assertThat(
                                plugin(CLUSTER.getTabletServerById(0))
                                        .getServiceControllerForTesting()
                                        .isEnabled())
                        .isFalse();
                assertDisabledSockets();
                table.newAppend().createWriter().append(GenericRow.of(6)).get(10, TimeUnit.SECONDS);
                assertRows(table, 1, 2, 3, 5, 6);
                admin.alterClusterConfigs(
                                Collections.singletonList(
                                        new AlterConfig(
                                                ConfigOptions.KAFKA_ENABLED.key(),
                                                null,
                                                AlterConfigOpType.DELETE)))
                        .get(30, TimeUnit.SECONDS);
                retry(
                        Duration.ofSeconds(30),
                        () -> {
                            assertThat(
                                            plugin(CLUSTER.getTabletServerById(0))
                                                    .getServiceControllerForTesting()
                                                    .isEnabled())
                                    .isTrue();
                            assertThat(
                                            plugin(CLUSTER.getTabletServerById(1))
                                                    .getServiceControllerForTesting()
                                                    .isEnabled())
                                    .isFalse();
                        });
                setEnabled(admin, true);
                try (KafkaProducer<String, String> producer = producer()) {
                    send(producer, path, 7);
                }
                assertRows(table, 1, 2, 3, 5, 6, 7);
                assertThatThrownBy(
                                () ->
                                        admin.alterClusterConfigs(
                                                        Collections.singletonList(
                                                                new AlterConfig(
                                                                        ConfigOptions.KAFKA_ENABLED
                                                                                .key(),
                                                                        "invalid",
                                                                        AlterConfigOpType.SET)))
                                                .get(30, TimeUnit.SECONDS))
                        .hasMessageContaining("Cannot parse");
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    private static void setEnabled(Admin admin, boolean enabled) throws Exception {
        admin.alterClusterConfigs(
                        Collections.singletonList(
                                new AlterConfig(
                                        ConfigOptions.KAFKA_ENABLED.key(),
                                        Boolean.toString(enabled),
                                        AlterConfigOpType.SET)))
                .get(30, TimeUnit.SECONDS);
        retry(
                Duration.ofSeconds(30),
                () ->
                        assertThat(CLUSTER.getTabletServers())
                                .allSatisfy(
                                        server ->
                                                assertThat(
                                                                plugin(server)
                                                                        .getServiceControllerForTesting()
                                                                        .isEnabled())
                                                        .isEqualTo(enabled)));
    }

    private static KafkaProtocolPlugin plugin(TabletServer server) {
        return (KafkaProtocolPlugin)
                server.getRpcServer().getServerReconfigurables().stream()
                        .filter(KafkaProtocolPlugin.class::isInstance)
                        .findFirst()
                        .get();
    }

    private static void assertDisabledSockets() throws Exception {
        for (ServerNode node : CLUSTER.getTabletServerNodes("KAFKA")) {
            try (Socket socket = new Socket(node.host(), node.port())) {
                socket.setSoTimeout(5000);
                assertThat(socket.getInputStream().read()).isEqualTo(-1);
            }
        }
    }

    private static void assertRows(Table table, Integer... expected) throws Exception {
        List<Integer> values = new ArrayList<>();
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (values.size() < expected.length && System.nanoTime() < deadline) {
                for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                    values.add(record.getRow().getInt(0));
                }
            }
            assertThat(values).containsExactly(expected);
            assertThat(scanner.poll(Duration.ofMillis(200)).isEmpty()).isTrue();
        }
    }

    private static void send(KafkaProducer<String, String> producer, TablePath path, int id)
            throws Exception {
        producer.send(new ProducerRecord<>(path.toString(), 0, null, "{\"id\":" + id + "}"))
                .get(10, TimeUnit.SECONDS);
    }

    private static KafkaProducer<String, String> producer() {
        Map<String, Object> config = new HashMap<>();
        config.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                CLUSTER.getTabletServerNodes("KAFKA").stream()
                        .map(node -> node.host() + ":" + node.port())
                        .collect(Collectors.joining(",")));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2000);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        return new KafkaProducer<>(config);
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, false);
        config.set(ConfigOptions.KAFKA_SERVICE_DRAIN_TIMEOUT, Duration.ofSeconds(1));
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return config;
    }
}
