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
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.kafka.KafkaServiceProtocolTest.request;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
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
    void testDynamicConfigAndRestart() throws Exception {
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
                                                    .column("_kafka_value", DataTypes.STRING())
                                                    .build())
                                    .distributedBy(1)
                                    .logFormat(LogFormat.ARROW)
                                    .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                    .property(
                                            ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(),
                                            "string")
                                    .build(),
                            false)
                    .get();
            TableInfo info = admin.getTableInfo(path).get();
            CLUSTER.waitUntilAllReplicaReady(new TableBucket(info.getTableId(), 0));
            try (Table table = connection.getTable(path)) {
                table.newAppend()
                        .createWriter()
                        .append(GenericRow.of(BinaryString.fromString("1")))
                        .get(10, TimeUnit.SECONDS);
                assertRows(table, 1);
                setEnabled(admin, true);
                try (KafkaProducer<String, String> producer = producer()) {
                    send(producer, path, 2);
                    assertRows(table, 1, 2);
                    setEnabled(admin, false);
                    assertDisabledSockets();
                    table.newAppend()
                            .createWriter()
                            .append(GenericRow.of(BinaryString.fromString("3")))
                            .get(10, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> send(producer, path, 4))
                            .hasRootCauseInstanceOf(
                                    org.apache.kafka.common.errors.InvalidRequestException.class);
                    assertRows(table, 1, 2, 3);
                    setEnabled(admin, true);
                    try (KafkaProducer<String, String> reopened = producer()) {
                        send(reopened, path, 5);
                    }
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
                table.newAppend()
                        .createWriter()
                        .append(GenericRow.of(BinaryString.fromString("6")))
                        .get(10, TimeUnit.SECONDS);
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableAndServiceGates(boolean tableFirst) throws Exception {
        TablePath path = TablePath.of("fluss", "service_aliases");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(path.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(
                            path,
                            TableDescriptor.builder()
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("_kafka_value", DataTypes.STRING())
                                                    .build())
                                    .distributedBy(1)
                                    .logFormat(LogFormat.ARROW)
                                    .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                    .property(
                                            ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(),
                                            "string")
                                    .build(),
                            false)
                    .get();
            TableInfo info = admin.getTableInfo(path).get();
            CLUSTER.waitUntilAllReplicaReady(new TableBucket(info.getTableId(), 0));
            setEnabled(admin, true);
            try (Table table = connection.getTable(path);
                    KafkaProducer<String, String> producer = producer()) {
                org.apache.kafka.common.Node leader =
                        producer.partitionsFor(path.toString()).get(0).leader();
                try (Socket warm = new Socket(leader.host(), leader.port())) {
                    assertAliasMetadata(warm, path, info.getTableId());
                    assertGate(warm, path, Errors.NONE, 1);
                    assertRows(table, 1, 1);
                    if (tableFirst) {
                        setTableEnabled(path, false);
                        assertGate(warm, path, Errors.UNKNOWN_TOPIC_OR_PARTITION, 2);
                    }
                    setEnabled(admin, false);
                    if (!tableFirst) {
                        setTableEnabled(path, false);
                    }
                    assertGate(warm, path, Errors.INVALID_REQUEST, 3);
                    try (Socket freshDisabled = new Socket(leader.host(), leader.port())) {
                        assertGate(freshDisabled, path, Errors.INVALID_REQUEST, 4);
                    }
                    // Enabling the table cannot bypass a closed cluster service, including caches
                    // warmed using both names before disable.
                    setTableEnabled(path, true);
                    assertGate(warm, path, Errors.INVALID_REQUEST, 5);
                    setTableEnabled(path, false);
                    table.newAppend()
                            .createWriter()
                            .append(GenericRow.of(BinaryString.fromString("99")))
                            .get(10, TimeUnit.SECONDS);
                    assertRows(table, 1, 1, 99);
                    setEnabled(admin, true);
                    assertGate(warm, path, Errors.INVALID_REQUEST, 6);
                    try (Socket reopened = new Socket(leader.host(), leader.port())) {
                        assertGate(reopened, path, Errors.UNKNOWN_TOPIC_OR_PARTITION, 7);
                        setTableEnabled(path, true);
                        assertAliasMetadata(reopened, path, info.getTableId());
                        assertGate(reopened, path, Errors.NONE, 8);
                        assertRows(table, 1, 1, 99, 8, 8);
                        assertThat(admin.getTableInfo(path).get().getTableId())
                                .isEqualTo(info.getTableId());
                    }
                }
            } finally {
                setEnabled(admin, false);
                admin.dropTable(path, true).get();
            }
        }
    }

    private static void setTableEnabled(TablePath path, boolean enabled) throws Exception {
        CLUSTER.newCoordinatorClient()
                .alterTable(
                        newAlterTableRequest(
                                path,
                                Collections.singletonMap(
                                        ConfigOptions.TABLE_KAFKA_ENABLED.key(),
                                        Boolean.toString(enabled)),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();
    }

    private static MetadataRequest aliasMetadata(TablePath path) {
        return new MetadataRequest(
                new MetadataRequestData()
                        .setTopics(
                                MetadataRequest.convertToMetadataRequestTopic(
                                        Arrays.asList(path.getTableName(), path.toString()))),
                (short) 10);
    }

    private static void assertAliasMetadata(Socket socket, TablePath path, long tableId)
            throws Exception {
        MetadataResponse response = (MetadataResponse) query(socket, aliasMetadata(path));
        assertThat(response.errorCounts()).containsOnlyKeys(Errors.NONE);
        assertThat(response.data().topics().find(path.getTableName()).topicId())
                .isEqualTo(new Uuid(0x466c757373410000L, tableId));
        assertThat(response.data().topics().find(path.toString()).topicId())
                .isEqualTo(new Uuid(0x466c757373000000L, tableId))
                .isNotEqualTo(response.data().topics().find(path.getTableName()).topicId());
        assertThat(response.data().topics().find(path.getTableName()).partitions())
                .isEqualTo(response.data().topics().find(path.toString()).partitions());
    }

    private static void assertGate(Socket socket, TablePath path, Errors expected, int value)
            throws Exception {
        MetadataResponse metadata = (MetadataResponse) query(socket, aliasMetadata(path));
        assertThat(metadata.data().topics())
                .extracting(topic -> topic.name())
                .containsExactly(path.getTableName(), path.toString());
        assertThat(metadata.errorCounts()).containsOnlyKeys(expected);
        // Exercise both cached aliases on the same connection. Each request targets one name,
        // matching main's native append map, which is keyed by the physical table bucket.
        for (String name : Arrays.asList(path.getTableName(), path.toString())) {
            List<ProduceRequestData.TopicProduceData> topics = new ArrayList<>();
            topics.add(
                    new ProduceRequestData.TopicProduceData()
                            .setName(name)
                            .setPartitionData(
                                    Collections.singletonList(
                                            new ProduceRequestData.PartitionProduceData()
                                                    .setIndex(0)
                                                    .setRecords(
                                                            MemoryRecords.withRecords(
                                                                    Compression.NONE,
                                                                    new SimpleRecord(
                                                                            Integer.toString(value)
                                                                                    .getBytes(
                                                                                            StandardCharsets
                                                                                                    .UTF_8)))))));
            AbstractResponse produce =
                    query(
                            socket,
                            new ProduceRequest(
                                    new ProduceRequestData()
                                            .setAcks((short) -1)
                                            .setTimeoutMs(10_000)
                                            .setTopicData(
                                                    new ProduceRequestData
                                                            .TopicProduceDataCollection(
                                                            topics.iterator())),
                                    (short) 9));
            assertThat(produce.errorCounts()).containsOnlyKeys(expected);
            assertThat(
                            ((org.apache.kafka.common.requests.ProduceResponse) produce)
                                    .data()
                                    .responses())
                    .extracting(topic -> topic.name())
                    .containsExactly(name);
        }
    }

    private static AbstractResponse query(Socket socket, AbstractRequest body) throws Exception {
        RequestHeader header = send(socket, body);
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
        assertThat(length).isBetween(1, 1024 * 1024);
        byte[] response = new byte[length];
        input.readFully(response);
        return AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
    }

    private static RequestHeader send(Socket socket, AbstractRequest body) throws Exception {
        socket.setSoTimeout(15_000);
        RequestHeader header =
                new RequestHeader(body.apiKey(), body.version(), "service-switch", 87);
        ByteBuffer encoded = body.serializeWithHeader(header);
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(bytes.length);
        output.write(bytes);
        output.flush();
        return header;
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
                for (ApiKeys key :
                        new ApiKeys[] {ApiKeys.API_VERSIONS, ApiKeys.METADATA, ApiKeys.PRODUCE}) {
                    short version = key == ApiKeys.API_VERSIONS ? (short) 4 : (short) 9;
                    assertThat(query(socket, request(key, version, (short) 1)).errorCounts())
                            .containsOnlyKeys(Errors.INVALID_REQUEST);
                }
                send(socket, request(ApiKeys.PRODUCE, (short) 9, (short) 0));
                assertThat(socket.getInputStream().read()).isEqualTo(-1);
            }
            for (List<MetadataRequestData.MetadataRequestTopic> topics :
                    Arrays.asList(
                            null,
                            Collections.<MetadataRequestData.MetadataRequestTopic>emptyList())) {
                try (Socket socket = new Socket(node.host(), node.port())) {
                    send(
                            socket,
                            new MetadataRequest(
                                    new MetadataRequestData().setTopics(topics), (short) 9));
                    assertThat(socket.getInputStream().read()).isEqualTo(-1);
                }
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
                    values.add(Integer.parseInt(record.getRow().getString(0).toString()));
                }
            }
            assertThat(values).containsExactly(expected);
            assertThat(scanner.poll(Duration.ofMillis(200)).isEmpty()).isTrue();
        }
    }

    private static void send(KafkaProducer<String, String> producer, TablePath path, int id)
            throws Exception {
        producer.send(new ProducerRecord<>(path.toString(), 0, null, Integer.toString(id)))
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
