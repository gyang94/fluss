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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration test for Kafka SASL_PLAINTEXT with the PLAIN mechanism. */
public class KafkaSaslPlainAuthenticationITCase {

    private static final String DATABASE = "kafka";
    private static final String TABLE_NAME = "sasl-plain-topic";
    private static final String TOPIC = DATABASE + "." + TABLE_NAME;
    private static final String USERNAME = "writer";
    private static final String PASSWORD = "writer-secret";
    private static final byte[] KEY = "authenticated-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE =
            "{\"message\":\"authenticated-value\",\"count\":42}".getBytes(StandardCharsets.UTF_8);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    private Connection connection;
    private org.apache.fluss.client.admin.Admin flussAdmin;
    private String bootstrapServers;

    @BeforeEach
    public void setup() throws Exception {
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        flussAdmin = connection.getAdmin();
        flussAdmin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
        flussAdmin
                .createTable(
                        TablePath.of(DATABASE, TABLE_NAME),
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("key", DataTypes.BYTES())
                                                .column("message", DataTypes.STRING())
                                                .column("count", DataTypes.INT())
                                                .build())
                                .distributedBy(1)
                                .logFormat(LogFormat.ARROW)
                                .customProperty("kafka.key.format", "raw")
                                .customProperty("kafka.key.fields", "key")
                                .customProperty("kafka.value.format", "json")
                                .customProperty("kafka.value.fields-include", "EXCEPT_KEY")
                                .build(),
                        false)
                .get();
        long tableId =
                flussAdmin.getTableInfo(TablePath.of(DATABASE, TABLE_NAME)).get().getTableId();
        // The raw socket tests do not retry Produce while bucket leadership is being initialized.
        FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(tableId, 0));
        bootstrapServers =
                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("KAFKA").stream()
                        .map(node -> node.host() + ":" + node.port())
                        .collect(Collectors.joining(","));
    }

    @AfterEach
    public void teardown() throws Exception {
        if (flussAdmin != null) {
            try {
                flussAdmin.dropTable(TablePath.of(DATABASE, TABLE_NAME), true).get();
            } catch (Exception ignored) {
                // Preserve the primary test failure when cleanup cannot complete.
            }
            try {
                Collection<AclBinding> deletedAcls =
                        flussAdmin
                                .dropAcls(Collections.singletonList(AclBindingFilter.ANY))
                                .all()
                                .get();
                // The next test reuses the same principal and table path on this cluster.
                FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(deletedAcls, false);
            } catch (Exception ignored) {
                // Preserve the primary test failure when cleanup cannot complete.
            }
            flussAdmin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testAuthenticatedMetadataAndJsonProduce() throws Exception {
        grantWriterAccess(OperationType.ALL);
        try (Admin admin = Admin.create(kafkaClientConfig(USERNAME, PASSWORD));
                KafkaProducer<byte[], byte[]> producer = producer()) {
            assertThat(
                            admin.describeTopics(Collections.singleton(TOPIC))
                                    .allTopicNames()
                                    .get(30, TimeUnit.SECONDS))
                    .containsKey(TOPIC);
            assertThat(
                            producer.send(new ProducerRecord<>(TOPIC, KEY, VALUE))
                                    .get(30, TimeUnit.SECONDS))
                    .isNotNull();
            assertFlussRecord();
        }
    }

    @Test
    public void testMetadataHidesTablesWithoutDescribePermission() throws Exception {
        try (Admin admin = Admin.create(kafkaClientConfig(USERNAME, PASSWORD))) {
            // Native Fluss Metadata omits unauthorized tables, keeping their existence private.
            assertThat(admin.listTopics().names().get(30, TimeUnit.SECONDS)).doesNotContain(TOPIC);
            assertThatThrownBy(
                            () ->
                                    admin.describeTopics(Collections.singleton(TOPIC))
                                            .allTopicNames()
                                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(UnknownTopicOrPartitionException.class);
        }
    }

    @Test
    public void testMetadataPermissionDoesNotGrantProducePermission() throws Exception {
        grantWriterAccess(OperationType.DESCRIBE);
        try (KafkaProducer<byte[], byte[]> producer = producer()) {
            assertThat(producer.partitionsFor(TOPIC)).hasSize(1);
            assertThatThrownBy(
                            () ->
                                    producer.send(new ProducerRecord<>(TOPIC, KEY, VALUE))
                                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TopicAuthorizationException.class);
        }
        try (Table table = connection.getTable(TablePath.of(DATABASE, TABLE_NAME));
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            assertThat(scanner.poll(Duration.ofSeconds(1)).isEmpty()).isTrue();
        }
    }

    @Test
    public void testV0RawAuthenticationMetadataAndJsonProduce() throws Exception {
        grantWriterAccess(OperationType.ALL);
        try (Socket socket = connectKafka()) {
            authenticateV0(socket);
            MetadataResponse metadata = (MetadataResponse) sendRequest(socket, metadataRequest());
            assertSuccessfulMetadata(metadata);
            ProduceResponse produced = (ProduceResponse) sendRequest(socket, produceRequest());
            assertThat(produced.errorCounts()).containsOnlyKeys(Errors.NONE);
            assertFlussRecord();
        }
    }

    @Test
    public void testV0RawAuthenticationPreservesMetadataPermissions() throws Exception {
        try (Socket socket = connectKafka()) {
            authenticateV0(socket);
            MetadataResponse metadata = (MetadataResponse) sendRequest(socket, metadataRequest());
            assertThat(metadata.errors()).containsEntry(TOPIC, Errors.UNKNOWN_TOPIC_OR_PARTITION);
        }
    }

    @Test
    public void testV0RawAuthenticationPreservesProducePermissions() throws Exception {
        grantWriterAccess(OperationType.DESCRIBE);
        try (Socket socket = connectKafka()) {
            authenticateV0(socket);
            MetadataResponse metadata = (MetadataResponse) sendRequest(socket, metadataRequest());
            assertSuccessfulMetadata(metadata);
            ProduceResponse produced = (ProduceResponse) sendRequest(socket, produceRequest());
            assertThat(produced.errorCounts()).containsOnlyKeys(Errors.TOPIC_AUTHORIZATION_FAILED);
        }
        try (Table table = connection.getTable(TablePath.of(DATABASE, TABLE_NAME));
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            assertThat(scanner.poll(Duration.ofSeconds(1)).isEmpty()).isTrue();
        }
    }

    @Test
    public void testV0WrongPasswordClosesWithoutKafkaResponse() throws Exception {
        try (Socket socket = connectKafka()) {
            handshakeV0(socket);
            writeRawToken(socket, "\u0000" + USERNAME + "\u0000wrong-password");
            // v0 has no SaslAuthenticate error envelope; the server terminates the connection.
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        }
    }

    @Test
    public void testV0UnsupportedMechanismIsFlushedBeforeClose() throws Exception {
        try (Socket socket = connectKafka()) {
            SaslHandshakeResponse response =
                    (SaslHandshakeResponse)
                            sendRequest(
                                    socket,
                                    new SaslHandshakeRequest(
                                            new SaslHandshakeRequestData()
                                                    .setMechanism("SCRAM-SHA-256"),
                                            (short) 0));
            assertThat(response.error()).isEqualTo(Errors.UNSUPPORTED_SASL_MECHANISM);
            assertThat(response.data().mechanisms()).containsExactly("PLAIN");
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        }
    }

    private static Socket connectKafka() throws Exception {
        ServerNode node = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("KAFKA").get(0);
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(node.host(), node.port()), 5000);
            socket.setSoTimeout(5000);
            return socket;
        } catch (Exception failure) {
            socket.close();
            throw failure;
        }
    }

    private static void authenticateV0(Socket socket) throws Exception {
        handshakeV0(socket);
        writeRawToken(socket, "\u0000" + USERNAME + "\u0000" + PASSWORD);
        // The final PLAIN challenge is a zero-length frame, without a Kafka response header.
        assertThat(new DataInputStream(socket.getInputStream()).readInt()).isZero();
    }

    private static void handshakeV0(Socket socket) throws Exception {
        ApiVersionsResponse versions =
                (ApiVersionsResponse)
                        sendRequest(socket, new ApiVersionsRequest.Builder().build((short) 0));
        assertThat(versions.apiVersion(ApiKeys.SASL_HANDSHAKE.id).minVersion()).isZero();
        assertThat(versions.apiVersion(ApiKeys.SASL_HANDSHAKE.id).maxVersion())
                .isEqualTo((short) 1);
        SaslHandshakeResponse handshake =
                (SaslHandshakeResponse)
                        sendRequest(
                                socket,
                                new SaslHandshakeRequest(
                                        new SaslHandshakeRequestData().setMechanism("PLAIN"),
                                        (short) 0));
        assertThat(handshake.error()).isEqualTo(Errors.NONE);
        assertThat(handshake.data().mechanisms()).containsExactly("PLAIN");
    }

    private static void writeRawToken(Socket socket, String token) throws Exception {
        byte[] bytes = token.getBytes(StandardCharsets.UTF_8);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(bytes.length);
        output.write(bytes);
        output.flush();
    }

    private static void assertSuccessfulMetadata(MetadataResponse metadata) {
        assertThat(metadata.data().topics())
                .singleElement()
                .satisfies(
                        topic -> {
                            assertThat(topic.name()).isEqualTo(TOPIC);
                            assertThat(topic.errorCode()).isEqualTo(Errors.NONE.code());
                            assertThat(topic.partitions()).hasSize(1);
                        });
    }

    private static MetadataRequest metadataRequest() {
        return new MetadataRequest.Builder(Collections.singletonList(TOPIC), false)
                .build((short) 9);
    }

    private static ProduceRequest produceRequest() {
        MemoryRecords records =
                MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(KEY, VALUE));
        ProduceRequestData.TopicProduceData topicData =
                new ProduceRequestData.TopicProduceData()
                        .setName(TOPIC)
                        .setPartitionData(
                                Collections.singletonList(
                                        new ProduceRequestData.PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(records)));
        return new ProduceRequest(
                new ProduceRequestData()
                        .setAcks((short) 1)
                        .setTimeoutMs(5000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        Collections.singletonList(topicData).iterator())),
                (short) 9);
    }

    private static AbstractResponse sendRequest(Socket socket, AbstractRequest request)
            throws Exception {
        RequestHeader header =
                new RequestHeader(request.apiKey(), request.version(), "sasl-v0-test", 1);
        ByteBuffer buffer =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(bytes.length);
        output.write(bytes);
        output.flush();
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int size = input.readInt();
        assertThat(size).isBetween(1, 1024 * 1024);
        byte[] response = new byte[size];
        input.readFully(response);
        return AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
    }

    private KafkaProducer<byte[], byte[]> producer() {
        Map<String, Object> config = kafkaClientConfig(USERNAME, PASSWORD);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        return new KafkaProducer<>(config);
    }

    @Test
    public void testWrongPasswordIsRejectedAsAuthenticationFailure() {
        try (Admin admin = Admin.create(kafkaClientConfig(USERNAME, "wrong-password"))) {
            assertThatThrownBy(() -> admin.describeCluster().nodes().get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(SaslAuthenticationException.class);
        }
    }

    private void assertFlussRecord() throws Exception {
        try (Table table = connection.getTable(TablePath.of(DATABASE, TABLE_NAME));
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            for (int attempt = 0; attempt < 30; attempt++) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    assertThat(record.getRow().getBytes(0)).containsExactly(KEY);
                    assertThat(record.getRow().getString(1).toString())
                            .isEqualTo("authenticated-value");
                    assertThat(record.getRow().getInt(2)).isEqualTo(42);
                    return;
                }
            }
        }
        throw new AssertionError("Authenticated Kafka record was not visible through Fluss.");
    }

    private void grantWriterAccess(OperationType operation) throws Exception {
        AclBinding aclBinding =
                new AclBinding(
                        Resource.table(TablePath.of(DATABASE, TABLE_NAME)),
                        new AccessControlEntry(
                                new FlussPrincipal(USERNAME, "User"),
                                AccessControlEntry.WILD_CARD_HOST,
                                operation,
                                PermissionType.ALLOW));
        flussAdmin.createAcls(Collections.singletonList(aclBinding)).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(
                Collections.singletonList(aclBinding), true);
    }

    private Map<String, Object> kafkaClientConfig(String username, String password) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        config.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                String.format(
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                        username, password));
        return config;
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        config.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "sasl"));
        config.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList("PLAIN"));
        config.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS,
                Collections.singletonMap(USERNAME, PASSWORD));
        config.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        config.set(ConfigOptions.SUPER_USERS, "User:ANONYMOUS");
        return config;
    }
}
