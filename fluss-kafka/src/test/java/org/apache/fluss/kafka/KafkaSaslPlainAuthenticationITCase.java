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
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
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
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration test for Kafka SASL_PLAINTEXT with the PLAIN mechanism. */
public class KafkaSaslPlainAuthenticationITCase {

    private static final String DATABASE = "fluss";
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
        flussAdmin
                .createTable(
                        TablePath.of(DATABASE, TABLE_NAME),
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("_kafka_key", DataTypes.BYTES())
                                                .column("message", DataTypes.STRING())
                                                .column("count", DataTypes.INT())
                                                .build())
                                .distributedBy(1)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(),
                                        "parse_json")
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
                            producer.send(new ProducerRecord<byte[], byte[]>(TOPIC, KEY, null))
                                    .get(30, TimeUnit.SECONDS)
                                    .offset())
                    .isZero();
            assertThat(
                            producer.send(new ProducerRecord<>(TOPIC, KEY, VALUE))
                                    .get(30, TimeUnit.SECONDS)
                                    .offset())
                    .isEqualTo(1L);
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
                                    producer.send(
                                                    new ProducerRecord<byte[], byte[]>(
                                                            TOPIC, KEY, null))
                                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TopicAuthorizationException.class);
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
            ProduceResponse nullWrite = (ProduceResponse) sendRequest(socket, produceRequest(null));
            assertThat(nullWrite.errorCounts()).containsOnlyKeys(Errors.NONE);
            assertThat(
                            nullWrite
                                    .data()
                                    .responses()
                                    .iterator()
                                    .next()
                                    .partitionResponses()
                                    .get(0)
                                    .baseOffset())
                    .isZero();
            ProduceResponse produced = (ProduceResponse) sendRequest(socket, produceRequest());
            assertPartition(produced, 0, Errors.NONE, 1L);
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

    @ParameterizedTest(name = "SASL v{0} all-null WRITE authorization")
    @ValueSource(shorts = {0, 1})
    public void testAuthenticatedNullOnlyProduceRequiresWrite(short version) throws Exception {
        grantWriterAccess(OperationType.DESCRIBE);
        try (Socket socket = connectKafka()) {
            authenticate(socket, version);
            MetadataResponse metadata = (MetadataResponse) sendRequest(socket, metadataRequest());
            assertSuccessfulMetadata(metadata);
            for (String topic : Arrays.asList(TOPIC, TABLE_NAME)) {
                ProduceRequest request =
                        produceRequest(
                                TablePath.of(DATABASE, TABLE_NAME),
                                contractPartition(0, (String) null, null));
                request.data().topicData().iterator().next().setName(topic);
                assertPartition(
                        (ProduceResponse) sendRequest(socket, request),
                        0,
                        Errors.TOPIC_AUTHORIZATION_FAILED,
                        -1L);
            }
            assertNativeRecords(TablePath.of(DATABASE, TABLE_NAME), 0, 0, record -> {});
            grantWriterAccess(OperationType.WRITE);
            for (String topic : Arrays.asList(TABLE_NAME, TOPIC)) {
                ProduceRequest request =
                        produceRequest(
                                TablePath.of(DATABASE, TABLE_NAME),
                                contractPartition(0, (String) null, null));
                request.data().topicData().iterator().next().setName(topic);
                assertPartition(
                        (ProduceResponse) sendRequest(socket, request),
                        0,
                        Errors.NONE,
                        topic.equals(TABLE_NAME) ? 0L : 2L);
            }
        }
        assertNativeRecords(
                TablePath.of(DATABASE, TABLE_NAME),
                0,
                4,
                record -> {
                    assertThat(record.getRow().getBytes(0)).containsExactly(KEY);
                    assertThat(record.getRow().isNullAt(1)).isTrue();
                    assertThat(record.getRow().isNullAt(2)).isTrue();
                });
    }

    @ParameterizedTest(name = "strict Rescue after SASL handshake v{0}")
    @ValueSource(shorts = {0, 1})
    public void testAuthenticatedRescueRejectsInvalidPartitionWithoutPartialAppend(
            short handshakeVersion) throws Exception {
        TablePath path = TablePath.of(DATABASE, "sasl-rescue-topic");
        flussAdmin
                .createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("_kafka_key", DataTypes.BYTES())
                                                .column("message", DataTypes.STRING())
                                                .column("count", DataTypes.INT())
                                                .column("_kafka_rescue", DataTypes.STRING())
                                                .build())
                                .distributedBy(2)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(),
                                        "parse_json")
                                .build(),
                        false)
                .get();
        try {
            long tableId = flussAdmin.getTableInfo(path).get().getTableId();
            for (int bucket = 0; bucket < 2; bucket++) {
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(tableId, bucket));
            }
            grantWriterAccess(path, OperationType.ALL);
            try (Socket socket = connectKafka()) {
                if (handshakeVersion == 0) {
                    authenticateV0(socket);
                } else {
                    authenticateV1(socket);
                }
                ProduceResponse mixed =
                        (ProduceResponse)
                                sendRequest(
                                        socket,
                                        produceRequest(
                                                path,
                                                jsonPartition(
                                                        0,
                                                        "{\"message\":\"must-not-append\",\"count\":7,\"extra\":true}",
                                                        "{\"message\":\"invalid\",\"count\":\"bad\",\"_kafka_rescue\":null,\"extra\":9}"),
                                                jsonPartition(
                                                        1,
                                                        "{\"message\":\"healthy\",\"count\":7,\"extra\":{\"source\":\"partition-1\"},\"_kafka_rescue\":null}")));
                assertThat(mixed.data().responses())
                        .singleElement()
                        .satisfies(
                                topic -> {
                                    assertThat(topic.name()).isEqualTo(path.toString());
                                    assertThat(topic.partitionResponses())
                                            .satisfiesExactly(
                                                    bad -> {
                                                        assertThat(bad.index()).isZero();
                                                        assertThat(bad.errorCode())
                                                                .isEqualTo(
                                                                        Errors.INVALID_RECORD
                                                                                .code());
                                                        assertThat(bad.baseOffset()).isEqualTo(-1L);
                                                    },
                                                    good -> {
                                                        assertThat(good.index()).isEqualTo(1);
                                                        assertThat(good.errorCode())
                                                                .isEqualTo(Errors.NONE.code());
                                                        assertThat(good.baseOffset()).isZero();
                                                    });
                                });

                // Reuse the authenticated connection. Offset zero proves that even the valid
                // prefix of the rejected partition was never appended.
                ProduceResponse recovered =
                        (ProduceResponse)
                                sendRequest(
                                        socket,
                                        produceRequest(
                                                path,
                                                jsonPartition(
                                                        0,
                                                        "{\"message\":\"recovered\",\"count\":42,\"extra\":{\"source\":\"partition-0\"},\"_kafka_rescue\":\"payload\"}",
                                                        "{\"message\":\"nullable\",\"count\":null,\"_kafka_rescue\":null}")));
                assertThat(recovered.data().responses())
                        .singleElement()
                        .satisfies(
                                topic -> {
                                    assertThat(topic.name()).isEqualTo(path.toString());
                                    assertThat(topic.partitionResponses())
                                            .singleElement()
                                            .satisfies(
                                                    partition -> {
                                                        assertThat(partition.index()).isZero();
                                                        assertThat(partition.errorCode())
                                                                .isEqualTo(Errors.NONE.code());
                                                        assertThat(partition.baseOffset()).isZero();
                                                    });
                                });
                assertNativeRecords(
                        path,
                        0,
                        2,
                        record -> {
                            InternalRow row = record.getRow();
                            assertThat(row.getBytes(0)).containsExactly(KEY);
                            if (record.logOffset() == 0) {
                                assertThat(row.getString(1).toString()).isEqualTo("recovered");
                                assertThat(row.getInt(2)).isEqualTo(42);
                                assertThat(row.getString(3).toString())
                                        .isEqualTo(
                                                "{\"extra\":{\"source\":\"partition-0\"},\"_kafka_rescue\":\"payload\"}");
                            } else {
                                assertThat(row.getString(1).toString()).isEqualTo("nullable");
                                assertThat(row.isNullAt(2)).isTrue();
                                assertThat(row.getString(3).toString())
                                        .isEqualTo("{\"_kafka_rescue\":null}");
                            }
                        });
                assertNativeRecords(
                        path,
                        1,
                        1,
                        record -> {
                            InternalRow row = record.getRow();
                            assertThat(row.getBytes(0)).containsExactly(KEY);
                            assertThat(row.getString(1).toString()).isEqualTo("healthy");
                            assertThat(row.getInt(2)).isEqualTo(7);
                            assertThat(row.getString(3).toString())
                                    .isEqualTo(
                                            "{\"extra\":{\"source\":\"partition-1\"},\"_kafka_rescue\":null}");
                        });
            }
        } finally {
            flussAdmin.dropTable(path, true).get();
        }
    }

    @Test
    public void testV0WrongPasswordClosesWithoutKafkaResponse() throws Exception {
        try (Socket socket = connectKafka()) {
            handshake(socket, (short) 0);
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

    @Test
    public void testTwoUsersAndCachedAliasesStillRequireWriteForNullValues() throws Exception {
        TablePath path = TablePath.of(DATABASE, TABLE_NAME);
        grantWriterAccess(OperationType.ALL);
        grantAccess(path, "reader", OperationType.DESCRIBE);
        try (KafkaProducer<byte[], byte[]> writer = producer();
                KafkaProducer<byte[], byte[]> reader = producer("reader", "reader-secret")) {
            for (String topic : Arrays.asList(TOPIC, TABLE_NAME, TOPIC, TABLE_NAME)) {
                assertThat(reader.partitionsFor(topic)).hasSize(1);
                assertDeniedValues(reader, topic);
                writer.send(new ProducerRecord<byte[], byte[]>(topic, KEY, null))
                        .get(30, TimeUnit.SECONDS);
                writer.send(new ProducerRecord<>(topic, KEY, VALUE)).get(30, TimeUnit.SECONDS);
            }
            Collection<AclBinding> removed =
                    flussAdmin
                            .dropAcls(Collections.singletonList(AclBindingFilter.ANY))
                            .all()
                            .get();
            FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(removed, false);
            grantWriterAccess(OperationType.DESCRIBE);
            // The same producer has already populated both topic caches and authenticated sockets.
            for (String topic : Arrays.asList(TOPIC, TABLE_NAME)) {
                assertDeniedValues(writer, topic);
            }
            grantAccess(path, "reader", OperationType.ALL);
            for (String topic : Arrays.asList(TABLE_NAME, TOPIC)) {
                reader.send(new ProducerRecord<byte[], byte[]>(topic, KEY, null))
                        .get(30, TimeUnit.SECONDS);
                assertDeniedValues(writer, topic);
            }
        }
        assertNativeRecords(
                path,
                0,
                10,
                record -> {
                    assertThat(record.getRow().getBytes(0)).containsExactly(KEY);
                    if (record.logOffset() >= 8 || record.logOffset() % 2 == 0) {
                        assertThat(record.getRow().isNullAt(1)).isTrue();
                        assertThat(record.getRow().isNullAt(2)).isTrue();
                    } else {
                        assertThat(record.getRow().getString(1).toString())
                                .isEqualTo("authenticated-value");
                        assertThat(record.getRow().getInt(2)).isEqualTo(42);
                    }
                });
    }

    private static void assertDeniedValues(KafkaProducer<byte[], byte[]> producer, String topic) {
        for (byte[] value : new byte[][] {null, VALUE}) {
            assertThatThrownBy(
                            () ->
                                    producer.send(new ProducerRecord<>(topic, KEY, value))
                                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TopicAuthorizationException.class);
        }
    }

    @ParameterizedTest(name = "SASL v{0} table lifecycle")
    @ValueSource(shorts = {0, 1})
    public void testAuthenticatedTableEnablementInvalidatesCachedNames(short version)
            throws Exception {
        TablePath path = TablePath.of(DATABASE, TABLE_NAME);
        grantWriterAccess(OperationType.ALL);
        try (Socket socket = connectKafka();
                KafkaProducer<byte[], byte[]> producer = producer()) {
            authenticate(socket, version);
            Map<String, org.apache.kafka.common.Uuid> ids = new HashMap<>();
            for (String topic : Arrays.asList(TOPIC, TABLE_NAME)) {
                producer.send(new ProducerRecord<>(topic, KEY, VALUE)).get(30, TimeUnit.SECONDS);
                MetadataResponse metadata = metadata(socket, topic);
                assertThat(metadata.data().topics().find(topic).errorCode())
                        .isEqualTo(Errors.NONE.code());
                ids.put(topic, metadata.data().topics().find(topic).topicId());
            }
            assertThat(ids.get(TOPIC))
                    .isNotEqualTo(org.apache.kafka.common.Uuid.ZERO_UUID)
                    .isNotEqualTo(ids.get(TABLE_NAME));
            flussAdmin
                    .alterTable(
                            path,
                            Collections.singletonList(
                                    TableChange.set(
                                            ConfigOptions.TABLE_KAFKA_ENABLED.key(), "false")),
                            false)
                    .get();
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        for (String topic : Arrays.asList(TOPIC, TABLE_NAME)) {
                            assertThat(
                                            metadata(socket, topic)
                                                    .data()
                                                    .topics()
                                                    .find(topic)
                                                    .errorCode())
                                    .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                            for (byte[] value : new byte[][] {null, VALUE}) {
                                ProduceRequest request = produceRequest(value);
                                request.data().topicData().iterator().next().setName(topic);
                                assertPartition(
                                        (ProduceResponse) sendRequest(socket, request),
                                        0,
                                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                        -1L);
                            }
                        }
                    });
            flussAdmin
                    .alterTable(
                            path,
                            Collections.singletonList(
                                    TableChange.set(
                                            ConfigOptions.TABLE_KAFKA_ENABLED.key(), "true")),
                            false)
                    .get();
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        for (String topic : Arrays.asList(TOPIC, TABLE_NAME)) {
                            MetadataResponse metadata = metadata(socket, topic);
                            assertThat(metadata.data().topics().find(topic).errorCode())
                                    .isEqualTo(Errors.NONE.code());
                            assertThat(metadata.data().topics().find(topic).topicId())
                                    .isEqualTo(ids.get(topic));
                        }
                    });
            for (String topic : Arrays.asList(TABLE_NAME, TOPIC)) {
                producer.send(new ProducerRecord<>(topic, KEY, VALUE)).get(30, TimeUnit.SECONDS);
            }
        }
        assertNativeRecords(
                path, 0, 4, record -> assertThat(record.getRow().getInt(2)).isEqualTo(42));
    }

    private static MetadataResponse metadata(Socket socket, String topic) throws Exception {
        return (MetadataResponse)
                sendRequest(
                        socket,
                        new MetadataRequest.Builder(Collections.singletonList(topic), false)
                                .build((short) 11));
    }

    private static void authenticate(Socket socket, short version) throws Exception {
        if (version == 0) {
            authenticateV0(socket);
        } else {
            authenticateV1(socket);
        }
    }

    @ParameterizedTest(name = "SASL v{0} {1} rescue={2}")
    @CsvSource({
        "0,flat,false",
        "0,flat,true",
        "0,nested,false",
        "0,nested,true",
        "1,flat,false",
        "1,flat,true",
        "1,nested,false",
        "1,nested,true"
    })
    public void testAuthenticatedFormalJsonContract(short version, String layout, boolean rescue)
            throws Exception {
        TablePath path = TablePath.of(DATABASE, "sasl_contract");
        RowType business =
                RowType.builder()
                        .field("message", DataTypes.STRING())
                        .field("count", DataTypes.INT())
                        .field("details", RowType.builder().field("n", DataTypes.INT()).build())
                        .build();
        Schema.Builder schema = Schema.newBuilder().column("_kafka_key", DataTypes.BYTES());
        boolean nested = layout.equals("nested");
        if (nested) {
            schema.column("_kafka_value", business);
        } else {
            for (int i = 0; i < business.getFieldCount(); i++) {
                schema.column(business.getFieldNames().get(i), business.getTypeAt(i));
            }
        }
        schema.column("_kafka_headers", DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()));
        schema.column("_kafka_metadata", metadataType());
        if (rescue) {
            schema.column("_kafka_rescue", DataTypes.STRING());
        }
        flussAdmin
                .createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(schema.build())
                                .distributedBy(2)
                                .logFormat(LogFormat.ARROW)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(),
                                        "parse_json")
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(),
                                        layout)
                                .build(),
                        false)
                .get();
        try {
            long id = flussAdmin.getTableInfo(path).get().getTableId();
            for (int bucket = 0; bucket < 2; bucket++) {
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(new TableBucket(id, bucket));
            }
            grantWriterAccess(path, OperationType.ALL);
            String valid = "{\"message\":\"ok\",\"count\":null,\"details\":{\"n\":2}}";
            String unknown =
                    "{\"message\":\"ok\",\"count\":null,\"details\":{\"n\":2,\"unknown\":{\"x\":null}},"
                            + "\"extra\":[1,null],\"_kafka_key\":\"payload\",\"_kafka_value\":null,"
                            + "\"_kafka_headers\":{},\"_kafka_metadata\":null,\"_kafka_rescue\":null}";
            List<String> invalid =
                    new ArrayList<>(
                            Arrays.asList(
                                    "{\"message\":\"bad\",\"count\":\"wrong\"}",
                                    "{\"message\":\"bad\",\"details\":[]}",
                                    "{\"message\":",
                                    "[]"));
            if (!rescue) {
                invalid.add(unknown);
                invalid.add("{\"message\":\"bad\",\"_kafka_key\":null}");
            }
            String[] recoveredValues =
                    rescue
                            ? new String[] {valid, "{\"message\":null}", "{\"count\":2}", unknown}
                            : new String[] {valid, "{\"message\":null}", "{\"count\":2}"};
            try (Socket socket = connectKafka()) {
                authenticate(socket, version);
                for (int i = 0; i < invalid.size(); i++) {
                    ProduceResponse mixed =
                            (ProduceResponse)
                                    sendRequest(
                                            socket,
                                            produceRequest(
                                                    path,
                                                    contractPartition(0, valid, invalid.get(i)),
                                                    contractPartition(1, valid)));
                    assertPartition(mixed, 0, Errors.INVALID_RECORD, -1L);
                    assertPartition(mixed, 1, Errors.NONE, i);
                }
                ProduceResponse recovered =
                        (ProduceResponse)
                                sendRequest(
                                        socket,
                                        produceRequest(
                                                path, contractPartition(0, recoveredValues)));
                assertPartition(recovered, 0, Errors.NONE, 0L);
            }
            assertNativeRecords(
                    path,
                    0,
                    recoveredValues.length,
                    record -> {
                        InternalRow row = record.getRow();
                        assertThat(row.getBytes(0)).containsExactly(KEY);
                        InternalRow value = nested ? row.getRow(1, 3) : row;
                        int first = nested ? 0 : 1;
                        if (record.logOffset() == 0
                                || (rescue && record.logOffset() == recoveredValues.length - 1)) {
                            assertThat(value.getString(first).toString()).isEqualTo("ok");
                            assertThat(value.isNullAt(first + 1)).isTrue();
                            assertThat(value.getRow(first + 2, 1).getInt(0)).isEqualTo(2);
                        } else if (record.logOffset() == 1) {
                            assertThat(value.isNullAt(first)).isTrue();
                            assertThat(value.isNullAt(first + 1)).isTrue();
                            assertThat(value.isNullAt(first + 2)).isTrue();
                        } else {
                            assertThat(value.isNullAt(first)).isTrue();
                            assertThat(value.getInt(first + 1)).isEqualTo(2);
                            assertThat(value.isNullAt(first + 2)).isTrue();
                        }
                        int headers = nested ? 2 : 4;
                        assertThat(row.getMap(headers).size()).isEqualTo(1);
                        assertThat(row.getMap(headers).keyArray().getString(0).toString())
                                .isEqualTo("source");
                        assertThat(row.getMap(headers).valueArray().getBytes(0))
                                .containsExactly((byte) 2);
                        assertMetadata(row, nested ? 3 : 5, 123L, 0);
                        if (rescue && record.logOffset() == recoveredValues.length - 1) {
                            assertJson(
                                    row.getString(nested ? 4 : 6).toString(),
                                    "{\"details\":{\"unknown\":{\"x\":null}},\"extra\":[1,null],"
                                            + "\"_kafka_key\":\"payload\",\"_kafka_value\":null,"
                                            + "\"_kafka_headers\":{},\"_kafka_metadata\":null,\"_kafka_rescue\":null}");
                        } else if (rescue) {
                            assertThat(row.isNullAt(nested ? 4 : 6)).isTrue();
                        }
                    });
            Consumer<ScanRecord> check =
                    record -> {
                        InternalRow row = record.getRow();
                        assertThat(row.getBytes(0)).containsExactly(KEY);
                        InternalRow value = nested ? row.getRow(1, 3) : row;
                        int first = nested ? 0 : 1;
                        assertThat(value.getString(first).toString()).isEqualTo("ok");
                        assertThat(value.isNullAt(first + 1)).isTrue();
                        assertThat(value.getRow(first + 2, 1).getInt(0)).isEqualTo(2);
                        int headers = nested ? 2 : 4;
                        assertThat(row.getMap(headers).size()).isEqualTo(1);
                        assertThat(row.getMap(headers).keyArray().getString(0).toString())
                                .isEqualTo("source");
                        assertThat(row.getMap(headers).valueArray().getBytes(0))
                                .containsExactly((byte) 2);
                    };
            assertNativeRecords(
                    path,
                    1,
                    invalid.size(),
                    record -> {
                        check.accept(record);
                        assertMetadata(record.getRow(), nested ? 3 : 5, 123L, 1);
                        if (rescue) {
                            assertThat(record.getRow().isNullAt(nested ? 4 : 6)).isTrue();
                        }
                    });
        } finally {
            flussAdmin.dropTable(path, true).get();
        }
    }

    private static ProduceRequestData.PartitionProduceData contractPartition(
            int bucket, String... values) {
        SimpleRecord[] records =
                Arrays.stream(values)
                        .map(
                                value ->
                                        new SimpleRecord(
                                                123L,
                                                KEY,
                                                value == null
                                                        ? null
                                                        : value.getBytes(StandardCharsets.UTF_8),
                                                new Header[] {
                                                    new RecordHeader("source", new byte[] {1}),
                                                    new RecordHeader("source", new byte[] {2})
                                                }))
                        .toArray(SimpleRecord[]::new);
        return new ProduceRequestData.PartitionProduceData()
                .setIndex(bucket)
                .setRecords(MemoryRecords.withRecords(Compression.NONE, records));
    }

    @ParameterizedTest(name = "SASL v{0} {1} value")
    @CsvSource({"0,binary", "1,binary", "0,string", "1,string"})
    public void testAuthenticatedBinaryAndStringContract(short version, String valueType)
            throws Exception {
        TablePath path = TablePath.of(DATABASE, "sasl_scalar");
        boolean binary = valueType.equals("binary");
        flussAdmin
                .createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("_kafka_key", DataTypes.BYTES())
                                                .column(
                                                        "_kafka_value",
                                                        binary
                                                                ? DataTypes.BYTES()
                                                                : DataTypes.STRING())
                                                .column("_kafka_rescue", DataTypes.STRING())
                                                .build())
                                .distributedBy(1)
                                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                                .property(
                                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(),
                                        valueType)
                                .build(),
                        false)
                .get();
        try {
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(
                    new TableBucket(flussAdmin.getTableInfo(path).get().getTableId(), 0));
            grantWriterAccess(path, OperationType.ALL);
            byte[] value =
                    binary
                            ? new byte[] {(byte) 0xff, 0, 42}
                            : "plain-文本".getBytes(StandardCharsets.UTF_8);
            try (Socket socket = connectKafka()) {
                authenticate(socket, version);
                for (String topic : Arrays.asList(path.toString(), path.getTableName())) {
                    ProduceRequest request =
                            produceRequest(
                                    path,
                                    new ProduceRequestData.PartitionProduceData()
                                            .setIndex(0)
                                            .setRecords(
                                                    MemoryRecords.withRecords(
                                                            Compression.NONE,
                                                            new SimpleRecord(KEY, null),
                                                            new SimpleRecord(KEY, value))));
                    request.data().topicData().iterator().next().setName(topic);
                    assertPartition(
                            (ProduceResponse) sendRequest(socket, request),
                            0,
                            Errors.NONE,
                            topic.equals(path.toString()) ? 0L : 2L);
                }
            }
            try (KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(
                                producer.send(new ProducerRecord<>(path.getTableName(), KEY, value))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isEqualTo(4L);
            }
            assertNativeRecords(
                    path,
                    0,
                    5,
                    record -> {
                        assertThat(record.getRow().getBytes(0)).containsExactly(KEY);
                        if (record.logOffset() == 0 || record.logOffset() == 2) {
                            assertThat(record.getRow().isNullAt(1)).isTrue();
                        } else if (binary) {
                            assertThat(record.getRow().getBytes(1)).containsExactly(value);
                        } else {
                            assertThat(record.getRow().getString(1).toString())
                                    .isEqualTo("plain-文本");
                        }
                        assertThat(record.getRow().isNullAt(2)).isTrue();
                    });
        } finally {
            flussAdmin.dropTable(path, true).get();
        }
    }

    private static void assertJson(String actual, String expected) {
        org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper mapper =
                new org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper();
        try {
            assertThat(mapper.readTree(actual)).isEqualTo(mapper.readTree(expected));
        } catch (java.io.IOException failure) {
            throw new AssertionError("Invalid Rescue JSON", failure);
        }
    }

    private static void assertPartition(
            ProduceResponse response, int bucket, Errors error, long offset) {
        assertThat(response.data().responses()).hasSize(1);
        assertThat(response.data().responses().iterator().next().partitionResponses())
                .filteredOn(partition -> partition.index() == bucket)
                .singleElement()
                .satisfies(
                        partition -> {
                            assertThat(partition.errorCode()).isEqualTo(error.code());
                            assertThat(partition.baseOffset()).isEqualTo(offset);
                        });
    }

    private static RowType metadataType() {
        return DataTypes.ROW(
                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                DataTypes.FIELD("partition", DataTypes.INT()));
    }

    private static void assertMetadata(
            InternalRow row, int position, long timestamp, int partition) {
        InternalRow metadata = row.getRow(position, 3);
        assertThat(metadata.getFieldCount()).isEqualTo(3);
        assertThat(metadata.getTimestampLtz(0, 3).getEpochMillisecond()).isEqualTo(timestamp);
        assertThat(metadata.getInt(1)).isZero();
        assertThat(metadata.getInt(2)).isEqualTo(partition);
    }

    @ParameterizedTest(name = "SASL v{0} {1} nullable={2} native null append")
    @CsvSource({
        "0,flat,true",
        "1,flat,true",
        "0,nested,true",
        "1,nested,true",
        "0,flat,false",
        "1,flat,false",
        "0,nested,false",
        "1,nested,false"
    })
    public void testAuthenticatedJsonNullAppendAndNotNullAtomicity(
            short version, String layout, boolean nullable) throws Exception {
        boolean nested = layout.equals("nested");
        TablePath path = TablePath.of(DATABASE, "sasl_null_json");
        Schema.Builder schema = Schema.newBuilder().column("_kafka_key", DataTypes.BYTES());
        if (nested) {
            // Kafka table fields and nested children follow the nullable schema contract.
            schema.column(
                    "_kafka_value",
                    RowType.builder()
                            .field("message", DataTypes.STRING())
                            .field("count", DataTypes.INT())
                            .build()
                            .copy(nullable));
        } else {
            schema.column("message", DataTypes.STRING().copy(nullable))
                    .column("count", DataTypes.INT());
        }
        schema.column("_kafka_headers", DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()))
                .column("_kafka_metadata", metadataType())
                .column("_kafka_rescue", DataTypes.STRING());
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema.build())
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "parse_json")
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), layout)
                        .build();
        if (!nullable) {
            assertThatThrownBy(() -> flussAdmin.createTable(path, descriptor, false).get())
                    .hasCauseInstanceOf(InvalidTableException.class)
                    .hasStackTraceContaining("nullable fields");
            return;
        }
        flussAdmin.createTable(path, descriptor, false).get();
        try {
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(
                    new TableBucket(flussAdmin.getTableInfo(path).get().getTableId(), 0));
            grantWriterAccess(path, OperationType.ALL);
            String valid = new String(VALUE, StandardCharsets.UTF_8);
            try (Socket socket = connectKafka()) {
                authenticate(socket, version);
                ProduceRequest allNull =
                        produceRequest(path, contractPartition(0, (String) null, null));
                assertPartition((ProduceResponse) sendRequest(socket, allNull), 0, Errors.NONE, 0L);
                ProduceRequest mixed =
                        produceRequest(path, contractPartition(0, valid, null, valid));
                mixed.data().topicData().iterator().next().setName(path.getTableName());
                assertPartition((ProduceResponse) sendRequest(socket, mixed), 0, Errors.NONE, 2L);
            }
            assertNativeRecords(
                    path,
                    0,
                    5,
                    record -> {
                        InternalRow row = record.getRow();
                        assertThat(row.getBytes(0)).containsExactly(KEY);
                        boolean nullValue = record.logOffset() < 2 || record.logOffset() == 3;
                        if (nullValue) {
                            assertThat(row.isNullAt(1)).isTrue();
                            if (!nested) {
                                assertThat(row.isNullAt(2)).isTrue();
                            }
                        } else {
                            InternalRow value = nested ? row.getRow(1, 2) : row;
                            int start = nested ? 0 : 1;
                            assertThat(value.getString(start).toString())
                                    .isEqualTo("authenticated-value");
                            assertThat(value.getInt(start + 1)).isEqualTo(42);
                        }
                        int headers = nested ? 2 : 3;
                        assertThat(row.getMap(headers).size()).isEqualTo(1);
                        assertThat(row.getMap(headers).keyArray().getString(0).toString())
                                .isEqualTo("source");
                        assertThat(row.getMap(headers).valueArray().getBytes(0))
                                .containsExactly((byte) 2);
                        assertMetadata(row, headers + 1, 123L, 0);
                        assertThat(row.isNullAt(headers + 2)).isTrue();
                    });
        } finally {
            flussAdmin.dropTable(path, true).get();
        }
    }

    @Test
    public void testAuthenticatedRequestsDoNotRecreateDeletedDefaultDatabase() throws Exception {
        String defaultDatabase = "sasl_default";
        String full = defaultDatabase + "." + TABLE_NAME;
        Configuration config = clusterConfig();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, defaultDatabase);
        // The native API protects the built-in fluss database from deletion. A configured
        // default database exercises real DDL deletion without bypassing that Core guard.
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(1)
                        .setClusterConf(config)
                        .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                        .build();
        cluster.start();
        try (Connection nativeConnection =
                        ConnectionFactory.createConnection(cluster.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = nativeConnection.getAdmin()) {
            assertThat(admin.databaseExists(DATABASE).get()).isTrue();
            assertThat(admin.databaseExists(defaultDatabase).get()).isTrue();
            admin.createDatabase("other", org.apache.fluss.metadata.DatabaseDescriptor.EMPTY, false)
                    .get();
            TablePath other = TablePath.of("other", TABLE_NAME);
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("_kafka_value", DataTypes.BYTES())
                                            .build())
                            .distributedBy(1)
                            .logFormat(LogFormat.ARROW)
                            .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                            .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "binary")
                            .build();
            for (TablePath path : Arrays.asList(TablePath.of(defaultDatabase, TABLE_NAME), other)) {
                admin.createTable(path, descriptor, false).get();
                cluster.waitAndGetLeader(
                        new TableBucket(admin.getTableInfo(path).get().getTableId(), 0));
                AclBinding acl =
                        new AclBinding(
                                Resource.table(path),
                                new AccessControlEntry(
                                        new FlussPrincipal(USERNAME, "User"),
                                        AccessControlEntry.WILD_CARD_HOST,
                                        OperationType.ALL,
                                        PermissionType.ALLOW));
                admin.createAcls(Collections.singletonList(acl)).all().get();
                cluster.waitUntilAuthenticationSync(Collections.singletonList(acl), true);
            }
            try (Socket v0 = connectKafka(cluster.getTabletServerNodes("KAFKA").get(0));
                    Socket v1 = connectKafka(cluster.getTabletServerNodes("KAFKA").get(0))) {
                authenticateV0(v0);
                authenticateV1(v1);
                // Warm both names on both authenticated connections before deleting the database.
                for (Socket socket : Arrays.asList(v0, v1)) {
                    for (String topic : Arrays.asList(full, TABLE_NAME)) {
                        assertThat(metadata(socket, topic).data().topics().find(topic).errorCode())
                                .isEqualTo(Errors.NONE.code());
                    }
                }
                admin.dropDatabase(defaultDatabase, false, true).get();
                for (Socket socket : Arrays.asList(v0, v1)) {
                    retry(
                            Duration.ofSeconds(30),
                            () -> {
                                for (String topic : Arrays.asList(TABLE_NAME, full)) {
                                    assertThat(
                                                    metadata(socket, topic)
                                                            .data()
                                                            .topics()
                                                            .find(topic)
                                                            .errorCode())
                                            .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                                    ProduceRequest request = produceRequest((byte[]) null);
                                    request.data().topicData().iterator().next().setName(topic);
                                    assertPartition(
                                            (ProduceResponse) sendRequest(socket, request),
                                            0,
                                            Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                            -1L);
                                }
                            });
                    assertThat(
                                    metadata(socket, other.toString())
                                            .data()
                                            .topics()
                                            .find(other.toString())
                                            .errorCode())
                            .isEqualTo(Errors.NONE.code());
                    ProduceRequest request =
                            produceRequest(other, contractPartition(0, (String) null));
                    assertPartition(
                            (ProduceResponse) sendRequest(socket, request),
                            0,
                            Errors.NONE,
                            socket == v0 ? 0L : 1L);
                    assertThat(admin.databaseExists(defaultDatabase).get()).isFalse();
                }
                try (Table table = nativeConnection.getTable(other);
                        LogScanner scanner = table.newScan().createLogScanner()) {
                    scanner.subscribeFromBeginning(0);
                    List<ScanRecord> records = new ArrayList<>();
                    retry(
                            Duration.ofSeconds(30),
                            () -> {
                                scanner.poll(Duration.ofMillis(100)).forEach(records::add);
                                assertThat(records).hasSize(2);
                            });
                    assertThat(records).extracting(ScanRecord::logOffset).containsExactly(0L, 1L);
                    assertThat(records)
                            .allSatisfy(record -> assertThat(record.getRow().isNullAt(0)).isTrue());
                }
            }
        } finally {
            cluster.close();
        }
    }

    @Test
    public void testV1AuthenticationFailureDoesNotExposeCredentials() throws Exception {
        try (Socket socket = connectKafka()) {
            handshake(socket, (short) 1);
            SaslAuthenticateResponse response =
                    (SaslAuthenticateResponse)
                            sendRequest(
                                    socket,
                                    new SaslAuthenticateRequest(
                                            new SaslAuthenticateRequestData()
                                                    .setAuthBytes(
                                                            ("\u0000"
                                                                            + USERNAME
                                                                            + "\u0000incorrect-secret")
                                                                    .getBytes(
                                                                            StandardCharsets
                                                                                    .UTF_8)),
                                            (short) 2));
            assertThat(response.error()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED);
            assertThat(response.data().errorMessage())
                    .doesNotContain(USERNAME, PASSWORD, "incorrect-secret");
            assertThat(response.data().authBytes()).isEmpty();
        }
    }

    private static Socket connectKafka() throws Exception {
        return connectKafka(FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("KAFKA").get(0));
    }

    private static Socket connectKafka(ServerNode node) throws Exception {
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
        handshake(socket, (short) 0);
        writeRawToken(socket, "\u0000" + USERNAME + "\u0000" + PASSWORD);
        // The final PLAIN challenge is a zero-length frame, without a Kafka response header.
        assertThat(new DataInputStream(socket.getInputStream()).readInt()).isZero();
    }

    private static void authenticateV1(Socket socket) throws Exception {
        handshake(socket, (short) 1);
        SaslAuthenticateResponse response =
                (SaslAuthenticateResponse)
                        sendRequest(
                                socket,
                                new SaslAuthenticateRequest(
                                        new SaslAuthenticateRequestData()
                                                .setAuthBytes(
                                                        ("\u0000" + USERNAME + "\u0000" + PASSWORD)
                                                                .getBytes(StandardCharsets.UTF_8)),
                                        (short) 2));
        assertThat(response.error()).isEqualTo(Errors.NONE);
        assertThat(response.data().authBytes()).isEmpty();
    }

    private static void handshake(Socket socket, short version) throws Exception {
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
                                        version));
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
        return produceRequest(VALUE);
    }

    private static ProduceRequest produceRequest(byte[] value) {
        MemoryRecords records =
                MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(KEY, value));
        return produceRequest(
                TablePath.of(DATABASE, TABLE_NAME),
                new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records));
    }

    private static ProduceRequestData.PartitionProduceData jsonPartition(
            int index, String... values) {
        SimpleRecord[] records =
                Arrays.stream(values)
                        .map(value -> new SimpleRecord(KEY, value.getBytes(StandardCharsets.UTF_8)))
                        .toArray(SimpleRecord[]::new);
        return new ProduceRequestData.PartitionProduceData()
                .setIndex(index)
                .setRecords(MemoryRecords.withRecords(Compression.NONE, records));
    }

    private static ProduceRequest produceRequest(
            TablePath path, ProduceRequestData.PartitionProduceData... partitions) {
        ProduceRequestData.TopicProduceData topicData =
                new ProduceRequestData.TopicProduceData()
                        .setName(path.toString())
                        .setPartitionData(Arrays.asList(partitions));
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
        return producer(USERNAME, PASSWORD);
    }

    private KafkaProducer<byte[], byte[]> producer(String username, String password) {
        Map<String, Object> config = kafkaClientConfig(username, password);
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
        assertNativeRecords(
                TablePath.of(DATABASE, TABLE_NAME),
                0,
                2,
                record -> {
                    InternalRow row = record.getRow();
                    assertThat(row.getBytes(0)).containsExactly(KEY);
                    if (record.logOffset() == 0) {
                        assertThat(row.isNullAt(1)).isTrue();
                        assertThat(row.isNullAt(2)).isTrue();
                    } else {
                        assertThat(row.getString(1).toString()).isEqualTo("authenticated-value");
                        assertThat(row.getInt(2)).isEqualTo(42);
                    }
                });
    }

    private void grantWriterAccess(OperationType operation) throws Exception {
        grantWriterAccess(TablePath.of(DATABASE, TABLE_NAME), operation);
    }

    private void grantWriterAccess(TablePath path, OperationType operation) throws Exception {
        grantAccess(path, USERNAME, operation);
    }

    private void grantAccess(TablePath path, String username, OperationType operation)
            throws Exception {
        AclBinding aclBinding =
                new AclBinding(
                        Resource.table(path),
                        new AccessControlEntry(
                                new FlussPrincipal(username, "User"),
                                AccessControlEntry.WILD_CARD_HOST,
                                operation,
                                PermissionType.ALLOW));
        flussAdmin.createAcls(Collections.singletonList(aclBinding)).all().get();
        FLUSS_CLUSTER_EXTENSION.waitUntilAuthenticationSync(
                Collections.singletonList(aclBinding), true);
    }

    private void assertNativeRecords(
            TablePath path, int bucket, int expectedCount, Consumer<ScanRecord> check)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(bucket);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            int count = 0;
            while (count < expectedCount && System.nanoTime() < deadline) {
                for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                    assertThat(record.logOffset()).isEqualTo(count);
                    check.accept(record);
                    count++;
                }
            }
            assertThat(count).isEqualTo(expectedCount);
            assertThat(scanner.poll(Duration.ofMillis(200)).isEmpty()).isTrue();
        }
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
        Map<String, String> credentials = new HashMap<>();
        credentials.put(USERNAME, PASSWORD);
        credentials.put("reader", "reader-secret");
        config.set(ConfigOptions.SERVER_SASL_CREDENTIALS, credentials);
        config.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        config.set(ConfigOptions.SUPER_USERS, "User:ANONYMOUS");
        return config;
    }
}
