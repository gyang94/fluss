/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies DDL-discovered JSON writes and native Arrow readback. */
class KafkaJsonProduceITCase {
    private static final String DATABASE = "fluss";
    private static final byte[] KEY = new byte[] {1, 2};
    private static final RowType VALUE_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("amount", DataTypes.DECIMAL(10, 2))
                    .field("created_at", DataTypes.TIMESTAMP(3))
                    .field("label", DataTypes.STRING())
                    .field(
                            "items",
                            DataTypes.ARRAY(
                                    DataTypes.ROW(
                                            DataTypes.FIELD("sku", DataTypes.STRING()),
                                            DataTypes.FIELD("quantity", DataTypes.INT()))))
                    .field(
                            "attributes",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())))
                    .build();

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @ParameterizedTest
    @ValueSource(strings = {"flat", "nested"})
    void testScalarComplexAndNullValuesRoundTripThroughBothTopicNames(String layout)
            throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_round_trip_" + layout);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(layout));
            try (KafkaProducer<byte[], byte[]> producer = producer()) {
                for (int i = 0; i < 3; i++) {
                    String topic = i == 0 ? path.toString() : path.getTableName();
                    byte[] value = i == 1 ? null : bytes(json(i));
                    assertThat(
                                    producer.send(
                                                    new ProducerRecord<>(
                                                            topic,
                                                            0,
                                                            123L,
                                                            KEY,
                                                            value,
                                                            Collections.singletonList(
                                                                    new RecordHeader(
                                                                            "source", KEY))))
                                            .get(30, TimeUnit.SECONDS)
                                            .offset())
                            .isEqualTo(i);
                }
                read(
                        connection,
                        path,
                        0,
                        3,
                        (row, index) -> {
                            if (index == 1) {
                                int valueFields =
                                        layout.equals("nested") ? 1 : VALUE_TYPE.getFieldCount();
                                for (int field = 1; field <= valueFields; field++) {
                                    assertThat(row.isNullAt(field)).isTrue();
                                }
                            } else {
                                InternalRow value =
                                        layout.equals("nested")
                                                ? row.getRow(1, VALUE_TYPE.getFieldCount())
                                                : row;
                                int start = layout.equals("nested") ? 0 : 1;
                                assertThat(value.getInt(start)).isEqualTo(index);
                                assertThat(value.getDecimal(start + 1, 10, 2).toBigDecimal())
                                        .isEqualByComparingTo("12.34");
                                assertThat(value.getTimestampNtz(start + 2, 3).toLocalDateTime())
                                        .isEqualTo(LocalDateTime.parse("2026-01-02T03:04:05.123"));
                                assertThat(value.getString(start + 3).toString())
                                        .isEqualTo("hello");
                                InternalArray items = value.getArray(start + 4);
                                assertThat(items.size()).isEqualTo(2);
                                assertThat(items.getRow(0, 2).getString(0).toString())
                                        .isEqualTo("A");
                                assertThat(items.getRow(0, 2).getInt(1)).isEqualTo(2);
                                assertThat(items.isNullAt(1)).isTrue();
                                InternalMap attributes = value.getMap(start + 5);
                                assertThat(attributes.keyArray().getString(0).toString())
                                        .isEqualTo("scores");
                                InternalArray scores = attributes.valueArray().getArray(0);
                                assertThat(scores.size()).isEqualTo(3);
                                assertThat(scores.getInt(0)).isEqualTo(7);
                                assertThat(scores.isNullAt(1)).isTrue();
                                assertThat(scores.getInt(2)).isEqualTo(9);
                            }
                            assertThat(row.getBytes(0)).isEqualTo(KEY);
                            int headersPosition =
                                    layout.equals("nested") ? 2 : 1 + VALUE_TYPE.getFieldCount();
                            InternalMap headers = row.getMap(headersPosition);
                            assertThat(headers.size()).isEqualTo(1);
                            assertThat(headers.keyArray().getString(0).toString())
                                    .isEqualTo("source");
                            assertThat(headers.valueArray().getBytes(0)).isEqualTo(KEY);
                            InternalRow metadata = row.getRow(headersPosition + 1, 3);
                            assertThat(metadata.getTimestampLtz(0, 3).getEpochMillisecond())
                                    .isEqualTo(123L);
                            assertThat(metadata.getInt(1)).isZero();
                            assertThat(metadata.getInt(2)).isZero();
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"flat", "nested"})
    void testInvalidBatchIsAtomicAndOtherPartitionsStillAppend(String layout) throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_atomicity_" + layout);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(layout));
            ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port());
                    KafkaProducer<byte[], byte[]> producer = producer()) {
                ProduceResponse response =
                        produce(
                                socket,
                                path.toString(),
                                MemoryRecords.withRecords(
                                        Compression.NONE,
                                        record(json(1000)),
                                        record("{\"items\":[{\"quantity\":\"bad\"}]}")),
                                MemoryRecords.withRecords(
                                        Compression.NONE,
                                        record(json(7)),
                                        record(null),
                                        record(json(8))));
                PartitionProduceResponse rejected = partition(response, path.toString(), 0);
                assertThat(rejected.errorCode()).isEqualTo(Errors.INVALID_RECORD.code());
                assertThat(rejected.baseOffset()).isEqualTo(-1L);
                PartitionProduceResponse accepted = partition(response, path.toString(), 1);
                assertThat(accepted.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(accepted.baseOffset()).isZero();
                // A later append starts at zero: the valid prefix of the rejected batch was not
                // written.
                assertThat(
                                producer.send(
                                                new ProducerRecord<>(
                                                        path.getTableName(),
                                                        0,
                                                        KEY,
                                                        bytes(json(9))))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isZero();
                read(
                        connection,
                        path,
                        0,
                        1,
                        (row, index) -> assertThat(valueId(row, layout)).isEqualTo(9));
                read(
                        connection,
                        path,
                        1,
                        3,
                        (row, index) -> {
                            if (index == 1) {
                                assertThat(row.isNullAt(1)).isTrue();
                            } else {
                                assertThat(valueId(row, layout)).isEqualTo(index == 0 ? 7 : 8);
                            }
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    private static int valueId(InternalRow row, String layout) {
        return layout.equals("nested")
                ? row.getRow(1, VALUE_TYPE.getFieldCount()).getInt(0)
                : row.getInt(1);
    }

    private static String json(int id) {
        return "{\"id\":"
                + id
                + ",\"amount\":12.34,"
                + "\"created_at\":\"2026-01-02T03:04:05.123\",\"label\":\"hello\","
                + "\"items\":[{\"sku\":\"A\",\"quantity\":2},null],"
                + "\"attributes\":{\"scores\":[7,null,9]}}";
    }

    private static TableDescriptor descriptor(String layout) {
        Schema.Builder schema =
                Schema.newBuilder().column(KafkaTableSchema.KEY_COLUMN, DataTypes.BYTES());
        if (layout.equals("nested")) {
            schema.column(KafkaTableSchema.VALUE_COLUMN, VALUE_TYPE);
        } else {
            for (int i = 0; i < VALUE_TYPE.getFieldCount(); i++) {
                schema.column(VALUE_TYPE.getFieldNames().get(i), VALUE_TYPE.getTypeAt(i));
            }
        }
        schema.column(
                KafkaTableSchema.HEADERS_COLUMN,
                DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()));
        schema.column(
                KafkaTableSchema.METADATA_COLUMN,
                DataTypes.ROW(
                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                        DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                        DataTypes.FIELD("partition", DataTypes.INT())));
        return TableDescriptor.builder()
                .schema(schema.build())
                .distributedBy(2)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE.key(), "parse_json")
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT.key(), layout)
                .build();
    }

    private static void create(Admin admin, TablePath path, TableDescriptor descriptor)
            throws Exception {
        admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(path, descriptor, false).get();
        TableInfo table = admin.getTableInfo(path).get();
        CLUSTER.waitUntilTableReady(table.getTableId());
        for (int bucket = 0; bucket < table.getNumBuckets(); bucket++) {
            CLUSTER.waitAndGetLeaderReplica(new TableBucket(table.getTableId(), bucket));
        }
        CLUSTER.waitUntilAllGatewayHasSameMetadata();
    }

    private static SimpleRecord record(String json) {
        return new SimpleRecord(123L, KEY, json == null ? null : bytes(json), new Header[0]);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ProduceResponse produce(
            Socket socket, String topic, MemoryRecords rejected, MemoryRecords accepted)
            throws Exception {
        TopicProduceData topicData =
                new TopicProduceData()
                        .setName(topic)
                        .setPartitionData(
                                Arrays.asList(
                                        new PartitionProduceData().setIndex(0).setRecords(rejected),
                                        new PartitionProduceData()
                                                .setIndex(1)
                                                .setRecords(accepted)));
        ProduceRequest request =
                new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks((short) -1)
                                .setTimeoutMs(10000)
                                .setTopicData(
                                        new ProduceRequestData.TopicProduceDataCollection(
                                                Collections.singletonList(topicData).iterator())),
                        (short) 9);
        RequestHeader header =
                new RequestHeader(ApiKeys.PRODUCE, request.version(), "json-produce-test", 1);
        ByteBuffer encoded = request.serializeWithHeader(header);
        byte[] requestBytes = new byte[encoded.remaining()];
        encoded.get(requestBytes);
        socket.setSoTimeout(15000);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(requestBytes.length);
        output.write(requestBytes);
        output.flush();
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int responseSize = input.readInt();
        assertThat(responseSize).isBetween(1, 1024 * 1024);
        byte[] response = new byte[responseSize];
        input.readFully(response);
        return (ProduceResponse) AbstractResponse.parseResponse(ByteBuffer.wrap(response), header);
    }

    private static PartitionProduceResponse partition(
            ProduceResponse response, String topic, int index) {
        return response.data().responses().find(topic).partitionResponses().stream()
                .filter(partition -> partition.index() == index)
                .findFirst()
                .get();
    }

    private static void read(
            Connection connection,
            TablePath path,
            int bucket,
            int expectedCount,
            BiConsumer<InternalRow, Integer> check)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(bucket);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            int count = 0;
            while (count < expectedCount && System.nanoTime() < deadline) {
                for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                    assertThat(record.logOffset()).isEqualTo(count);
                    check.accept(record.getRow(), count);
                    count++;
                }
            }
            assertThat(count).isEqualTo(expectedCount);
            assertThat(scanner.poll(Duration.ofMillis(100))).isEmpty();
        }
    }

    private static KafkaProducer<byte[], byte[]> producer() {
        ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        return new KafkaProducer<>(config);
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);
        return config;
    }
}
