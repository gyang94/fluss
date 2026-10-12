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
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies DDL-discovered JSON writes and native Arrow readback. */
class KafkaJsonProduceITCase {
    private static final String DATABASE = "fluss";
    private static final byte[] KEY = new byte[] {(byte) 0xff, 1};
    private static final RowType SCALAR_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT().copy(false))
                    .field("amount", DataTypes.DECIMAL(30, 10))
                    .field("received_at", DataTypes.TIMESTAMP(6).copy(false))
                    .field("emitted_at", DataTypes.TIMESTAMP_LTZ(6).copy(false))
                    .field("float_value", DataTypes.FLOAT())
                    .field("double_value", DataTypes.DOUBLE())
                    .field("optional", DataTypes.STRING())
                    .field("rounded_float", DataTypes.FLOAT())
                    .build();

    private static final RowType NUMBER_TYPE =
            RowType.builder()
                    .field("float_value", DataTypes.FLOAT().copy(false))
                    .field("double_value", DataTypes.DOUBLE().copy(false))
                    .field("rounded_float", DataTypes.FLOAT().copy(false))
                    .field("amount", DataTypes.DECIMAL(30, 10).copy(false))
                    .build();
    private static final RowType CUSTOMER_TYPE =
            RowType.builder()
                    .field("name", DataTypes.STRING().copy(false))
                    .field(KafkaTableSchema.KEY_COLUMN, DataTypes.INT())
                    .field(KafkaTableSchema.VALUE_COLUMN, DataTypes.INT())
                    .field(KafkaTableSchema.HEADERS_COLUMN, DataTypes.INT())
                    .field(KafkaTableSchema.METADATA_COLUMN, DataTypes.INT())
                    .field(KafkaTableSchema.RESCUE_COLUMN, DataTypes.INT())
                    .build();
    private static final RowType COMPLEX_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT().copy(false))
                    .field("customer", CUSTOMER_TYPE)
                    .field("measurements", NUMBER_TYPE)
                    .field("float_values", DataTypes.ARRAY(DataTypes.FLOAT().copy(false)))
                    .field("double_values", DataTypes.ARRAY(DataTypes.DOUBLE().copy(false)))
                    .field(
                            "float_attributes",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.FLOAT().copy(false)))
                    .field(
                            "double_attributes",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE().copy(false)))
                    .field("items", DataTypes.ARRAY(NUMBER_TYPE.copy(false)))
                    .field("attributes", DataTypes.MAP(DataTypes.STRING(), NUMBER_TYPE.copy(false)))
                    .build();

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @ParameterizedTest
    @CsvSource({"flat", "nested"})
    void testComplexJsonRoundTripPreservesNumericValues(String layout) throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_complex_" + layout);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(COMPLEX_TYPE, layout, true, false, 1));
            try (KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(1);
                String[] negativeZeros = {"-0", "-0.0", "-0e0"};
                for (int index = 0; index < negativeZeros.length; index++) {
                    assertThat(
                                    producer.send(
                                                    new ProducerRecord<>(
                                                            path.toString(),
                                                            0,
                                                            KEY,
                                                            bytes(
                                                                    complexJson(
                                                                            index,
                                                                            negativeZeros[index]))))
                                            .get(30, TimeUnit.SECONDS)
                                            .offset())
                            .isEqualTo(index);
                }
                read(
                        connection,
                        path,
                        0,
                        negativeZeros.length,
                        (row, index) -> {
                            assertThat(row.getBytes(0)).isEqualTo(KEY);
                            InternalRow value =
                                    layout.equals("nested")
                                            ? row.getRow(1, COMPLEX_TYPE.getFieldCount())
                                            : row;
                            int offset = layout.equals("nested") ? 0 : 1;
                            assertThat(value.getInt(offset)).isEqualTo(index);
                            InternalRow customer =
                                    value.getRow(offset + 1, CUSTOMER_TYPE.getFieldCount());
                            assertThat(customer.getString(0).toString()).isEqualTo("Alice");
                            // Reserved system names are ordinary declared business fields below
                            // root.
                            for (int field = 1; field < CUSTOMER_TYPE.getFieldCount(); field++) {
                                assertThat(customer.getInt(field)).isEqualTo(field);
                            }
                            assertNumbers(value.getRow(offset + 2, NUMBER_TYPE.getFieldCount()));
                            assertThat(value.getArray(offset + 3).size()).isEqualTo(2);
                            assertNegativeZero(value.getArray(offset + 3).getFloat(0));
                            assertThat(value.getArray(offset + 3).getFloat(1))
                                    .isEqualTo(1.0000001F);
                            assertThat(value.getArray(offset + 4).size()).isEqualTo(1);
                            assertNegativeZero(value.getArray(offset + 4).getDouble(0));
                            InternalMap floats = value.getMap(offset + 5);
                            assertThat(floats.size()).isEqualTo(2);
                            assertThat(floats.keyArray().getString(0).toString()).isEqualTo("zero");
                            assertNegativeZero(floats.valueArray().getFloat(0));
                            assertThat(floats.keyArray().getString(1).toString())
                                    .isEqualTo("rounded");
                            assertThat(floats.valueArray().getFloat(1)).isEqualTo(1.0000001F);
                            InternalMap doubles = value.getMap(offset + 6);
                            assertThat(doubles.size()).isEqualTo(1);
                            assertNegativeZero(doubles.valueArray().getDouble(0));
                            assertThat(value.getArray(offset + 7).size()).isEqualTo(1);
                            assertNumbers(
                                    value.getArray(offset + 7)
                                            .getRow(0, NUMBER_TYPE.getFieldCount()));
                            InternalMap attributes = value.getMap(offset + 8);
                            assertThat(attributes.size()).isEqualTo(1);
                            assertThat(attributes.keyArray().getString(0).toString())
                                    .isEqualTo("dynamic");
                            assertNumbers(
                                    attributes.valueArray().getRow(0, NUMBER_TYPE.getFieldCount()));
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"flat,false", "flat,true", "nested,false", "nested,true"})
    void testInvalidComplexPartitionIsAtomicAndDoesNotBlockOtherPartitions(
            String layout, boolean withRescue) throws Exception {
        TablePath path =
                TablePath.of(DATABASE, "json_complex_rejections_" + layout + "_" + withRescue);
        List<String> invalidValues =
                new ArrayList<>(
                        Arrays.asList(
                                "{\"id\":1,\"customer\":{\"name\":\"Alice\",\"unknown\":1}}",
                                "{\"id\":1,\"customer\":{}}",
                                "{\"id\":1,\"customer\":{\"name\":null}}",
                                "{\"id\":1,\"customer\":{\"name\":1}}",
                                "{\"id\":1,\"customer\":[]}",
                                "{\"id\":1,\"float_values\":[1,null]}",
                                "{\"id\":1,\"double_values\":[1,\"bad\"]}",
                                "{\"id\":1,\"float_values\":{}}",
                                "{\"id\":1,\"float_attributes\":[]}",
                                "{\"id\":1,\"double_attributes\":{\"bad\":null}}",
                                "{\"id\":1,\"items\":[null]}",
                                "{\"id\":1,\"items\":[{}]}",
                                "{\"id\":1,\"attributes\":{\"bad\":null}}",
                                "{\"id\":1,\"attributes\":{\"bad\":{}}}",
                                // The escaped surrogate must not be encoded as '?' and overwrite
                                // that key.
                                "{\"id\":1,\"float_attributes\":{\"?\":1,\"\\ud800\":2}}",
                                "{\"id\":1,\"double_attributes\":{\"?\":1,\"\\udc00\":2}}"));
        if (withRescue) {
            invalidValues.remove(0);
        }
        invalidValues.add("{\"id\":1,\"measurements\":{\"float_value\":1e40}}");
        invalidValues.add(
                "{\"id\":1,\"measurements\":{\"float_value\":0,\"double_value\":0,\"rounded_float\":0,\"amount\":1.12345678901}}");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(COMPLEX_TYPE, layout, true, withRescue, 2));
            ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port());
                    KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(2);
                for (int index = 0; index < invalidValues.size(); index++) {
                    String name = index % 2 == 0 ? path.toString() : path.getTableName();
                    ProduceResponse response =
                            produce(
                                    socket,
                                    name,
                                    MemoryRecords.withRecords(
                                            Compression.NONE,
                                            record(complexJson(1000, "-0")),
                                            record(invalidValues.get(index))),
                                    MemoryRecords.withRecords(
                                            Compression.NONE, record(complexJson(index, "-0e0"))));
                    PartitionProduceResponse rejected = partition(response, name, 0);
                    assertThat(rejected.errorCode())
                            .as(
                                    "rejection for %s with rescue=%s",
                                    invalidValues.get(index), withRescue)
                            .isEqualTo(Errors.INVALID_RECORD.code());
                    assertThat(rejected.baseOffset()).isEqualTo(-1L);
                    PartitionProduceResponse accepted = partition(response, name, 1);
                    assertThat(accepted.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(accepted.baseOffset()).isEqualTo(index);
                }
                assertThat(
                                producer.send(
                                                new ProducerRecord<>(
                                                        path.toString(),
                                                        0,
                                                        KEY,
                                                        bytes(complexJson(-1, "-0.0"))))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isZero();
                read(
                        connection,
                        path,
                        0,
                        1,
                        (row, index) ->
                                assertThat(valueId(row, layout, COMPLEX_TYPE)).isEqualTo(-1));
                read(
                        connection,
                        path,
                        1,
                        invalidValues.size(),
                        (row, index) ->
                                assertThat(valueId(row, layout, COMPLEX_TYPE)).isEqualTo(index));
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"flat,false", "flat,true", "nested,false", "nested,true"})
    void testScalarJsonAliasesAndOptionalSystemColumnsRoundTrip(
            String layout, boolean withSystemColumns) throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_scalars_" + layout + "_" + withSystemColumns);
        TableDescriptor descriptor =
                descriptor(SCALAR_TYPE, layout, withSystemColumns, withSystemColumns, 1);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor);
            try (KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(1);
                assertThat(producer.partitionsFor(path.getTableName())).hasSize(1);
                // Null values are filtered before validating required JSON business fields.
                assertThat(
                                producer.send(
                                                new ProducerRecord<byte[], byte[]>(
                                                        path.toString(), 0, null, null))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isEqualTo(-1L);
                RecordMetadata first =
                        producer.send(
                                        new ProducerRecord<>(
                                                path.toString(),
                                                0,
                                                123L,
                                                KEY,
                                                bytes(scalarJson(7, false)),
                                                Arrays.asList(
                                                        new RecordHeader("source", bytes("old")),
                                                        new RecordHeader(
                                                                "source", new byte[] {1, 2}),
                                                        new RecordHeader("nullable", null))))
                                .get(30, TimeUnit.SECONDS);
                assertThat(first.topic()).isEqualTo(path.toString());
                assertThat(first.offset()).isZero();
                assertThat(
                                producer.send(
                                                new ProducerRecord<byte[], byte[]>(
                                                        path.getTableName(), 0, KEY, null))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isEqualTo(-1L);
                // A bare alias and the full name append to the same native table and bucket.
                RecordMetadata second =
                        producer.send(
                                        new ProducerRecord<byte[], byte[]>(
                                                path.getTableName(),
                                                0,
                                                null,
                                                bytes(scalarJson(8, true))))
                                .get(30, TimeUnit.SECONDS);
                assertThat(second.topic()).isEqualTo(path.getTableName());
                assertThat(second.offset()).isEqualTo(1L);
                read(
                        connection,
                        path,
                        0,
                        2,
                        (row, index) -> {
                            int valuePosition = withSystemColumns ? 1 : 0;
                            InternalRow value =
                                    layout.equals("nested")
                                            ? row.getRow(valuePosition, SCALAR_TYPE.getFieldCount())
                                            : row;
                            int fieldOffset = layout.equals("nested") ? 0 : valuePosition;
                            assertScalars(value, fieldOffset, 7 + index);
                            if (withSystemColumns) {
                                if (index == 0) {
                                    assertThat(row.getBytes(0)).isEqualTo(KEY);
                                } else {
                                    assertThat(row.isNullAt(0)).isTrue();
                                }
                                int headersPosition =
                                        valuePosition
                                                + (layout.equals("nested")
                                                        ? 1
                                                        : SCALAR_TYPE.getFieldCount());
                                assertHeaders(row.getMap(headersPosition), index == 0);
                                assertThat(row.isNullAt(headersPosition + 1)).isTrue();
                            }
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"gzip,flat", "snappy,nested", "lz4,nested", "zstd,flat"})
    void testCompressedJsonPreservesStrictRescueAndPartitionAtomicity(String codec, String layout)
            throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_compressed_" + codec + "_" + layout);
        RowType valueType =
                RowType.builder()
                        .field("id", DataTypes.INT().copy(false))
                        .field(
                                "profile",
                                RowType.builder().field("name", DataTypes.STRING()).build())
                        .build();
        List<String> invalidValues =
                Arrays.asList("{\"id\":1,\"profile\":{\"name\":{}}}", "{\"id\":null}", "{\"id\":1");
        Compression compression = Compression.of(CompressionType.forName(codec)).build();
        SimpleRecord filtered = new SimpleRecord(123L, KEY, (byte[]) null, new Header[0]);
        int headersPosition = layout.equals("nested") ? 2 : 3;
        int rescuePosition = headersPosition + 1;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expectedRescue =
                mapper.readTree(
                        "{\"extra\":3,\"profile\":{\"extra\":9},\"_kafka_key\":\"payload\"}");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(valueType, layout, true, true, 2));
            ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port());
                    KafkaProducer<byte[], byte[]> producer = producer(codec)) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(2);
                for (int index = 0; index < invalidValues.size(); index++) {
                    String name = index % 2 == 0 ? path.toString() : path.getTableName();
                    ProduceResponse response =
                            produce(
                                    socket,
                                    name,
                                    MemoryRecords.withRecords(
                                            compression,
                                            record("{\"id\":1000}"),
                                            record(invalidValues.get(index))),
                                    MemoryRecords.withRecords(
                                            compression, record("{\"id\":" + index + "}")));
                    assertThat(partition(response, name, 0).errorCode())
                            .as("compressed rejection for %s", invalidValues.get(index))
                            .isEqualTo(Errors.INVALID_RECORD.code());
                    assertThat(partition(response, name, 0).baseOffset()).isEqualTo(-1L);
                    assertThat(partition(response, name, 1).errorCode())
                            .isEqualTo(Errors.NONE.code());
                    assertThat(partition(response, name, 1).baseOffset()).isEqualTo(index);
                }
                // Filtering makes the batch offset ambiguous even when some records are appended.
                // An all-filtered batch must not append to the otherwise empty rejected bucket.
                ProduceResponse filteredResponse =
                        produce(
                                socket,
                                path.toString(),
                                MemoryRecords.withRecords(compression, filtered, filtered),
                                MemoryRecords.withRecords(
                                        compression, filtered, record("{\"id\":3}"), filtered));
                for (int bucket = 0; bucket < 2; bucket++) {
                    assertThat(partition(filteredResponse, path.toString(), bucket).errorCode())
                            .isEqualTo(Errors.NONE.code());
                    assertThat(partition(filteredResponse, path.toString(), bucket).baseOffset())
                            .isEqualTo(-1L);
                }
                List<String> payloads =
                        Arrays.asList(
                                "{\"id\":10,\"profile\":{\"name\":\"你好\",\"extra\":9},"
                                        + "\"extra\":3,\"_kafka_key\":\"payload\"}",
                                "{\"id\":11,\"profile\":{\"name\":\"你好\"}}");
                for (int index = 0; index < payloads.size(); index++) {
                    String name = index == 0 ? path.toString() : path.getTableName();
                    // Both names share the same native bucket and continuous native offsets.
                    assertThat(
                                    producer.send(
                                                    new ProducerRecord<>(
                                                            name,
                                                            0,
                                                            123L,
                                                            KEY,
                                                            bytes(payloads.get(index)),
                                                            Arrays.asList(
                                                                    new RecordHeader(
                                                                            "source", null),
                                                                    new RecordHeader(
                                                                            "source",
                                                                            new byte[] {1, 2}),
                                                                    new RecordHeader(
                                                                            "nullable", KEY),
                                                                    new RecordHeader(
                                                                            "nullable", null))))
                                            .get(30, TimeUnit.SECONDS)
                                            .offset())
                            .isEqualTo(index);
                }
                read(
                        connection,
                        path,
                        0,
                        2,
                        (row, index) -> {
                            assertThat(valueId(row, layout, valueType)).isEqualTo(10 + index);
                            InternalRow profile =
                                    layout.equals("nested")
                                            ? row.getRow(1, valueType.getFieldCount()).getRow(1, 1)
                                            : row.getRow(2, 1);
                            assertThat(profile.getString(0).toString()).isEqualTo("你好");
                            assertThat(row.getBytes(0)).isEqualTo(KEY);
                            assertThat(row.getMap(headersPosition).size()).isEqualTo(2);
                            assertHeaders(row.getMap(headersPosition), true);
                            if (index == 0) {
                                try {
                                    assertThat(
                                                    mapper.readTree(
                                                            row.getString(rescuePosition)
                                                                    .toString()))
                                            .isEqualTo(expectedRescue);
                                } catch (java.io.IOException e) {
                                    throw new AssertionError(e);
                                }
                            } else {
                                assertThat(row.isNullAt(rescuePosition)).isTrue();
                            }
                        });
                read(
                        connection,
                        path,
                        1,
                        4,
                        (row, index) -> {
                            assertThat(valueId(row, layout, valueType)).isEqualTo(index);
                            assertThat(row.getBytes(0)).isEqualTo(KEY);
                            assertHeaders(row.getMap(headersPosition), false);
                            assertThat(row.isNullAt(rescuePosition)).isTrue();
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"flat,false", "flat,true", "nested,false", "nested,true"})
    void testInvalidPartitionIsAtomicAndDoesNotBlockOtherPartitions(
            String layout, boolean withRescue) throws Exception {
        TablePath path = TablePath.of(DATABASE, "json_rejections_" + layout + "_" + withRescue);
        RowType valueType = RowType.builder().field("id", DataTypes.INT().copy(false)).build();
        List<String> invalidValues =
                new ArrayList<>(
                        Arrays.asList(
                                "",
                                "{",
                                "null",
                                "{\"id\":\"bad\"}",
                                "{\"id\":null}",
                                "{\"id\":1} {}",
                                "{\"id\":1,\"id\":2}"));
        if (!withRescue) {
            invalidValues.add("{\"id\":1,\"unknown\":2}");
            for (String reserved :
                    new String[] {
                        KafkaTableSchema.KEY_COLUMN,
                        KafkaTableSchema.HEADERS_COLUMN,
                        KafkaTableSchema.METADATA_COLUMN,
                        KafkaTableSchema.RESCUE_COLUMN,
                        KafkaTableSchema.VALUE_COLUMN
                    }) {
                // This also rejects the erroneous nested payload wrapper {"_kafka_value":{...}}.
                invalidValues.add("{\"id\":1,\"" + reserved + "\":{\"id\":2}}");
            }
        }
        invalidValues.add("{\"id\":2147483648}");
        invalidValues.add("{\"id\":1,\"_kafka_rescue\":\"\\ud800\"}");
        invalidValues.add("{\"id\":1,\"extra\":{\"x\":1,\"x\":2}}");
        StringBuilder oversized = new StringBuilder("{\"id\":1,\"extra\":[");
        for (int i = 0; i < 10000; i++) {
            if (i > 0) {
                oversized.append(',');
            }
            oversized.append('0');
        }
        invalidValues.add(oversized.append(",0]}").toString());
        String deep = "0";
        for (int i = 0; i < 67; i++) {
            deep = "[" + deep + "]";
        }
        invalidValues.add("{\"id\":1,\"extra\":" + deep + "}");
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor(valueType, layout, true, withRescue, 2));
            ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
            try (Socket socket = new Socket(node.host(), node.port());
                    KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(2);
                for (int index = 0; index < invalidValues.size(); index++) {
                    String name = index % 2 == 0 ? path.toString() : path.getTableName();
                    ProduceResponse response =
                            produce(
                                    socket,
                                    name,
                                    MemoryRecords.withRecords(
                                            Compression.NONE,
                                            record("{\"id\":1000}"),
                                            record(invalidValues.get(index))),
                                    MemoryRecords.withRecords(
                                            Compression.NONE, record("{\"id\":" + index + "}")));
                    assertThat(response.data().responses()).hasSize(1);
                    assertThat(response.data().responses().find(name).partitionResponses())
                            .hasSize(2);
                    PartitionProduceResponse rejected = partition(response, name, 0);
                    assertThat(rejected.errorCode())
                            .as(
                                    "rejection for %s with rescue=%s",
                                    invalidValues.get(index), withRescue)
                            .isEqualTo(Errors.INVALID_RECORD.code());
                    assertThat(rejected.baseOffset()).isEqualTo(-1L);
                    PartitionProduceResponse accepted = partition(response, name, 1);
                    assertThat(accepted.errorCode()).isEqualTo(Errors.NONE.code());
                    assertThat(accepted.baseOffset()).isEqualTo(index);
                }
                // Known-field and JSON validation failures remain atomic with rescue enabled.
                // A later successful write must be the first actual append to the rejected bucket.
                assertThat(
                                producer.send(
                                                new ProducerRecord<>(
                                                        path.toString(),
                                                        0,
                                                        KEY,
                                                        bytes("{\"id\":-1}")))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isZero();
                read(
                        connection,
                        path,
                        0,
                        1,
                        (row, index) -> assertThat(valueId(row, layout)).isEqualTo(-1));
                read(
                        connection,
                        path,
                        1,
                        invalidValues.size(),
                        (row, index) -> assertThat(valueId(row, layout)).isEqualTo(index));
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "flat,false,false",
        "flat,false,true",
        "flat,true,false",
        "flat,true,true",
        "nested,false,false",
        "nested,false,true",
        "nested,true,false",
        "nested,true,true"
    })
    void testRescueRoundTripWithReorderedSystemColumns(
            String layout, boolean withSystemColumns, boolean rescueFirst) throws Exception {
        TablePath path =
                TablePath.of(
                        DATABASE,
                        "json_rescue_" + layout + "_" + withSystemColumns + "_" + rescueFirst);
        TableDescriptor descriptor =
                reorderedRescueDescriptor(layout, withSystemColumns, rescueFirst);
        KafkaTableSchema contract = KafkaTableSchema.resolve(descriptor);
        ObjectMapper mapper = new ObjectMapper();
        List<String> payloads = new ArrayList<>();
        List<JsonNode> expected = new ArrayList<>();
        String extra = "{\"values\":[1,null,2]}";
        String[] rescueValues = {"null", "123", "\"text\"", "{\"a\":[1,null]}"};
        String[] zeros = {"-0", "-0.0", "-0e0", "-0"};
        for (int i = 0; i < rescueValues.length; i++) {
            String json =
                    complexJson(i, zeros[i])
                            .replace(
                                    "\"amount\":12345678901234567890.1234567890}",
                                    "\"amount\":12345678901234567890.1234567890,\"extra\":"
                                            + extra
                                            + "}");
            String collisions =
                    "\"_kafka_key\":{\"id\":2},\"_kafka_headers\":null,"
                            + "\"_kafka_metadata\":{\"id\":2},\"_kafka_value\":{\"id\":2},"
                            + "\"_kafka_rescue\":"
                            + rescueValues[i];
            payloads.add(json.substring(0, json.length() - 1) + "," + collisions + "}");
            expected.add(
                    mapper.readTree(
                            "{"
                                    + collisions
                                    + ",\"measurements\":{\"extra\":"
                                    + extra
                                    + "},\"items\":[{\"extra\":"
                                    + extra
                                    + "}],\"attributes\":{\"dynamic\":{\"extra\":"
                                    + extra
                                    + "}}}"));
        }
        payloads.add(complexJson(4, "-0e0"));
        expected.add(null);
        // These were rejection cases in the inherited no-capture implementation.
        payloads.add("{\"id\":1,\"unknown\":2}");
        expected.add(mapper.readTree("{\"unknown\":2}"));
        payloads.add("{\"id\":1,\"customer\":{\"name\":\"Alice\",\"unknown\":1}}");
        expected.add(mapper.readTree("{\"customer\":{\"unknown\":1}}"));
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            create(admin, path, descriptor);
            try (KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(1);
                for (int i = 0; i < payloads.size(); i++) {
                    assertThat(
                                    producer.send(
                                                    new ProducerRecord<>(
                                                            path.toString(),
                                                            0,
                                                            123L,
                                                            KEY,
                                                            bytes(payloads.get(i)),
                                                            Arrays.asList(
                                                                    new RecordHeader(
                                                                            "source",
                                                                            new byte[] {1, 2}),
                                                                    new RecordHeader(
                                                                            "nullable", null))))
                                            .get(30, TimeUnit.SECONDS)
                                            .offset())
                            .isEqualTo(i);
                }
                read(
                        connection,
                        path,
                        0,
                        payloads.size(),
                        (physical, index) -> {
                            InternalRow value = jsonBusinessRow(physical, contract);
                            assertThat(value.getInt(0)).isEqualTo(index < 5 ? index : 1);
                            if (index < 5) {
                                InternalRow customer =
                                        value.getRow(1, CUSTOMER_TYPE.getFieldCount());
                                assertThat(customer.getString(0).toString()).isEqualTo("Alice");
                                for (int field = 1;
                                        field < CUSTOMER_TYPE.getFieldCount();
                                        field++) {
                                    assertThat(customer.getInt(field)).isEqualTo(field);
                                }
                                assertNumbers(value.getRow(2, NUMBER_TYPE.getFieldCount()));
                                assertNumbers(
                                        value.getArray(7).getRow(0, NUMBER_TYPE.getFieldCount()));
                                assertNumbers(
                                        value.getMap(8)
                                                .valueArray()
                                                .getRow(0, NUMBER_TYPE.getFieldCount()));
                            }
                            if (expected.get(index) == null) {
                                assertThat(physical.isNullAt(contract.rescuePosition())).isTrue();
                            } else {
                                try {
                                    assertThat(
                                                    mapper.readTree(
                                                            physical.getString(
                                                                            contract
                                                                                    .rescuePosition())
                                                                    .toString()))
                                            .isEqualTo(expected.get(index));
                                } catch (java.io.IOException e) {
                                    throw new AssertionError(e);
                                }
                            }
                            if (withSystemColumns) {
                                assertThat(physical.getBytes(contract.keyPosition()))
                                        .isEqualTo(KEY);
                                assertHeaders(physical.getMap(contract.headersPosition()), true);
                            }
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    private static InternalRow jsonBusinessRow(InternalRow row, KafkaTableSchema contract) {
        if (contract.valueLayout() == KafkaTableSchema.ValueLayout.NESTED) {
            return row.getRow(
                    contract.valueProjection().positionAt(0), COMPLEX_TYPE.getFieldCount());
        }
        GenericRow business = new GenericRow(COMPLEX_TYPE.getFieldCount());
        for (int i = 0; i < business.getFieldCount(); i++) {
            business.setField(
                    i,
                    InternalRow.createFieldGetter(
                                    COMPLEX_TYPE.getTypeAt(i),
                                    contract.valueProjection().positionAt(i))
                            .getFieldOrNull(row));
        }
        return business;
    }

    private static TableDescriptor reorderedRescueDescriptor(
            String layout, boolean withSystemColumns, boolean rescueFirst) {
        List<Schema.Column> columns =
                new ArrayList<>(
                        descriptor(COMPLEX_TYPE, layout, withSystemColumns, false, 1)
                                .getSchema()
                                .getColumns());
        Schema.Builder schema = Schema.newBuilder();
        int rescuePosition = rescueFirst ? 0 : columns.size() / 2 + columns.size() % 2;
        for (int i = 0; i <= columns.size(); i++) {
            if (i == rescuePosition) {
                schema.column(KafkaTableSchema.RESCUE_COLUMN, DataTypes.STRING());
            }
            if (i < columns.size()) {
                schema.column(columns.get(i).getName(), columns.get(i).getDataType());
            }
        }
        return TableDescriptor.builder()
                .schema(schema.build())
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "parse_json")
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, layout)
                .build();
    }

    private static void assertNegativeZero(float value) {
        assertThat(Float.floatToRawIntBits(value)).isEqualTo(Float.floatToRawIntBits(-0.0F));
    }

    private static void assertNegativeZero(double value) {
        assertThat(Double.doubleToRawLongBits(value)).isEqualTo(Double.doubleToRawLongBits(-0.0D));
    }

    private static void assertNumbers(InternalRow row) {
        assertNegativeZero(row.getFloat(0));
        assertNegativeZero(row.getDouble(1));
        assertThat(row.getFloat(2)).isEqualTo(1.0000001F);
        assertThat(row.getDecimal(3, 30, 10).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345678901234567890.1234567890"));
    }

    private static String complexJson(int id, String negativeZero) {
        String numbers =
                "{\"float_value\":"
                        + negativeZero
                        + ",\"double_value\":"
                        + negativeZero
                        + ",\"rounded_float\":1.0000000596046448,"
                        + "\"amount\":12345678901234567890.1234567890}";
        return "{\"id\":"
                + id
                + ",\"customer\":{\"name\":\"Alice\",\"_kafka_key\":1,\"_kafka_value\":2,"
                + "\"_kafka_headers\":3,\"_kafka_metadata\":4,\"_kafka_rescue\":5},"
                + "\"measurements\":"
                + numbers
                + ",\"float_values\":["
                + negativeZero
                + ",1.0000000596046448],\"double_values\":["
                + negativeZero
                + "],\"float_attributes\":{\"zero\":"
                + negativeZero
                + ",\"rounded\":1.0000000596046448},\"double_attributes\":{\"zero\":"
                + negativeZero
                + "},\"items\":["
                + numbers
                + "],\"attributes\":{\"dynamic\":"
                + numbers
                + "}}";
    }

    private static int valueId(InternalRow row, String layout, RowType valueType) {
        return layout.equals("nested")
                ? row.getRow(1, valueType.getFieldCount()).getInt(0)
                : row.getInt(1);
    }

    private static int valueId(InternalRow row, String layout) {
        return layout.equals("nested") ? row.getRow(1, 1).getInt(0) : row.getInt(1);
    }

    private static void assertScalars(InternalRow row, int start, int id) {
        assertThat(row.getInt(start)).isEqualTo(id);
        assertThat(row.getDecimal(start + 1, 30, 10).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345678901234567890.1234567890"));
        assertThat(row.getTimestampNtz(start + 2, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.parse("1969-12-31T23:59:59.999999"));
        assertThat(row.getTimestampLtz(start + 3, 6).toInstant())
                .isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
        assertThat(Float.floatToRawIntBits(row.getFloat(start + 4)))
                .isEqualTo(Float.floatToRawIntBits(-0.0F));
        assertThat(Double.doubleToRawLongBits(row.getDouble(start + 5)))
                .isEqualTo(Double.doubleToRawLongBits(-0.0D));
        assertThat(row.isNullAt(start + 6)).isTrue();
        assertThat(row.getFloat(start + 7)).isEqualTo(1.0000001F);
    }

    private static void assertHeaders(InternalMap headers, boolean populated) {
        if (!populated) {
            assertThat(headers.size()).isZero();
            return;
        }
        Map<String, byte[]> values = new HashMap<>();
        for (int index = 0; index < headers.size(); index++) {
            values.put(
                    headers.keyArray().getString(index).toString(),
                    headers.valueArray().isNullAt(index)
                            ? null
                            : headers.valueArray().getBytes(index));
        }
        assertThat(values).containsOnlyKeys("source", "nullable");
        assertThat(values.get("source")).containsExactly(1, 2);
        assertThat(values.get("nullable")).isNull();
    }

    private static String scalarJson(int id, boolean explicitNull) {
        // Both layouts consume this same root object; nested does not require a value wrapper.
        return "{\"id\":"
                + id
                + ",\"amount\":12345678901234567890.1234567890,"
                + "\"received_at\":\"1969-12-31T23:59:59.999999\","
                + "\"emitted_at\":\"1970-01-01T07:59:59.999999+08:00\","
                + "\"float_value\":-0.0,\"double_value\":-0.0,"
                + "\"rounded_float\":1.0000000596046448"
                + (explicitNull ? ",\"optional\":null" : "")
                + "}";
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

    private static TableDescriptor descriptor(
            RowType valueType,
            String layout,
            boolean withSystemColumns,
            boolean withRescue,
            int buckets) {
        Schema.Builder schema = Schema.newBuilder();
        if (withSystemColumns) {
            schema.column(KafkaTableSchema.KEY_COLUMN, DataTypes.BYTES());
        }
        if (layout.equals("nested")) {
            schema.column(KafkaTableSchema.VALUE_COLUMN, valueType);
        } else {
            for (int index = 0; index < valueType.getFieldCount(); index++) {
                schema.column(valueType.getFieldNames().get(index), valueType.getTypeAt(index));
            }
        }
        if (withSystemColumns) {
            schema.column(
                    KafkaTableSchema.HEADERS_COLUMN,
                    DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()));
        }
        if (withRescue) {
            schema.column(KafkaTableSchema.RESCUE_COLUMN, DataTypes.STRING());
        }
        return TableDescriptor.builder()
                .schema(schema.build())
                .distributedBy(buckets)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "parse_json")
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_LAYOUT, layout)
                .build();
    }

    private static SimpleRecord record(String json) {
        return new SimpleRecord(123L, KEY, bytes(json), new Header[0]);
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
        return producer("gzip");
    }

    private static KafkaProducer<byte[], byte[]> producer(String codec) {
        ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
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
