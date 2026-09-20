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
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies DDL-discovered JSON writes and native Arrow readback. */
class KafkaJsonProduceITCase {
    private static final String DATABASE = "kafka";

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @Test
    void testScalarJsonRoundTripAndRejectedWritesDoNotAppend() throws Exception {
        TableDescriptor descriptor =
                descriptor(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT().copy(false))
                                        .column("message_key", DataTypes.STRING())
                                        .column("amount", DataTypes.DECIMAL(30, 10))
                                        .column(
                                                "received_at",
                                                DataTypes.TIMESTAMP_LTZ(3).copy(false))
                                        .column("float_value", DataTypes.FLOAT())
                                        .column("double_value", DataTypes.DOUBLE())
                                        .build())
                        .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                        .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "message_key")
                        .customProperty(KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                        .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "received_at")
                        .build();
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            TablePath path = TablePath.of(DATABASE, "json_scalars");
            create(admin, path, descriptor);
            try (KafkaProducer<byte[], byte[]> producer = producer()) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(1);
                assertThatThrownBy(
                                () ->
                                        producer.send(record(path, "{\"id\":\"bad\"}"))
                                                .get(30, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(InvalidRecordException.class);
                assertThat(
                                producer.send(
                                                record(
                                                        path,
                                                        "{\"id\":7,\"amount\":12345678901234567890.1234567890,"
                                                                + "\"float_value\":-0.0,\"double_value\":-0.0}"))
                                        .get(30, TimeUnit.SECONDS)
                                        .offset())
                        .isZero();
                read(
                        connection,
                        path,
                        row -> {
                            assertThat(row.getInt(0)).isEqualTo(7);
                            assertThat(row.getString(1).toString()).isEqualTo("key");
                            assertThat(row.getDecimal(2, 30, 10).toBigDecimal())
                                    .isEqualByComparingTo(
                                            new BigDecimal("12345678901234567890.1234567890"));
                            assertThat(row.getTimestampLtz(3, 3).getEpochMillisecond())
                                    .isEqualTo(123L);
                            assertThat(Float.floatToRawIntBits(row.getFloat(4)))
                                    .isEqualTo(Float.floatToRawIntBits(-0.0F));
                            assertThat(Double.doubleToRawLongBits(row.getDouble(5)))
                                    .isEqualTo(Double.doubleToRawLongBits(-0.0D));
                        });
            } finally {
                admin.dropTable(path, true).get();
            }
        }
    }

    private static void create(Admin admin, TablePath path, TableDescriptor descriptor)
            throws Exception {
        admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(path, descriptor, false).get();
        CLUSTER.waitUntilAllGatewayHasSameMetadata();
    }

    private static TableDescriptor.Builder descriptor(Schema schema) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json");
    }

    private static ProducerRecord<byte[], byte[]> record(TablePath path, String json) {
        return new ProducerRecord<>(
                path.toString(),
                0,
                123L,
                "key".getBytes(StandardCharsets.UTF_8),
                json.getBytes(StandardCharsets.UTF_8));
    }

    private static void read(Connection connection, TablePath path, Consumer<InternalRow> check)
            throws Exception {
        try (Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < deadline) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                int count = 0;
                for (ScanRecord record : records) {
                    assertThat(record.logOffset()).isZero();
                    check.accept(record.getRow());
                    count++;
                }
                if (count > 0) {
                    assertThat(count).isEqualTo(1);
                    return;
                }
            }
            throw new AssertionError("JSON record was not visible to the native scanner.");
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
