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
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies Kafka upserts against native lookups across multiple tablet leaders. */
class KafkaPrimaryKeyITCase {
    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @Test
    void testPrimaryKeyRoutingIgnoresKafkaPartitionAndMessageKey() throws Exception {
        TablePath path = TablePath.of("kafka", "primary-key-routing");
        int stoppedLeader = -1;
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                org.apache.fluss.client.admin.Admin admin = connection.getAdmin()) {
            admin.createDatabase("kafka", DatabaseDescriptor.EMPTY, true).get();
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("tenant", DataTypes.INT())
                                            .column("id", DataTypes.INT())
                                            .column("amount", DataTypes.INT())
                                            .primaryKey("tenant", "id")
                                            .build())
                            .distributedBy(6, "tenant")
                            .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 2)
                            .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                            .build();
            admin.createTable(path, descriptor, false).get();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig())) {
                assertThat(producer.partitionsFor(path.toString())).hasSize(6);
                for (int id = 0; id < 24; id++) {
                    RecordMetadata metadata =
                            producer.send(
                                            new ProducerRecord<>(
                                                    path.toString(),
                                                    0,
                                                    "unrelated-key",
                                                    json(id, 1)))
                                    .get(30, TimeUnit.SECONDS);
                    assertThat(metadata.hasOffset()).isFalse();
                }
                stoppedLeader = producer.partitionsFor(path.toString()).get(5).leader().id();
                CLUSTER.stopTabletServer(stoppedLeader);
                for (int id = 0; id < 24; id++) {
                    producer.send(
                                    new ProducerRecord<>(
                                            path.toString(), 5, "different-key", json(id, 2)))
                            .get(30, TimeUnit.SECONDS);
                }
            }
            try (Table table = connection.getTable(path)) {
                Lookuper lookuper = table.newLookup().createLookuper();
                for (int id = 0; id < 24; id++) {
                    InternalRow row =
                            lookuper.lookup(GenericRow.of(id, id))
                                    .get(30, TimeUnit.SECONDS)
                                    .getSingletonRow();
                    assertThat(row).as("primary key %s", id).isNotNull();
                    assertThat(row.getInt(2)).isEqualTo(2);
                }
            }
            admin.dropTable(path, false).get();
        } finally {
            if (stoppedLeader >= 0 && CLUSTER.getTabletServerById(stoppedLeader) == null) {
                CLUSTER.startTabletServer(stoppedLeader);
                CLUSTER.assertHasTabletServerNumber(3);
            }
        }
    }

    private static String json(int id, int amount) {
        return String.format("{\"tenant\":%d,\"id\":%d,\"amount\":%d}", id, id, amount);
    }

    private static Map<String, Object> producerConfig() {
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
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20000);
        return config;
    }

    private static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 2);
        return config;
    }
}
