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

package org.apache.fluss.kafka.schema;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbAddColumn;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.json.DataTypeJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.server.coordinator.CoordinatorServer.DEFAULT_DATABASE;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateDatabaseRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDatabaseExistsRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newTableExistsRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies Kafka DDL contracts through real coordinator RPC and persisted metadata. */
class KafkaTableMappingITCase {

    private static final Configuration CONFIG = new Configuration();

    static {
        CONFIG.set(ConfigOptions.KAFKA_ENABLED, true);
        CONFIG.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "missing_default");
        CONFIG.setString("kafka.database", "legacy_database");
    }

    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(1).setClusterConf(CONFIG).build();

    private CoordinatorGateway gateway;

    @BeforeEach
    void setup() {
        gateway = CLUSTER.newCoordinatorClient();
    }

    @Test
    void testNoAutomaticKafkaDatabaseAndQualifiedAccessWithoutDefaultDatabase() throws Exception {
        for (String database : Arrays.asList("missing_default", "kafka", "legacy_database")) {
            assertThat(gateway.databaseExists(newDatabaseExistsRequest(database)).get().isExists())
                    .isFalse();
        }
        assertThat(
                        gateway.databaseExists(newDatabaseExistsRequest(DEFAULT_DATABASE))
                                .get()
                                .isExists())
                .isTrue();
        KafkaTopicMapper mapper = new KafkaTopicMapper(CONFIG);
        TablePath sales = mapper.toTablePath("sales.orders");
        TablePath warehouse = mapper.toTablePath("warehouse.orders");
        long salesId = createTable(CLUSTER, sales, enabled().build());
        long warehouseId = createTable(CLUSTER, warehouse, enabled().build());
        assertThat(salesId).isNotEqualTo(warehouseId);
        assertThat(mapper.toTablePath("orders"))
                .isEqualTo(TablePath.of("missing_default", "orders"));
        assertThatThrownBy(
                        () ->
                                gateway.createTable(
                                                newCreateTableRequest(
                                                        mapper.toTablePath("orders"),
                                                        enabled().build(),
                                                        false))
                                        .get())
                .hasCauseInstanceOf(DatabaseNotExistException.class);
        assertThat(
                        gateway.databaseExists(newDatabaseExistsRequest("missing_default"))
                                .get()
                                .isExists())
                .isFalse();
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, "sales");
        KafkaTopicMapper aliasMapper = new KafkaTopicMapper(config);
        assertThat(info(aliasMapper.toTablePath("orders")).getTableId())
                .isEqualTo(info(sales).getTableId());
        assertThat(aliasMapper.toTopicName(sales)).isEqualTo("sales.orders");
    }

    @Test
    void testOnlyRequestedKafkaPropertiesArePersisted() throws Exception {
        TablePath path = TablePath.of("ddl", "roundtrip");
        Schema schema =
                Schema.newBuilder()
                        .column("_kafka_key", DataTypes.BYTES())
                        .column(
                                "payload",
                                DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.STRING())))
                        .column(
                                "_kafka_headers",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()))
                        .column("_kafka_rescue", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                enabled()
                        .schema(schema)
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "parse_json")
                        .build();
        createTable(CLUSTER, path, descriptor);
        TableDescriptor stored = stored(path);
        assertThat(stored.getSchema()).isEqualTo(schema);
        assertThat(kafkaProperties(stored)).isEqualTo(kafkaProperties(descriptor));
        assertThat(new KafkaTopicSchemaResolver().resolve(stored).valueProjection().positions())
                .containsExactly(1);
    }

    @Test
    void testEnableDisableAndFailedPropertyAlterAreAtomic() throws Exception {
        TablePath path = TablePath.of("ddl", "enable_disable");
        TableDescriptor disabled =
                enabled().property(ConfigOptions.TABLE_KAFKA_ENABLED, false).build();
        createTable(CLUSTER, path, disabled);
        TableDescriptor before = stored(path);
        assertThat(KafkaTableSchema.isEnabled(before)).isFalse();
        set(path, Collections.singletonMap("table.kafka.enabled", "true"));
        TableDescriptor enabled = stored(path);
        assertThat(KafkaTableSchema.isEnabled(enabled)).isTrue();
        Map<String, String> expected = new HashMap<>(before.getProperties());
        expected.put("table.kafka.enabled", "true");
        assertThat(enabled.getProperties()).isEqualTo(expected);
        assertThat(enabled.getSchema()).isEqualTo(before.getSchema());
        Map<String, String> invalid = new HashMap<>();
        invalid.put("table.kafka.convert.value.type", "string");
        invalid.put("table.kafka.convert.key.type", "binary");
        assertThatThrownBy(() -> set(path, invalid))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(enabled);
        assertThatThrownBy(() -> reset(path, "table.kafka.convert.value.type"))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(enabled);
        reset(path, "table.kafka.enabled");
        assertThat(KafkaTableSchema.isEnabled(stored(path))).isFalse();
        assertThat(stored(path).getProperties()).doesNotContainKey("table.kafka.enabled");
        // Disabled tables can stage incompatible Kafka conversion properties without Kafka
        // validation.
        set(path, Collections.singletonMap("table.kafka.convert.value.type", "string"));
        TableDescriptor staged = stored(path);
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(staged);
    }

    @Test
    void testSchemaAlterValidatesBeforePersistenceAndCanBeDisabled() throws Exception {
        TablePath path = TablePath.of("ddl", "schema_change");
        createTable(CLUSTER, path, enabled().build());
        GetTableInfoResponse before = info(path);
        assertThatThrownBy(() -> addColumn(path, "_kafka_headers", DataTypes.STRING()))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(info(path).getTableJson()).isEqualTo(before.getTableJson());
        assertThat(info(path).getSchemaId()).isEqualTo(before.getSchemaId());
        addColumn(path, "_kafka_headers", DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES()));
        assertThat(KafkaTableSchema.resolve(stored(path)).headersPosition()).isEqualTo(1);
        assertThat(info(path).getSchemaId()).isGreaterThan(before.getSchemaId());
        TableDescriptor withHeaders = stored(path);
        assertThatThrownBy(() -> addColumn(path, "_kafka_rescue", DataTypes.INT()))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(withHeaders);
        set(path, Collections.singletonMap("table.kafka.enabled", "false"));
        addColumn(path, "unmapped_business_field", DataTypes.INT());
        assertThat(stored(path).getSchema().getRowType().getFieldNames())
                .contains("unmapped_business_field");
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(KafkaTableSchema.isEnabled(stored(path))).isFalse();
    }

    @Test
    void testCreateRejectsInvalidContractWithoutPublishingMetadata() throws Exception {
        gateway.createDatabase(newCreateDatabaseRequest("ddl_invalid", true)).get();
        TablePath path = TablePath.of("ddl_invalid", "bad");
        assertThatThrownBy(
                        () ->
                                gateway.createTable(
                                                newCreateTableRequest(
                                                        path,
                                                        enabled()
                                                                .property(
                                                                        ConfigOptions
                                                                                .TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                                                        "string")
                                                                .build(),
                                                        false))
                                        .get())
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(gateway.tableExists(newTableExistsRequest(path)).get().isExists()).isFalse();
        assertThat(CLUSTER.getZooKeeperClient().getCurrentSchemaId(path)).isZero();
        // Non-null added columns are rejected by generic schema evolution first, so exercise
        // Kafka-specific rescue nullability through CREATE, where such columns are allowed.
        TableDescriptor nonNullableRescue =
                enabled()
                        .schema(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.BYTES())
                                        .column("_kafka_rescue", DataTypes.STRING().copy(false))
                                        .build())
                        .build();
        assertThatThrownBy(
                        () ->
                                gateway.createTable(
                                                newCreateTableRequest(
                                                        path, nonNullableRescue, false))
                                        .get())
                .hasCauseInstanceOf(InvalidTableException.class)
                .hasStackTraceContaining("nullable STRING");
        assertThat(CLUSTER.getZooKeeperClient().getCurrentSchemaId(path)).isZero();
        assertThatThrownBy(
                        () ->
                                gateway.createTable(
                                                newCreateTableRequest(
                                                        path,
                                                        enabled()
                                                                .property(
                                                                        "table.kafka.convert.rescue-column",
                                                                        "rescue")
                                                                .build(),
                                                        false))
                                        .get())
                .hasCauseInstanceOf(InvalidConfigException.class);
        assertThat(gateway.tableExists(newTableExistsRequest(path)).get().isExists()).isFalse();
        createTable(CLUSTER, path, enabled().build());
        assertThat(KafkaTableSchema.isEnabled(stored(path))).isTrue();
    }

    @Test
    void testUnsupportedMetadataOffsetCannotBeEnabled() throws Exception {
        TablePath path = TablePath.of("ddl", "offset_capability");
        Schema schema =
                Schema.newBuilder()
                        .column("_kafka_value", DataTypes.BYTES())
                        .column(
                                "_kafka_metadata",
                                DataTypes.ROW(
                                        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3)),
                                        DataTypes.FIELD("timestamp_type", DataTypes.INT()),
                                        DataTypes.FIELD("partition", DataTypes.INT()),
                                        DataTypes.FIELD("offset", DataTypes.BIGINT())))
                        .build();
        createTable(
                CLUSTER,
                path,
                enabled()
                        .schema(schema)
                        .property(ConfigOptions.TABLE_KAFKA_ENABLED, false)
                        .build());
        TableDescriptor before = stored(path);
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class)
                .hasStackTraceContaining("actual per-row offsets");
        assertThat(stored(path)).isEqualTo(before);
    }

    @Test
    void testCanonicalNameCheckedOnCreateAndEnable() throws Exception {
        String database = String.join("", Collections.nCopies(128, "d"));
        TablePath path = TablePath.of(database, String.join("", Collections.nCopies(128, "t")));
        gateway.createDatabase(newCreateDatabaseRequest(database, true)).get();
        assertThatThrownBy(
                        () ->
                                gateway.createTable(
                                                newCreateTableRequest(
                                                        path, enabled().build(), false))
                                        .get())
                .hasCauseInstanceOf(InvalidTableException.class)
                .hasStackTraceContaining("Kafka topic");
        createTable(
                CLUSTER,
                path,
                enabled().property(ConfigOptions.TABLE_KAFKA_ENABLED, false).build());
        TableDescriptor before = stored(path);
        assertThatThrownBy(() -> set(path, Collections.singletonMap("table.kafka.enabled", "true")))
                .hasCauseInstanceOf(InvalidTableException.class);
        assertThat(stored(path)).isEqualTo(before);
    }

    private GetTableInfoResponse info(TablePath path) throws Exception {
        return gateway.getTableInfo(newGetTableInfoRequest(path)).get();
    }

    private TableDescriptor stored(TablePath path) throws Exception {
        return TableDescriptor.fromJsonBytes(info(path).getTableJson());
    }

    private void set(TablePath path, Map<String, String> properties) throws Exception {
        gateway.alterTable(
                        newAlterTableRequest(
                                path,
                                properties,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();
    }

    private void reset(TablePath path, String property) throws Exception {
        gateway.alterTable(
                        newAlterTableRequest(
                                path,
                                Collections.emptyMap(),
                                Collections.singletonList(property),
                                Collections.emptyList(),
                                false))
                .get();
    }

    private void addColumn(TablePath path, String name, DataType type) throws Exception {
        PbAddColumn column =
                new PbAddColumn()
                        .setColumnName(name)
                        .setDataTypeJson(
                                JsonSerdeUtils.writeValueAsBytes(type, DataTypeJsonSerde.INSTANCE))
                        .setColumnPositionType(0);
        AlterTableRequest request =
                newAlterTableRequest(
                        path,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        Collections.singletonList(column),
                        false);
        gateway.alterTable(request).get();
    }

    private static Map<String, String> kafkaProperties(TableDescriptor table) {
        Map<String, String> properties = new HashMap<>();
        table.getProperties()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith("table.kafka.")) {
                                properties.put(key, value);
                            }
                        });
        return properties;
    }

    private static TableDescriptor.Builder enabled() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("_kafka_value", DataTypes.BYTES()).build())
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "binary");
    }
}
