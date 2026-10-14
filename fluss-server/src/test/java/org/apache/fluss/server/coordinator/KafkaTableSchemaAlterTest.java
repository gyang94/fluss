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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.entity.TablePropertyChanges;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Deterministic interleavings of schema ALTER and Kafka enablement using real ZooKeeper. */
class KafkaTableSchemaAlterTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZooKeeperClient zk;
    private MetadataManager manager;
    private int epoch;

    @BeforeEach
    void setup() throws Exception {
        zk = ZK.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        epoch = zk.fenceBecomeCoordinatorLeader("kafka-ddl").getCoordinatorEpochZkVersion();
        manager =
                new MetadataManager(
                        zk,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        manager.createDatabase("ddl", DatabaseDescriptor.EMPTY, true);
    }

    @Test
    void testConcurrentSchemaChangePreventsStaleEnablement() throws Exception {
        TablePath path = create("schema_wins");
        ZooKeeperClient.VersionedData<TableRegistration> before =
                zk.getTableWithVersion(path).get();
        int schemaZkVersion = zk.getSchemaZkVersion(path);
        assertThatThrownBy(
                        () ->
                                enable(
                                        path,
                                        (oldTable, updated) ->
                                                manager.alterTableSchema(
                                                        path,
                                                        Collections.singletonList(
                                                                TableChange.addColumn(
                                                                        "unmapped",
                                                                        DataTypes.STRING(),
                                                                        null,
                                                                        TableChange.ColumnPosition
                                                                                .last())),
                                                        false,
                                                        null,
                                                        epoch)))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Concurrent metadata change");
        ZooKeeperClient.VersionedData<TableRegistration> after = zk.getTableWithVersion(path).get();
        assertThat(after.zkVersion()).isEqualTo(before.zkVersion());
        assertThat(after.data()).isEqualTo(before.data());
        assertThat(zk.getSchemaZkVersion(path)).isEqualTo(schemaZkVersion + 1);
        assertThat(manager.getTable(path).getTableConfig().isKafkaEnabled()).isFalse();
        assertThat(manager.getTable(path).getSchema().getRowType().getFieldNames())
                .contains("unmapped");
    }

    @Test
    void testConcurrentEnablementPreventsStaleSchemaPublication() throws Exception {
        TablePath path = create("enable_wins");
        ZooKeeperClient.VersionedData<TableRegistration> snapshot =
                zk.getTableWithVersion(path).get();
        int schemaZkVersion = zk.getSchemaZkVersion(path);
        int schemaId = zk.getCurrentSchemaId(path);
        Schema incompatible =
                Schema.newBuilder()
                        .column("_kafka_value", DataTypes.BYTES())
                        .column("unmapped", DataTypes.STRING())
                        .build();
        enable(path, (oldTable, updated) -> {});
        assertThatThrownBy(
                        () ->
                                zk.registerSchemaWithVersionCheck(
                                        path,
                                        incompatible,
                                        schemaId + 1,
                                        snapshot.zkVersion(),
                                        schemaZkVersion,
                                        epoch))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(zk.getCurrentSchemaId(path)).isEqualTo(schemaId);
        assertThat(manager.getTable(path).getTableConfig().isKafkaEnabled()).isTrue();
        assertThat(manager.getTable(path).getSchema().getRowType().getFieldNames())
                .containsExactly("_kafka_value");
    }

    @Test
    void testSchemaFailureDoesNotAdvanceSchemaVersion() throws Exception {
        TablePath path = create("atomic_failure");
        ZooKeeperClient.VersionedData<TableRegistration> snapshot =
                zk.getTableWithVersion(path).get();
        int schemaZkVersion = zk.getSchemaZkVersion(path);
        int schemaId = zk.getCurrentSchemaId(path);
        Schema schema = manager.getTable(path).getSchema();
        assertThatThrownBy(
                        () ->
                                zk.registerSchemaWithVersionCheck(
                                        path,
                                        schema,
                                        schemaId,
                                        snapshot.zkVersion(),
                                        schemaZkVersion,
                                        epoch))
                .isInstanceOf(KeeperException.NodeExistsException.class);
        assertThat(zk.getTableWithVersion(path).get().zkVersion()).isEqualTo(snapshot.zkVersion());
        assertThat(zk.getSchemaZkVersion(path)).isEqualTo(schemaZkVersion);
        assertThat(zk.getCurrentSchemaId(path)).isEqualTo(schemaId);
        zk.fenceBecomeCoordinatorLeader("next-coordinator");
        assertThatThrownBy(
                        () ->
                                zk.registerSchemaWithVersionCheck(
                                        path,
                                        schema,
                                        schemaId + 1,
                                        snapshot.zkVersion(),
                                        schemaZkVersion,
                                        epoch))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(zk.getTableWithVersion(path).get().zkVersion()).isEqualTo(snapshot.zkVersion());
        assertThat(zk.getSchemaZkVersion(path)).isEqualTo(schemaZkVersion);
        assertThat(zk.getCurrentSchemaId(path)).isEqualTo(schemaId);
    }

    private TablePath create(String name) {
        TablePath path = TablePath.of("ddl", name);
        TableDescriptor table =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("_kafka_value", DataTypes.BYTES())
                                        .build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE, "binary")
                        .build()
                        .withReplicationFactor(1);
        manager.createTable(path, zk.getDefaultRemoteDataDir(), table, null, false);
        return path;
    }

    private void enable(
            TablePath path,
            BiConsumer<org.apache.fluss.metadata.TableInfo, TableDescriptor> beforeUpdate) {
        TablePropertyChanges.Builder changes = new TablePropertyChanges.Builder();
        changes.setTableProperty("table.kafka.enabled", "true");
        manager.alterTableProperties(
                path,
                Collections.singletonList(TableChange.set("table.kafka.enabled", "true")),
                changes.build(),
                false,
                null,
                beforeUpdate,
                (oldTable, updated) -> {},
                ZkVersion.MATCH_ANY_VERSION.getVersion());
    }
}
