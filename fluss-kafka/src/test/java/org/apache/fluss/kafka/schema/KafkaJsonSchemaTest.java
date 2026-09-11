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

import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests JSON table admission shared by Metadata and Produce. */
class KafkaJsonSchemaTest {
    private final KafkaTopicSchemaResolver resolver = new KafkaTopicSchemaResolver();

    @Test
    void testJsonProjectsValuesAroundKeyAndMetadataColumns() {
        KafkaTopicSchema schema =
                resolver.resolve(
                        descriptor()
                                .customProperty(KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                                .customProperty(KafkaDataFormat.KEY_FIELDS_CONFIG, "key")
                                .customProperty(
                                        KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG, "EXCEPT_KEY")
                                .customProperty(KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG, "time")
                                .build());
        assertThat(schema.keyProjection().positions()).containsExactly(1);
        assertThat(schema.valueProjection().positions()).containsExactly(0, 3);
        assertThat(schema.timestampPosition()).isEqualTo(2);
    }

    @Test
    void testRejectsJsonKeysAndUnsupportedFieldTypes() {
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        descriptor()
                                                .customProperty(
                                                        KafkaDataFormat.KEY_FORMAT_CONFIG, "json")
                                                .build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("only supported for record values");
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        TableDescriptor.builder()
                                                .schema(
                                                        Schema.newBuilder()
                                                                .column(
                                                                        "items",
                                                                        DataTypes.MAP(
                                                                                DataTypes.INT(),
                                                                                DataTypes.STRING()))
                                                                .build())
                                                .distributedBy(1)
                                                .logFormat(LogFormat.ARROW)
                                                .customProperty(
                                                        KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                                                .build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("only supports STRING map keys");
    }

    @Test
    void testRescueColumnMustBeNullableStringInTheValueProjection() {
        KafkaTopicSchema schema = resolver.resolve(rescueDescriptor(DataTypes.STRING()).build());
        assertThat(schema.valueRescueColumn()).isEqualTo("rescue");
        for (String name : new String[] {"", " ", "missing", "id"}) {
            assertThatThrownBy(
                            () ->
                                    resolver.resolve(
                                            rescueDescriptor(DataTypes.STRING())
                                                    .customProperty(
                                                            KafkaDataFormat
                                                                    .VALUE_RESCUE_COLUMN_CONFIG,
                                                            name)
                                                    .build()))
                    .isInstanceOf(KafkaTopicSchemaException.class);
        }
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        rescueDescriptor(DataTypes.STRING().copy(false)).build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("nullable STRING");
        assertThatThrownBy(() -> resolver.resolve(rescueDescriptor(DataTypes.INT()).build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("nullable STRING");
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        descriptor()
                                                .customProperty(
                                                        KafkaDataFormat.KEY_FORMAT_CONFIG, "string")
                                                .customProperty(
                                                        KafkaDataFormat.KEY_FIELDS_CONFIG, "key")
                                                .customProperty(
                                                        KafkaDataFormat.VALUE_FIELDS_INCLUDE_CONFIG,
                                                        "EXCEPT_KEY")
                                                .customProperty(
                                                        KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG,
                                                        "key")
                                                .build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("not in the value projection");
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                        descriptor()
                                                .customProperty(
                                                        KafkaDataFormat.TIMESTAMP_COLUMN_CONFIG,
                                                        "time")
                                                .customProperty(
                                                        KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG,
                                                        "time")
                                                .build()))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("not in the value projection");
    }

    @Test
    void testRawAndStringFormatsRejectRescueOptions() {
        for (String format : new String[] {"raw", "string"}) {
            TableDescriptor table =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column(
                                                    "body",
                                                    format.equals("raw")
                                                            ? DataTypes.BYTES()
                                                            : DataTypes.STRING())
                                            .build())
                            .distributedBy(1)
                            .logFormat(LogFormat.ARROW)
                            .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, format)
                            .customProperty(KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG, "body")
                            .build();
            assertThatThrownBy(() -> resolver.resolve(table))
                    .isInstanceOf(KafkaTopicSchemaException.class)
                    .hasMessageContaining("only supported for JSON");
        }
    }

    private static TableDescriptor.Builder rescueDescriptor(
            org.apache.fluss.types.DataType rescueType) {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("rescue", rescueType)
                                .build())
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                .customProperty(KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG, "rescue");
    }

    private static TableDescriptor.Builder descriptor() {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT().copy(false))
                                .column("key", DataTypes.STRING())
                                .column("time", DataTypes.TIMESTAMP_LTZ(3).copy(false))
                                .column("amount", DataTypes.DECIMAL(8, 2))
                                .build())
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json");
    }
}
