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

package org.apache.fluss.kafka.format.json;

import org.apache.fluss.kafka.schema.KafkaFieldProjection;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests recursive JSON conversion, nested validation and resource bounds. */
class JsonKafkaComplexTypesTest {

    @Test
    void testDecodesRecursiveRowArrayAndMapTypes() {
        RowType rowType = complexRowType();

        Object[] values =
                decoder(rowType)
                        .decode(
                                bytes(
                                        "{"
                                                + "\"customer\":{\"name\":\"Alice\","
                                                + "\"address\":{\"city\":\"Hangzhou\"}},"
                                                + "\"items\":[{\"sku\":\"A-100\","
                                                + "\"quantity\":2}],"
                                                + "\"attributes\":{\"scores\":[7,null,9]}"
                                                + "}"));

        GenericRow customer = (GenericRow) values[0];
        assertThat(customer.getString(0).toString()).isEqualTo("Alice");
        assertThat(customer.getRow(1, 1).getString(0).toString()).isEqualTo("Hangzhou");

        GenericArray items = (GenericArray) values[1];
        assertThat(items.size()).isEqualTo(1);
        assertThat(items.getRow(0, 2).getString(0).toString()).isEqualTo("A-100");
        assertThat(items.getRow(0, 2).getInt(1)).isEqualTo(2);

        GenericMap attributes = (GenericMap) values[2];
        GenericArray scores = (GenericArray) attributes.get(BinaryString.fromString("scores"));
        assertThat(scores.toObjectArray()).containsExactly(7, null, 9);
    }

    @Test
    void testRejectsInvalidNestedValuesWithPrecisePaths() {
        assertFailure(
                complexRowType(),
                "{\"customer\":{\"address\":{\"city\":\"Hangzhou\"}},"
                        + "\"items\":[],\"attributes\":{}}",
                "$[\"customer\"][\"name\"]",
                "missing or null");
        assertFailure(
                complexRowType(),
                "{\"customer\":{\"name\":\"Alice\","
                        + "\"address\":{\"city\":\"Hangzhou\"}},"
                        + "\"items\":[{\"sku\":\"A-100\",\"quantity\":2},"
                        + "{\"sku\":\"A-200\",\"quantity\":\"bad\"}],"
                        + "\"attributes\":{}}",
                "$[\"items\"][1][\"quantity\"]",
                "expected a JSON integer");
        assertFailure(
                complexRowType(),
                "{\"customer\":{\"name\":\"Alice\","
                        + "\"address\":{\"city\":\"Hangzhou\"}},"
                        + "\"items\":[],\"attributes\":{\"bad\\\"key\":[\"bad\"]}}",
                "$[\"attributes\"][\"bad\\\"key\"][0]",
                "expected a JSON integer");
    }

    @Test
    void testRejectsUnknownRootAndNestedRowFieldsButAllowsMapKeys() {
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())),
                "{\"id\":1,\"extra\":\"unexpected\"}",
                "$[\"extra\"]",
                "unknown field");
        assertFailure(
                complexRowType(),
                "{\"customer\":{\"name\":\"Alice\","
                        + "\"address\":{\"city\":\"Hangzhou\",\"extra\":1}},"
                        + "\"items\":[],\"attributes\":{}}",
                "$[\"customer\"][\"address\"][\"extra\"]",
                "unknown field");

        Object[] values =
                decoder(
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "attributes",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.INT()))))
                        .decode(bytes("{\"attributes\":{\"dynamic-key\":1}}"));
        GenericMap attributes = (GenericMap) values[0];
        assertThat(attributes.size()).isEqualTo(1);
        assertThat(attributes.get(BinaryString.fromString("dynamic-key"))).isEqualTo(1);
    }

    @Test
    void testRejectsNonStringMapKeyBeforeDecodingRecords() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "attributes", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())));

        assertThatThrownBy(() -> decoder(rowType))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("only supports STRING map keys")
                .hasMessageContaining("MAP<INT");
    }

    @Test
    void testEnforcesSchemaJsonAndContainerResourceBounds() {
        DataType nestedType = DataTypes.STRING();
        for (int i = 0; i <= JsonToFlussConverters.MAX_SCHEMA_NESTING_DEPTH; i++) {
            nestedType = DataTypes.ARRAY(nestedType);
        }
        final DataType tooDeepType = nestedType;
        assertThatThrownBy(() -> decoder(DataTypes.ROW(DataTypes.FIELD("nested", tooDeepType))))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("maximum nesting depth");

        StringBuilder nestedJson = new StringBuilder("{\"value\":");
        for (int i = 0; i < JsonKafkaFieldDecoder.MAX_JSON_NESTING_DEPTH; i++) {
            nestedJson.append("{\"nested\":");
        }
        nestedJson.append('0');
        for (int i = 0; i < JsonKafkaFieldDecoder.MAX_JSON_NESTING_DEPTH; i++) {
            nestedJson.append('}');
        }
        nestedJson.append('}');
        JsonKafkaFieldDecoder complexDecoder =
                decoder(
                        DataTypes.ROW(
                                DataTypes.FIELD(
                                        "value",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))));
        assertThatThrownBy(() -> complexDecoder.decode(bytes(nestedJson.toString())))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("not valid strict UTF-8 JSON");

        StringBuilder oversizedArray = new StringBuilder("{\"values\":[");
        for (int i = 0; i <= JsonToFlussConverters.MAX_CONTAINER_ELEMENTS; i++) {
            if (i > 0) {
                oversizedArray.append(',');
            }
            oversizedArray.append('0');
        }
        oversizedArray.append("]}");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT()))),
                oversizedArray.toString(),
                "$[\"values\"]",
                "container size exceeds");
    }

    @Test
    void testBoundsAndEscapesMapKeysInErrorPaths() {
        StringBuilder mapKey = new StringBuilder("quote\"");
        for (int i = 0; i < 300; i++) {
            mapKey.append('x');
        }
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));

        assertThatThrownBy(
                        () ->
                                decoder(rowType)
                                        .decode(
                                                bytes(
                                                        "{\"attributes\":{"
                                                                + quoteJson(mapKey.toString())
                                                                + ":\"bad\"}}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("$[\"attributes\"][\"quote\\\"")
                .hasMessageContaining("...")
                .satisfies(
                        error -> assertThat(error.getMessage()).doesNotContain(mapKey.toString()));

        StringBuilder controlCharacters = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            controlCharacters.append('\u0001');
        }
        String boundedPath = JsonPath.field(JsonPath.ROOT, controlCharacters.toString());
        assertThat(boundedPath)
                .hasSizeLessThanOrEqualTo(512)
                .endsWith("\"]")
                .doesNotContain("\\u0...");
    }

    private static void assertFailure(RowType rowType, String json, String path, String reason) {
        assertThatThrownBy(() -> decoder(rowType).decode(bytes(json)))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining(path)
                .hasMessageContaining(reason);
    }

    private static JsonKafkaFieldDecoder decoder(RowType rowType) {
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            positions.add(i);
        }
        return new JsonKafkaFieldDecoder(new KafkaFieldProjection(rowType, positions));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static RowType complexRowType() {
        return DataTypes.ROW(
                DataTypes.FIELD(
                        "customer",
                        DataTypes.ROW(
                                DataTypes.FIELD("name", DataTypes.STRING().copy(false)),
                                DataTypes.FIELD(
                                        "address",
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "city", DataTypes.STRING().copy(false)))))),
                DataTypes.FIELD(
                        "items",
                        DataTypes.ARRAY(
                                DataTypes.ROW(
                                        DataTypes.FIELD("sku", DataTypes.STRING().copy(false)),
                                        DataTypes.FIELD("quantity", DataTypes.INT().copy(false))))),
                DataTypes.FIELD(
                        "attributes",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))));
    }

    private static String quoteJson(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
