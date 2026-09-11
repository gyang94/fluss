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
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests optional unknown-field rescue while preserving strict type validation. */
class JsonKafkaRescueTest {

    @Test
    void testRescuesUnknownRootAndNestedFieldsAsJsonString() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT().copy(false)),
                        DataTypes.FIELD(
                                "customer",
                                DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING()))),
                        DataTypes.FIELD(
                                "items",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.FIELD("sku", DataTypes.STRING())))),
                        DataTypes.FIELD(
                                "attributes",
                                DataTypes.MAP(
                                        DataTypes.STRING(),
                                        DataTypes.ROW(DataTypes.FIELD("known", DataTypes.INT())))),
                        DataTypes.FIELD("kafka_rescue", DataTypes.STRING()));
        JsonKafkaFieldDecoder decoder = rescueDecoder(rowType, "kafka_rescue");

        Object[] values =
                decoder.decode(
                        bytes(
                                "{"
                                        + "\"id\":1,"
                                        + "\"customer\":{\"name\":\"Alice\",\"extra_nested\":2},"
                                        + "\"items\":[{\"sku\":\"A-1\",\"extra_item\":3}],"
                                        + "\"attributes\":{\"dynamic\":{\"known\":1,"
                                        + "\"extra_map_value\":4}},"
                                        + "\"extra\":\"unexpected\","
                                        + "\"kafka_rescue\":null"
                                        + "}"));

        assertThat(values[0]).isEqualTo(1L);
        assertThat(((GenericRow) values[1]).getString(0).toString()).isEqualTo("Alice");
        assertThat(((GenericArray) values[2]).getRow(0, 1).getString(0).toString())
                .isEqualTo("A-1");
        GenericMap attributes = (GenericMap) values[3];
        assertThat(((GenericRow) attributes.get(BinaryString.fromString("dynamic"))).getInt(0))
                .isEqualTo(1);
        assertThat(((BinaryString) values[4]).toString())
                .isEqualTo(
                        "{\"extra\":\"unexpected\","
                                + "\"customer\":{\"extra_nested\":2},"
                                + "\"items\":[{\"extra_item\":3}],"
                                + "\"attributes\":{\"dynamic\":{\"extra_map_value\":4}}}");

        assertThat(
                        decoder.decode(
                                        bytes(
                                                "{\"id\":1,\"customer\":null,\"items\":null,"
                                                        + "\"attributes\":null}"))[4])
                .isNull();
    }

    @Test
    void testRejectsExplicitNonNullRescueColumnAndKeepsTypeChecksStrict() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("kafka_rescue", DataTypes.STRING()));
        JsonKafkaFieldDecoder decoder = rescueDecoder(rowType, "kafka_rescue");

        assertThatThrownBy(() -> decoder.decode(bytes("{\"id\":1,\"kafka_rescue\":\"supplied\"}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("configured rescue column is reserved");
        assertThatThrownBy(() -> decoder.decode(bytes("{\"id\":\"not-a-number\"}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("expected a JSON integer");
    }

    @Test
    void testRescueArrayPreservesOriginalElementPositions() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "items",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())))),
                        DataTypes.FIELD("rescue", DataTypes.STRING()));
        Object[] values =
                rescueDecoder(rowType, "rescue")
                        .decode(bytes("{\"items\":[{\"id\":1},{\"id\":2,\"extra\":3}]}"));
        assertThat(((BinaryString) values[1]).toString())
                .isEqualTo("{\"items\":[null,{\"extra\":3}]}");
        assertThat(((GenericArray) values[0]).getRow(1, 1).getInt(0)).isEqualTo(2);
    }

    @Test
    void testRescuesRootObjectAtLimitAndRejectsExcessFieldBeforeReadingItsValue() {
        JsonKafkaFieldDecoder decoder =
                rescueDecoder(
                        DataTypes.ROW(DataTypes.FIELD("rescue", DataTypes.STRING())), "rescue");
        String object = containerAtLimit(true);
        assertThat(((BinaryString) decoder.decode(bytes(object))[0]).toString()).isEqualTo(object);

        String excessField = object.substring(0, object.length() - 1) + ",\"excess\":{";
        assertThatThrownBy(() -> decoder.decode(bytes(excessField)))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining(
                        "maximum field count of " + JsonToFlussConverters.MAX_CONTAINER_ELEMENTS);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("rescuedContainers")
    void testBoundsUnknownContainersBeforeReadingExcessChild(
            String description, String prefix, String suffix, String path, boolean object) {
        RowType nestedRow = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));
        JsonKafkaFieldDecoder decoder =
                rescueDecoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("details", nestedRow),
                                DataTypes.FIELD("items", DataTypes.ARRAY(nestedRow)),
                                DataTypes.FIELD(
                                        "attributes", DataTypes.MAP(DataTypes.STRING(), nestedRow)),
                                DataTypes.FIELD("rescue", DataTypes.STRING())),
                        "rescue");
        String container = containerAtLimit(object);
        String json = prefix + container + suffix;
        assertThat(((BinaryString) decoder.decode(bytes(json))[3]).toString()).isEqualTo(json);

        // An unfinished excess child distinguishes the container limit from a syntax error.
        String excessChild =
                prefix
                        + container.substring(0, container.length() - 1)
                        + (object ? ",\"excess\":{" : ",[");
        assertThatThrownBy(() -> decoder.decode(bytes(excessChild)))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining(path)
                .hasMessageContaining(
                        "container size exceeds " + JsonToFlussConverters.MAX_CONTAINER_ELEMENTS);
    }

    private static Stream<Arguments> rescuedContainers() {
        return Stream.of(false, true)
                .flatMap(
                        object ->
                                Stream.of(
                                        Arguments.of(
                                                "unknown root " + (object ? "object" : "array"),
                                                "{\"extra\":",
                                                "}",
                                                "$[\"extra\"]",
                                                object),
                                        Arguments.of(
                                                "unknown subtree " + (object ? "object" : "array"),
                                                "{\"extra\":{\"nested\":",
                                                "}}",
                                                "$[\"extra\"][\"nested\"]",
                                                object),
                                        Arguments.of(
                                                "inside ROW " + (object ? "object" : "array"),
                                                "{\"details\":{\"extra\":",
                                                "}}",
                                                "$[\"details\"][\"extra\"]",
                                                object),
                                        Arguments.of(
                                                "inside ARRAY " + (object ? "object" : "array"),
                                                "{\"items\":[{\"extra\":",
                                                "}]}",
                                                "$[\"items\"][0][\"extra\"]",
                                                object),
                                        Arguments.of(
                                                "inside MAP " + (object ? "object" : "array"),
                                                "{\"attributes\":{\"dynamic\":{\"extra\":",
                                                "}}}",
                                                "$[\"attributes\"][\"dynamic\"][\"extra\"]",
                                                object)));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {"-0", "-0.0", "-0e3", "-0e-3", "1.0000000596046448", "-1.0000000596046448"})
    void testRescuePreservesRootFloatToken(String number) {
        JsonKafkaFieldDecoder decoder =
                rescueDecoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("value", DataTypes.FLOAT()),
                                DataTypes.FIELD("rescue", DataTypes.STRING())),
                        "rescue");
        Object[] values = decoder.decode(bytes("{\"extra\":1,\"value\":" + number + "}"));
        assertThat(Float.floatToRawIntBits((Float) values[0]))
                .isEqualTo(Float.floatToRawIntBits(Float.parseFloat(number)));
        assertThat(((BinaryString) values[1]).toString()).isEqualTo("{\"extra\":1}");
    }

    @ParameterizedTest
    @ValueSource(strings = {"[", "{"})
    void testRescueRejectsContainersForKnownScalarsBeforeReadingTheirContents(String container) {
        JsonKafkaFieldDecoder decoder =
                rescueDecoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("value", DataTypes.STRING()),
                                DataTypes.FIELD("rescue", DataTypes.STRING())),
                        "rescue");
        assertThatThrownBy(() -> decoder.decode(bytes("{\"extra\":1,\"value\":" + container)))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("$[\"value\"]")
                .hasMessageContaining("expected a JSON scalar");
    }

    private static String containerAtLimit(boolean object) {
        StringBuilder json = new StringBuilder(object ? "{" : "[");
        for (int i = 0; i < JsonToFlussConverters.MAX_CONTAINER_ELEMENTS; i++) {
            if (i > 0) {
                json.append(',');
            }
            if (object) {
                json.append('"').append('k').append(i).append("\":");
            }
            json.append('0');
        }
        return json.append(object ? '}' : ']').toString();
    }

    private static JsonKafkaFieldDecoder rescueDecoder(RowType rowType, String rescueColumn) {
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            positions.add(i);
        }
        return new JsonKafkaFieldDecoder(
                new KafkaFieldProjection(rowType, positions), rescueColumn);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
