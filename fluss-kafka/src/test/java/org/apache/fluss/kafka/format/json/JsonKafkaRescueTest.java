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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests unknown-field rescue while keeping known-field and resource violations fatal. */
class JsonKafkaRescueTest {

    @Test
    void testUnexpectedConversionFailureIsNotRescued() {
        JsonNode broken = mock(JsonNode.class);
        IllegalStateException failure = new IllegalStateException("unexpected conversion failure");
        when(broken.getNodeType()).thenReturn(JsonNodeType.BOOLEAN);
        when(broken.booleanValue()).thenThrow(failure);
        assertThatThrownBy(
                        () ->
                                JsonToFlussConverters.create(DataTypes.BOOLEAN())
                                        .convert(broken, "$"))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasCause(failure);
    }

    @ParameterizedTest
    @MethodSource("invalidKnownValues")
    void testKnownFailuresRemainFatalAcrossNullableAndRequiredContainers(
            DataType dataType, String jsonValue, String reason) {
        for (boolean nullable : new boolean[] {true, false}) {
            assertRejectedInAllContainers(dataType.copy(nullable), jsonValue, reason);
        }
    }

    private static Stream<Arguments> invalidKnownValues() {
        return Stream.of(
                Arguments.of(DataTypes.BOOLEAN(), "1", "expected a JSON boolean"),
                Arguments.of(DataTypes.TINYINT(), "128", "integer overflow"),
                Arguments.of(DataTypes.SMALLINT(), "32768", "integer overflow"),
                Arguments.of(DataTypes.INT(), "2147483648", "integer overflow"),
                Arguments.of(DataTypes.INT(), "1.5", "expected a JSON integer"),
                Arguments.of(DataTypes.INT(), "\"bad\"", "expected a JSON integer"),
                Arguments.of(DataTypes.BIGINT(), "9223372036854775808", "integer overflow"),
                Arguments.of(DataTypes.DECIMAL(3, 1), "123.4", "decimal precision exceeds target"),
                Arguments.of(DataTypes.DECIMAL(3, 1), "1.23", "decimal scale exceeds target"),
                Arguments.of(DataTypes.FLOAT(), "1e40", "non-finite or overflowing number"),
                Arguments.of(DataTypes.DOUBLE(), "1e400", "non-finite or overflowing number"),
                Arguments.of(DataTypes.DATE(), "\"invalid\"", "invalid ISO-8601 date"),
                Arguments.of(DataTypes.TIME(3), "\"invalid\"", "invalid ISO-8601 local time"),
                Arguments.of(
                        DataTypes.TIMESTAMP(3), "\"invalid\"", "invalid ISO-8601 local timestamp"),
                Arguments.of(
                        DataTypes.TIMESTAMP_LTZ(3), "\"invalid\"", "invalid ISO-8601 timestamp"),
                Arguments.of(DataTypes.CHAR(1), "\"ab\"", "character length exceeds target"),
                Arguments.of(DataTypes.BINARY(2), "\"YQ==\"", "decoded binary length must equal 2"),
                Arguments.of(DataTypes.BYTES(), "\"%%%\"", "invalid Base64 string"),
                Arguments.of(DataTypes.STRING(), "1", "expected a JSON string"),
                Arguments.of(DataTypes.STRING(), "{}", "expected a JSON"),
                Arguments.of(DataTypes.STRING(), "[]", "expected a JSON"),
                Arguments.of(
                        DataTypes.ROW(DataTypes.FIELD("n", DataTypes.INT())),
                        "[]",
                        "expected a JSON object"),
                Arguments.of(DataTypes.ARRAY(DataTypes.INT()), "{}", "expected a JSON array"),
                Arguments.of(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        "[]",
                        "expected a JSON object"));
    }

    private static void assertRejectedInAllContainers(
            DataType dataType, String value, String reason) {
        JsonKafkaFieldDecoder decoder =
                rescueDecoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("value", dataType),
                                DataTypes.FIELD(
                                        "row", DataTypes.ROW(DataTypes.FIELD("value", dataType))),
                                DataTypes.FIELD("items", DataTypes.ARRAY(dataType)),
                                DataTypes.FIELD(
                                        "attributes", DataTypes.MAP(DataTypes.STRING(), dataType)),
                                DataTypes.FIELD("rescue", DataTypes.STRING())),
                        "rescue");
        String[][] cases = {
            {"\"value\":" + value, "$[\"value\"]"},
            {"\"row\":{\"value\":" + value + ",\"extra\":2}", "$[\"row\"][\"value\"]"},
            {"\"items\":[" + value + "]", "$[\"items\"][0]"},
            {"\"attributes\":{\"dynamic\":" + value + "}", "$[\"attributes\"][\"dynamic\"]"}
        };
        for (String[] entry : cases) {
            String json = "{\"extra\":1," + entry[0] + "}";
            assertThatThrownBy(() -> decoder.decode(bytes(json)))
                    .as("%s: %s", dataType, json)
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining(entry[1])
                    .hasMessageContaining(reason);
        }
    }

    @Test
    void testNullableParentNeverRescuesRequiredDescendantFailures() {
        assertRejectedInAllContainers(
                DataTypes.INT().copy(false), "null", "field is missing or null");
        RowType required = DataTypes.ROW(DataTypes.FIELD("required", DataTypes.INT().copy(false)));
        for (String value : new String[] {"{\"required\":\"bad\"}", "{}", "{\"required\":null}"}) {
            assertRejectedInAllContainers(required, value, "required");
        }
        JsonKafkaFieldDecoder decoder =
                rescueDecoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("required", DataTypes.INT().copy(false)),
                                DataTypes.FIELD("rescue", DataTypes.STRING())),
                        "rescue");
        assertThatThrownBy(() -> decoder.decode(bytes("{\"extra\":1}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("field is missing or null");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{",
                "[]",
                "{\"extra\":1,\"extra\":2}",
                "{\"rescue\":null,\"rescue\":1}",
                "{\"extra\":{\"x\":1,\"x\":2}}",
                "{} {}",
                "{\"rescue\":\"\\uD800\"}",
                "{\"rescue\":1e400}"
            })
    void testInvalidJsonCannotBeRescued(String json) {
        assertThatThrownBy(() -> fidelityDecoder().decode(bytes(json)))
                .isInstanceOf(KafkaRecordEncodingException.class);
    }

    @Test
    void testRescueNameStillEnforcesResourceLimitsAndUtf8() {
        JsonKafkaFieldDecoder decoder =
                rescueDecoder(
                        DataTypes.ROW(DataTypes.FIELD("rescue", DataTypes.STRING())), "rescue");
        String atLimit = containerAtLimit(false);
        assertThat(decoder.decode(bytes("{\"rescue\":" + atLimit + "}"))[0].toString())
                .isEqualTo("{\"rescue\":" + atLimit + "}");
        assertThatThrownBy(
                        () ->
                                decoder.decode(
                                        bytes(
                                                "{\"rescue\":"
                                                        + atLimit.substring(0, atLimit.length() - 1)
                                                        + ",0]}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("container size exceeds");
        String nested = "0";
        for (int i = 0; i < JsonKafkaFieldDecoder.MAX_JSON_NESTING_DEPTH; i++) {
            nested = "[" + nested + "]";
        }
        byte[] tooDeep = bytes("{\"rescue\":" + nested + "}");
        assertThatThrownBy(() -> decoder.decode(tooDeep))
                .isInstanceOf(KafkaRecordEncodingException.class);
        byte[] invalidUtf8 = bytes("{\"rescue\":\"x\"}");
        invalidUtf8[invalidUtf8.length - 3] = (byte) 0xff;
        assertThatThrownBy(() -> decoder.decode(invalidUtf8))
                .isInstanceOf(KafkaRecordEncodingException.class);
    }

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
                        "{\"extra\":\"unexpected\",\"kafka_rescue\":null,"
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

    @ParameterizedTest
    @ValueSource(
            strings = {
                "null",
                "\"payload\"",
                "7",
                "false",
                "[1,{\"x\":2}]",
                "{\"rescue\":\"nested\",\"x\":1}"
            })
    void testPayloadRescueNameIsPreservedAsJsonMember(String value) {
        JsonKafkaFieldDecoder decoder =
                rescueDecoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.INT()),
                                DataTypes.FIELD("rescue", DataTypes.STRING())),
                        "rescue");
        Object[] decoded = decoder.decode(bytes("{\"id\":1,\"rescue\":" + value + ",\"extra\":2}"));
        assertThat(decoded[0]).isEqualTo(1);
        assertThat(decoded[1].toString()).isEqualTo("{\"rescue\":" + value + ",\"extra\":2}");
        assertThat(decoder.decode(bytes("{\"id\":1}"))[1]).isNull();
        assertThat(decoder.decode(bytes("{\"rescue\":null}"))[1].toString())
                .isEqualTo("{\"rescue\":null}");
    }

    @Test
    void testRescueNameIsConfigurationDrivenAndNestedBusinessFieldsKeepTheirMeaning() {
        RowType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("_kafka_key", DataTypes.STRING()),
                        DataTypes.FIELD("_kafka_headers", DataTypes.STRING()),
                        DataTypes.FIELD("_kafka_metadata", DataTypes.STRING()),
                        DataTypes.FIELD("_kafka_value", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "row", DataTypes.ROW(DataTypes.FIELD("rescue", DataTypes.INT()))),
                        DataTypes.FIELD("rescue", DataTypes.STRING()));
        String json =
                "{\"_kafka_key\":\"k\",\"_kafka_headers\":\"h\",\"_kafka_metadata\":\"m\","
                        + "\"_kafka_value\":\"v\",\"row\":{\"rescue\":7,\"extra\":8}}";
        Object[] values = rescueDecoder(schema, "rescue").decode(bytes(json));
        for (int i = 0; i < 4; i++) {
            assertThat(values[i].toString()).isEqualTo(new String[] {"k", "h", "m", "v"}[i]);
        }
        assertThat(((GenericRow) values[4]).getInt(0)).isEqualTo(7);
        assertThat(values[5].toString()).isEqualTo("{\"row\":{\"extra\":8}}");
        assertThatThrownBy(() -> rescueDecoder(schema, null).decode(bytes(json)))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("unknown field");
        assertThat(
                        rescueDecoder(schema, null)
                                .decode(bytes("{\"rescue\":\"business\"}"))[5]
                                .toString())
                .isEqualTo("business");
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

    @ParameterizedTest
    @ValueSource(strings = {"\\uD800", "\\uDC00", "a\\uD800b", "\\uD800\\uD800", "\\uDC00\\uD800"})
    void testRejectsUnpairedSurrogatesBeforeRescueEncoding(String escaped) {
        JsonKafkaFieldDecoder decoder = fidelityDecoder();
        for (String json :
                new String[] {
                    "{\"" + escaped + "\":1,\"?\":2}",
                    "{\"extra\":\"" + escaped + "\"}",
                    "{\"extra\":{\"" + escaped + "\":1,\"?\":2}}",
                    "{\"extra\":[{\"key\":\"" + escaped + "\"}]}",
                    "{\"details\":{\"" + escaped + "\":1,\"?\":2}}",
                    "{\"items\":[{\"extra\":\"" + escaped + "\"}]}",
                    "{\"attributes\":{\"" + escaped + "\":{\"known\":1},\"?\":{\"known\":2}}}",
                    "{\"attributes\":{\"dynamic\":{\"extra\":\"" + escaped + "\"}}}"
                }) {
            assertThatThrownBy(() -> decoder.decode(bytes(json)))
                    .as(json)
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("unpaired UTF-16 surrogate");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"1e400", "-1e400", "1e10000", "-1e10000", "1.7976931348623159e308"})
    void testRejectsOverflowingNumbersBeforeRescueEncoding(String number) {
        JsonKafkaFieldDecoder decoder = fidelityDecoder();
        for (String json :
                new String[] {
                    "{\"extra\":" + number + "}",
                    "{\"extra\":{\"nested\":" + number + "}}",
                    "{\"extra\":[" + number + "]}",
                    "{\"details\":{\"extra\":" + number + "}}",
                    "{\"items\":[{\"extra\":" + number + "}]}",
                    "{\"attributes\":{\"dynamic\":{\"extra\":" + number + "}}}"
                }) {
            assertThatThrownBy(() -> decoder.decode(bytes(json)))
                    .as(json)
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("non-finite or overflowing number");
        }
    }

    @Test
    void testRescuePreservesValidUnicodeAndExactNumbers() throws Exception {
        String largeInteger = BigInteger.TEN.pow(400).subtract(BigInteger.ONE).toString();
        String decimal = "12345678901234567890.1234567890123456789";
        Object[] values =
                fidelityDecoder()
                        .decode(
                                bytes(
                                        "{\"\\uD83D\\uDE00\":\"\\uD83D\\uDE00\","
                                                + "\"extra\":{\"integer\":"
                                                + largeInteger
                                                + ",\"decimal\":"
                                                + decimal
                                                + ",\"small\":1e-400,\"large\":1e308,"
                                                + "\"text\":\"Infinity\",\"literal\":\"\\\\uD800\"},"
                                                + "\"details\":{\"known\":7,\"\\uD83D\\uDE00\":\"ok\"},"
                                                + "\"items\":[{\"known\":8,\"extra\":\"\\uD83D\\uDE00\"}],"
                                                + "\"attributes\":{\"\\uD83D\\uDE00\":{\"known\":9,\"extra\":1}}}"));
        ObjectMapper mapper =
                new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        JsonNode rescued = mapper.readTree(((BinaryString) values[3]).toString());
        assertThat(rescued.get("😀").textValue()).isEqualTo("😀");
        JsonNode extra = rescued.get("extra");
        assertThat(extra.get("integer").bigIntegerValue()).isEqualTo(new BigInteger(largeInteger));
        assertThat(extra.get("decimal").decimalValue())
                .isEqualByComparingTo(new BigDecimal(decimal));
        assertThat(extra.get("small").decimalValue())
                .isEqualByComparingTo(new BigDecimal("1e-400"));
        assertThat(extra.get("large").decimalValue()).isEqualByComparingTo(new BigDecimal("1e308"));
        assertThat(extra.get("text").textValue()).isEqualTo("Infinity");
        assertThat(extra.get("literal").textValue()).isEqualTo("\\uD800");
        assertThat(rescued.get("details").get("😀").textValue()).isEqualTo("ok");
        assertThat(rescued.get("items").get(0).get("extra").textValue()).isEqualTo("😀");
        assertThat(rescued.get("attributes").get("😀").get("extra").intValue()).isEqualTo(1);
        assertThat(((GenericRow) values[0]).getInt(0)).isEqualTo(7);
        assertThat(((GenericArray) values[1]).getRow(0, 1).getInt(0)).isEqualTo(8);
        assertThat(
                        ((GenericRow) ((GenericMap) values[2]).get(BinaryString.fromString("😀")))
                                .getInt(0))
                .isEqualTo(9);
    }

    private static JsonKafkaFieldDecoder fidelityDecoder() {
        RowType nestedRow = DataTypes.ROW(DataTypes.FIELD("known", DataTypes.INT()));
        return rescueDecoder(
                DataTypes.ROW(
                        DataTypes.FIELD("details", nestedRow),
                        DataTypes.FIELD("items", DataTypes.ARRAY(nestedRow)),
                        DataTypes.FIELD("attributes", DataTypes.MAP(DataTypes.STRING(), nestedRow)),
                        DataTypes.FIELD("rescue", DataTypes.STRING())),
                "rescue");
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
