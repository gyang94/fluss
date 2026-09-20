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
import org.apache.fluss.row.Decimal;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests strict schema-aware JSON scalar conversion. */
class JsonKafkaFieldDecoderTest {

    @Test
    void testRejectsMalformedAndAmbiguousJson() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT().copy(false)),
                        DataTypes.FIELD("optional", DataTypes.STRING()));
        JsonKafkaFieldDecoder decoder = decoder(rowType);

        assertThatThrownBy(() -> decoder.decode(bytes("[]")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("object root");
        assertThatThrownBy(() -> decoder.decode(bytes("{\"id\":1,\"id\":2}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("strict UTF-8 JSON");
        assertThatThrownBy(() -> decoder.decode(bytes("{\"id\":1} {}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("strict UTF-8 JSON");
        assertThatThrownBy(() -> decoder.decode(bytes("{")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("strict UTF-8 JSON");
    }

    @Test
    void testRejectsMissingTypeOverflowAndPrecisionLoss() {
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT().copy(false))),
                "{}",
                "$[\"id\"]",
                "missing or null");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())),
                "{\"id\":2147483648}",
                "$[\"id\"]",
                "integer overflow");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("amount", DataTypes.DECIMAL(5, 2))),
                "{\"amount\":1.234}",
                "$[\"amount\"]",
                "scale exceeds");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP_LTZ(3))),
                "{\"timestamp\":\"2026-07-27T19:30:12.123\"}",
                "$[\"timestamp\"]",
                "offset or Z is required");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("fixed", DataTypes.BINARY(2))),
                "{\"fixed\":\"AQ==\"}",
                "$[\"fixed\"]",
                "length must equal 2");
    }

    @Test
    void testNullKafkaValueRequiresNullableProjection() {
        JsonKafkaFieldDecoder nullableDecoder =
                decoder(DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING())));
        assertThat(nullableDecoder.decode(null)).containsExactly((Object) null);

        JsonKafkaFieldDecoder notNullDecoder =
                decoder(DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING().copy(false))));
        assertThatThrownBy(() -> notNullDecoder.decode(null))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("NOT NULL field 'value'");
    }

    @Test
    void testExactDecimalParsingAndUnknownFields() {
        JsonKafkaFieldDecoder decoder =
                decoder(DataTypes.ROW(DataTypes.FIELD("amount", DataTypes.DECIMAL(30, 10))));
        Decimal value =
                (Decimal) decoder.decode(bytes("{\"amount\":12345678901234567890.1234567890}"))[0];
        assertThat(value.toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("12345678901234567890.1234567890"));
        assertThatThrownBy(() -> decoder.decode(bytes("{\"extra\":1}")))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("unknown field");
        assertThat(decoder.decode(bytes("{}"))).containsExactly((Object) null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"[", "{"})
    void testRejectsContainersBeforeReadingTheirContents(String container) {
        // The unfinished value must fail the scalar check before parsing its contents.
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING())),
                "{\"value\":" + container,
                "$[\"value\"]",
                "scalar");
        assertFailure(
                DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING())),
                "{\"extra\":" + container,
                "$[\"extra\"]",
                "unknown field");
    }

    @Test
    void testFieldCountLimitBeforeReadingExcessFieldValue() {
        int maximumFields = JsonKafkaFieldDecoder.MAX_CONTAINER_ELEMENTS;
        List<DataField> fields = new ArrayList<>();
        StringBuilder json = new StringBuilder("{");
        Object[] expected = new Object[maximumFields + 1];
        for (int i = 0; i <= maximumFields; i++) {
            fields.add(DataTypes.FIELD("field" + i, DataTypes.INT()));
            if (i < maximumFields) {
                if (i > 0) {
                    json.append(',');
                }
                json.append("\"field").append(i).append("\":").append(i);
                expected[i] = i;
            }
        }
        JsonKafkaFieldDecoder decoder = decoder(new RowType(fields));
        assertThat(decoder.decode(bytes(json + "}"))).containsExactly(expected);

        String excessField = json + ",\"field" + maximumFields + "\":";
        for (String value : new String[] {"0}", "["}) {
            assertThatThrownBy(() -> decoder.decode(bytes(excessField + value)))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("maximum field count of " + maximumFields);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"-0", "-0.0", "-0e3", "-0e-3", "0", "0.0", "0e3", "0e-3"})
    void testPreservesFloatingPointZeroSign(String number) {
        JsonKafkaFieldDecoder decoder =
                decoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("floatValue", DataTypes.FLOAT()),
                                DataTypes.FIELD("doubleValue", DataTypes.DOUBLE())));
        Object[] values =
                decoder.decode(
                        bytes("{\"floatValue\":" + number + ",\"doubleValue\":" + number + "}"));
        float expectedFloat = number.startsWith("-") ? -0.0F : 0.0F;
        double expectedDouble = number.startsWith("-") ? -0.0D : 0.0D;
        assertThat(Float.floatToRawIntBits((Float) values[0]))
                .isEqualTo(Float.floatToRawIntBits(expectedFloat));
        assertThat(Double.doubleToRawLongBits((Double) values[1]))
                .isEqualTo(Double.doubleToRawLongBits(expectedDouble));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "-"})
    void testFloatingPointRoundingAndExactDecimalParsing(String sign) {
        JsonKafkaFieldDecoder decoder =
                decoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("floatValue", DataTypes.FLOAT()),
                                DataTypes.FIELD("doubleValue", DataTypes.DOUBLE()),
                                DataTypes.FIELD("amount", DataTypes.DECIMAL(38, 18))));
        String amount = sign + "12345678901234567890.123456789012345678";
        Object[] values =
                decoder.decode(
                        bytes(
                                "{\"floatValue\":"
                                        + sign
                                        + "1.0000000596046448,\"doubleValue\":"
                                        + sign
                                        + "1.00000000000000011102230246251565404236316680908203126,"
                                        + "\"amount\":"
                                        + amount
                                        + "}"));
        float direction = sign.isEmpty() ? 1.0F : -1.0F;
        assertThat(values[0]).isEqualTo(direction * 1.0000001F);
        assertThat(values[1]).isEqualTo(direction * 1.0000000000000002D);
        assertThat(((Decimal) values[2]).toBigDecimal()).isEqualTo(new BigDecimal(amount));

        assertThat(
                        decoder.decode(
                                bytes(
                                        "{\"floatValue\":"
                                                + sign
                                                + "1.0000000596046447,\"doubleValue\":"
                                                + sign
                                                + "1.00000000000000011102230246251565404236316680908203124}")))
                .containsExactly(direction, (double) direction, null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "-"})
    void testFloatingPointOverflowBoundaries(String sign) {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("floatValue", DataTypes.FLOAT()),
                        DataTypes.FIELD("doubleValue", DataTypes.DOUBLE()));
        JsonKafkaFieldDecoder decoder = decoder(rowType);
        float direction = sign.isEmpty() ? 1.0F : -1.0F;
        String largestFloat = sign + "340282356779733661637539395458142568447";
        for (String suffix : new String[] {"", ".0"}) {
            assertThat(decoder.decode(bytes("{\"floatValue\":" + largestFloat + suffix + "}")))
                    .containsExactly(direction * Float.MAX_VALUE, null);
            assertFailure(
                    rowType,
                    "{\"floatValue\":"
                            + sign
                            + "340282356779733661637539395458142568448"
                            + suffix
                            + "}",
                    "$[\"floatValue\"]",
                    "overflowing number");
        }
        assertThat(decoder.decode(bytes("{\"doubleValue\":" + sign + "1.7976931348623157e308}")))
                .containsExactly(null, direction * Double.MAX_VALUE);
        assertFailure(
                rowType,
                "{\"doubleValue\":" + sign + "1e309}",
                "$[\"doubleValue\"]",
                "overflowing number");
    }

    @Test
    void testMixedScalarTokensPreserveProjectionAndMissingFields() {
        JsonKafkaFieldDecoder decoder =
                decoder(
                        DataTypes.ROW(
                                DataTypes.FIELD("text", DataTypes.STRING()),
                                DataTypes.FIELD("amount", DataTypes.DECIMAL(5, 2)),
                                DataTypes.FIELD("floatValue", DataTypes.FLOAT()),
                                DataTypes.FIELD("doubleValue", DataTypes.DOUBLE()),
                                DataTypes.FIELD("optional", DataTypes.INT())));
        Object[] values =
                decoder.decode(
                        bytes(
                                "{\"floatValue\":-0.0,\"text\":\"hello\",\"amount\":12.34,"
                                        + "\"doubleValue\":2.5,\"optional\":null}"));
        assertThat(values[0].toString()).isEqualTo("hello");
        assertThat(((Decimal) values[1]).toBigDecimal()).isEqualTo(new BigDecimal("12.34"));
        assertThat(Float.floatToRawIntBits((Float) values[2]))
                .isEqualTo(Float.floatToRawIntBits(-0.0F));
        assertThat(values[3]).isEqualTo(2.5D);
        assertThat(values[4]).isNull();
        assertThat(decoder.decode(bytes("{\"doubleValue\":2.5}")))
                .containsExactly(null, null, null, 2.5D, null);
    }

    @Test
    void testFloatingPointParsingStillRequiresStrictJsonSyntax() {
        JsonKafkaFieldDecoder decoder =
                decoder(DataTypes.ROW(DataTypes.FIELD("value", DataTypes.DOUBLE())));
        for (String json :
                new String[] {
                    "{\"value\":}",
                    "{\"value\":-0.0 \"value\":1}",
                    "{\"value\":-0.0,}",
                    "{\"value\":01}",
                    "{\"value\":NaN}",
                    "{\"value\":-Infinity}",
                    "{\"value\":0x1.0p0}",
                    "{\"value\":1f}",
                    "{\"value\":1}null"
                }) {
            assertThatThrownBy(() -> decoder.decode(bytes(json)))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("strict UTF-8 JSON");
        }
    }

    @Test
    void testRejectsInvalidUtf8AfterTrailingWhitespaceAcrossReaderBuffers() {
        JsonKafkaFieldDecoder decoder =
                decoder(DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING())));
        byte[] json = bytes("{\"value\":\"ok\"}");
        byte[] input = Arrays.copyOf(json, json.length + 32_768);
        Arrays.fill(input, json.length, input.length - 1, (byte) ' ');
        input[input.length - 1] = (byte) 0xff;
        assertThatThrownBy(() -> decoder.decode(input))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("strict UTF-8 JSON");
    }

    @Test
    void testRejectsInvalidUtf8AndAlternateEncodings() {
        JsonKafkaFieldDecoder decoder =
                decoder(DataTypes.ROW(DataTypes.FIELD("value", DataTypes.STRING())));
        for (byte[] input :
                new byte[][] {
                    new byte[] {(byte) 0xff}, "{\"value\":\"ok\"}".getBytes(StandardCharsets.UTF_16)
                }) {
            assertThatThrownBy(() -> decoder.decode(input))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("strict UTF-8 JSON");
        }
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
}
