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
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
