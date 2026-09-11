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

package org.apache.fluss.kafka.format;

import org.apache.fluss.kafka.schema.KafkaFieldProjection;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests independently constructed record component decoders. */
class KafkaFormatFactoryRegistryTest {
    private final KafkaFormatFactoryRegistry formats = new KafkaFormatFactoryRegistry();

    @Test
    void testRawPreservesNullEmptyAndOpaqueBytes() {
        KafkaFieldDecoder decoder =
                formats.createDecoder(KafkaDataFormat.RAW, projection(DataTypes.BYTES()));
        byte[] opaque = new byte[] {(byte) 0xff, 0};
        assertThat(decoder.decode(opaque)).containsExactly((Object) opaque);
        assertThat(decoder.decode(null)).containsExactly((Object) null);
        assertThat((byte[]) decoder.decode(new byte[0])[0]).isEmpty();
    }

    @Test
    void testStringRejectsMalformedUtf8AndCanBeReused() {
        KafkaFieldDecoder decoder =
                formats.createDecoder(KafkaDataFormat.STRING, projection(DataTypes.STRING()));
        assertThatThrownBy(() -> decoder.decode(new byte[] {(byte) 0xc3, 0x28}))
                .isInstanceOf(KafkaRecordEncodingException.class)
                .hasMessageContaining("UTF-8");
        assertThat(decoder.decode("键".getBytes(StandardCharsets.UTF_8)))
                .containsExactly(BinaryString.fromString("键"));
        assertThat(decoder.decode(null)).containsExactly((Object) null);
        assertThat(decoder.decode(new byte[0])).containsExactly(BinaryString.fromString(""));
    }

    @Test
    void testFactoriesRejectIncompatibleProjections() {
        assertThatThrownBy(
                        () ->
                                formats.createDecoder(
                                        KafkaDataFormat.RAW, projection(DataTypes.STRING())))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("BYTES");
        assertThatThrownBy(
                        () ->
                                formats.createDecoder(
                                        KafkaDataFormat.STRING, projection(DataTypes.BYTES())))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("STRING");
        KafkaFieldProjection multiple =
                new KafkaFieldProjection(
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.STRING()),
                                DataTypes.FIELD("b", DataTypes.STRING())),
                        Arrays.asList(0, 1));
        assertThatThrownBy(() -> formats.createDecoder(KafkaDataFormat.STRING, multiple))
                .isInstanceOf(KafkaTopicSchemaException.class)
                .hasMessageContaining("exactly one");
    }

    private static KafkaFieldProjection projection(DataType type) {
        return new KafkaFieldProjection(
                DataTypes.ROW(DataTypes.FIELD("value", type)), Collections.singletonList(0));
    }
}
