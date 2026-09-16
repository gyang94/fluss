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
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests strict UTF-8 acceptance independently of the decoder implementation. */
class StringKafkaFormatFactoryTest {
    private final KafkaFieldDecoder decoder =
            new StringKafkaFormatFactory()
                    .createDecoder(
                            new KafkaFieldProjection(
                                    DataTypes.ROW(DataTypes.FIELD("payload", DataTypes.STRING())),
                                    Collections.singletonList(0)),
                            null);

    @Test
    void testUnicodeBoundariesAndNull() {
        assertThat(decoder.decode(null)).containsExactly((Object) null);
        for (String value :
                new String[] {"", "ASCII", "中文", "é", "\u0000", "\uD83D\uDE00", "\uDBFF\uDFFF"}) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            BinaryString actual = (BinaryString) decoder.decode(bytes)[0];
            assertThat(actual.toString()).isEqualTo(value);
            assertThat(actual.toBytes()).containsExactly(bytes);
        }
    }

    @Test
    void testRejectsOverlongSurrogateOutOfRangeAndTruncatedSequences() {
        for (byte[] bytes :
                new byte[][] {
                    {(byte) 0xC0, (byte) 0x80},
                    {(byte) 0xE0, (byte) 0x80, (byte) 0x80},
                    {(byte) 0xED, (byte) 0xA0, (byte) 0x80},
                    {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80},
                    {(byte) 0xE2, (byte) 0x82},
                    {(byte) 0x80},
                    {(byte) 0xFF}
                }) {
            assertThatThrownBy(() -> decoder.decode(bytes))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .hasMessageContaining("not valid UTF-8");
        }
    }

    @Test
    void testMatchesStrictJdkDecoderOnDeterministicByteCorpus() throws Exception {
        Random random = new Random(20260916L);
        for (int sample = 0; sample < 10000; sample++) {
            byte[] bytes = new byte[random.nextInt(12)];
            random.nextBytes(bytes);
            String expected;
            try {
                expected =
                        StandardCharsets.UTF_8
                                .newDecoder()
                                .onMalformedInput(CodingErrorAction.REPORT)
                                .onUnmappableCharacter(CodingErrorAction.REPORT)
                                .decode(ByteBuffer.wrap(bytes))
                                .toString();
            } catch (CharacterCodingException invalid) {
                assertThatThrownBy(() -> decoder.decode(bytes))
                        .isInstanceOf(KafkaRecordEncodingException.class);
                continue;
            }
            assertThat(((BinaryString) decoder.decode(bytes)[0]).toString()).isEqualTo(expected);
        }
    }
}
