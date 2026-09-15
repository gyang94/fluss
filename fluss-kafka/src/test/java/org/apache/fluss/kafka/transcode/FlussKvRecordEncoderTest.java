/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FlussKvRecordEncoderTest {
    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testEncodesNativeRowsWithCompositeKeysAndSubsetBucketKeys(KvFormat format)
            throws Exception {
        TableInfo table = table(format);
        ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder();
        List<Record> input = new ArrayList<>();
        for (int id = 0; id < 20; id++) {
            input.add(
                    record(
                            String.format(
                                    "{\"tenant\":%d,\"id\":%d,\"amount\":%d}", id % 4, id, id)));
        }
        CountingBudget budget = new CountingBudget();
        Map<Integer, BytesView> output =
                transcoder.transcodePrimaryKey(input, transcoder.prepare(table), budget);
        assertThat(output).hasSizeGreaterThan(1);
        List<Integer> ids = new ArrayList<>();
        for (BytesView bytes : output.values()) {
            for (KvRecord record :
                    DefaultKvRecordBatch.pointToBytesView(bytes)
                            .records(
                                    KvRecordReadContext.createReadContext(
                                            format, schemaGetter(table)))) {
                assertThat(record.getRow()).isNotNull();
                ids.add(record.getRow().getInt(1));
                assertThat(record.getRow().getInt(2)).isEqualTo(record.getRow().getInt(1));
            }
        }
        assertThat(ids).hasSize(20).doesNotHaveDuplicates();
        assertThat(budget.retained).isGreaterThan(0);
    }

    @Test
    void testInvalidPrimaryKeyReleasesPartiallyEncodedOutput() {
        ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder();
        CountingBudget budget = new CountingBudget();
        assertThatThrownBy(
                        () ->
                                transcoder.transcodePrimaryKey(
                                        Arrays.asList(
                                                record("{\"tenant\":1,\"id\":1,\"amount\":1}"),
                                                record("{\"tenant\":1,\"id\":null,\"amount\":2}")),
                                        transcoder.prepare(table(KvFormat.COMPACTED)),
                                        budget))
                .isInstanceOf(KafkaRecordEncodingException.class);
        assertThat(budget.retained).isZero();
    }

    private static SchemaGetter schemaGetter(TableInfo table) {
        SchemaGetter getter = mock(SchemaGetter.class);
        when(getter.getSchema(anyInt())).thenReturn(table.getSchema());
        return getter;
    }

    private static Record record(String value) {
        return new Record(
                1L, null, value.getBytes(StandardCharsets.UTF_8), Collections.emptyList());
    }

    private static TableInfo table(KvFormat format) {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("tenant", DataTypes.INT())
                                        .column("id", DataTypes.INT())
                                        .column("amount", DataTypes.INT())
                                        .primaryKey("tenant", "id")
                                        .build())
                        .distributedBy(8, "tenant")
                        .property(ConfigOptions.TABLE_KV_FORMAT, format)
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json")
                        .build();
        return TableInfo.of(TablePath.of("kafka", "orders"), 1L, 1, descriptor, null, 1L, 1L);
    }

    private static final class CountingBudget implements KafkaOutputMemoryBudget {
        private long retained;

        @Override
        public void reserve(long bytes) {
            retained += bytes;
        }

        @Override
        public void release(long bytes) {
            retained -= bytes;
            assertThat(retained).isGreaterThanOrEqualTo(0);
        }
    }
}
