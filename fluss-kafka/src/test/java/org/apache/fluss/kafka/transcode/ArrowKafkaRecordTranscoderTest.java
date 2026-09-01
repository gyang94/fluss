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
import org.apache.fluss.kafka.format.KafkaFormatFactoryRegistry;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.schema.KafkaTopicSchema;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.bytesview.ByteBufBytesView;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArrowKafkaRecordTranscoderTest {

    @Test
    void testDecodesAndAppendsRecordsOneAtATime() throws Exception {
        int recordCount = 512;
        AtomicInteger decodedRecords = new AtomicInteger();
        AtomicInteger appendedRows = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger checkpoints = new AtomicInteger();
        KafkaArrowWriterManager manager = manager();
        FlussArrowRecordEncoder encoder =
                new FlussArrowRecordEncoder(
                        manager,
                        (tableInfo, writer, outputView) ->
                                new FlussArrowRecordEncoder.ArrowBatchBuilder() {
                                    @Override
                                    public boolean isFull() {
                                        return false;
                                    }

                                    @Override
                                    public void append(GenericRow row) {
                                        assertThat(decodedRecords).hasValue(appendedRows.get() + 1);
                                        assertThat(checkpoints)
                                                .hasValue(3 + (2 * appendedRows.get()));
                                        appendedRows.incrementAndGet();
                                    }

                                    @Override
                                    public BytesView build() {
                                        assertThat(checkpoints).hasValue(2 + (2 * recordCount));
                                        return new ByteBufBytesView(new byte[0]);
                                    }

                                    @Override
                                    public void abort() {
                                        aborts.incrementAndGet();
                                    }
                                });
        try {
            TableInfo tableInfo = tableInfo();
            KafkaTopicSchemaResolver schemaResolver = new KafkaTopicSchemaResolver();
            KafkaTopicSchema topicSchema = schemaResolver.resolve(tableInfo);
            KafkaTopicWritePlan writePlan =
                    new KafkaTopicWritePlan(
                            tableInfo,
                            topicSchema,
                            bytes -> new Object[0],
                            bytes -> {
                                assertThat(appendedRows).hasValue(decodedRecords.get());
                                assertThat(checkpoints).hasValue(2 + (2 * decodedRecords.get()));
                                decodedRecords.incrementAndGet();
                                return new Object[] {bytes};
                            },
                            new KafkaRowAssembler(topicSchema));
            ArrowKafkaRecordTranscoder transcoder =
                    new ArrowKafkaRecordTranscoder(
                            schemaResolver, new KafkaFormatFactoryRegistry(), encoder);
            KafkaOutputMemoryBudget budget =
                    new KafkaOutputMemoryBudget() {
                        @Override
                        public void reserve(long bytes) {}

                        @Override
                        public void release(long bytes) {}

                        @Override
                        public void checkpoint() {
                            checkpoints.incrementAndGet();
                        }
                    };

            BytesView output = transcoder.transcode(records(recordCount), writePlan, budget);

            assertThat(output.getBytesLength()).isZero();
            assertThat(decodedRecords).hasValue(recordCount);
            assertThat(appendedRows).hasValue(recordCount);
            assertThat(checkpoints).hasValue(2 + (2 * recordCount));
            assertThat(aborts).hasValue(0);
            assertThat(manager.activeWriterCount()).isZero();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    @Test
    void testCancellationBeforeDecodeAbortsAndReleasesResources() {
        CancellationException expected = new CancellationException("cancelled");
        AtomicInteger checkpoints = new AtomicInteger();
        AtomicInteger decodes = new AtomicInteger();
        AtomicInteger appends = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        AtomicLong retainedBytes = new AtomicLong();
        KafkaArrowWriterManager manager = manager();
        FlussArrowRecordEncoder encoder =
                new FlussArrowRecordEncoder(
                        manager,
                        (tableInfo, writer, outputView) ->
                                new FlussArrowRecordEncoder.ArrowBatchBuilder() {
                                    @Override
                                    public boolean isFull() {
                                        return false;
                                    }

                                    @Override
                                    public void append(GenericRow row) {
                                        appends.incrementAndGet();
                                    }

                                    @Override
                                    public BytesView build() {
                                        throw new AssertionError(
                                                "A cancelled conversion must not build.");
                                    }

                                    @Override
                                    public void abort() {
                                        aborts.incrementAndGet();
                                    }
                                });
        try {
            TableInfo tableInfo = tableInfo();
            KafkaTopicSchemaResolver schemaResolver = new KafkaTopicSchemaResolver();
            KafkaTopicSchema topicSchema = schemaResolver.resolve(tableInfo);
            KafkaTopicWritePlan writePlan =
                    new KafkaTopicWritePlan(
                            tableInfo,
                            topicSchema,
                            bytes -> new Object[0],
                            bytes -> {
                                decodes.incrementAndGet();
                                return new Object[] {bytes};
                            },
                            new KafkaRowAssembler(topicSchema));
            ArrowKafkaRecordTranscoder transcoder =
                    new ArrowKafkaRecordTranscoder(
                            schemaResolver, new KafkaFormatFactoryRegistry(), encoder);
            KafkaOutputMemoryBudget budget =
                    new KafkaOutputMemoryBudget() {
                        @Override
                        public void reserve(long bytes) {
                            retainedBytes.addAndGet(bytes);
                        }

                        @Override
                        public void release(long bytes) {
                            retainedBytes.addAndGet(-bytes);
                        }

                        @Override
                        public void checkpoint() {
                            if (checkpoints.incrementAndGet() == 2) {
                                throw expected;
                            }
                        }
                    };

            assertThatThrownBy(() -> transcoder.transcode(records(1), writePlan, budget))
                    .isSameAs(expected);
            assertThat(checkpoints).hasValue(2);
            assertThat(decodes).hasValue(0);
            assertThat(appends).hasValue(0);
            assertThat(aborts).hasValue(1);
            assertThat(retainedBytes).hasValue(0);
            assertThat(manager.activeWriterCount()).isZero();
        } finally {
            manager.close();
        }
        assertThat(manager.allocatedMemoryBytes()).isZero();
    }

    private static List<Record> records(int count) {
        List<Record> records = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            records.add(new Record(index, null, new byte[8 * 1_024], Collections.emptyList()));
        }
        return records;
    }

    private static KafkaArrowWriterManager manager() {
        return new KafkaArrowWriterManager(
                16L << 20, 1, 4, Duration.ofSeconds(1), 1 << 20, KafkaProduceMetrics.noOp());
    }

    private static TableInfo tableInfo() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("payload", DataTypes.BYTES()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty(
                                KafkaDataFormat.VALUE_FORMAT_CONFIG, KafkaDataFormat.RAW.value())
                        .build();
        return TableInfo.of(TablePath.of("kafka", "topic"), 1L, 1, descriptor, null, 1L, 1L);
    }
}
