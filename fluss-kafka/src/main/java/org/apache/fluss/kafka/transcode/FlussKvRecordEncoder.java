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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.utils.ExceptionUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Encodes a Kafka partition into bounded native KV batches using the table's key format. */
final class FlussKvRecordEncoder {
    private final KafkaProduceMetrics metrics;

    FlussKvRecordEncoder() {
        this(KafkaProduceMetrics.noOp());
    }

    FlussKvRecordEncoder(KafkaProduceMetrics metrics) {
        this.metrics = metrics;
    }

    Map<Integer, BytesView> encode(
            List<Record> records, KafkaTopicWritePlan plan, KafkaOutputMemoryBudget budget)
            throws Exception {
        TableInfo table = plan.tableInfo();
        Map<Integer, BudgetedUnmanagedPagedOutputView> outputs = new LinkedHashMap<>();
        Map<Integer, KvRecordBatchBuilder> builders = new LinkedHashMap<>();
        BucketingFunction bucketing =
                BucketingFunction.of(table.getTableConfig().getDataLakeFormat().orElse(null));
        try {
            for (Record record : records) {
                budget.checkpoint();
                // Key and row encoders are scoped to this reservation because they retain scratch
                // buffers.
                long transientBytes =
                        ArrowKafkaRecordTranscoder.estimateDecodeTransientBytes(record, plan);
                budget.reserve(transientBytes);
                try (RowEncoder rowEncoder =
                        RowEncoder.create(
                                table.getTableConfig().getKvFormat(), table.getRowType())) {
                    GenericRow row = KafkaRowDecoder.decode(record, plan, metrics);
                    KeyEncoder primaryKey =
                            KeyEncoder.ofPrimaryKeyEncoder(
                                    table.getRowType(),
                                    table.getPhysicalPrimaryKeys(),
                                    table.getTableConfig(),
                                    table.isDefaultBucketKey());
                    KeyEncoder bucketKey =
                            KeyEncoder.ofBucketKeyEncoder(
                                    table.getRowType(),
                                    table.getBucketKeys(),
                                    table.getTableConfig(),
                                    table.isDefaultBucketKey(),
                                    primaryKey);
                    byte[] key = primaryKey.encodeKey(row);
                    byte[] routingKey = bucketKey == primaryKey ? key : bucketKey.encodeKey(row);
                    int bucket = bucketing.bucketing(routingKey, table.getNumBuckets());
                    rowEncoder.startNewRow();
                    for (int field = 0; field < table.getRowType().getFieldCount(); field++) {
                        rowEncoder.encodeField(field, row.getField(field));
                    }
                    BinaryRow encoded = rowEncoder.finishRow();
                    KvRecordBatchBuilder builder = builders.get(bucket);
                    if (builder == null) {
                        BudgetedUnmanagedPagedOutputView output =
                                new BudgetedUnmanagedPagedOutputView(4096, budget);
                        outputs.put(bucket, output);
                        builder =
                                KvRecordBatchBuilder.builder(
                                        table.getSchemaId(),
                                        Integer.MAX_VALUE,
                                        output,
                                        table.getTableConfig().getKvFormat());
                        builders.put(bucket, builder);
                    }
                    builder.append(key, encoded);
                } finally {
                    budget.release(transientBytes);
                }
            }
            Map<Integer, BytesView> result = new LinkedHashMap<>();
            for (Map.Entry<Integer, KvRecordBatchBuilder> entry : builders.entrySet()) {
                budget.checkpoint();
                result.put(entry.getKey(), entry.getValue().build());
                entry.getValue().close();
            }
            return result;
        } catch (Throwable failure) {
            for (KvRecordBatchBuilder builder : builders.values()) {
                builder.abort();
            }
            for (BudgetedUnmanagedPagedOutputView output : outputs.values()) {
                try {
                    output.releaseRetainedCapacity();
                } catch (Throwable releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            ExceptionUtils.rethrow(failure);
            throw new AssertionError("Unreachable");
        }
    }
}
