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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.format.KafkaFormatFactoryRegistry;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.schema.KafkaTopicSchema;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Transcodes Kafka records into native Arrow rows using a table-level Kafka format contract. */
@Internal
@ThreadSafe
public final class ArrowKafkaRecordTranscoder implements KafkaRecordTranscoder {

    private static final long ROW_FIXED_TRANSIENT_BYTES = 256;
    private static final long ROW_FIELD_TRANSIENT_BYTES = 16;
    private static final long HEADER_TRANSIENT_BYTES = 128;
    private static final long STRING_TRANSIENT_MULTIPLIER = 8;
    private static final long JSON_TRANSIENT_MULTIPLIER = 64;

    /** Default fixed-envelope key column created through Kafka CreateTopics. */
    public static final String KEY_COLUMN = "record_key";

    /** Default fixed-envelope value column created through Kafka CreateTopics. */
    public static final String VALUE_COLUMN = "payload";

    /** Default fixed-envelope timestamp column created through Kafka CreateTopics. */
    public static final String TIMESTAMP_COLUMN = "event_time";

    /** Default fixed-envelope headers column created through Kafka CreateTopics. */
    public static final String HEADERS_COLUMN = "headers";

    private final KafkaTopicSchemaResolver schemaResolver;
    private final KafkaFormatFactoryRegistry formatFactoryRegistry;
    private final FlussArrowRecordEncoder arrowRecordEncoder;
    private final KafkaProduceMetrics produceMetrics;
    private final KafkaCompiledWritePlanCache writePlanCache;

    /** Creates a transcoder with all built-in Kafka formats. */
    public ArrowKafkaRecordTranscoder() {
        this(
                new KafkaTopicSchemaResolver(),
                new KafkaFormatFactoryRegistry(),
                new FlussArrowRecordEncoder(),
                KafkaProduceMetrics.noOp(),
                new KafkaCompiledWritePlanCache());
    }

    /** Creates a transcoder with all built-in Kafka formats and runtime metrics. */
    public ArrowKafkaRecordTranscoder(KafkaProduceMetrics produceMetrics) {
        this(
                new KafkaTopicSchemaResolver(),
                new KafkaFormatFactoryRegistry(),
                new FlussArrowRecordEncoder(),
                produceMetrics,
                new KafkaCompiledWritePlanCache());
    }

    /** Creates a transcoder backed by the shared, bounded Arrow writer manager. */
    public ArrowKafkaRecordTranscoder(
            KafkaProduceMetrics produceMetrics, KafkaArrowWriterManager writerManager) {
        this(
                new KafkaTopicSchemaResolver(),
                new KafkaFormatFactoryRegistry(),
                new FlussArrowRecordEncoder(writerManager),
                produceMetrics,
                new KafkaCompiledWritePlanCache());
    }

    ArrowKafkaRecordTranscoder(
            KafkaTopicSchemaResolver schemaResolver,
            KafkaFormatFactoryRegistry formatFactoryRegistry,
            FlussArrowRecordEncoder arrowRecordEncoder) {
        this(
                schemaResolver,
                formatFactoryRegistry,
                arrowRecordEncoder,
                KafkaProduceMetrics.noOp(),
                new KafkaCompiledWritePlanCache());
    }

    ArrowKafkaRecordTranscoder(
            KafkaTopicSchemaResolver schemaResolver,
            KafkaFormatFactoryRegistry formatFactoryRegistry,
            FlussArrowRecordEncoder arrowRecordEncoder,
            KafkaProduceMetrics produceMetrics) {
        this(
                schemaResolver,
                formatFactoryRegistry,
                arrowRecordEncoder,
                produceMetrics,
                new KafkaCompiledWritePlanCache());
    }

    ArrowKafkaRecordTranscoder(
            KafkaTopicSchemaResolver schemaResolver,
            KafkaFormatFactoryRegistry formatFactoryRegistry,
            FlussArrowRecordEncoder arrowRecordEncoder,
            KafkaProduceMetrics produceMetrics,
            KafkaCompiledWritePlanCache writePlanCache) {
        this.schemaResolver = checkNotNull(schemaResolver);
        this.formatFactoryRegistry = checkNotNull(formatFactoryRegistry);
        this.arrowRecordEncoder = checkNotNull(arrowRecordEncoder);
        this.produceMetrics = checkNotNull(produceMetrics);
        this.writePlanCache = checkNotNull(writePlanCache);
    }

    @Override
    public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
        checkNotNull(tableInfo);
        KafkaTopicWritePlan cachedPlan = writePlanCache.getIfPresent(tableInfo);
        if (cachedPlan != null) {
            return cachedPlan;
        }
        return writePlanCache.getOrCompile(tableInfo, () -> compileWritePlan(tableInfo));
    }

    @Override
    public BytesView transcode(List<Record> records, KafkaTopicWritePlan writePlan)
            throws Exception {
        return transcode(records, writePlan, KafkaOutputMemoryBudget.UNBOUNDED);
    }

    @Override
    public BytesView transcode(
            List<Record> records,
            KafkaTopicWritePlan writePlan,
            KafkaOutputMemoryBudget outputMemoryBudget)
            throws Exception {
        checkNotNull(records);
        checkNotNull(writePlan);
        checkNotNull(outputMemoryBudget);

        long transcodeStartedNanos = produceMetrics.nowNanos();
        long[] decodeAssembleNanos = new long[1];
        int outputBytes = -1;
        try {
            BytesView output =
                    arrowRecordEncoder.encodeStreaming(
                            consumer -> {
                                for (Record record : records) {
                                    outputMemoryBudget.checkpoint();
                                    long transientBytes =
                                            estimateDecodeTransientBytes(record, writePlan);
                                    outputMemoryBudget.reserve(transientBytes);
                                    try {
                                        decodeAssembleAndAppend(
                                                record, writePlan, consumer, decodeAssembleNanos);
                                    } finally {
                                        outputMemoryBudget.release(transientBytes);
                                    }
                                }
                            },
                            writePlan.tableInfo(),
                            outputMemoryBudget);
            outputBytes = output.getBytesLength();
            return output;
        } finally {
            long completedNanos = produceMetrics.nowNanos();
            long totalNanos = elapsedNanos(transcodeStartedNanos, completedNanos);
            long convertNanos = Math.min(totalNanos, decodeAssembleNanos[0]);
            long arrowNanos = totalNanos - convertNanos;
            final int completedOutputBytes = outputBytes;
            // Encoding has already transferred the output pages to its caller. Metric reporting
            // must not turn that successful transfer into a failure because the caller cannot
            // then release those pages.
            recordMetricsBestEffort(
                    () -> produceMetrics.recordDecodeAssemble(completedNanos - convertNanos));
            recordMetricsBestEffort(
                    () ->
                            produceMetrics.recordArrowEncode(
                                    completedNanos - arrowNanos, completedOutputBytes));
        }
    }

    @Override
    public Map<Integer, BytesView> transcodePrimaryKey(
            List<Record> records,
            KafkaTopicWritePlan writePlan,
            KafkaOutputMemoryBudget outputMemoryBudget)
            throws Exception {
        return new FlussKvRecordEncoder().encode(records, writePlan, outputMemoryBudget);
    }

    @Override
    public void invalidate(TableInfo tableInfo) {
        writePlanCache.invalidate(checkNotNull(tableInfo));
    }

    private KafkaTopicWritePlan compileWritePlan(TableInfo tableInfo) {
        long compileStartedNanos = produceMetrics.nowNanos();
        try {
            KafkaTopicSchema topicSchema = schemaResolver.resolve(tableInfo);
            KafkaFieldDecoder keyDecoder =
                    topicSchema.keyFormat() == null
                            ? bytes -> new Object[0]
                            : formatFactoryRegistry.createDecoder(
                                    topicSchema.keyFormat(), topicSchema.keyProjection(), null);
            KafkaFieldDecoder valueDecoder =
                    formatFactoryRegistry.createDecoder(
                            topicSchema.valueFormat(),
                            topicSchema.valueProjection(),
                            topicSchema.valueRescueColumn());
            return new KafkaTopicWritePlan(
                    tableInfo,
                    topicSchema,
                    keyDecoder,
                    valueDecoder,
                    new KafkaRowAssembler(topicSchema));
        } finally {
            recordMetricsBestEffort(
                    () -> produceMetrics.recordContractCompile(compileStartedNanos));
        }
    }

    private void decodeAssembleAndAppend(
            Record record,
            KafkaTopicWritePlan writePlan,
            FlussArrowRecordEncoder.RowConsumer consumer,
            long[] decodeAssembleNanos)
            throws Exception {
        long recordStartedNanos = produceMetrics.nowNanos();
        GenericRow row;
        try {
            Object[] keyValues = writePlan.keyDecoder().decode(record.borrowedKey());
            Object[] valueValues = writePlan.valueDecoder().decode(record.borrowedValue());
            row =
                    writePlan
                            .rowAssembler()
                            .assemble(keyValues, valueValues, record.timestamp(), record.headers());
        } finally {
            decodeAssembleNanos[0] += elapsedNanos(recordStartedNanos, produceMetrics.nowNanos());
        }
        consumer.append(row);
    }

    private static long elapsedNanos(long startedNanos, long completedNanos) {
        return Math.max(0L, completedNanos - startedNanos);
    }

    private static void recordMetricsBestEffort(Runnable metricOperation) {
        try {
            metricOperation.run();
        } catch (Throwable ignored) {
            // Metrics are observational and cannot invalidate converted output ownership.
        }
    }

    static long estimateDecodeTransientBytes(Record record, KafkaTopicWritePlan writePlan) {
        long bytes =
                saturatedAdd(
                        ROW_FIXED_TRANSIENT_BYTES,
                        saturatedMultiply(
                                writePlan.topicSchema().rowType().getFieldCount(),
                                ROW_FIELD_TRANSIENT_BYTES));
        bytes =
                saturatedAdd(
                        bytes,
                        estimateFormatTransientBytes(
                                writePlan.topicSchema().keyFormat(), record.borrowedKey()));
        bytes =
                saturatedAdd(
                        bytes,
                        estimateFormatTransientBytes(
                                writePlan.topicSchema().valueFormat(), record.borrowedValue()));
        if (writePlan.topicSchema().headersPosition() >= 0) {
            for (RecordHeader header : record.headers()) {
                bytes = saturatedAdd(bytes, HEADER_TRANSIENT_BYTES);
                bytes =
                        saturatedAdd(
                                bytes, saturatedMultiply(header.name().length(), Character.BYTES));
            }
        }
        return bytes;
    }

    private static long estimateFormatTransientBytes(KafkaDataFormat format, byte[] payload) {
        if (format == null || payload == null || format == KafkaDataFormat.RAW) {
            return 0;
        }
        long multiplier =
                format == KafkaDataFormat.JSON
                        ? JSON_TRANSIENT_MULTIPLIER
                        : STRING_TRANSIENT_MULTIPLIER;
        return saturatedMultiply(payload.length, multiplier);
    }

    private static long saturatedMultiply(long left, long right) {
        if (left == 0 || right == 0) {
            return 0;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
