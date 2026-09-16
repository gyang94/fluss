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

package org.apache.fluss.kafka.metrics;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.kafka.backend.produce.GatewayKafkaProduceBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceResult;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaOutputMemoryBudget;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException.Reason;
import org.apache.fluss.kafka.transcode.KafkaTopicWritePlan;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.util.TestMetricGroup;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.AdminOperationAuthorizer;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Checks observed message errors separately from partition failures and accepted drops. */
class KafkaMessageMetricsTest {
    @Test
    void testMixedPartitionOutcomesAndLastSuccessfulWrite() {
        CapturingGroup group = new CapturingGroup();
        ManualClock clock = new ManualClock();
        KafkaProduceMetrics metrics = new KafkaProduceMetrics(group, clock);
        TabletServerGateway gateway =
                mock(
                        TabletServerGateway.class,
                        withSettings().extraInterfaces(AdminOperationAuthorizer.class));
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new GetTableInfoResponse()
                                        .setTableId(1L)
                                        .setSchemaId(1)
                                        .setCreatedTime(1L)
                                        .setModifiedTime(1L)
                                        .setTableJson(descriptor(false, true).toJsonBytes())));
        CompletableFuture<ProduceLogResponse> nativeWrite = new CompletableFuture<>();
        when(gateway.produceLog(any(ProduceLogRequest.class))).thenReturn(nativeWrite);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        mock(RpcGatewayService.class),
                        gateway,
                        new ArrowKafkaRecordTranscoder(metrics),
                        metrics);
        TopicWrite topic =
                new TopicWrite(
                        "kafka.metrics",
                        Arrays.asList(
                                new PartitionWrite(
                                        0,
                                        Collections.singletonList(
                                                record(
                                                        "{\"id\":1,\"value\":\"bad\",\"extra\":1,\"other\":2}"))),
                                new PartitionWrite(
                                        1,
                                        Arrays.asList(
                                                record("{\"id\":2}"),
                                                record("{}"),
                                                record("{\"id\":3}"))),
                                new PartitionWrite(2, Collections.singletonList(record(null)))));
        CompletableFuture<KafkaProduceResult> result = backend.write(command(topic));
        assertThat(result).isNotDone();
        assertThat(group.count(KafkaMetricNames.RECORD_ERRORS)).isEqualTo(2);
        assertThat(group.count(KafkaMetricNames.INVALID_RECORDS)).isOne();
        assertThat(group.count(KafkaMetricNames.RESCUED_RECORDS)).isOne();
        assertThat(metrics.recordErrors(Reason.UNKNOWN_FIELD).getCount()).isOne();
        assertThat(metrics.recordErrors(Reason.NULLABLE_TYPE).getCount()).isOne();
        assertThat(metrics.recordErrors(Reason.NOT_NULL_MISSING).getCount()).isOne();
        assertThat(group.gauge(KafkaMetricNames.LAST_SUCCESSFUL_WRITE_TIME_MILLIS)).isZero();
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        ProduceLogResponse response = new ProduceLogResponse();
        response.addBucketsResp().setBucketId(0).setBaseOffset(0L);
        nativeWrite.complete(response);
        assertThat(result.join().topics().get(0).partitions()).hasSize(3);
        assertThat(group.count(KafkaMetricNames.FAILED_RECORDS)).isEqualTo(3);
        assertThat(group.count(KafkaMetricNames.DROPPED_RECORDS)).isOne();
        assertThat(group.count(KafkaMetricNames.SUCCESSFUL_RECORDS)).isOne();
        assertThat(group.gauge(KafkaMetricNames.LAST_SUCCESSFUL_WRITE_TIME_MILLIS)).isEqualTo(50L);

        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        backend.write(
                        command(
                                new TopicWrite(
                                        "kafka.metrics",
                                        Collections.singletonList(
                                                new PartitionWrite(
                                                        0,
                                                        Collections.singletonList(record(null)))))))
                .join();
        assertThat(group.count(KafkaMetricNames.DROPPED_RECORDS)).isEqualTo(2);
        assertThat(group.gauge(KafkaMetricNames.LAST_SUCCESSFUL_WRITE_TIME_MILLIS)).isEqualTo(50L);

        when(gateway.produceLog(any(ProduceLogRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new ProduceLogResponse()));
        backend.write(
                        command(
                                new TopicWrite(
                                        "kafka.metrics",
                                        Collections.singletonList(
                                                new PartitionWrite(
                                                        0,
                                                        Arrays.asList(
                                                                record(null),
                                                                record("{\"id\":4}")))))))
                .join();
        assertThat(group.count(KafkaMetricNames.FAILED_RECORDS)).isEqualTo(5);
        assertThat(group.count(KafkaMetricNames.DROPPED_RECORDS)).isEqualTo(2);
        assertThat(group.gauge(KafkaMetricNames.LAST_SUCCESSFUL_WRITE_TIME_MILLIS)).isEqualTo(50L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testArrowAndKvCountRecoveredErrorsOncePerMessage(boolean primaryKey) throws Exception {
        CapturingGroup group = new CapturingGroup();
        KafkaProduceMetrics metrics = new KafkaProduceMetrics(group);
        ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder(metrics);
        TableInfo table =
                TableInfo.of(
                        TablePath.of("kafka", "metrics"),
                        1L,
                        1,
                        descriptor(primaryKey, true),
                        null,
                        1L,
                        1L);
        KafkaTopicWritePlan plan = transcoder.prepare(table);
        java.util.List<Record> input =
                Collections.singletonList(
                        record("{\"id\":1,\"value\":false,\"items\":[\"a\",\"b\"],\"extra\":1}"));
        if (primaryKey) {
            transcoder.transcodePrimaryKey(input, plan, KafkaOutputMemoryBudget.UNBOUNDED);
        } else {
            transcoder.transcode(input, plan);
        }
        assertThat(group.count(KafkaMetricNames.RECORD_ERRORS)).isOne();
        assertThat(group.count(KafkaMetricNames.RESCUED_RECORDS)).isOne();
        assertThat(group.count(KafkaMetricNames.INVALID_RECORDS)).isZero();
        assertThat(metrics.recordErrors(Reason.NULLABLE_TYPE).getCount()).isOne();
        assertThat(metrics.recordErrors(Reason.UNKNOWN_FIELD).getCount()).isOne();
        assertThat(group.count(KafkaMetricNames.DROPPED_RECORDS)).isZero();
    }

    @Test
    void testFatalErrorCategoriesUseStructuredReasons() {
        CapturingGroup group = new CapturingGroup();
        KafkaProduceMetrics metrics = new KafkaProduceMetrics(group);
        ArrowKafkaRecordTranscoder transcoder = new ArrowKafkaRecordTranscoder(metrics);
        KafkaTopicWritePlan plan =
                transcoder.prepare(
                        TableInfo.of(
                                TablePath.of("kafka", "metrics"),
                                1L,
                                1,
                                descriptor(false, false),
                                null,
                                1L,
                                1L));
        String[] inputs = {
            "{", "{}", "{\"id\":\"bad\"}", "{\"id\":1,\"value\":\"bad\"}", "{\"id\":1,\"extra\":1}"
        };
        Reason[] reasons = {
            Reason.JSON_SYNTAX,
            Reason.NOT_NULL_MISSING,
            Reason.NOT_NULL_TYPE,
            Reason.NULLABLE_TYPE,
            Reason.UNKNOWN_FIELD
        };
        for (int i = 0; i < inputs.length; i++) {
            String input = inputs[i];
            Reason reason = reasons[i];
            assertThatThrownBy(
                            () ->
                                    transcoder.transcode(
                                            Collections.singletonList(record(input)), plan))
                    .isInstanceOf(KafkaRecordEncodingException.class)
                    .satisfies(
                            error ->
                                    assertThat(((KafkaRecordEncodingException) error).reason())
                                            .isEqualTo(reason));
            assertThat(metrics.recordErrors(reason).getCount()).isOne();
        }
        assertThat(group.count(KafkaMetricNames.RECORD_ERRORS)).isEqualTo(5);
        assertThat(group.count(KafkaMetricNames.INVALID_RECORDS)).isEqualTo(5);
        assertThat(group.count(KafkaMetricNames.DROPPED_RECORDS)).isZero();
    }

    private static KafkaProduceCommand command(TopicWrite topic) {
        return new KafkaProduceCommand(
                (short) 1, 1000, Collections.singletonList(topic), "KAFKA", null);
    }

    private static Record record(String json) {
        return new Record(
                1L,
                null,
                json == null ? null : json.getBytes(StandardCharsets.UTF_8),
                Collections.emptyList());
    }

    private static TableDescriptor descriptor(boolean primaryKey, boolean rescue) {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("value", DataTypes.INT())
                        .column("items", DataTypes.ARRAY(DataTypes.INT()));
        if (primaryKey) {
            schema.primaryKey("id");
        }
        if (rescue) {
            schema.column("rescue", DataTypes.STRING());
        }
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(schema.build())
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, "json");
        if (rescue) {
            builder.customProperty(KafkaDataFormat.VALUE_RESCUE_COLUMN_CONFIG, "rescue");
        }
        return builder.build();
    }

    private static final class CapturingGroup extends TestMetricGroup {
        private final Map<String, Counter> counters = new HashMap<>();
        private final Map<String, Gauge<?>> gauges = new HashMap<>();

        private CapturingGroup() {
            super(
                    new String[0],
                    Collections.emptyMap(),
                    (name, filter) -> name,
                    (filter, delimiter) -> "kafka.request.produce");
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            counters.putIfAbsent(name, counter);
            return counter;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(name, gauge);
            return gauge;
        }

        private long count(String name) {
            return counters.get(name).getCount();
        }

        private long gauge(String name) {
            return ((Number) gauges.get(name).getValue()).longValue();
        }
    }
}
