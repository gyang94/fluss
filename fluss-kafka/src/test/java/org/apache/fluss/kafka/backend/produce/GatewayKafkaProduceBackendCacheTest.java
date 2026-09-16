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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaOutputMemoryBudget;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaTopicWritePlan;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.AdminOperationAuthorizer;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class GatewayKafkaProduceBackendCacheTest {

    @Test
    void testQualifiedTopicsRouteAndCacheByFullTablePath() {
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        List<TablePath> lookups = new ArrayList<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenAnswer(
                        invocation -> {
                            GetTableInfoRequest request = invocation.getArgument(0);
                            TablePath path =
                                    TablePath.of(
                                            request.getTablePath().getDatabaseName(),
                                            request.getTablePath().getTableName());
                            lookups.add(path);
                            return CompletableFuture.completedFuture(
                                    new GetTableInfoResponse()
                                            .setTableId(
                                                    "sales".equals(path.getDatabaseName())
                                                            ? 12L
                                                            : 13L)
                                            .setSchemaId(3)
                                            .setTableJson(descriptor().toJsonBytes())
                                            .setCreatedTime(1L)
                                            .setModifiedTime(2L));
                        });
        when(gateway.produceLog(any(ProduceLogRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(produceResponse()));
        CountingTranscoder transcoder = new CountingTranscoder();
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, gateway, transcoder);
        for (String name : Arrays.asList("sales.orders", "archive.orders", "sales.orders")) {
            assertThat(backend.write(command(name)).join().topics().get(0).partitions())
                    .allSatisfy(partition -> assertThat(partition.error()).isEqualTo(Errors.NONE));
        }
        assertThat(lookups)
                .contains(TablePath.of("sales", "orders"), TablePath.of("archive", "orders"));
        assertThat(transcoder.preparedTableInfos.get(0))
                .isNotSameAs(transcoder.preparedTableInfos.get(1));
        assertThat(transcoder.preparedTableInfos.get(0))
                .isSameAs(transcoder.preparedTableInfos.get(2));
        assertThat(transcoder.preparedTableInfos.get(0).getTableId()).isEqualTo(12L);
        assertThat(transcoder.preparedTableInfos.get(1).getTableId()).isEqualTo(13L);
    }

    @Test
    void testBareTopicIsRejectedBeforeNativeLookupOrWrite() {
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, gateway, new CountingTranscoder());
        assertThat(backend.write(command("orders")).join().topics().get(0).partitions())
                .allSatisfy(
                        partition ->
                                assertThat(partition.error())
                                        .isEqualTo(Errors.INVALID_TOPIC_EXCEPTION));
        verifyNoInteractions(gateway);
    }

    @Test
    void testReusesTableInfoAndOneWritePlanAcrossPartitionsAndRequests() {
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        TableDescriptor descriptor = descriptor();
        GetTableInfoResponse tableInfoResponse =
                new GetTableInfoResponse()
                        .setTableId(12L)
                        .setSchemaId(3)
                        .setTableJson(descriptor.toJsonBytes())
                        .setCreatedTime(1L)
                        .setModifiedTime(2L);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse));
        when(gateway.produceLog(any(ProduceLogRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(produceResponse()));

        CountingTranscoder transcoder = new CountingTranscoder();
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, gateway, transcoder);
        KafkaProduceCommand firstCommand = command();
        KafkaProduceCommand secondCommand = command();

        KafkaProduceResult first = backend.write(firstCommand).join();
        KafkaProduceResult second = backend.write(secondCommand).join();

        assertThat(first.topics().get(0).partitions())
                .allSatisfy(partition -> assertThat(partition.error().code()).isZero());
        assertThat(second.topics().get(0).partitions())
                .allSatisfy(partition -> assertThat(partition.error().code()).isZero());
        assertThat(transcoder.preparedTableInfos).hasSize(2);
        assertThat(transcoder.preparedTableInfos.get(1))
                .isSameAs(transcoder.preparedTableInfos.get(0));
        assertThat(transcoder.preparedPlans).hasSize(2);
        assertThat(transcoder.preparedPlans.get(1)).isSameAs(transcoder.preparedPlans.get(0));
        assertThat(transcoder.transcodedPlans)
                .hasSize(4)
                .allSatisfy(plan -> assertThat(plan).isSameAs(transcoder.preparedPlans.get(0)));
    }

    @Test
    void testMapsArrowResourceTimeoutAndOversizedBatch() {
        assertPrepareFailureMapsTo(
                new TimeoutException("writer acquire timeout"), Errors.REQUEST_TIMED_OUT);
        assertPrepareFailureMapsTo(
                new RecordTooLargeException("batch too large"), Errors.MESSAGE_TOO_LARGE);
    }

    @Test
    void testMixedAndAllNullPartitionsDoNotWriteNullRows() {
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway =
                mock(
                        TabletServerGateway.class,
                        withSettings().extraInterfaces(AdminOperationAuthorizer.class));
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new GetTableInfoResponse()
                                        .setTableId(12L)
                                        .setSchemaId(3)
                                        .setTableJson(descriptor().toJsonBytes())
                                        .setCreatedTime(1L)
                                        .setModifiedTime(2L)));
        List<Integer> written = new ArrayList<>();
        when(gateway.produceLog(any(ProduceLogRequest.class)))
                .thenAnswer(
                        invocation -> {
                            ProduceLogRequest request = invocation.getArgument(0);
                            for (int i = 0; i < request.getBucketsReqsCount(); i++) {
                                written.add(request.getBucketsReqAt(i).getBucketId());
                            }
                            return CompletableFuture.completedFuture(produceResponse());
                        });
        CountingTranscoder transcoder = new CountingTranscoder();
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(service, gateway, transcoder);
        Record tombstone = new Record(1L, new byte[] {1}, null, Collections.emptyList());
        Record value = new Record(1L, null, new byte[0], Collections.emptyList());
        TopicWrite topic =
                new TopicWrite(
                        "kafka.orders",
                        Arrays.asList(
                                new PartitionWrite(0, Arrays.asList(value, tombstone, value)),
                                new PartitionWrite(1, Collections.singletonList(tombstone))));
        KafkaProduceResult result =
                backend.write(
                                new KafkaProduceCommand(
                                        (short) 1,
                                        1000,
                                        Collections.singletonList(topic),
                                        "KAFKA",
                                        null))
                        .join();
        assertThat(result.topics().get(0).partitions())
                .allSatisfy(
                        partition -> {
                            assertThat(partition.error()).isEqualTo(Errors.NONE);
                            assertThat(partition.baseOffset()).isEqualTo(-1L);
                        });
        assertThat(written).containsExactly(0);
        assertThat(transcoder.transcodedPlans).hasSize(1);
        assertThat(topic.copiedRecords(topic.partitions().get(0))).isEmpty();
        assertThat(topic.copiedRecords(topic.partitions().get(1))).isEmpty();
    }

    @Test
    void testAllNullCannotBypassLocalAuthorizationCapability() {
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new GetTableInfoResponse()
                                        .setTableId(12L)
                                        .setSchemaId(3)
                                        .setTableJson(descriptor().toJsonBytes())
                                        .setCreatedTime(1L)
                                        .setModifiedTime(2L)));
        TopicWrite topic =
                new TopicWrite(
                        "kafka.orders",
                        Collections.singletonList(
                                new PartitionWrite(
                                        0,
                                        Collections.singletonList(
                                                new Record(
                                                        1L,
                                                        null,
                                                        null,
                                                        Collections.emptyList())))));
        KafkaProduceResult result =
                new GatewayKafkaProduceBackend(
                                mock(RpcGatewayService.class), gateway, new CountingTranscoder())
                        .write(
                                new KafkaProduceCommand(
                                        (short) 1,
                                        1000,
                                        Collections.singletonList(topic),
                                        "KAFKA",
                                        null))
                        .join();
        assertThat(result.topics().get(0).partitions().get(0).error()).isNotEqualTo(Errors.NONE);
        verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
    }

    private static void assertPrepareFailureMapsTo(RuntimeException failure, Errors expectedError) {
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        GetTableInfoResponse response =
                new GetTableInfoResponse()
                        .setTableId(12L)
                        .setSchemaId(3)
                        .setTableJson(descriptor().toJsonBytes())
                        .setCreatedTime(1L)
                        .setModifiedTime(2L);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));
        KafkaRecordTranscoder transcoder =
                new KafkaRecordTranscoder() {
                    @Override
                    public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
                        throw failure;
                    }

                    @Override
                    public BytesView transcode(
                            List<Record> records,
                            KafkaTopicWritePlan writePlan,
                            KafkaOutputMemoryBudget outputMemoryBudget) {
                        throw new AssertionError("transcode should not be called");
                    }
                };

        KafkaProduceResult result =
                new GatewayKafkaProduceBackend(service, gateway, transcoder)
                        .write(command())
                        .join();

        assertThat(result.topics().get(0).partitions())
                .allSatisfy(partition -> assertThat(partition.error()).isEqualTo(expectedError));
    }

    private static KafkaProduceCommand command() {
        return command("kafka.orders");
    }

    private static KafkaProduceCommand command(String topicName) {
        Record record =
                new Record(
                        1L,
                        null,
                        "value".getBytes(StandardCharsets.UTF_8),
                        Collections.emptyList());
        TopicWrite topic =
                new TopicWrite(
                        topicName,
                        Arrays.asList(
                                new PartitionWrite(0, Collections.singletonList(record)),
                                new PartitionWrite(1, Collections.singletonList(record))));
        return new KafkaProduceCommand(
                (short) 1, 1_000, Collections.singletonList(topic), "KAFKA", null);
    }

    private static TableDescriptor descriptor() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("payload", DataTypes.BYTES()).build())
                .distributedBy(2)
                .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                .customProperty(KafkaDataFormat.VALUE_FORMAT_CONFIG, KafkaDataFormat.RAW.value())
                .build();
    }

    private static ProduceLogResponse produceResponse() {
        return new ProduceLogResponse()
                .addAllBucketsResps(
                        Arrays.asList(
                                new PbProduceLogRespForBucket().setBucketId(0).setBaseOffset(1L),
                                new PbProduceLogRespForBucket().setBucketId(1).setBaseOffset(2L)));
    }

    private static final class CountingTranscoder implements KafkaRecordTranscoder {
        private final ArrowKafkaRecordTranscoder delegate = new ArrowKafkaRecordTranscoder();
        private final List<TableInfo> preparedTableInfos = new ArrayList<>();
        private final List<KafkaTopicWritePlan> preparedPlans = new ArrayList<>();
        private final List<KafkaTopicWritePlan> transcodedPlans = new ArrayList<>();

        @Override
        public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
            preparedTableInfos.add(tableInfo);
            KafkaTopicWritePlan plan = delegate.prepare(tableInfo);
            preparedPlans.add(plan);
            return plan;
        }

        @Override
        public BytesView transcode(
                List<Record> records,
                KafkaTopicWritePlan writePlan,
                KafkaOutputMemoryBudget outputMemoryBudget)
                throws Exception {
            transcodedPlans.add(writePlan);
            return delegate.transcode(records, writePlan, outputMemoryBudget);
        }
    }
}
