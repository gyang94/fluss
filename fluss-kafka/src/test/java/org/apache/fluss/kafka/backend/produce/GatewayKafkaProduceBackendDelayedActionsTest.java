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
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GatewayKafkaProduceBackendDelayedActionsTest {

    @Test
    void testDrainsOnlyAfterAsynchronousLookupSubmitsProduce() {
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        CompletableFuture<GetTableInfoResponse> tableInfoFuture = new CompletableFuture<>();
        CompletableFuture<ProduceLogResponse> produceFuture = new CompletableFuture<>();
        AtomicBoolean drainedIncompleteProduce = new AtomicBoolean();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class))).thenReturn(tableInfoFuture);
        when(gateway.produceLog(any(ProduceLogRequest.class))).thenReturn(produceFuture);
        doAnswer(
                        ignored -> {
                            drainedIncompleteProduce.set(!produceFuture.isDone());
                            produceFuture.complete(produceResponse());
                            return null;
                        })
                .when(service)
                .tryCompleteActions();

        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service, gateway, "kafka", new ArrowKafkaRecordTranscoder());
        CompletableFuture<KafkaProduceResult> resultFuture = backend.write(command());

        assertThat(resultFuture).isNotDone();
        verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
        verify(service, never()).tryCompleteActions();

        tableInfoFuture.complete(tableInfoResponse());

        assertThat(resultFuture).isCompleted();
        assertThat(drainedIncompleteProduce).isTrue();
        assertThat(resultFuture.join().topics().get(0).partitions().get(0).error().code()).isZero();
        InOrder submissionOrder = inOrder(gateway, service);
        submissionOrder.verify(gateway).produceLog(any(ProduceLogRequest.class));
        submissionOrder.verify(service).tryCompleteActions();
    }

    @Test
    void testDrainsWhenProduceSubmissionThrowsSynchronously() {
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        when(gateway.produceLog(any(ProduceLogRequest.class)))
                .thenThrow(new RuntimeException("synchronous submission failure"));

        KafkaProduceResult result =
                new GatewayKafkaProduceBackend(
                                service, gateway, "kafka", new ArrowKafkaRecordTranscoder())
                        .write(command())
                        .join();

        assertThat(result.topics().get(0).partitions().get(0).error().code()).isNotZero();
        verify(service).tryCompleteActions();
    }

    @Test
    void testDoesNotDrainWithoutNativeProduceSubmission() {
        assertLocalFailureDoesNotDrain(
                new KafkaRecordTranscoder() {
                    @Override
                    public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
                        throw new IllegalArgumentException("prepare failure");
                    }

                    @Override
                    public BytesView transcode(
                            List<Record> records,
                            KafkaTopicWritePlan writePlan,
                            KafkaOutputMemoryBudget outputMemoryBudget) {
                        throw new AssertionError("transcode should not be called");
                    }
                });

        ArrowKafkaRecordTranscoder delegate = new ArrowKafkaRecordTranscoder();
        assertLocalFailureDoesNotDrain(
                new KafkaRecordTranscoder() {
                    @Override
                    public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
                        return delegate.prepare(tableInfo);
                    }

                    @Override
                    public BytesView transcode(
                            List<Record> records,
                            KafkaTopicWritePlan writePlan,
                            KafkaOutputMemoryBudget outputMemoryBudget) {
                        throw new IllegalArgumentException("transcode failure");
                    }
                });
    }

    @Test
    void testTopicFailureSharesBoundedErrorMessageAcrossPartitions() {
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        String hugeErrorMessage = repeat("错误", 16 * 1024);
        CompletableFuture<GetTableInfoResponse> failedLookup = new CompletableFuture<>();
        failedLookup.completeExceptionally(new RuntimeException(hugeErrorMessage));
        when(gateway.getTableInfo(any(GetTableInfoRequest.class))).thenReturn(failedLookup);

        KafkaProduceResult result =
                new GatewayKafkaProduceBackend(
                                service, gateway, "kafka", new ArrowKafkaRecordTranscoder())
                        .write(command(8))
                        .join();

        List<KafkaProduceResult.PartitionResult> partitions = result.topics().get(0).partitions();
        assertThat(partitions).hasSize(8);
        String sharedMessage = partitions.get(0).errorMessage();
        assertThat(sharedMessage.getBytes(StandardCharsets.UTF_8).length)
                .isLessThanOrEqualTo(KafkaProduceResult.MAX_PARTITION_ERROR_MESSAGE_BYTES);
        for (KafkaProduceResult.PartitionResult partition : partitions) {
            assertThat(partition.errorMessage()).isSameAs(sharedMessage);
        }
    }

    @Test
    void testErrorBudgetIsAppliedBeforeMultiTopicAggregationCompletes() {
        int requestBytes = 4096;
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        CompletableFuture<ProduceLogResponse> secondProduce = new CompletableFuture<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        when(gateway.produceLog(any(ProduceLogRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(distinctErrorResponse(8)), secondProduce);
        KafkaProduceCommand command =
                command(Arrays.asList(topic("first", 8), topic("second", 1)), requestBytes);

        CompletableFuture<KafkaProduceResult> resultFuture =
                new GatewayKafkaProduceBackend(
                                service, gateway, "kafka", new ArrowKafkaRecordTranscoder())
                        .write(command);

        assertThat(resultFuture).isNotDone();
        assertThat(command.remainingErrorMessageBytes()).isZero();

        secondProduce.complete(produceResponse());
        KafkaProduceResult result = resultFuture.join();
        long totalErrorMessageBytes = 0;
        for (KafkaProduceResult.TopicResult topicResult : result.topics()) {
            for (KafkaProduceResult.PartitionResult partition : topicResult.partitions()) {
                totalErrorMessageBytes +=
                        KafkaProduceResult.errorMessageBytes(partition.errorMessage());
            }
        }
        assertThat(totalErrorMessageBytes).isLessThanOrEqualTo(requestBytes);
    }

    private static void assertLocalFailureDoesNotDrain(KafkaRecordTranscoder transcoder) {
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));

        new GatewayKafkaProduceBackend(service, gateway, "kafka", transcoder)
                .write(command())
                .join();

        verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
        verify(service, never()).tryCompleteActions();
    }

    private static KafkaProduceCommand command() {
        return command(1);
    }

    private static KafkaProduceCommand command(int partitionCount) {
        return command(Collections.singletonList(topic("orders", partitionCount)), 0);
    }

    private static KafkaProduceCommand command(List<TopicWrite> topics, int requestBytes) {
        return new KafkaProduceCommand(
                (short) -1,
                1_000,
                topics,
                "KAFKA",
                null,
                org.apache.fluss.security.acl.FlussPrincipal.ANONYMOUS,
                null,
                null,
                requestBytes);
    }

    private static TopicWrite topic(String topicName, int partitionCount) {
        List<PartitionWrite> partitions = new ArrayList<>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Record record =
                    new Record(
                            1L,
                            null,
                            "value".getBytes(StandardCharsets.UTF_8),
                            Collections.emptyList());
            partitions.add(new PartitionWrite(partitionId, Collections.singletonList(record)));
        }
        return new TopicWrite(topicName, partitions);
    }

    private static String repeat(String value, int repetitions) {
        StringBuilder builder = new StringBuilder(value.length() * repetitions);
        for (int index = 0; index < repetitions; index++) {
            builder.append(value);
        }
        return builder.toString();
    }

    private static GetTableInfoResponse tableInfoResponse() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("payload", DataTypes.BYTES()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_FORMAT, LogFormat.ARROW)
                        .customProperty(
                                KafkaDataFormat.VALUE_FORMAT_CONFIG, KafkaDataFormat.RAW.value())
                        .build();
        return new GetTableInfoResponse()
                .setTableId(12L)
                .setSchemaId(3)
                .setTableJson(descriptor.toJsonBytes())
                .setCreatedTime(1L)
                .setModifiedTime(2L);
    }

    private static ProduceLogResponse produceResponse() {
        return new ProduceLogResponse()
                .addAllBucketsResps(
                        Collections.singletonList(
                                new PbProduceLogRespForBucket().setBucketId(0).setBaseOffset(1L)));
    }

    private static ProduceLogResponse distinctErrorResponse(int partitionCount) {
        ProduceLogResponse response = new ProduceLogResponse();
        String hugeSuffix = repeat("错误", 16 * 1024);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            response.addBucketsResp()
                    .setBucketId(partitionId)
                    .setErrorCode(org.apache.fluss.rpc.protocol.Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(partitionId + hugeSuffix);
        }
        return response;
    }
}
