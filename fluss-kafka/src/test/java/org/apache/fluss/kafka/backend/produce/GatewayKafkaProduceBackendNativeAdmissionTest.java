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
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.ConnectionHandle;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController.RequestLease;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.PartitionWrite;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.TopicWrite;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaOutputMemoryBudget;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaTopicWritePlan;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.bytesview.ByteBufBytesView;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.OutOfMemoryException;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Lifecycle tests for converted/native Kafka Produce admission. */
class GatewayKafkaProduceBackendNativeAdmissionTest {

    @Test
    void testDisconnectAfterSubmitRetainsLeaseUntilOriginalFutureCompletes() {
        TopicWrite topic = topic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        CompletableFuture<ProduceLogResponse> originalProduceFuture = new CompletableFuture<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        when(gateway.produceLog(any(ProduceLogRequest.class))).thenReturn(originalProduceFuture);
        GatewayKafkaProduceBackend backend = backend(service, gateway, 100);

        try {
            CompletableFuture<KafkaProduceResult> result =
                    backend.write(command((short) -1, connection, scheduler, topic));

            assertThat(result).isNotDone();
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.convertedBytes()).isEqualTo(retainedBytes(topic, 100));

            connection.close();
            assertThat(controller.registeredConnections()).isZero();
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.convertedBytes()).isEqualTo(retainedBytes(topic, 100));

            originalProduceFuture.complete(produceResponse());
            assertThat(result.join().topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.NONE);
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.convertedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testDisconnectBeforeSubmitDropsConvertedRequest() {
        TopicWrite topic = topic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        CompletableFuture<GetTableInfoResponse> tableInfoFuture = new CompletableFuture<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class))).thenReturn(tableInfoFuture);
        GatewayKafkaProduceBackend backend = backend(service, gateway, 100);

        try {
            CompletableFuture<KafkaProduceResult> result =
                    backend.write(command((short) 1, connection, scheduler, topic));
            assertThat(controller.inFlightRequests()).isOne();

            connection.close();
            tableInfoFuture.complete(tableInfoResponse());

            assertThat(result.join().topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.convertedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testDisconnectCancelsNeverCompletingMetadataLookupImmediately() throws Exception {
        TopicWrite topic = topic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        KafkaNativeProduceOperationTracker operationTracker =
                new KafkaNativeProduceOperationTracker();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        CompletableFuture<GetTableInfoResponse> metadataFuture = new CompletableFuture<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class))).thenReturn(metadataFuture);
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service,
                        gateway,
                        new FixedSizeTranscoder(100),
                        KafkaProduceMetrics.noOp(),
                        Runnable::run,
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(5),
                        operationTracker);
        KafkaProduceCommand command = command((short) 1, connection, scheduler, topic);

        try {
            CompletableFuture<KafkaProduceResult> result = backend.write(command);
            assertThat(result).isNotDone();
            assertThat(operationTracker.activeOperations()).isOne();
            assertThat(controller.inFlightRequests()).isOne();

            connection.close();

            KafkaProduceResult completed = result.get(5, TimeUnit.SECONDS);
            assertThat(completed.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            assertThat(metadataFuture).isCancelled();
            assertThat(command.nativeAdmissionTransferFuture()).isCompleted();
            assertThat(topic.copiedRecords(topic.partitions().get(0))).isEmpty();
            assertThat(operationTracker.activeOperations()).isZero();
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.convertedBytes()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
            verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
        } finally {
            operationTracker.close();
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testWaitingReservationTimesOutBeforeTableLookup() throws Exception {
        TopicWrite topic = topic("orders");
        long estimate = topic.estimatedConvertedBytes();
        KafkaNativeProduceAdmissionController controller = controller(1, estimate + 1);
        ConnectionHandle blockerConnection = controller.registerConnection();
        RequestLease blocker = blockerConnection.reserve(1).getFuture().join();
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        GatewayKafkaProduceBackend backend = backend(service, gateway, 1, Duration.ofMillis(20));

        try {
            KafkaProduceResult result =
                    backend.write(command((short) 1, connection, scheduler, topic))
                            .get(5, TimeUnit.SECONDS);

            assertThat(result.topics().get(0).partitions().get(0).error())
                    .as(result.topics().get(0).partitions().get(0).errorMessage())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            verify(gateway, never()).getTableInfo(any(GetTableInfoRequest.class));
            verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
            assertThat(controller.pendingReservations()).isZero();
            assertThat(controller.inFlightRequests()).isOne();
        } finally {
            blocker.close();
            blockerConnection.close();
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testAdmissionSchedulerRuntimeFailureTimesOutPendingReservation() throws Exception {
        TopicWrite topic = topic("orders");
        long estimate = topic.estimatedConvertedBytes();
        KafkaNativeProduceAdmissionController controller = controller(1, estimate + 1);
        ConnectionHandle blockerConnection = controller.registerConnection();
        RequestLease blocker = blockerConnection.reserve(1).getFuture().join();
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        doThrow(new IllegalStateException("scheduler unavailable"))
                .when(scheduler)
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        GatewayKafkaProduceBackend backend = backend(service, gateway, 1, Duration.ofSeconds(1));

        try {
            KafkaProduceResult result =
                    backend.write(command((short) 1, connection, scheduler, topic))
                            .get(5, TimeUnit.SECONDS);

            assertThat(result.topics().get(0).partitions().get(0).error())
                    .as(result.topics().get(0).partitions().get(0).errorMessage())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            verify(gateway, never()).getTableInfo(any(GetTableInfoRequest.class));
            assertThat(controller.pendingReservations()).isZero();
            assertThat(controller.inFlightRequests()).isOne();
        } finally {
            blocker.close();
            blockerConnection.close();
            connection.close();
            controller.close();
        }
    }

    @Test
    void testPreSubmitSchedulerRuntimeFailureReleasesGrantedLease() throws Exception {
        TopicWrite topic = topic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> admissionTimer = mock(ScheduledFuture.class);
        doReturn(admissionTimer)
                .doThrow(new IllegalStateException("scheduler unavailable"))
                .when(scheduler)
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        GatewayKafkaProduceBackend backend = backend(service, gateway, 100);

        try {
            KafkaProduceResult result =
                    backend.write(command((short) 1, connection, scheduler, topic))
                            .get(5, TimeUnit.SECONDS);

            assertThat(result.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            verify(gateway, never()).getTableInfo(any(GetTableInfoRequest.class));
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
        }
    }

    @Test
    void testCompletionGraceSchedulerRuntimeFailureKeepsOriginalFutureAuthoritative()
            throws Exception {
        TopicWrite topic = topic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> admissionTimer = mock(ScheduledFuture.class);
        ScheduledFuture<?> preSubmitTimer = mock(ScheduledFuture.class);
        doReturn(admissionTimer)
                .doReturn(preSubmitTimer)
                .doThrow(new IllegalStateException("scheduler unavailable"))
                .when(scheduler)
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        CompletableFuture<ProduceLogResponse> originalProduceFuture = new CompletableFuture<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        when(gateway.produceLog(any(ProduceLogRequest.class))).thenReturn(originalProduceFuture);
        GatewayKafkaProduceBackend backend = backend(service, gateway, 100);

        try {
            CompletableFuture<KafkaProduceResult> result =
                    backend.write(command((short) 1, connection, scheduler, topic));
            assertThat(result).isNotDone();
            assertThat(controller.inFlightRequests()).isOne();

            originalProduceFuture.complete(produceResponse());

            assertThat(result.get(5, TimeUnit.SECONDS).topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.NONE);
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
        }
    }

    @Test
    void testGrantedMetadataLookupIsBoundedByPreSubmitDeadline() throws Exception {
        TopicWrite topic = topic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(new CompletableFuture<>());
        GatewayKafkaProduceBackend backend =
                backend(
                        service,
                        gateway,
                        new FixedSizeTranscoder(100),
                        Duration.ofSeconds(1),
                        Duration.ofMillis(20));

        try {
            KafkaProduceCommand command = command((short) 1, connection, scheduler, topic);
            KafkaProduceResult result = backend.write(command).get(5, TimeUnit.SECONDS);

            assertThat(result.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            assertThat(command.nativeAdmissionTransferFuture()).isCompleted();
            assertThat(topic.copiedRecords(topic.partitions().get(0))).isEmpty();
            verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testOperationTrackerCloseCancelsGrantedMetadataLookupImmediately() {
        TopicWrite topic = topic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        KafkaNativeProduceOperationTracker operationTracker =
                new KafkaNativeProduceOperationTracker();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(new CompletableFuture<>());
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service,
                        gateway,
                        new FixedSizeTranscoder(100),
                        KafkaProduceMetrics.noOp(),
                        Runnable::run,
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(5),
                        operationTracker);

        try {
            KafkaProduceCommand command = command((short) 1, connection, scheduler, topic);
            CompletableFuture<KafkaProduceResult> result = backend.write(command);
            assertThat(result).isNotDone();
            assertThat(operationTracker.activeOperations()).isOne();
            assertThat(controller.inFlightRequests()).isOne();

            operationTracker.close();

            assertThat(result.join().topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            assertThat(operationTracker.activeOperations()).isZero();
            assertThat(topic.copiedRecords(topic.partitions().get(0))).isEmpty();
            verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
        } finally {
            operationTracker.close();
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testRunningTranscodeQuiescesBeforeNativeLeaseReturnsToWaiter() throws Exception {
        TopicWrite runningTopic = topic("orders-running");
        TopicWrite waitingTopic = topic("orders-waiting");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle runningConnection = controller.registerConnection();
        ConnectionHandle waitingConnection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ExecutorService conversionExecutor = Executors.newSingleThreadExecutor();
        KafkaNativeProduceOperationTracker operationTracker =
                new KafkaNativeProduceOperationTracker();
        BlockingTranscoder transcoder = new BlockingTranscoder(100);
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        GatewayKafkaProduceBackend backend =
                new GatewayKafkaProduceBackend(
                        service,
                        gateway,
                        transcoder,
                        KafkaProduceMetrics.noOp(),
                        conversionExecutor,
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(5),
                        operationTracker);
        RequestLease waitingLease = null;

        try {
            CompletableFuture<KafkaProduceResult> runningResult =
                    backend.write(command((short) 1, runningConnection, scheduler, runningTopic));
            assertThat(transcoder.started.await(5, TimeUnit.SECONDS)).isTrue();
            KafkaNativeProduceAdmissionController.Reservation waitingReservation =
                    waitingConnection.reserve(waitingTopic.estimatedConvertedBytes());

            assertThat(waitingReservation.getFuture()).isNotDone();
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.pendingReservations()).isOne();
            assertThat(runningTopic.copiedRecords(runningTopic.partitions().get(0))).isNotEmpty();

            operationTracker.close();

            assertThat(runningResult).isNotDone();
            assertThat(waitingReservation.getFuture()).isNotDone();
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.pendingReservations()).isOne();
            assertThat(runningTopic.copiedRecords(runningTopic.partitions().get(0))).isNotEmpty();

            transcoder.release.countDown();

            KafkaProduceResult completed = runningResult.get(5, TimeUnit.SECONDS);
            assertThat(completed.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            waitingLease = waitingReservation.getFuture().get(5, TimeUnit.SECONDS);
            assertThat(runningTopic.copiedRecords(runningTopic.partitions().get(0))).isEmpty();
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.pendingReservations()).isZero();

            waitingLease.close();
            waitingLease = null;
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
        } finally {
            transcoder.release.countDown();
            if (waitingLease != null) {
                waitingLease.close();
            }
            operationTracker.close();
            runningConnection.close();
            waitingConnection.close();
            controller.close();
            scheduler.shutdownNow();
            conversionExecutor.shutdownNow();
        }
    }

    @Test
    void testPostSubmitMaintenanceFailureDoesNotOverrideOriginalFuture() {
        TopicWrite topic = topic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        CompletableFuture<ProduceLogResponse> originalProduceFuture = new CompletableFuture<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        when(gateway.produceLog(any(ProduceLogRequest.class))).thenReturn(originalProduceFuture);
        doThrow(new RuntimeException("maintenance failure")).when(service).tryCompleteActions();
        GatewayKafkaProduceBackend backend = backend(service, gateway, 100);

        try {
            CompletableFuture<KafkaProduceResult> result =
                    backend.write(command((short) -1, connection, scheduler, topic));

            assertThat(result).isNotDone();
            assertThat(controller.inFlightRequests()).isOne();
            originalProduceFuture.complete(produceResponse());
            assertThat(result.join().topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.NONE);
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
            verify(service).tryCompleteActions();
        } finally {
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testCompletedNativeFutureClearsRequestRecordsBeforeGrantingWaiter() {
        TopicWrite topic = topic("orders");
        TopicWrite waitingTopic = topic("orders-waiting");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ConnectionHandle waitingConnection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        AtomicReference<ProduceLogRequest> capturedRequest = new AtomicReference<>();
        AtomicReference<CompletableFuture<RequestLease>> waitingFuture = new AtomicReference<>();
        AtomicReference<RequestLease> waitingLease = new AtomicReference<>();
        AtomicReference<Boolean> recordsClearedAtGrant = new AtomicReference<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        when(gateway.produceLog(any(ProduceLogRequest.class)))
                .thenAnswer(
                        invocation -> {
                            ProduceLogRequest request = invocation.getArgument(0);
                            capturedRequest.set(request);
                            CompletableFuture<RequestLease> future =
                                    waitingConnection
                                            .reserve(waitingTopic.estimatedConvertedBytes())
                                            .getFuture();
                            waitingFuture.set(future);
                            assertThat(future).isNotDone();
                            future.thenAccept(
                                    lease -> {
                                        recordsClearedAtGrant.set(
                                                !request.getBucketsReqAt(0).hasRecords());
                                        waitingLease.set(lease);
                                    });
                            return CompletableFuture.completedFuture(produceResponse());
                        });
        GatewayKafkaProduceBackend backend = backend(service, gateway, 100);

        try {
            KafkaProduceResult result =
                    backend.write(command((short) 1, connection, scheduler, topic)).join();

            assertThat(result.topics().get(0).partitions().get(0).error()).isEqualTo(Errors.NONE);
            assertThat(waitingFuture.get()).isCompleted();
            assertThat(recordsClearedAtGrant.get()).isTrue();
            assertThat(capturedRequest.get().getBucketsReqAt(0).hasRecords()).isFalse();
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.totalReservedBytes())
                    .isEqualTo(waitingTopic.estimatedConvertedBytes());

            waitingLease.get().close();
            waitingLease.set(null);
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
        } finally {
            if (waitingLease.get() != null) {
                waitingLease.get().close();
            }
            connection.close();
            waitingConnection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testResizeHardLimitMapsToMessageTooLarge() {
        TopicWrite topic = topic("orders");
        long estimate = topic.estimatedConvertedBytes();
        KafkaNativeProduceAdmissionController controller = controller(1, estimate);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        GatewayKafkaProduceBackend backend = backend(service, gateway, (int) estimate + 1);

        try {
            KafkaProduceResult result =
                    backend.write(command((short) 1, connection, scheduler, topic)).join();

            assertThat(result.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.MESSAGE_TOO_LARGE);
            verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.convertedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testResizeTemporaryPressureMapsToRequestTimeout() {
        TopicWrite topic = topic("orders");
        long estimate = topic.estimatedConvertedBytes();
        int nativeBytes = (int) estimate + 1;
        long peakBytes = estimate + nativeBytes;
        KafkaNativeProduceAdmissionController controller = controller(2, peakBytes + 19);
        ConnectionHandle blockerConnection = controller.registerConnection();
        RequestLease blocker = blockerConnection.reserve(20).getFuture().join();
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        GatewayKafkaProduceBackend backend = backend(service, gateway, nativeBytes);

        try {
            KafkaProduceResult result =
                    backend.write(command((short) 1, connection, scheduler, topic)).join();

            assertThat(result.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.convertedBytes()).isEqualTo(20);
        } finally {
            blocker.close();
            blockerConnection.close();
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testSecondPartitionPageGrowthFailurePreventsPartialNativeSubmit() {
        TopicWrite topic = twoPartitionTopic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 200);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        GatewayKafkaProduceBackend backend =
                backend(
                        service,
                        gateway,
                        new SequenceSizeTranscoder(50, 100),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1));

        try {
            KafkaProduceResult result =
                    backend.write(command((short) 1, connection, scheduler, topic)).join();

            assertThat(result.topics().get(0).partitions())
                    .allSatisfy(
                            partition ->
                                    assertThat(partition.error())
                                            .isEqualTo(Errors.MESSAGE_TOO_LARGE));
            verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
            assertThat(topic.copiedRecords(topic.partitions().get(0))).isEmpty();
            assertThat(topic.copiedRecords(topic.partitions().get(1))).isEmpty();
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testArrowMemoryExhaustionMapsToRequestTimeoutAndReleasesAdmission() {
        TopicWrite topic = topic("orders");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        GatewayKafkaProduceBackend backend =
                backend(
                        service,
                        gateway,
                        new ArrowMemoryExhaustingTranscoder(),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1));

        try {
            KafkaProduceResult result =
                    backend.write(command((short) 1, connection, scheduler, topic)).join();

            assertThat(result.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.REQUEST_TIMED_OUT);
            verify(gateway, never()).produceLog(any(ProduceLogRequest.class));
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.totalReservedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testMultiTopicLeasesReleaseIndependently() {
        TopicWrite firstTopic = topic("orders-a");
        TopicWrite secondTopic = topic("orders-b");
        KafkaNativeProduceAdmissionController controller = controller(1, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        CompletableFuture<ProduceLogResponse> firstProduceFuture = new CompletableFuture<>();
        CompletableFuture<ProduceLogResponse> secondProduceFuture = new CompletableFuture<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        when(gateway.produceLog(any(ProduceLogRequest.class)))
                .thenReturn(firstProduceFuture, secondProduceFuture);
        GatewayKafkaProduceBackend backend = backend(service, gateway, 100);

        try {
            KafkaProduceCommand command =
                    command((short) -1, connection, scheduler, firstTopic, secondTopic);
            CompletableFuture<KafkaProduceResult> result = backend.write(command);

            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.pendingReservations()).isOne();
            assertThat(controller.convertedBytes()).isEqualTo(retainedBytes(firstTopic, 100));
            assertThat(command.nativeAdmissionTransferFuture()).isCompleted();
            firstProduceFuture.complete(produceResponse());
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.convertedBytes()).isEqualTo(retainedBytes(secondTopic, 100));
            assertThat(controller.pendingReservations()).isZero();
            assertThat(command.nativeAdmissionTransferFuture()).isCompleted();
            assertThat(result).isNotDone();

            secondProduceFuture.complete(produceResponse());
            assertThat(result.join().topics()).hasSize(2);
            assertThat(command.nativeAdmissionTransferFuture()).isCompleted();
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.convertedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testMixedRejectedAndSlowTopicRetainsRawOwnershipUntilAggregateCompletes() {
        TopicWrite slowTopic = topic("orders-slow");
        TopicWrite rejectedTopic = topic("orders-too-large", 10_001);
        KafkaNativeProduceAdmissionController controller = controller(2, 10_000);
        ConnectionHandle connection = controller.registerConnection();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RpcGatewayService service = mock(RpcGatewayService.class);
        TabletServerGateway gateway = mock(TabletServerGateway.class);
        CompletableFuture<ProduceLogResponse> slowProduceFuture = new CompletableFuture<>();
        when(gateway.getTableInfo(any(GetTableInfoRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(tableInfoResponse()));
        when(gateway.produceLog(any(ProduceLogRequest.class))).thenReturn(slowProduceFuture);
        GatewayKafkaProduceBackend backend = backend(service, gateway, 100);

        try {
            KafkaProduceCommand command =
                    command((short) -1, connection, scheduler, slowTopic, rejectedTopic);
            CompletableFuture<KafkaProduceResult> result = backend.write(command);

            assertThat(result).isNotDone();
            assertThat(command.nativeAdmissionTransferFuture()).isNotDone();
            assertThat(command.topics()).hasSize(2);
            assertThat(rejectedTopic.copiedRecords(rejectedTopic.partitions().get(0))).isEmpty();
            assertThat(controller.inFlightRequests()).isOne();
            assertThat(controller.convertedBytes()).isEqualTo(retainedBytes(slowTopic, 100));

            slowProduceFuture.complete(produceResponse());
            KafkaProduceResult completed = result.join();
            assertThat(completed.topics().get(0).partitions().get(0).error())
                    .isEqualTo(Errors.NONE);
            assertThat(completed.topics().get(1).partitions().get(0).error())
                    .isEqualTo(Errors.MESSAGE_TOO_LARGE);
            assertThat(command.nativeAdmissionTransferFuture()).isCompleted();
            assertThat(command.topics()).isEmpty();
            assertThat(controller.inFlightRequests()).isZero();
            assertThat(controller.convertedBytes()).isZero();
        } finally {
            connection.close();
            controller.close();
            scheduler.shutdownNow();
        }
    }

    private static GatewayKafkaProduceBackend backend(
            RpcGatewayService service, TabletServerGateway gateway, int nativeBytes) {
        return backend(service, gateway, nativeBytes, Duration.ofSeconds(30));
    }

    private static GatewayKafkaProduceBackend backend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            int nativeBytes,
            Duration acquireTimeout) {
        return backend(
                service,
                gateway,
                new FixedSizeTranscoder(nativeBytes),
                acquireTimeout,
                Duration.ofMinutes(5));
    }

    private static GatewayKafkaProduceBackend backend(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaRecordTranscoder transcoder,
            Duration acquireTimeout,
            Duration completionGraceTimeout) {
        return new GatewayKafkaProduceBackend(
                service,
                gateway,
                transcoder,
                KafkaProduceMetrics.noOp(),
                Runnable::run,
                acquireTimeout,
                completionGraceTimeout);
    }

    private static KafkaNativeProduceAdmissionController controller(
            long maxRequests, long maxBytes) {
        return new KafkaNativeProduceAdmissionController(
                maxRequests, maxBytes, maxRequests, maxBytes, 10);
    }

    private static KafkaProduceCommand command(
            short acks,
            ConnectionHandle connection,
            ScheduledExecutorService scheduler,
            TopicWrite... topics) {
        List<TopicWrite> topicWrites = new ArrayList<>();
        Collections.addAll(topicWrites, topics);
        return new KafkaProduceCommand(
                acks,
                1_000,
                topicWrites,
                "KAFKA",
                null,
                FlussPrincipal.ANONYMOUS,
                connection,
                scheduler);
    }

    private static TopicWrite topic(String topicName) {
        return topic(topicName, "value".getBytes(StandardCharsets.UTF_8).length);
    }

    private static TopicWrite topic(String topicName, int valueBytes) {
        Record record = new Record(1L, null, new byte[valueBytes], Collections.emptyList());
        return new TopicWrite(
                "kafka." + topicName,
                Collections.singletonList(
                        new PartitionWrite(0, Collections.singletonList(record))));
    }

    private static TopicWrite twoPartitionTopic(String topicName) {
        Record first = new Record(1L, null, new byte[5], Collections.emptyList());
        Record second = new Record(2L, null, new byte[5], Collections.emptyList());
        return new TopicWrite(
                "kafka." + topicName,
                java.util.Arrays.asList(
                        new PartitionWrite(0, Collections.singletonList(first)),
                        new PartitionWrite(1, Collections.singletonList(second))));
    }

    private static long retainedBytes(TopicWrite topic, long outputBytes) {
        long copiedRecordBytes = 0;
        for (PartitionWrite partition : topic.partitions()) {
            copiedRecordBytes += partition.estimatedCopiedRecordBytes();
        }
        return topic.estimatedConvertedBytes() - copiedRecordBytes + outputBytes;
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

    private static final class FixedSizeTranscoder implements KafkaRecordTranscoder {
        private final ArrowKafkaRecordTranscoder delegate = new ArrowKafkaRecordTranscoder();
        private final int nativeBytes;

        private FixedSizeTranscoder(int nativeBytes) {
            this.nativeBytes = nativeBytes;
        }

        @Override
        public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
            return delegate.prepare(tableInfo);
        }

        @Override
        public BytesView transcode(
                List<Record> records,
                KafkaTopicWritePlan writePlan,
                KafkaOutputMemoryBudget outputMemoryBudget) {
            outputMemoryBudget.reserve(nativeBytes);
            try {
                return new ByteBufBytesView(new byte[nativeBytes]);
            } catch (Throwable failure) {
                outputMemoryBudget.release(nativeBytes);
                throw failure;
            }
        }
    }

    private static final class SequenceSizeTranscoder implements KafkaRecordTranscoder {
        private final ArrowKafkaRecordTranscoder delegate = new ArrowKafkaRecordTranscoder();
        private final int[] outputSizes;
        private final AtomicInteger invocation = new AtomicInteger();

        private SequenceSizeTranscoder(int... outputSizes) {
            this.outputSizes = outputSizes;
        }

        @Override
        public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
            return delegate.prepare(tableInfo);
        }

        @Override
        public BytesView transcode(
                List<Record> records,
                KafkaTopicWritePlan writePlan,
                KafkaOutputMemoryBudget outputMemoryBudget) {
            int bytes = outputSizes[invocation.getAndIncrement()];
            outputMemoryBudget.reserve(bytes);
            try {
                return new ByteBufBytesView(new byte[bytes]);
            } catch (Throwable failure) {
                outputMemoryBudget.release(bytes);
                throw failure;
            }
        }
    }

    private static final class ArrowMemoryExhaustingTranscoder implements KafkaRecordTranscoder {
        private final ArrowKafkaRecordTranscoder delegate = new ArrowKafkaRecordTranscoder();

        @Override
        public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
            return delegate.prepare(tableInfo);
        }

        @Override
        public BytesView transcode(
                List<Record> records,
                KafkaTopicWritePlan writePlan,
                KafkaOutputMemoryBudget outputMemoryBudget) {
            outputMemoryBudget.reserve(100);
            try {
                throw new OutOfMemoryException("allocator exhausted");
            } finally {
                outputMemoryBudget.release(100);
            }
        }
    }

    private static final class BlockingTranscoder implements KafkaRecordTranscoder {
        private final ArrowKafkaRecordTranscoder delegate = new ArrowKafkaRecordTranscoder();
        private final int nativeBytes;
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private BlockingTranscoder(int nativeBytes) {
            this.nativeBytes = nativeBytes;
        }

        @Override
        public KafkaTopicWritePlan prepare(TableInfo tableInfo) {
            return delegate.prepare(tableInfo);
        }

        @Override
        public BytesView transcode(
                List<Record> records,
                KafkaTopicWritePlan writePlan,
                KafkaOutputMemoryBudget outputMemoryBudget)
                throws Exception {
            outputMemoryBudget.reserve(nativeBytes);
            started.countDown();
            release.await();
            return new ByteBufBytesView(new byte[nativeBytes]);
        }
    }
}
