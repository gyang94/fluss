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

package org.apache.fluss.kafka;

import org.apache.fluss.kafka.api.admin.CreateTopicsHandler;
import org.apache.fluss.kafka.api.admin.DeleteTopicsHandler;
import org.apache.fluss.kafka.api.metadata.MetadataHandler;
import org.apache.fluss.kafka.api.produce.ProduceHandler;
import org.apache.fluss.kafka.api.sasl.SaslAuthenticateHandler;
import org.apache.fluss.kafka.api.sasl.SaslHandshakeHandler;
import org.apache.fluss.kafka.api.versions.ApiVersionsHandler;
import org.apache.fluss.kafka.backend.admin.GatewayKafkaTopicAdminBackend;
import org.apache.fluss.kafka.backend.metadata.GatewayKafkaMetadataBackend;
import org.apache.fluss.kafka.backend.produce.GatewayKafkaProduceBackend;
import org.apache.fluss.kafka.backend.produce.KafkaNativeProduceOperationTracker;
import org.apache.fluss.kafka.dispatcher.KafkaApiRegistry;
import org.apache.fluss.kafka.dispatcher.KafkaRequestDispatcher;
import org.apache.fluss.kafka.error.KafkaErrorMapper;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminOperationAuthorizer;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.RequestType;

import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Entry point that dispatches Kafka protocol requests to registered API handlers. */
public class KafkaRequestHandler implements RequestHandler<KafkaRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRequestHandler.class);
    private static final Executor DIRECT_EXECUTOR = Runnable::run;
    private static final Duration DEFAULT_NATIVE_ADMISSION_ACQUIRE_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_NATIVE_COMPLETION_GRACE_TIMEOUT = Duration.ofMinutes(5);

    private final KafkaRequestDispatcher dispatcher;
    private final KafkaProduceMetrics produceMetrics;

    /** Creates a Kafka request handler with the capabilities provided by a TabletServer. */
    public KafkaRequestHandler(RpcGatewayService service, TabletServerGateway gateway) {
        this(service, gateway, KafkaProduceMetrics.noOp());
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaProduceMetrics produceMetrics) {
        this(service, gateway, produceMetrics, new ArrowKafkaRecordTranscoder(produceMetrics));
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaProduceMetrics produceMetrics,
            KafkaRecordTranscoder transcoder) {
        this(
                service,
                gateway,
                produceMetrics,
                transcoder,
                DIRECT_EXECUTOR,
                DEFAULT_NATIVE_ADMISSION_ACQUIRE_TIMEOUT,
                DEFAULT_NATIVE_COMPLETION_GRACE_TIMEOUT);
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaProduceMetrics produceMetrics,
            KafkaRecordTranscoder transcoder,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout) {
        this(
                service,
                gateway,
                produceMetrics,
                transcoder,
                conversionExecutor,
                nativeAdmissionAcquireTimeout,
                nativeCompletionGraceTimeout,
                Long.MAX_VALUE,
                Long.MAX_VALUE);
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaProduceMetrics produceMetrics,
            KafkaRecordTranscoder transcoder,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout,
            long maxCopiedBytesPerRequest,
            long maxCopiedBytesPerRecord) {
        this(
                service,
                gateway,
                produceMetrics,
                transcoder,
                conversionExecutor,
                nativeAdmissionAcquireTimeout,
                nativeCompletionGraceTimeout,
                maxCopiedBytesPerRequest,
                maxCopiedBytesPerRecord,
                new KafkaNativeProduceOperationTracker());
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            KafkaProduceMetrics produceMetrics,
            KafkaRecordTranscoder transcoder,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout,
            long maxCopiedBytesPerRequest,
            long maxCopiedBytesPerRecord,
            KafkaNativeProduceOperationTracker operationTracker) {
        checkNotNull(service);
        checkNotNull(gateway);
        this.produceMetrics = checkNotNull(produceMetrics);
        KafkaApiRegistry registry = new KafkaApiRegistry();
        registry.register(new ApiVersionsHandler(registry));
        registry.register(new SaslHandshakeHandler());
        registry.register(new SaslAuthenticateHandler());
        registry.register(new MetadataHandler(new GatewayKafkaMetadataBackend(service, gateway)));
        registry.register(
                new ProduceHandler(
                        new GatewayKafkaProduceBackend(
                                service,
                                gateway,
                                checkNotNull(transcoder),
                                produceMetrics,
                                checkNotNull(conversionExecutor),
                                checkNotNull(nativeAdmissionAcquireTimeout),
                                checkNotNull(nativeCompletionGraceTimeout),
                                checkNotNull(operationTracker)),
                        produceMetrics,
                        maxCopiedBytesPerRequest,
                        maxCopiedBytesPerRecord));
        registry.freeze();
        this.dispatcher = new KafkaRequestDispatcher(registry, new KafkaErrorMapper());
    }

    /** Creates a Kafka request handler including topic lifecycle capabilities. */
    public KafkaRequestHandler(
            RpcGatewayService service, TabletServerGateway gateway, AdminGateway adminGateway) {
        this(service, gateway, adminGateway, adminOperationAuthorizer(service));
    }

    /** Creates a Kafka request handler with explicit authorization for topic lifecycle requests. */
    public KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            AdminOperationAuthorizer adminOperationAuthorizer) {
        this(
                service,
                gateway,
                adminGateway,
                adminOperationAuthorizer,
                KafkaDataFormat.RAW,
                KafkaDataFormat.RAW);
    }

    /**
     * Creates a Kafka request handler including topic lifecycle and default format capabilities.
     */
    public KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat) {
        this(
                service,
                gateway,
                adminGateway,
                adminOperationAuthorizer(service),
                defaultKeyFormat,
                defaultValueFormat);
    }

    /**
     * Creates a Kafka request handler with explicit authorization and default format capabilities.
     */
    public KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            AdminOperationAuthorizer adminOperationAuthorizer,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat) {
        this(
                service,
                gateway,
                adminGateway,
                adminOperationAuthorizer,
                defaultKeyFormat,
                defaultValueFormat,
                KafkaProduceMetrics.noOp());
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            AdminOperationAuthorizer adminOperationAuthorizer,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat,
            KafkaProduceMetrics produceMetrics) {
        this(
                service,
                gateway,
                adminGateway,
                adminOperationAuthorizer,
                defaultKeyFormat,
                defaultValueFormat,
                produceMetrics,
                new ArrowKafkaRecordTranscoder(produceMetrics));
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            AdminOperationAuthorizer adminOperationAuthorizer,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat,
            KafkaProduceMetrics produceMetrics,
            KafkaRecordTranscoder transcoder) {
        this(
                service,
                gateway,
                adminGateway,
                adminOperationAuthorizer,
                defaultKeyFormat,
                defaultValueFormat,
                produceMetrics,
                transcoder,
                DIRECT_EXECUTOR,
                DEFAULT_NATIVE_ADMISSION_ACQUIRE_TIMEOUT,
                DEFAULT_NATIVE_COMPLETION_GRACE_TIMEOUT);
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            AdminOperationAuthorizer adminOperationAuthorizer,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat,
            KafkaProduceMetrics produceMetrics,
            KafkaRecordTranscoder transcoder,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout) {
        this(
                service,
                gateway,
                adminGateway,
                adminOperationAuthorizer,
                defaultKeyFormat,
                defaultValueFormat,
                produceMetrics,
                transcoder,
                conversionExecutor,
                nativeAdmissionAcquireTimeout,
                nativeCompletionGraceTimeout,
                Long.MAX_VALUE,
                Long.MAX_VALUE);
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            AdminOperationAuthorizer adminOperationAuthorizer,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat,
            KafkaProduceMetrics produceMetrics,
            KafkaRecordTranscoder transcoder,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout,
            long maxCopiedBytesPerRequest,
            long maxCopiedBytesPerRecord) {
        this(
                service,
                gateway,
                adminGateway,
                adminOperationAuthorizer,
                defaultKeyFormat,
                defaultValueFormat,
                produceMetrics,
                transcoder,
                conversionExecutor,
                nativeAdmissionAcquireTimeout,
                nativeCompletionGraceTimeout,
                maxCopiedBytesPerRequest,
                maxCopiedBytesPerRecord,
                new KafkaNativeProduceOperationTracker());
    }

    KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            AdminOperationAuthorizer adminOperationAuthorizer,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat,
            KafkaProduceMetrics produceMetrics,
            KafkaRecordTranscoder transcoder,
            Executor conversionExecutor,
            Duration nativeAdmissionAcquireTimeout,
            Duration nativeCompletionGraceTimeout,
            long maxCopiedBytesPerRequest,
            long maxCopiedBytesPerRecord,
            KafkaNativeProduceOperationTracker operationTracker) {
        checkNotNull(service);
        checkNotNull(gateway);
        checkNotNull(adminGateway);
        checkNotNull(adminOperationAuthorizer);
        checkNotNull(defaultKeyFormat);
        checkNotNull(defaultValueFormat);
        this.produceMetrics = checkNotNull(produceMetrics);
        KafkaApiRegistry registry = new KafkaApiRegistry();
        registry.register(new ApiVersionsHandler(registry));
        registry.register(new SaslHandshakeHandler());
        registry.register(new SaslAuthenticateHandler());
        registry.register(
                new MetadataHandler(new GatewayKafkaMetadataBackend(service, gateway), true));
        registry.register(
                new ProduceHandler(
                        new GatewayKafkaProduceBackend(
                                service,
                                gateway,
                                checkNotNull(transcoder),
                                produceMetrics,
                                checkNotNull(conversionExecutor),
                                checkNotNull(nativeAdmissionAcquireTimeout),
                                checkNotNull(nativeCompletionGraceTimeout),
                                checkNotNull(operationTracker)),
                        produceMetrics,
                        maxCopiedBytesPerRequest,
                        maxCopiedBytesPerRecord));
        GatewayKafkaTopicAdminBackend topicAdminBackend =
                new GatewayKafkaTopicAdminBackend(service, adminGateway, adminOperationAuthorizer);
        registry.register(
                new CreateTopicsHandler(topicAdminBackend, defaultKeyFormat, defaultValueFormat));
        registry.register(new DeleteTopicsHandler(topicAdminBackend));
        registry.freeze();
        this.dispatcher = new KafkaRequestDispatcher(registry, new KafkaErrorMapper());
    }

    private static AdminOperationAuthorizer adminOperationAuthorizer(RpcGatewayService service) {
        if (!(service instanceof AdminOperationAuthorizer)) {
            throw new IllegalArgumentException(
                    "Kafka topic administration requires an AdminOperationAuthorizer.");
        }
        return (AdminOperationAuthorizer) service;
    }

    @Override
    public RequestType requestType() {
        return RequestType.KAFKA;
    }

    @Override
    public void processRequest(KafkaRequest request) {
        boolean isProduce = request.apiKey() == ApiKeys.PRODUCE;
        if (isProduce) {
            recordMetric(
                    () ->
                            produceMetrics.requestStarted(
                                    request.receivedTimeNanos(), request.requestBytes()));
        }
        if (request.cancelled()) {
            if (isProduce) {
                recordMetric(
                        () ->
                                produceMetrics.requestCompleted(
                                        request.receivedTimeNanos(), true, 0));
            }
            request.fail(
                    new LeaderNotAvailableException(
                            "Kafka connection closed before the request was dispatched."));
            return;
        }

        CompletableFuture<AbstractResponse> responseFuture;
        try {
            responseFuture = dispatcher.dispatch(request);
        } catch (Throwable failure) {
            if (isProduce) {
                recordMetric(
                        () ->
                                produceMetrics.requestCompleted(
                                        request.receivedTimeNanos(), true, 0));
            }
            request.fail(failure);
            return;
        }
        if (isProduce) {
            try {
                request.detachProducePayload();
            } catch (Throwable detachFailure) {
                // Retain ordered ownership until response/cancellation when partition metadata
                // cannot be cached safely. The request can still complete normally.
                LOG.warn(
                        "Unable to detach copied Kafka Produce payload for request {}.",
                        request.requestId(),
                        detachFailure);
            }
        }
        responseFuture.whenComplete(
                (response, failure) -> {
                    try {
                        if (isProduce) {
                            int failedPartitions = countFailedPartitions(response);
                            recordMetric(
                                    () ->
                                            produceMetrics.requestCompleted(
                                                    request.receivedTimeNanos(),
                                                    failure != null || failedPartitions > 0,
                                                    failedPartitions));
                        }
                        if (failure == null) {
                            request.complete(response);
                        } else {
                            request.fail(failure);
                        }
                    } catch (Throwable completionFailure) {
                        request.fail(completionFailure);
                    }
                });
    }

    private void recordMetric(Runnable recorder) {
        try {
            recorder.run();
        } catch (Throwable ignored) {
            // Metrics are observational and must not affect request dispatch or completion.
        }
    }

    private static int countFailedPartitions(AbstractResponse response) {
        if (!(response instanceof ProduceResponse)) {
            return 0;
        }
        int failedPartitions = 0;
        for (ProduceResponseData.TopicProduceResponse topic :
                ((ProduceResponse) response).data().responses()) {
            for (ProduceResponseData.PartitionProduceResponse partition :
                    topic.partitionResponses()) {
                if (partition.errorCode() != 0) {
                    failedPartitions++;
                }
            }
        }
        return failedPartitions;
    }
}
