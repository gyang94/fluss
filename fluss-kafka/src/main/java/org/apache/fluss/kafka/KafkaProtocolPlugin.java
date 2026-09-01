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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaRequestAdmissionController;
import org.apache.fluss.kafka.backend.produce.KafkaNativeProduceOperationTracker;
import org.apache.fluss.kafka.backend.produce.KafkaProduceConversionExecutor;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaArrowWriterManager;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.AdminGatewayProvider;
import org.apache.fluss.rpc.gateway.AdminOperationAuthorizer;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServerConfigManager;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.ExecutorUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The Kafka protocol plugin. */
public class KafkaProtocolPlugin implements NetworkProtocolPlugin, ServerReconfigurable {

    private static final String SASL_AUTH_PROTOCOL = "sasl";
    private static final String PLAINTEXT_AUTH_PROTOCOL = "plaintext";
    private static final int MAX_CONVERSION_QUEUE_CAPACITY = 1024;

    private Configuration conf;
    private PlainSaslServerConfigManager plainSaslServerConfigManager;
    private Map<String, Supplier<ServerAuthenticator>> authenticatorSuppliers =
            Collections.emptyMap();
    private Set<String> saslListenerNames = Collections.emptySet();
    private KafkaProduceMetrics produceMetrics = KafkaProduceMetrics.noOp();
    private KafkaRequestAdmissionController admissionController;
    private KafkaNativeProduceAdmissionController nativeAdmissionController;
    private KafkaNativeProduceOperationTracker nativeOperationTracker;
    private KafkaArrowWriterManager arrowWriterManager;
    private KafkaRecordTranscoder recordTranscoder;
    private ExecutorService conversionExecutor;
    private CompletableFuture<Void> closeFuture;

    @Override
    public String name() {
        return KAFKA_PROTOCOL_NAME;
    }

    @Override
    public void setup(Configuration conf) {
        setup(conf, KafkaProduceMetrics.noOp());
    }

    private void setup(Configuration conf, KafkaProduceMetrics produceMetrics) {
        validateKafkaAuthenticationConfiguration(conf);
        validateArrowConfiguration(conf);
        validateAdmissionConfiguration(conf);
        validateNativeAdmissionConfiguration(conf);
        this.saslListenerNames = saslListenerNames(conf);
        this.plainSaslServerConfigManager = new PlainSaslServerConfigManager(conf);
        this.conf = plainSaslServerConfigManager.getConfiguration();
        this.authenticatorSuppliers =
                AuthenticationFactory.loadServerAuthenticatorSuppliers(this.conf);
        this.produceMetrics = produceMetrics;
        if (admissionController == null) {
            admissionController = createAdmissionController(this.conf, produceMetrics);
        }
        registerAdmissionMetrics(produceMetrics, admissionController);
        if (nativeAdmissionController == null) {
            nativeAdmissionController = createNativeAdmissionController(this.conf);
        }
        produceMetrics.registerNativeAdmissionGauges(nativeAdmissionController);
        KafkaNativeProduceOperationTracker previousOperationTracker = nativeOperationTracker;
        nativeOperationTracker = new KafkaNativeProduceOperationTracker();
        ExecutorService previousExecutor = this.conversionExecutor;
        this.conversionExecutor = createConversionExecutor(this.conf);
        KafkaArrowWriterManager previousManager = this.arrowWriterManager;
        this.arrowWriterManager = createArrowWriterManager(this.conf, produceMetrics);
        this.recordTranscoder =
                new ArrowKafkaRecordTranscoder(produceMetrics, this.arrowWriterManager);
        if (previousManager != null) {
            previousManager.closeAsync();
        }
        if (previousExecutor != null) {
            cancelQueuedConversionTasks(previousExecutor);
            ExecutorUtils.nonBlockingShutdown(30, TimeUnit.SECONDS, previousExecutor);
        }
        if (previousOperationTracker != null) {
            previousOperationTracker.close();
        }
    }

    @Override
    public void setup(Configuration conf, MetricGroup serverMetricGroup) {
        setup(conf, new KafkaProduceMetrics(serverMetricGroup));
    }

    @Override
    public List<String> listenerNames() {
        return conf.get(ConfigOptions.KAFKA_LISTENER_NAMES);
    }

    @Override
    public ChannelHandler createChannelHandler(
            RequestChannel[] requestChannels, String listenerName) {
        Supplier<ServerAuthenticator> authenticatorSupplier = null;
        if (saslListenerNames.contains(listenerName)) {
            authenticatorSupplier =
                    checkNotNull(
                            authenticatorSuppliers.get(listenerName),
                            "No SASL server authenticator is configured for Kafka listener %s.",
                            listenerName);
        }
        return new KafkaChannelInitializer(
                requestChannels,
                listenerName,
                conf.get(ConfigOptions.KAFKA_CONNECTION_MAX_IDLE_TIME).getSeconds(),
                (int) conf.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes(),
                conf.getBoolean(ConfigOptions.NETTY_CLIENT_ALLOCATOR_HEAP_BUFFER_FIRST),
                authenticatorSupplier,
                produceMetrics,
                admissionController,
                nativeAdmissionController,
                conf.get(ConfigOptions.KAFKA_ADMISSION_PRE_FRAME_WAIT_TIMEOUT),
                conf.get(ConfigOptions.KAFKA_ADMISSION_BODY_READ_TIMEOUT));
    }

    @Override
    public RequestHandler<?> createRequestHandler(RpcGatewayService service) {
        if (!(service instanceof TabletServerGateway)) {
            throw new IllegalArgumentException(
                    "Kafka protocol endpoints can only be enabled on TabletServers, but the service is "
                            + service.getClass().getSimpleName());
        }
        TabletServerGateway gateway = (TabletServerGateway) service;
        long maxCopiedBytesPerRequest = maxNativeAdmissionBytesPerRequest(conf);
        long maxCopiedBytesPerRecord = maxCopiedBytesPerRequest;
        if (service instanceof AdminGatewayProvider) {
            if (!(service instanceof AdminOperationAuthorizer)) {
                throw new IllegalArgumentException(
                        "Kafka topic administration requires the TabletServer service to authorize external admin operations before internal forwarding.");
            }
            return new KafkaRequestHandler(
                    service,
                    gateway,
                    ((AdminGatewayProvider) service).getAdminGateway(),
                    (AdminOperationAuthorizer) service,
                    conf.get(ConfigOptions.KAFKA_DATABASE),
                    KafkaDataFormat.parse(conf.get(ConfigOptions.KAFKA_DEFAULT_KEY_FORMAT)),
                    KafkaDataFormat.parse(conf.get(ConfigOptions.KAFKA_DEFAULT_VALUE_FORMAT)),
                    produceMetrics,
                    recordTranscoder,
                    conversionExecutor,
                    conf.get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_ACQUIRE_TIMEOUT),
                    conf.get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_COMPLETION_GRACE_TIMEOUT),
                    maxCopiedBytesPerRequest,
                    maxCopiedBytesPerRecord,
                    checkNotNull(nativeOperationTracker));
        }
        return new KafkaRequestHandler(
                service,
                gateway,
                conf.get(ConfigOptions.KAFKA_DATABASE),
                produceMetrics,
                recordTranscoder,
                conversionExecutor,
                conf.get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_ACQUIRE_TIMEOUT),
                conf.get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_COMPLETION_GRACE_TIMEOUT),
                maxCopiedBytesPerRequest,
                maxCopiedBytesPerRecord,
                checkNotNull(nativeOperationTracker));
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        validateKafkaAuthenticationConfiguration(newConfig);
        validateArrowConfiguration(newConfig);
        validateAdmissionConfiguration(newConfig);
        validateNativeAdmissionConfiguration(newConfig);
        plainSaslServerConfigManager.validate(newConfig);
    }

    @Override
    public void validate(Configuration newConfig, @Nullable FlussPrincipal requester)
            throws ConfigException {
        validateKafkaAuthenticationConfiguration(newConfig);
        plainSaslServerConfigManager.validate(newConfig, requester);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        KafkaRequestAdmissionController controller;
        KafkaNativeProduceAdmissionController nativeController;
        KafkaNativeProduceOperationTracker operationTracker;
        KafkaArrowWriterManager manager;
        ExecutorService executor;
        CompletableFuture<Void> result;
        synchronized (this) {
            if (closeFuture != null) {
                return closeFuture;
            }
            result = new CompletableFuture<>();
            closeFuture = result;
            controller = admissionController;
            nativeController = nativeAdmissionController;
            operationTracker = nativeOperationTracker;
            manager = arrowWriterManager;
            executor = conversionExecutor;
        }

        Throwable admissionFailure = null;
        if (operationTracker != null) {
            try {
                operationTracker.close();
            } catch (Throwable closeFailure) {
                admissionFailure = closeFailure;
            }
        }
        if (controller != null) {
            try {
                controller.close();
            } catch (Throwable closeFailure) {
                admissionFailure = closeFailure;
            }
        }
        if (nativeController != null) {
            try {
                nativeController.close();
            } catch (Throwable closeFailure) {
                admissionFailure = ExceptionUtils.firstOrSuppressed(closeFailure, admissionFailure);
            }
        }

        CompletableFuture<Void> managerCloseFuture;
        try {
            managerCloseFuture =
                    manager == null
                            ? CompletableFuture.completedFuture(null)
                            : checkNotNull(
                                    manager.closeAsync(), "Arrow writer manager close future");
        } catch (Throwable closeFailure) {
            managerCloseFuture = new CompletableFuture<>();
            managerCloseFuture.completeExceptionally(closeFailure);
        }

        if (executor != null) {
            cancelQueuedConversionTasks(executor);
        }
        CompletableFuture<Void> executorCloseFuture =
                executor == null
                        ? CompletableFuture.completedFuture(null)
                        : ExecutorUtils.nonBlockingShutdown(30, TimeUnit.SECONDS, executor);
        CompletableFuture<Void> resourcesCloseFuture =
                CompletableFuture.allOf(managerCloseFuture, executorCloseFuture);

        final Throwable finalAdmissionFailure = admissionFailure;
        resourcesCloseFuture.whenComplete(
                (ignored, resourcesFailure) -> {
                    Throwable failure = finalAdmissionFailure;
                    if (resourcesFailure != null) {
                        failure = ExceptionUtils.firstOrSuppressed(resourcesFailure, failure);
                    }
                    if (failure == null) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(failure);
                    }
                });
        return result;
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        // Admission limits are startup configuration in this milestone. Cluster-wide dynamic
        // admission updates require coordinator-side validation and are intentionally deferred.
        plainSaslServerConfigManager.reconfigure(newConfig);
    }

    @VisibleForTesting
    KafkaRequestAdmissionController getAdmissionControllerForTesting() {
        return admissionController;
    }

    @VisibleForTesting
    KafkaNativeProduceAdmissionController getNativeAdmissionControllerForTesting() {
        return nativeAdmissionController;
    }

    @VisibleForTesting
    KafkaNativeProduceOperationTracker getNativeOperationTrackerForTesting() {
        return nativeOperationTracker;
    }

    @VisibleForTesting
    ExecutorService getConversionExecutorForTesting() {
        return conversionExecutor;
    }

    @VisibleForTesting
    KafkaArrowWriterManager getArrowWriterManagerForTesting() {
        return arrowWriterManager;
    }

    private static void validateKafkaAuthenticationConfiguration(Configuration configuration) {
        Map<String, String> protocolMap =
                configuration.get(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP);
        List<String> kafkaListeners = configuration.get(ConfigOptions.KAFKA_LISTENER_NAMES);
        boolean saslEnabled = false;
        for (String listenerName : kafkaListeners) {
            String protocol = protocolMap.get(listenerName);
            if (protocol == null) {
                continue;
            }
            if (PLAINTEXT_AUTH_PROTOCOL.equalsIgnoreCase(protocol)) {
                continue;
            }
            if (!SASL_AUTH_PROTOCOL.equalsIgnoreCase(protocol)) {
                throw new ConfigException(
                        String.format(
                                "Kafka listener '%s' supports only PLAINTEXT or SASL authentication, but '%s' is configured.",
                                listenerName, protocol));
            }
            saslEnabled = true;
        }
        if (!saslEnabled) {
            return;
        }

        List<String> mechanisms =
                configuration.get(ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG);
        if (mechanisms == null
                || !mechanisms.stream()
                        .anyMatch(mechanism -> "PLAIN".equalsIgnoreCase(mechanism))) {
            throw new ConfigException(
                    "Kafka SASL listeners require PLAIN in security.sasl.enabled.mechanisms.");
        }
    }

    private static Set<String> saslListenerNames(Configuration configuration) {
        Map<String, String> protocolMap =
                configuration.get(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP);
        Set<String> listenerNames = new HashSet<>();
        for (String listenerName : configuration.get(ConfigOptions.KAFKA_LISTENER_NAMES)) {
            if (SASL_AUTH_PROTOCOL.equalsIgnoreCase(protocolMap.get(listenerName))) {
                listenerNames.add(listenerName);
            }
        }
        return Collections.unmodifiableSet(listenerNames);
    }

    private static KafkaArrowWriterManager createArrowWriterManager(
            Configuration configuration, KafkaProduceMetrics produceMetrics) {
        int maxBatchSizeBytes =
                (int) Math.min(maxNativeAdmissionBytesPerRequest(configuration), Integer.MAX_VALUE);
        return new KafkaArrowWriterManager(
                configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_ALLOCATOR_MEMORY).getBytes(),
                configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_MAX_CONCURRENT_WRITERS),
                configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_WRITER_CACHE_MAX_SCHEMA_KEYS),
                configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_ACQUIRE_TIMEOUT),
                maxBatchSizeBytes,
                produceMetrics);
    }

    private static long maxNativeAdmissionBytesPerRequest(Configuration configuration) {
        return Math.min(
                configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES)
                        .getBytes(),
                configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES_PER_CONNECTION)
                        .getBytes());
    }

    private static KafkaRequestAdmissionController createAdmissionController(
            Configuration configuration, KafkaProduceMetrics produceMetrics) {
        int maxPendingReservations =
                configuration.get(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_PENDING_RESERVATIONS);
        KafkaProduceAdmissionController produceController =
                new KafkaProduceAdmissionController(
                        configuration.get(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS),
                        configuration
                                .get(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES)
                                .getBytes(),
                        configuration.get(
                                ConfigOptions
                                        .KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION),
                        configuration
                                .get(
                                        ConfigOptions
                                                .KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION)
                                .getBytes(),
                        maxPendingReservations);
        KafkaProduceAdmissionController controlController =
                new KafkaProduceAdmissionController(
                        configuration.get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS),
                        configuration
                                .get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES)
                                .getBytes(),
                        configuration.get(
                                ConfigOptions
                                        .KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION),
                        configuration
                                .get(
                                        ConfigOptions
                                                .KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION)
                                .getBytes(),
                        maxPendingReservations);
        return new KafkaRequestAdmissionController(
                produceController,
                controlController,
                configuration.get(ConfigOptions.KAFKA_CONNECTION_MAX_CONNECTIONS),
                configuration.get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES).getBytes(),
                produceMetrics::recordKafkaConnectionRejected);
    }

    private static KafkaNativeProduceAdmissionController createNativeAdmissionController(
            Configuration configuration) {
        return new KafkaNativeProduceAdmissionController(
                configuration.get(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS),
                configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES)
                        .getBytes(),
                configuration.get(
                        ConfigOptions
                                .KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS_PER_CONNECTION),
                configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES_PER_CONNECTION)
                        .getBytes(),
                configuration.get(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_PENDING_RESERVATIONS));
    }

    private static ExecutorService createConversionExecutor(Configuration configuration) {
        int maxInFlight =
                configuration.get(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS);
        int threads =
                Math.min(
                        maxInFlight,
                        configuration.get(
                                ConfigOptions.KAFKA_PRODUCE_ARROW_MAX_CONCURRENT_WRITERS));
        int queueCapacity = Math.min(maxInFlight, MAX_CONVERSION_QUEUE_CAPACITY);
        return new KafkaProduceConversionExecutor(threads, queueCapacity);
    }

    private static void cancelQueuedConversionTasks(ExecutorService executor) {
        if (executor instanceof KafkaProduceConversionExecutor) {
            ((KafkaProduceConversionExecutor) executor).shutdownAndCancelQueuedTasks();
        }
    }

    private static void registerAdmissionMetrics(
            KafkaProduceMetrics metrics, KafkaRequestAdmissionController controller) {
        KafkaProduceAdmissionController produceController = controller.produceAdmissionController();
        KafkaProduceAdmissionController controlController = controller.controlAdmissionController();
        metrics.registerAdmissionGauges(produceController);
        metrics.registerControlAdmissionGauges(
                controlController::pendingReservations,
                controlController::liveRequests,
                controlController::rawBytes,
                controlController::maxLiveRequestsLimit,
                controlController::maxRawBytesLimit,
                controlController::maxLiveRequestsPerConnectionLimit,
                controlController::maxRawBytesPerConnectionLimit);
        metrics.registerKafkaConnectionGauges(controller::connections, controller::connectionLimit);
    }

    private static void validateArrowConfiguration(Configuration configuration) {
        long maxRequestBytes =
                configuration.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes();
        if (maxRequestBytes < 6 || maxRequestBytes > Integer.MAX_VALUE) {
            throw new ConfigException(
                    ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.key()
                            + " must be between 6 bytes and "
                            + Integer.MAX_VALUE
                            + " bytes for the Kafka protocol.");
        }
        if (configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_ALLOCATOR_MEMORY).getBytes() <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_ARROW_ALLOCATOR_MEMORY.key()
                            + " must be greater than 0.");
        }
        if (configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_MAX_CONCURRENT_WRITERS) <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_ARROW_MAX_CONCURRENT_WRITERS.key()
                            + " must be greater than 0.");
        }
        if (configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_WRITER_CACHE_MAX_SCHEMA_KEYS)
                <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_ARROW_WRITER_CACHE_MAX_SCHEMA_KEYS.key()
                            + " must be greater than 0.");
        }
        if (configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_ACQUIRE_TIMEOUT).isZero()
                || configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_ARROW_ACQUIRE_TIMEOUT)
                        .isNegative()) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_ARROW_ACQUIRE_TIMEOUT.key()
                            + " must be greater than 0.");
        }
    }

    private static void validateAdmissionConfiguration(Configuration configuration) {
        int maxLiveRequests =
                configuration.get(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS);
        int maxLiveRequestsPerConnection =
                configuration.get(
                        ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION);
        long maxRawBytes =
                configuration.get(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES).getBytes();
        long maxRawBytesPerConnection =
                configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION)
                        .getBytes();
        long maxRequestBytes =
                configuration.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes();
        int maxPendingReservations =
                configuration.get(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_PENDING_RESERVATIONS);
        int maxControlLiveRequests =
                configuration.get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS);
        int maxControlLiveRequestsPerConnection =
                configuration.get(
                        ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION);
        long maxControlRawBytes =
                configuration.get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES).getBytes();
        long maxControlFrameBytes =
                configuration.get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES).getBytes();
        long maxControlRawBytesPerConnection =
                configuration
                        .get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION)
                        .getBytes();

        if (maxLiveRequests <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS.key()
                            + " must be greater than 0.");
        }
        if (maxLiveRequestsPerConnection <= 0 || maxLiveRequestsPerConnection > maxLiveRequests) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION.key()
                            + " must be greater than 0 and not exceed "
                            + ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS.key()
                            + ".");
        }
        if (maxRawBytes < maxRequestBytes) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES.key()
                            + " must be at least "
                            + ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.key()
                            + ".");
        }
        if (maxRawBytesPerConnection < maxRequestBytes || maxRawBytesPerConnection > maxRawBytes) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION.key()
                            + " must be at least "
                            + ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.key()
                            + " and not exceed "
                            + ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES.key()
                            + ".");
        }
        if (maxPendingReservations <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_PENDING_RESERVATIONS.key()
                            + " must be greater than 0.");
        }
        if (configuration.get(ConfigOptions.KAFKA_ADMISSION_PRE_FRAME_WAIT_TIMEOUT).toMillis()
                <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_ADMISSION_PRE_FRAME_WAIT_TIMEOUT.key()
                            + " must be at least one millisecond.");
        }
        if (configuration.get(ConfigOptions.KAFKA_ADMISSION_BODY_READ_TIMEOUT).toMillis() <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_ADMISSION_BODY_READ_TIMEOUT.key()
                            + " must be at least one millisecond.");
        }
        if (maxControlLiveRequests <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS.key()
                            + " must be greater than 0.");
        }
        if (maxControlLiveRequestsPerConnection <= 0
                || maxControlLiveRequestsPerConnection > maxControlLiveRequests) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION.key()
                            + " must be greater than 0 and not exceed "
                            + ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS.key()
                            + ".");
        }
        if (maxControlFrameBytes < 6 || maxControlFrameBytes > maxRequestBytes) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES.key()
                            + " must be between 6 bytes and "
                            + ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.key()
                            + ".");
        }
        if (maxControlRawBytes < maxControlFrameBytes) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES.key()
                            + " must be at least "
                            + ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES.key()
                            + ".");
        }
        if (maxControlRawBytesPerConnection < maxControlFrameBytes
                || maxControlRawBytesPerConnection > maxControlRawBytes) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION.key()
                            + " must be at least "
                            + ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES.key()
                            + " and not exceed "
                            + ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES.key()
                            + ".");
        }
        if (configuration.get(ConfigOptions.KAFKA_CONNECTION_MAX_CONNECTIONS) <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_CONNECTION_MAX_CONNECTIONS.key()
                            + " must be greater than 0.");
        }
    }

    private static void validateNativeAdmissionConfiguration(Configuration configuration) {
        int maxInFlight =
                configuration.get(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS);
        int maxInFlightPerConnection =
                configuration.get(
                        ConfigOptions
                                .KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS_PER_CONNECTION);
        long maxBytes =
                configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES)
                        .getBytes();
        long maxBytesPerConnection =
                configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES_PER_CONNECTION)
                        .getBytes();
        int maxPending =
                configuration.get(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_PENDING_RESERVATIONS);

        if (maxInFlight <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS.key()
                            + " must be greater than 0.");
        }
        if (maxInFlightPerConnection <= 0 || maxInFlightPerConnection > maxInFlight) {
            throw new ConfigException(
                    ConfigOptions
                                    .KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS_PER_CONNECTION
                                    .key()
                            + " must be greater than 0 and not exceed "
                            + ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS
                                    .key()
                            + ".");
        }
        if (maxBytes <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES.key()
                            + " must be greater than 0.");
        }
        if (maxBytesPerConnection <= 0 || maxBytesPerConnection > maxBytes) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES_PER_CONNECTION.key()
                            + " must be greater than 0 and not exceed "
                            + ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES.key()
                            + ".");
        }
        if (maxPending <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_PENDING_RESERVATIONS.key()
                            + " must be greater than 0.");
        }
        if (configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_ACQUIRE_TIMEOUT)
                        .toMillis()
                <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_ACQUIRE_TIMEOUT.key()
                            + " must be at least one millisecond.");
        }
        if (configuration
                        .get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_COMPLETION_GRACE_TIMEOUT)
                        .toMillis()
                <= 0) {
            throw new ConfigException(
                    ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_COMPLETION_GRACE_TIMEOUT.key()
                            + " must be at least one millisecond.");
        }
    }
}
