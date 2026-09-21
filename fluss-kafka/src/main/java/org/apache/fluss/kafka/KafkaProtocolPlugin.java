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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceConversionExecutor;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServerConfigManager;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** The Kafka protocol plugin. */
public class KafkaProtocolPlugin implements NetworkProtocolPlugin, ServerReconfigurable {

    private static final String SASL_AUTH_PROTOCOL = "sasl";
    private static final String PLAINTEXT_AUTH_PROTOCOL = "plaintext";

    private Configuration conf;
    private KafkaServiceController serviceController;
    private KafkaProduceConversionExecutor conversionExecutor;
    private CompletableFuture<Void> closeFuture;
    private PlainSaslServerConfigManager plainSaslServerConfigManager;
    private Map<String, Supplier<ServerAuthenticator>> authenticatorSuppliers =
            Collections.emptyMap();
    private Set<String> saslListenerNames = Collections.emptySet();

    @Override
    public String name() {
        return KAFKA_PROTOCOL_NAME;
    }

    @Override
    public void setup(Configuration conf) {
        validateKafkaAuthenticationConfiguration(conf);
        if (conf.get(ConfigOptions.KAFKA_SERVICE_DRAIN_TIMEOUT).isNegative()
                || conf.get(ConfigOptions.KAFKA_SERVICE_DRAIN_TIMEOUT).isZero()) {
            throw new ConfigException("kafka.service.drain-timeout must be positive.");
        }
        this.serviceController =
                new KafkaServiceController(
                        conf.get(ConfigOptions.KAFKA_ENABLED),
                        conf.get(ConfigOptions.KAFKA_SERVICE_DRAIN_TIMEOUT));
        this.saslListenerNames = saslListenerNames(conf);
        this.plainSaslServerConfigManager = new PlainSaslServerConfigManager(conf);
        this.conf = plainSaslServerConfigManager.getConfiguration();
        this.authenticatorSuppliers =
                AuthenticationFactory.loadServerAuthenticatorSuppliers(this.conf);
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
                serviceController);
    }

    @Override
    public synchronized RequestHandler<?> createRequestHandler(RpcGatewayService service) {
        if (!(service instanceof TabletServerGateway)) {
            throw new IllegalArgumentException(
                    "Kafka protocol endpoints can only be enabled on TabletServers, but the service is "
                            + service.getClass().getSimpleName());
        }
        checkState(closeFuture == null, "Kafka protocol plugin has already been closed.");
        if (conversionExecutor == null) {
            // Mirror the RPC worker concurrency, with a separate queue capped at 1024 requests.
            int queueCapacity =
                    Math.max(
                            1,
                            Math.min(
                                    1024,
                                    conf.get(ConfigOptions.NETTY_SERVER_MAX_QUEUED_REQUESTS)));
            int threads =
                    Math.max(
                            1,
                            Math.min(
                                    queueCapacity,
                                    conf.get(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS)));
            conversionExecutor = new KafkaProduceConversionExecutor(threads, queueCapacity);
        }
        TabletServerGateway gateway = (TabletServerGateway) service;
        return new KafkaRequestHandler(service, gateway, conversionExecutor);
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture == null) {
            serviceController.close();
            closeFuture =
                    conversionExecutor == null
                            ? CompletableFuture.completedFuture(null)
                            : conversionExecutor.closeAsync();
        }
        return closeFuture;
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        validateKafkaAuthenticationConfiguration(newConfig);
        plainSaslServerConfigManager.validate(newConfig);
    }

    @Override
    public void validate(Configuration newConfig, @Nullable FlussPrincipal requester)
            throws ConfigException {
        validateKafkaAuthenticationConfiguration(newConfig);
        plainSaslServerConfigManager.validate(newConfig, requester);
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        plainSaslServerConfigManager.reconfigure(newConfig);
        serviceController.setEnabled(newConfig.get(ConfigOptions.KAFKA_ENABLED));
    }

    KafkaServiceController getServiceControllerForTesting() {
        return serviceController;
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
}
