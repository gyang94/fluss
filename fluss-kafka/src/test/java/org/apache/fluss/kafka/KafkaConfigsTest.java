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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.ConfigException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Kafka configuration. */
public class KafkaConfigsTest {
    @Test
    public void testFromMap() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(ConfigOptions.KAFKA_ENABLED.key(), "true");
        map.put(ConfigOptions.KAFKA_LISTENER_NAMES.key(), "kafka,kafka_sasl");
        map.put(ConfigOptions.KAFKA_DATABASE.key(), "fluss");
        map.put(ConfigOptions.KAFKA_DEFAULT_KEY_FORMAT.key(), "string");
        map.put(ConfigOptions.KAFKA_DEFAULT_VALUE_FORMAT.key(), "string");
        Configuration configuration = Configuration.fromMap(map);

        assertThat(configuration.getBoolean(ConfigOptions.KAFKA_ENABLED)).isTrue();
        assertThat(configuration.get(ConfigOptions.KAFKA_LISTENER_NAMES))
                .isEqualTo(Arrays.asList("kafka", "kafka_sasl"));
        assertThat(configuration.getString(ConfigOptions.KAFKA_DATABASE)).isEqualTo("fluss");
        assertThat(configuration.getString(ConfigOptions.KAFKA_DEFAULT_KEY_FORMAT))
                .isEqualTo("string");
        assertThat(configuration.getString(ConfigOptions.KAFKA_DEFAULT_VALUE_FORMAT))
                .isEqualTo("string");
    }

    @Test
    public void testFromDefault() throws Exception {
        Configuration configuration = Configuration.fromMap(new HashMap<>());
        assertThat(configuration.getBoolean(ConfigOptions.KAFKA_ENABLED)).isFalse();
        assertThat(configuration.get(ConfigOptions.KAFKA_LISTENER_NAMES))
                .isEqualTo(Collections.singletonList("KAFKA"));
        assertThat(configuration.getString(ConfigOptions.KAFKA_DATABASE)).isEqualTo("kafka");
        assertThat(configuration.getString(ConfigOptions.KAFKA_DEFAULT_KEY_FORMAT))
                .isEqualTo("raw");
        assertThat(configuration.getString(ConfigOptions.KAFKA_DEFAULT_VALUE_FORMAT))
                .isEqualTo("raw");
        assertThat(configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_ALLOCATOR_MEMORY))
                .isEqualTo(MemorySize.parse("256mb"));
        assertThat(configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_MAX_CONCURRENT_WRITERS))
                .isEqualTo(8);
        assertThat(
                        configuration.get(
                                ConfigOptions.KAFKA_PRODUCE_ARROW_WRITER_CACHE_MAX_SCHEMA_KEYS))
                .isEqualTo(128);
        assertThat(configuration.get(ConfigOptions.KAFKA_PRODUCE_ARROW_ACQUIRE_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(30));
        assertThat(configuration.get(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS))
                .isEqualTo(1024);
        assertThat(configuration.get(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES))
                .isEqualTo(MemorySize.parse("512mb"));
        assertThat(
                        configuration.get(
                                ConfigOptions
                                        .KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION))
                .isEqualTo(64);
        assertThat(
                        configuration.get(
                                ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION))
                .isEqualTo(MemorySize.parse("128mb"));
        assertThat(
                        configuration.get(
                                ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_PENDING_RESERVATIONS))
                .isEqualTo(1024);
        assertThat(
                        configuration.get(
                                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS))
                .isEqualTo(256);
        assertThat(configuration.get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES))
                .isEqualTo(MemorySize.parse("512mb"));
        assertThat(
                        configuration.get(
                                ConfigOptions
                                        .KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS_PER_CONNECTION))
                .isEqualTo(32);
        assertThat(
                        configuration.get(
                                ConfigOptions
                                        .KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES_PER_CONNECTION))
                .isEqualTo(MemorySize.parse("128mb"));
        assertThat(
                        configuration.get(
                                ConfigOptions
                                        .KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_PENDING_RESERVATIONS))
                .isEqualTo(1024);
        assertThat(configuration.get(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_ACQUIRE_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(30));
        assertThat(
                        configuration.get(
                                ConfigOptions
                                        .KAFKA_PRODUCE_NATIVE_ADMISSION_COMPLETION_GRACE_TIMEOUT))
                .isEqualTo(Duration.ofMinutes(5));
        assertThat(configuration.get(ConfigOptions.KAFKA_ADMISSION_PRE_FRAME_WAIT_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(30));
        assertThat(configuration.get(ConfigOptions.KAFKA_ADMISSION_BODY_READ_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(30));
        assertThat(configuration.get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS))
                .isEqualTo(128);
        assertThat(configuration.get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES))
                .isEqualTo(MemorySize.parse("128mb"));
        assertThat(configuration.get(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES))
                .isEqualTo(MemorySize.parse("8mb"));
        assertThat(
                        configuration.get(
                                ConfigOptions
                                        .KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION))
                .isEqualTo(8);
        assertThat(
                        configuration.get(
                                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION))
                .isEqualTo(MemorySize.parse("100mb"));
        assertThat(configuration.get(ConfigOptions.KAFKA_CONNECTION_MAX_CONNECTIONS))
                .isEqualTo(10_000);
    }

    @Test
    public void testKafkaSaslPlainConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "sasl"));
        configuration.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList("PLAIN"));
        configuration.set(
                ConfigOptions.SERVER_SASL_CREDENTIALS,
                Collections.singletonMap("writer", "writer-secret"));

        new KafkaProtocolPlugin().setup(configuration);
    }

    @Test
    public void testKafkaSaslRequiresPlainMechanism() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "sasl"));
        configuration.set(
                ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList("SCRAM-SHA-256"));

        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(configuration))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("require PLAIN");
    }

    @Test
    public void testKafkaListenerRejectsNonSaslAuthenticationPlugin() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "custom"));

        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(configuration))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("supports only PLAINTEXT or SASL authentication");
    }

    @Test
    public void testKafkaListenerAcceptsExplicitPlaintextProtocol() {
        Configuration configuration = new Configuration();
        configuration.set(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP,
                Collections.singletonMap("KAFKA", "PLAINTEXT"));

        new KafkaProtocolPlugin().setup(configuration);
    }

    @Test
    public void testKafkaArrowConfigurationValidation() {
        Configuration invalidConcurrency = new Configuration();
        invalidConcurrency.set(ConfigOptions.KAFKA_PRODUCE_ARROW_MAX_CONCURRENT_WRITERS, 0);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidConcurrency))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_ARROW_MAX_CONCURRENT_WRITERS.key());

        Configuration invalidTimeout = new Configuration();
        invalidTimeout.set(ConfigOptions.KAFKA_PRODUCE_ARROW_ACQUIRE_TIMEOUT, Duration.ZERO);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidTimeout))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_PRODUCE_ARROW_ACQUIRE_TIMEOUT.key());

        Configuration invalidRequestSize = new Configuration();
        invalidRequestSize.set(
                ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE,
                new MemorySize((long) Integer.MAX_VALUE + 1L));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidRequestSize))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.key());

        Configuration probeTooSmall = new Configuration();
        probeTooSmall.set(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE, new MemorySize(5));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(probeTooSmall))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.key());
    }

    @Test
    public void testKafkaAdmissionConfigurationValidation() {
        Configuration invalidGlobalCount = new Configuration();
        invalidGlobalCount.set(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS, 0);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidGlobalCount))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS.key());

        Configuration invalidConnectionCount = new Configuration();
        invalidConnectionCount.set(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS, 10);
        invalidConnectionCount.set(
                ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION, 11);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidConnectionCount))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION
                                .key());

        Configuration invalidGlobalBytes = new Configuration();
        invalidGlobalBytes.set(
                ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES, MemorySize.parse("64mb"));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidGlobalBytes))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES.key());

        Configuration invalidConnectionBytes = new Configuration();
        invalidConnectionBytes.set(
                ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION,
                MemorySize.parse("64mb"));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidConnectionBytes))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION.key());

        Configuration invalidPendingReservations = new Configuration();
        invalidPendingReservations.set(
                ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_PENDING_RESERVATIONS, 0);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidPendingReservations))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_ADMISSION_MAX_PENDING_RESERVATIONS.key());

        Configuration invalidWaitTimeout = new Configuration();
        invalidWaitTimeout.set(ConfigOptions.KAFKA_ADMISSION_PRE_FRAME_WAIT_TIMEOUT, Duration.ZERO);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidWaitTimeout))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_ADMISSION_PRE_FRAME_WAIT_TIMEOUT.key());

        Configuration subMillisecondWaitTimeout = new Configuration();
        subMillisecondWaitTimeout.set(
                ConfigOptions.KAFKA_ADMISSION_PRE_FRAME_WAIT_TIMEOUT, Duration.ofNanos(1));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(subMillisecondWaitTimeout))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_ADMISSION_PRE_FRAME_WAIT_TIMEOUT.key());

        Configuration invalidBodyReadTimeout = new Configuration();
        invalidBodyReadTimeout.set(
                ConfigOptions.KAFKA_ADMISSION_BODY_READ_TIMEOUT, Duration.ofSeconds(-1));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidBodyReadTimeout))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_ADMISSION_BODY_READ_TIMEOUT.key());

        Configuration invalidControlCount = new Configuration();
        invalidControlCount.set(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS, 4);
        invalidControlCount.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION, 5);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidControlCount))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_LIVE_REQUESTS_PER_CONNECTION
                                .key());

        Configuration invalidControlGlobalBytes = new Configuration();
        invalidControlGlobalBytes.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES, MemorySize.parse("4mb"));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidControlGlobalBytes))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES.key());

        Configuration invalidControlConnectionFrameBytes = new Configuration();
        invalidControlConnectionFrameBytes.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION,
                MemorySize.parse("4mb"));
        assertThatThrownBy(
                        () -> new KafkaProtocolPlugin().setup(invalidControlConnectionFrameBytes))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION.key());

        Configuration invalidControlConnectionBytes = new Configuration();
        invalidControlConnectionBytes.set(
                ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE, MemorySize.parse("1mb"));
        invalidControlConnectionBytes.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES, MemorySize.parse("1mb"));
        invalidControlConnectionBytes.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES, MemorySize.parse("64mb"));
        invalidControlConnectionBytes.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION,
                MemorySize.parse("65mb"));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidControlConnectionBytes))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION.key());

        Configuration controlFrameTooSmall = new Configuration();
        controlFrameTooSmall.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES, new MemorySize(5));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(controlFrameTooSmall))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES.key());

        Configuration controlFrameExceedsNetty = new Configuration();
        controlFrameExceedsNetty.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES, MemorySize.parse("101mb"));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(controlFrameExceedsNetty))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES.key());

        Configuration exactMinimumControlFrame = new Configuration();
        exactMinimumControlFrame.set(
                ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE, new MemorySize(6));
        exactMinimumControlFrame.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_FRAME_BYTES, new MemorySize(6));
        exactMinimumControlFrame.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES, new MemorySize(6));
        exactMinimumControlFrame.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION,
                new MemorySize(6));
        new KafkaProtocolPlugin().setup(exactMinimumControlFrame);

        Configuration controlRawBudgetBelowNettyLimit = new Configuration();
        controlRawBudgetBelowNettyLimit.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES, MemorySize.parse("8mb"));
        controlRawBudgetBelowNettyLimit.set(
                ConfigOptions.KAFKA_CONTROL_ADMISSION_MAX_RAW_BYTES_PER_CONNECTION,
                MemorySize.parse("8mb"));
        new KafkaProtocolPlugin().setup(controlRawBudgetBelowNettyLimit);

        Configuration invalidMaxConnections = new Configuration();
        invalidMaxConnections.set(ConfigOptions.KAFKA_CONNECTION_MAX_CONNECTIONS, 0);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidMaxConnections))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_CONNECTION_MAX_CONNECTIONS.key());
    }

    @Test
    public void testKafkaNativeAdmissionConfigurationValidation() {
        Configuration invalidGlobalCount = new Configuration();
        invalidGlobalCount.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS, 0);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidGlobalCount))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS.key());

        Configuration invalidConnectionCount = new Configuration();
        invalidConnectionCount.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS, 2);
        invalidConnectionCount.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
                3);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidConnectionCount))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions
                                .KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_INFLIGHT_REQUESTS_PER_CONNECTION
                                .key());

        Configuration invalidGlobalBytes = new Configuration();
        invalidGlobalBytes.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES, new MemorySize(0));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidGlobalBytes))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES.key());

        Configuration invalidConnectionBytes = new Configuration();
        invalidConnectionBytes.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES, MemorySize.parse("1mb"));
        invalidConnectionBytes.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES_PER_CONNECTION,
                MemorySize.parse("2mb"));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidConnectionBytes))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_BYTES_PER_CONNECTION
                                .key());

        Configuration invalidPending = new Configuration();
        invalidPending.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_PENDING_RESERVATIONS, 0);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(invalidPending))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_MAX_PENDING_RESERVATIONS
                                .key());

        Configuration zeroAcquireTimeout = new Configuration();
        zeroAcquireTimeout.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_ACQUIRE_TIMEOUT, Duration.ZERO);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(zeroAcquireTimeout))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_ACQUIRE_TIMEOUT.key());

        Configuration subMillisecondAcquireTimeout = new Configuration();
        subMillisecondAcquireTimeout.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_ACQUIRE_TIMEOUT, Duration.ofNanos(1));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(subMillisecondAcquireTimeout))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_ACQUIRE_TIMEOUT.key());

        Configuration zeroCompletionTimeout = new Configuration();
        zeroCompletionTimeout.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_COMPLETION_GRACE_TIMEOUT,
                Duration.ZERO);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(zeroCompletionTimeout))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_COMPLETION_GRACE_TIMEOUT
                                .key());

        Configuration subMillisecondCompletionTimeout = new Configuration();
        subMillisecondCompletionTimeout.set(
                ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_COMPLETION_GRACE_TIMEOUT,
                Duration.ofNanos(1));
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(subMillisecondCompletionTimeout))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        ConfigOptions.KAFKA_PRODUCE_NATIVE_ADMISSION_COMPLETION_GRACE_TIMEOUT
                                .key());
    }
}
