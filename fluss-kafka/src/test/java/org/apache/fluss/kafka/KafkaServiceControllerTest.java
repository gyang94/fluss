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
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Service switching preserves submitted ownership and permanently retires old connections. */
class KafkaServiceControllerTest {
    @Test
    void testDisabledConnectionsRemainReadableAndOldGenerationsStayRetired() {
        KafkaServiceController controller =
                new KafkaServiceController(false, Duration.ofSeconds(30));
        EmbeddedChannel old = new EmbeddedChannel(controller.newConnection());
        try {
            assertThat(old.isActive()).isTrue();
            assertThat(KafkaServiceController.connection(old).isServing()).isFalse();
            ByteBuf bytes = Unpooled.buffer().writeInt(42);
            assertThat(old.writeInbound(bytes)).isTrue();
            ByteBuf decoded = old.readInbound();
            assertThat(decoded.readInt()).isEqualTo(42);
            decoded.release();
            controller.setEnabled(true);
            EmbeddedChannel fresh = new EmbeddedChannel(controller.newConnection());
            try {
                assertThat(KafkaServiceController.connection(fresh).isServing()).isTrue();
                assertThat(KafkaServiceController.connection(old).isServing()).isFalse();
                controller.setEnabled(false);
                fresh.runPendingTasks();
                assertThat(fresh.isActive()).isTrue();
                assertThat(KafkaServiceController.connection(fresh).isServing()).isFalse();
                controller.setEnabled(true);
                assertThat(KafkaServiceController.connection(fresh).isServing()).isFalse();
                assertThat(controller.retiredConnections()).isEqualTo(2);
            } finally {
                fresh.finishAndReleaseAll();
            }
        } finally {
            old.finishAndReleaseAll();
            controller.close();
        }
        assertThat(controller.retiredConnections()).isZero();
    }

    @Test
    void testRejectsNonPositiveDrainTimeout() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_SERVICE_DRAIN_TIMEOUT, Duration.ZERO);
        assertThatThrownBy(() -> new KafkaProtocolPlugin().setup(config))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("kafka.service.drain-timeout");
    }

    @Test
    void testPluginCanToggleAndCannotReopenAfterFinalClose() {
        Configuration config = new Configuration();
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
        plugin.setup(config);
        try {
            for (boolean enabled : new boolean[] {true, false, true, false}) {
                config.set(ConfigOptions.KAFKA_ENABLED, enabled);
                plugin.validate(config);
                plugin.reconfigure(config);
                assertThat(plugin.getServiceControllerForTesting().isEnabled()).isEqualTo(enabled);
            }
            plugin.closeAsync().join();
            config.set(ConfigOptions.KAFKA_ENABLED, true);
            plugin.reconfigure(config);
            assertThat(plugin.getServiceControllerForTesting().isEnabled()).isFalse();
        } finally {
            plugin.closeAsync().join();
        }
    }
}
