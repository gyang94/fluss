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

package org.apache.fluss.kafka.dispatcher;

import org.apache.fluss.kafka.KafkaRequestContext;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaApiRegistry}. */
public class KafkaApiRegistryTest {

    @Test
    public void testRejectDuplicateApiKeys() {
        assertThatThrownBy(
                        () ->
                                new KafkaApiRegistry(
                                        Arrays.asList(
                                                new TestingApiHandler(ApiKeys.API_VERSIONS, true),
                                                new TestingApiHandler(
                                                        ApiKeys.API_VERSIONS, false))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already registered");
    }

    @Test
    public void testEmptyRegistry() {
        KafkaApiRegistry registry = new KafkaApiRegistry(Collections.emptyList());

        assertThat(registry.lookup(ApiKeys.API_VERSIONS)).isNull();
        assertThat(registry.advertisedApiSpecs()).isEmpty();
    }

    @Test
    public void testChangesToInputCollectionDoNotAffectRegistry() {
        TestingApiHandler handler = new TestingApiHandler(ApiKeys.API_VERSIONS, true);
        List<KafkaApiHandler<?>> handlers = new ArrayList<>();
        handlers.add(handler);
        KafkaApiRegistry registry = new KafkaApiRegistry(handlers);

        handlers.clear();
        handlers.add(new TestingApiHandler(ApiKeys.METADATA, true));

        assertThat(registry.lookup(ApiKeys.API_VERSIONS)).isSameAs(handler);
        assertThat(registry.lookup(ApiKeys.METADATA)).isNull();
        assertThat(registry.advertisedApiSpecs()).containsExactly(handler.apiSpec());
    }

    @Test
    public void testAdvertisedSpecsAreSortedAndUnmodifiable() {
        TestingApiHandler apiVersions = new TestingApiHandler(ApiKeys.API_VERSIONS, true);
        TestingApiHandler metadata = new TestingApiHandler(ApiKeys.METADATA, true);
        KafkaApiRegistry registry = new KafkaApiRegistry(Arrays.asList(apiVersions, metadata));

        List<KafkaApiSpec> specs = registry.advertisedApiSpecs();

        assertThat(specs).containsExactly(metadata.apiSpec(), apiVersions.apiSpec());
        assertThatThrownBy(specs::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThat(registry.advertisedApiSpecs())
                .containsExactly(metadata.apiSpec(), apiVersions.apiSpec());
    }

    @Test
    public void testOnlyAdvertiseEnabledHandlers() {
        KafkaApiRegistry registry =
                new KafkaApiRegistry(
                        Collections.singletonList(
                                new TestingApiHandler(ApiKeys.API_VERSIONS, true)));
        assertThat(registry.advertisedApiSpecs()).hasSize(1);

        KafkaApiRegistry hiddenRegistry =
                new KafkaApiRegistry(
                        Collections.singletonList(
                                new TestingApiHandler(ApiKeys.API_VERSIONS, false)));
        assertThat(hiddenRegistry.advertisedApiSpecs()).isEmpty();
        assertThat(hiddenRegistry.lookup(ApiKeys.API_VERSIONS)).isNull();
    }

    @Test
    public void testAdvertisedSpecIsSameSpecUsedForRouting() {
        TestingApiHandler handler = new TestingApiHandler(ApiKeys.API_VERSIONS, true);
        KafkaApiRegistry registry = new KafkaApiRegistry(Collections.singletonList(handler));

        KafkaApiSpec advertisedSpec = registry.advertisedApiSpecs().get(0);
        KafkaApiHandler<?> routedHandler = registry.lookup(ApiKeys.API_VERSIONS);

        assertThat(routedHandler).isSameAs(handler);
        assertThat(routedHandler.apiSpec()).isSameAs(advertisedSpec);
        for (short version : ApiKeys.API_VERSIONS.allVersions()) {
            assertThat(advertisedSpec.supportsVersion(version)).isTrue();
        }
        assertThat(
                        advertisedSpec.supportsVersion(
                                (short) (ApiKeys.API_VERSIONS.latestVersion() + 1)))
                .isFalse();
    }

    @Test
    public void testRejectInvalidVersionRange() {
        assertThatThrownBy(() -> new KafkaApiSpec(ApiKeys.API_VERSIONS, (short) 1, (short) 0, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new KafkaApiSpec(
                                        ApiKeys.API_VERSIONS,
                                        ApiKeys.API_VERSIONS.oldestVersion(),
                                        (short) (ApiKeys.API_VERSIONS.latestVersion() + 1),
                                        true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static final class TestingApiHandler implements KafkaApiHandler<AbstractRequest> {

        private final KafkaApiSpec spec;

        private TestingApiHandler(ApiKeys apiKey, boolean advertised) {
            this.spec =
                    new KafkaApiSpec(
                            apiKey, apiKey.oldestVersion(), apiKey.latestVersion(), advertised);
        }

        @Override
        public KafkaApiSpec apiSpec() {
            return spec;
        }

        @Override
        public CompletableFuture<? extends AbstractResponse> handle(
                KafkaRequestContext context, AbstractRequest request) {
            throw new UnsupportedOperationException();
        }
    }
}
