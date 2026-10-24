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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.api.metadata.MetadataHandler;
import org.apache.fluss.kafka.api.produce.ProduceHandler;
import org.apache.fluss.kafka.api.versions.ApiVersionsHandler;
import org.apache.fluss.kafka.backend.metadata.GatewayKafkaMetadataBackend;
import org.apache.fluss.kafka.backend.produce.GatewayKafkaProduceBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceConversionExecutor;
import org.apache.fluss.kafka.dispatcher.KafkaApiRegistry;
import org.apache.fluss.kafka.dispatcher.KafkaRequestDispatcher;
import org.apache.fluss.kafka.error.KafkaErrorMapper;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.RequestType;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Entry point that dispatches Kafka protocol requests to registered API handlers. */
public class KafkaRequestHandler implements RequestHandler<KafkaRequest>, AutoCloseable {

    private final KafkaApiRegistry registry;
    private final KafkaRequestDispatcher dispatcher;
    private final KafkaRecordTranscoder transcoder;
    private final GatewayKafkaProduceBackend produceBackend;
    private final @Nullable KafkaProduceResources resources;
    private CompletableFuture<Void> closeFuture;

    /** Creates a Kafka request handler with the capabilities provided by a TabletServer. */
    public KafkaRequestHandler(Configuration conf, RpcGatewayService service) {
        this(conf, service, null);
    }

    /** Creates a handler with a plugin-owned executor for blocking Produce work. */
    public KafkaRequestHandler(
            Configuration conf,
            RpcGatewayService service,
            @Nullable KafkaProduceConversionExecutor conversionExecutor) {
        this(conf, service, conversionExecutor, new KafkaProduceResources(conf));
    }

    private KafkaRequestHandler(
            Configuration conf,
            RpcGatewayService service,
            @Nullable KafkaProduceConversionExecutor conversionExecutor,
            KafkaProduceResources resources) {
        this(conf, service, conversionExecutor, resources.createTranscoder(), resources);
    }

    KafkaRequestHandler(
            Configuration conf,
            RpcGatewayService service,
            @Nullable KafkaProduceConversionExecutor conversionExecutor,
            KafkaRecordTranscoder transcoder) {
        this(conf, service, conversionExecutor, transcoder, null);
    }

    private KafkaRequestHandler(
            Configuration conf,
            RpcGatewayService service,
            @Nullable KafkaProduceConversionExecutor conversionExecutor,
            KafkaRecordTranscoder transcoder,
            @Nullable KafkaProduceResources resources) {
        this.transcoder = checkNotNull(transcoder);
        this.resources = resources;
        KafkaTopicMapper topicMapper = new KafkaTopicMapper(checkNotNull(conf));
        this.produceBackend =
                new GatewayKafkaProduceBackend(
                        service,
                        (TabletServerGateway) service,
                        topicMapper,
                        transcoder,
                        conversionExecutor);
        this.registry =
                new KafkaApiRegistry(
                        Arrays.asList(
                                new ApiVersionsHandler(this::apiRegistry),
                                new MetadataHandler(
                                        new GatewayKafkaMetadataBackend(service, topicMapper),
                                        topicMapper),
                                new ProduceHandler(produceBackend)));
        this.dispatcher = new KafkaRequestDispatcher(registry, new KafkaErrorMapper());
    }

    @Override
    public RequestType requestType() {
        return RequestType.KAFKA;
    }

    @Override
    public void processRequest(KafkaRequest request) {
        dispatcher
                .dispatch(request)
                .whenComplete(
                        (response, failure) -> {
                            if (failure == null) {
                                request.complete(response);
                            } else {
                                request.fail(failure);
                            }
                        });
    }

    /** Stops new writes and releases conversion resources after native appends complete. */
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture == null) {
            closeFuture =
                    FutureUtils.runAfterwards(
                            produceBackend.closeAsync(),
                            () -> IOUtils.closeAll(transcoder, resources));
        }
        return closeFuture;
    }

    /** Waits for native appends before releasing conversion resources. */
    @Override
    public void close() throws Exception {
        closeAsync().get();
    }

    private KafkaApiRegistry apiRegistry() {
        return registry;
    }
}
