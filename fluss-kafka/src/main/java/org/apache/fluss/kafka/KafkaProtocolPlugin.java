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
import org.apache.fluss.kafka.backend.produce.KafkaProduceConversionExecutor;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkState;

/** The Kafka protocol plugin. */
public class KafkaProtocolPlugin implements NetworkProtocolPlugin {

    private final Supplier<KafkaRecordTranscoder> transcoderFactory;
    private final List<KafkaRequestHandler> requestHandlers = new ArrayList<>();
    private Configuration conf;
    private KafkaProduceResources resources;
    private KafkaProduceConversionExecutor conversionExecutor;
    private CompletableFuture<Void> closeFuture;

    /** Creates a plugin that owns the conversion resources of its request handlers. */
    public KafkaProtocolPlugin() {
        this(null);
    }

    @VisibleForTesting
    KafkaProtocolPlugin(Supplier<KafkaRecordTranscoder> transcoderFactory) {
        this.transcoderFactory = transcoderFactory;
    }

    @Override
    public String name() {
        return KAFKA_PROTOCOL_NAME;
    }

    @Override
    public void setup(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    @Override
    public List<String> listenerNames() {
        return conf.get(ConfigOptions.KAFKA_LISTENER_NAMES);
    }

    @Override
    public ChannelHandler createChannelHandler(
            RequestChannel[] requestChannels, String listenerName) {
        return new KafkaChannelInitializer(
                requestChannels,
                listenerName,
                conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds(),
                (int) conf.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes(),
                conf.getBoolean(ConfigOptions.NETTY_CLIENT_ALLOCATOR_HEAP_BUFFER_FIRST));
    }

    @Override
    public synchronized RequestHandler<?> createRequestHandler(RpcGatewayService service) {
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
        if (transcoderFactory == null && resources == null) {
            resources = new KafkaProduceResources(conf);
        }
        KafkaRecordTranscoder transcoder =
                transcoderFactory == null ? resources.createTranscoder() : transcoderFactory.get();
        try {
            KafkaRequestHandler handler =
                    new KafkaRequestHandler(conf, service, conversionExecutor, transcoder);
            requestHandlers.add(handler);
            return handler;
        } catch (RuntimeException | Error failure) {
            IOUtils.closeQuietly(transcoder);
            throw failure;
        }
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture == null) {
            CompletableFuture<Void> workersClosed =
                    conversionExecutor == null
                            ? CompletableFuture.completedFuture(null)
                            : conversionExecutor.closeAsync();
            // Handlers drain native acknowledgements before releasing writers and shared memory.
            List<AutoCloseable> resourcesToClose = new ArrayList<>(requestHandlers);
            resourcesToClose.add(resources);
            closeFuture =
                    FutureUtils.runAfterwardsAsync(
                            workersClosed, () -> IOUtils.closeAll(resourcesToClose));
        }
        return closeFuture;
    }
}
