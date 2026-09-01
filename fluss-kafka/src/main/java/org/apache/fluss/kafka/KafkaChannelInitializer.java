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

import org.apache.fluss.kafka.admission.KafkaNativeProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaProduceFrameAdmission;
import org.apache.fluss.kafka.admission.KafkaRequestAdmissionController;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.network.KafkaAdmissionFrameDecoder;
import org.apache.fluss.kafka.network.KafkaFrameAdmission;
import org.apache.fluss.kafka.network.KafkaFrameReadPauser;
import org.apache.fluss.rpc.netty.NettyChannelInitializer;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseLease;
import org.apache.fluss.rpc.netty.server.RequestChannel.PauseReason;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;
import org.apache.fluss.shaded.netty4.io.netty.handler.flow.FlowControlHandler;
import org.apache.fluss.utils.MathUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * A {@link ChannelInitializer} for initializing {@link SocketChannel} instances that will be used
 * by the server to handle the Kafka requests for the client.
 */
public class KafkaChannelInitializer extends NettyChannelInitializer {

    private enum PreFramePauseReason implements PauseReason {
        ADMISSION_WAIT
    }

    private final RequestChannel[] requestChannels;
    private final String listenerName;
    private final int maxRequestSize;
    private final @Nullable Supplier<ServerAuthenticator> authenticatorSupplier;
    private final KafkaProduceMetrics produceMetrics;
    private final @Nullable KafkaRequestAdmissionController admissionController;
    private final @Nullable KafkaNativeProduceAdmissionController nativeAdmissionController;
    private final Duration preFrameWaitTimeout;
    private final Duration bodyReadTimeout;
    private final LengthFieldPrepender prepender = new LengthFieldPrepender(4);
    private final boolean preferHeap;

    /** Creates a PLAINTEXT channel initializer. */
    public KafkaChannelInitializer(
            RequestChannel[] requestChannels,
            String listenerName,
            long maxIdleTimeSeconds,
            int maxRequestSize,
            boolean preferHeap) {
        this(requestChannels, listenerName, maxIdleTimeSeconds, maxRequestSize, preferHeap, null);
    }

    /** Creates a channel initializer with an optional per-connection authenticator supplier. */
    public KafkaChannelInitializer(
            RequestChannel[] requestChannels,
            String listenerName,
            long maxIdleTimeSeconds,
            int maxRequestSize,
            boolean preferHeap,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier) {
        this(
                requestChannels,
                listenerName,
                maxIdleTimeSeconds,
                maxRequestSize,
                preferHeap,
                authenticatorSupplier,
                KafkaProduceMetrics.noOp());
    }

    /** Creates a channel initializer with authentication and Produce runtime metrics. */
    public KafkaChannelInitializer(
            RequestChannel[] requestChannels,
            String listenerName,
            long maxIdleTimeSeconds,
            int maxRequestSize,
            boolean preferHeap,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier,
            KafkaProduceMetrics produceMetrics) {
        this(
                requestChannels,
                listenerName,
                maxIdleTimeSeconds,
                maxRequestSize,
                preferHeap,
                authenticatorSupplier,
                produceMetrics,
                null,
                null,
                Duration.ofSeconds(30),
                Duration.ofSeconds(30));
    }

    /** Creates a channel initializer with metrics, admission control, and bounded frame stages. */
    public KafkaChannelInitializer(
            RequestChannel[] requestChannels,
            String listenerName,
            long maxIdleTimeSeconds,
            int maxRequestSize,
            boolean preferHeap,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier,
            KafkaProduceMetrics produceMetrics,
            @Nullable KafkaRequestAdmissionController admissionController,
            Duration preFrameWaitTimeout,
            Duration bodyReadTimeout) {
        this(
                requestChannels,
                listenerName,
                maxIdleTimeSeconds,
                maxRequestSize,
                preferHeap,
                authenticatorSupplier,
                produceMetrics,
                admissionController,
                null,
                preFrameWaitTimeout,
                bodyReadTimeout);
    }

    /** Creates a channel initializer with frame and converted/native Produce admission control. */
    public KafkaChannelInitializer(
            RequestChannel[] requestChannels,
            String listenerName,
            long maxIdleTimeSeconds,
            int maxRequestSize,
            boolean preferHeap,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier,
            KafkaProduceMetrics produceMetrics,
            @Nullable KafkaRequestAdmissionController admissionController,
            @Nullable KafkaNativeProduceAdmissionController nativeAdmissionController,
            Duration preFrameWaitTimeout,
            Duration bodyReadTimeout) {
        super(maxIdleTimeSeconds);
        this.requestChannels = requestChannels;
        this.listenerName = listenerName;
        this.maxRequestSize = maxRequestSize;
        this.preferHeap = preferHeap;
        this.authenticatorSupplier = authenticatorSupplier;
        this.produceMetrics = produceMetrics;
        this.admissionController = admissionController;
        this.nativeAdmissionController = nativeAdmissionController;
        this.preFrameWaitTimeout = preFrameWaitTimeout;
        this.bodyReadTimeout = bodyReadTimeout;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        super.initChannel(ch);
        // NettyLogger dumps full buffers at TRACE. A SASL/PLAIN frame contains the clear-text
        // credential token, so authenticated listeners must never install the payload logger.
        if (authenticatorSupplier != null && ch.pipeline().get("loggingHandler") != null) {
            ch.pipeline().remove("loggingHandler");
        }
        RequestChannel requestChannel = selectRequestChannel(ch);
        KafkaFrameAdmission frameAdmission =
                admissionController == null
                        ? new KafkaProduceFrameAdmission(requestChannel, null)
                        : admissionController.createConnectionAdmission(requestChannel);
        KafkaFrameReadPauser readPauser =
                channel -> {
                    PauseLease lease =
                            requestChannel.pauseChannel(
                                    channel, PreFramePauseReason.ADMISSION_WAIT);
                    return lease::close;
                };
        KafkaAdmissionFrameDecoder frameDecoder =
                new KafkaAdmissionFrameDecoder(
                        maxRequestSize,
                        preferHeap,
                        frameAdmission,
                        readPauser,
                        preFrameWaitTimeout,
                        bodyReadTimeout,
                        produceMetrics);
        ch.config()
                .setRecvByteBufAllocator(
                        frameDecoder.newRecvByteBufAllocator(
                                ch.config().getRecvByteBufAllocator()));
        ch.pipeline().addLast("frameReadGate", frameDecoder.newReadGate());
        addIdleStateHandler(ch);
        ch.pipeline().addLast(prepender);
        ch.pipeline().addLast("frameDecoder", frameDecoder);
        ch.pipeline().addLast("flowController", new FlowControlHandler());
        ch.pipeline()
                .addLast(
                        new KafkaCommandDecoder(
                                requestChannels,
                                listenerName,
                                authenticatorSupplier,
                                produceMetrics,
                                null,
                                nativeAdmissionController));
    }

    private RequestChannel selectRequestChannel(SocketChannel channel) {
        int channelIndex =
                MathUtils.murmurHash(channel.id().asLongText().hashCode()) % requestChannels.length;
        return requestChannels[channelIndex];
    }
}
