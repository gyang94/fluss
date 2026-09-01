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

import org.apache.fluss.kafka.admission.KafkaProduceAdmissionController;
import org.apache.fluss.kafka.admission.KafkaRequestAdmissionController;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.network.KafkaAdmissionFrameDecoder;
import org.apache.fluss.kafka.network.KafkaFrameAdmission;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;
import org.apache.fluss.shaded.netty4.io.netty.handler.flow.FlowControlHandler;

import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests Kafka-framed SASL requests through the pre-frame production handler pipeline. */
class KafkaSaslAdmissionPipelineTest {

    private static final int MAX_FRAME_BYTES = 1 << 20;
    private static final byte[] AUTH_TOKEN = "valid-token".getBytes(StandardCharsets.UTF_8);

    @Test
    void testKafkaFramedSaslRequestsUseControlLane() throws Exception {
        RequestChannel requestChannel = new RequestChannel(100);
        KafkaProduceAdmissionController produceAdmission = admissionLane();
        KafkaProduceAdmissionController controlAdmission = admissionLane();
        KafkaRequestAdmissionController admissionController =
                new KafkaRequestAdmissionController(produceAdmission, controlAdmission, 10);
        KafkaFrameAdmission frameAdmission =
                admissionController.createConnectionAdmission(requestChannel);
        KafkaAdmissionFrameDecoder frameDecoder =
                new KafkaAdmissionFrameDecoder(
                        MAX_FRAME_BYTES,
                        true,
                        frameAdmission,
                        channel -> () -> {},
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(10),
                        KafkaProduceMetrics.noOp());
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        KafkaRequestHandler requestHandler = new KafkaRequestHandler(service, service, "kafka");
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        frameDecoder.newReadGate(),
                        new LengthFieldPrepender(4),
                        frameDecoder,
                        new FlowControlHandler(),
                        new KafkaCommandDecoder(
                                new RequestChannel[] {requestChannel},
                                "KAFKA",
                                TestingServerAuthenticator::new,
                                KafkaProduceMetrics.noOp(),
                                null));

        short handshakeVersion = 1;
        RequestHeader handshakeHeader =
                new RequestHeader(ApiKeys.SASL_HANDSHAKE, handshakeVersion, "client", 1);
        SaslHandshakeRequest handshakeRequest =
                new SaslHandshakeRequest(
                        new SaslHandshakeRequestData().setMechanism("PLAIN"), handshakeVersion);

        short authenticateVersion = 2;
        RequestHeader authenticateHeader =
                new RequestHeader(ApiKeys.SASL_AUTHENTICATE, authenticateVersion, "client", 2);
        SaslAuthenticateRequest authenticateRequest =
                new SaslAuthenticateRequest(
                        new SaslAuthenticateRequestData().setAuthBytes(AUTH_TOKEN),
                        authenticateVersion);

        try {
            KafkaRequest handshake =
                    writeFrameAndPoll(
                            channel,
                            requestChannel,
                            handshakeHeader,
                            handshakeRequest,
                            produceAdmission,
                            controlAdmission);
            assertThat(handshake.apiKey()).isEqualTo(ApiKeys.SASL_HANDSHAKE);
            processRequest(channel, requestHandler, handshake);
            assertThat(handshake.saslConnection().isAuthenticating()).isTrue();
            assertSuccessfulResponse(channel, handshakeHeader, SaslHandshakeResponse.class);
            assertLanesReleased(produceAdmission, controlAdmission);

            KafkaRequest authenticate =
                    writeFrameAndPoll(
                            channel,
                            requestChannel,
                            authenticateHeader,
                            authenticateRequest,
                            produceAdmission,
                            controlAdmission);
            assertThat(authenticate.apiKey()).isEqualTo(ApiKeys.SASL_AUTHENTICATE);
            processRequest(channel, requestHandler, authenticate);
            assertThat(authenticate.saslConnection().isReady()).isTrue();
            assertThat(authenticate.saslConnection().principal())
                    .isEqualTo(new FlussPrincipal("alice", "User"));
            assertSuccessfulResponse(channel, authenticateHeader, SaslAuthenticateResponse.class);
            assertLanesReleased(produceAdmission, controlAdmission);

            assertThat(produceAdmission.registeredConnections()).isZero();
            assertThat(controlAdmission.registeredConnections()).isOne();
        } finally {
            channel.close();
            channel.runPendingTasks();
            releaseQueuedRequests(requestChannel);
            channel.finishAndReleaseAll();
        }

        assertThat(admissionController.connections()).isZero();
        assertThat(produceAdmission.registeredConnections()).isZero();
        assertThat(controlAdmission.registeredConnections()).isZero();
    }

    private static KafkaRequest writeFrameAndPoll(
            EmbeddedChannel channel,
            RequestChannel requestChannel,
            RequestHeader header,
            AbstractRequest request,
            KafkaProduceAdmissionController produceAdmission,
            KafkaProduceAdmissionController controlAdmission) {
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        ByteBuf frame = Unpooled.buffer(Integer.BYTES + serialized.remaining());
        frame.writeInt(serialized.remaining());
        frame.writeBytes(serialized);
        int frameBytes = frame.readableBytes();
        ByteBuf probe = frame.readRetainedSlice(Integer.BYTES + Short.BYTES);
        ByteBuf body = frame.readRetainedSlice(frame.readableBytes());
        frame.release();

        channel.writeInbound(probe);
        channel.runPendingTasks();
        channel.writeInbound(body);
        channel.runPendingTasks();

        KafkaRequest admitted = (KafkaRequest) requestChannel.pollRequest(1000);
        assertThat(admitted).isNotNull();
        assertThat(produceAdmission.liveRequests()).isZero();
        assertThat(produceAdmission.rawBytes()).isZero();
        assertThat(produceAdmission.registeredConnections()).isZero();
        assertThat(controlAdmission.liveRequests()).isOne();
        assertThat(controlAdmission.rawBytes()).isEqualTo(frameBytes);
        assertThat(controlAdmission.registeredConnections()).isOne();
        return admitted;
    }

    private static void processRequest(
            EmbeddedChannel channel, KafkaRequestHandler requestHandler, KafkaRequest request) {
        try {
            requestHandler.processRequest(request);
        } finally {
            request.releaseBuffer();
        }
        channel.runPendingTasks();
    }

    private static void assertSuccessfulResponse(
            EmbeddedChannel channel,
            RequestHeader requestHeader,
            Class<? extends AbstractResponse> expectedType) {
        ByteBuf responseLength = channel.readOutbound();
        ByteBuf responsePayload = channel.readOutbound();
        try {
            assertThat(responseLength).isNotNull();
            assertThat(responsePayload).isNotNull();
            assertThat(responseLength.readInt()).isEqualTo(responsePayload.readableBytes());
            AbstractResponse response =
                    AbstractResponse.parseResponse(responsePayload.nioBuffer(), requestHeader);
            assertThat(response).isInstanceOf(expectedType);
            assertThat(response.errorCounts())
                    .containsExactlyEntriesOf(java.util.Collections.singletonMap(Errors.NONE, 1));
        } finally {
            if (responseLength != null) {
                responseLength.release();
            }
            if (responsePayload != null) {
                responsePayload.release();
            }
        }
    }

    private static void assertLanesReleased(
            KafkaProduceAdmissionController produceAdmission,
            KafkaProduceAdmissionController controlAdmission) {
        assertThat(produceAdmission.liveRequests()).isZero();
        assertThat(produceAdmission.rawBytes()).isZero();
        assertThat(controlAdmission.liveRequests()).isZero();
        assertThat(controlAdmission.rawBytes()).isZero();
    }

    private static void releaseQueuedRequests(RequestChannel requestChannel) {
        org.apache.fluss.rpc.netty.server.RpcRequest request;
        while ((request = requestChannel.pollRequest(0)) != null) {
            request.releaseBuffer();
        }
    }

    private static KafkaProduceAdmissionController admissionLane() {
        return new KafkaProduceAdmissionController(10, MAX_FRAME_BYTES, 10, MAX_FRAME_BYTES);
    }

    private static final class TestingServerAuthenticator implements ServerAuthenticator {
        private boolean completed;

        @Override
        public String protocol() {
            return "sasl";
        }

        @Override
        public byte[] evaluateResponse(byte[] token) {
            assertThat(token).containsExactly(AUTH_TOKEN);
            completed = true;
            return new byte[0];
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        @Override
        public FlussPrincipal createPrincipal() {
            return new FlussPrincipal("alice", "User");
        }
    }
}
