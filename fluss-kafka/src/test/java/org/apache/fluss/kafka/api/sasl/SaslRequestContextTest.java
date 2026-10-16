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

package org.apache.fluss.kafka.api.sasl;

import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.kafka.KafkaRequest;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests public SASL handlers with request-owned close flags and versioned wire responses. */
class SaslRequestContextTest {

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    void testInitializationFailureDefersChannelCloseUntilResponse(short version) throws Exception {
        ServerAuthenticator authenticator = mock(ServerAuthenticator.class);
        doThrow(sensitiveFailure()).when(authenticator).initialize(any());
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        SaslHandshakeRequest payload =
                new SaslHandshakeRequest(
                        new SaslHandshakeRequestData().setMechanism("PLAIN"), version);

        try (Fixture fixture = new Fixture(connection, payload)) {
            AbstractResponse response =
                    new SaslHandshakeHandler().handle(fixture.context(), payload).join();
            SaslHandshakeResponse decoded = (SaslHandshakeResponse) fixture.serialize(response);

            assertThat(decoded.error()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED);
            assertThat(decoded.data().mechanisms()).containsExactly("PLAIN");
            assertThat(connection.isAuthenticatingWithRawTokens()).isFalse();
            assertThat(connection.shouldClose()).isTrue();
            assertThat(connection.principal()).isEqualTo(FlussPrincipal.ANONYMOUS);
            fixture.assertChannelCloseDeferred();
            verify(authenticator).close();
            verify(authenticator, never()).evaluateResponse(any());
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2})
    void testAuthenticationFailureSanitizesCauseAndDefersChannelClose(short version)
            throws Exception {
        ServerAuthenticator authenticator = mock(ServerAuthenticator.class);
        when(authenticator.evaluateResponse(any())).thenThrow(sensitiveFailure());
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        connection.beginAuthentication("PLAIN", "KAFKA", null);
        SaslAuthenticateRequest payload =
                new SaslAuthenticateRequest(
                        new SaslAuthenticateRequestData()
                                .setAuthBytes(
                                        "request-token-secret".getBytes(StandardCharsets.UTF_8)),
                        version);

        try (Fixture fixture = new Fixture(connection, payload)) {
            AbstractResponse response =
                    new SaslAuthenticateHandler().handle(fixture.context(), payload).join();
            SaslAuthenticateResponse decoded =
                    (SaslAuthenticateResponse) fixture.serialize(response);

            assertThat(decoded.error()).isEqualTo(Errors.SASL_AUTHENTICATION_FAILED);
            assertThat(decoded.data().errorMessage())
                    .contains("invalid credentials")
                    .doesNotContain("password-secret", "cause-secret", "request-token-secret");
            assertThat(decoded.data().authBytes()).isEmpty();
            assertThat(connection.shouldClose()).isTrue();
            assertThat(connection.isReady()).isFalse();
            assertThat(connection.principal()).isEqualTo(FlussPrincipal.ANONYMOUS);
            fixture.assertChannelCloseDeferred();
            verify(authenticator).close();
            verify(authenticator, never()).createPrincipal();
        }
    }

    private static AuthenticationException sensitiveFailure() {
        return new AuthenticationException(
                "password-secret", new IllegalStateException("cause-secret"));
    }

    private static final class Fixture implements AutoCloseable {
        private final EmbeddedChannel channel =
                new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        private final KafkaRequest request;

        private Fixture(KafkaSaslConnection connection, AbstractRequest payload) {
            ByteBuf buffer = Unpooled.buffer(16);
            request =
                    new KafkaRequest(
                            payload.apiKey(),
                            payload.version(),
                            new RequestHeader(payload.apiKey(), payload.version(), "sasl-test", 41),
                            payload,
                            "KAFKA",
                            connection,
                            buffer,
                            channel.pipeline().firstContext(),
                            new CompletableFuture<>()) {};
            buffer.release();
        }

        private KafkaRequestContext context() {
            return KafkaRequestContext.fromRequest(request);
        }

        private AbstractResponse serialize(AbstractResponse response) {
            request.complete(response);
            ByteBuf wire = request.responseBuffer();
            try {
                return AbstractResponse.parseResponse(wire.nioBuffer(), request.header());
            } finally {
                wire.release();
            }
        }

        private void assertChannelCloseDeferred() {
            assertThat(request.shouldCloseConnectionAfterResponse()).isTrue();
            // The network handler owns flush/close; the API handler keeps the error serializable.
            assertThat(channel.isActive()).isTrue();
        }

        @Override
        public void close() {
            request.releaseBuffer();
            channel.finishAndReleaseAll();
        }
    }
}
