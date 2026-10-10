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
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;

import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Protocol response tests for the Kafka SASL handlers. */
public class SaslHandlersTest {

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testHandshakeSupportsV0AndV1WithPlain(short version) {
        SaslHandshakeHandler handler = new SaslHandshakeHandler();
        assertThat(handler.apiSpec().minVersion()).isZero();
        assertThat(handler.apiSpec().maxVersion()).isEqualTo((short) 1);

        KafkaSaslConnection connection = KafkaSaslConnection.sasl(TestingServerAuthenticator::new);
        SaslHandshakeResponse response =
                handler.handle(
                                connection,
                                "KAFKA",
                                new InetSocketAddress("127.0.0.1", 9092),
                                handshake("PLAIN", version))
                        .join();

        response = roundTrip(response, ApiKeys.SASL_HANDSHAKE, version);

        assertThat(Errors.forCode(response.data().errorCode())).isEqualTo(Errors.NONE);
        assertThat(response.data().mechanisms()).containsExactly("PLAIN");
        assertThat(connection.isAuthenticating()).isTrue();
        assertThat(connection.isAuthenticatingWithRawTokens()).isEqualTo(version == 0);
        connection.close();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testUnsupportedHandshakeMechanismClosesAfterErrorResponse(short version) {
        SaslHandshakeHandler handler = new SaslHandshakeHandler();
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(TestingServerAuthenticator::new);
        AtomicBoolean closeAfterResponse = new AtomicBoolean();

        SaslHandshakeResponse response =
                handler.handle(
                                connection,
                                "KAFKA",
                                new InetSocketAddress("127.0.0.1", 9092),
                                handshake("SCRAM-SHA-256", version),
                                () -> closeAfterResponse.set(true))
                        .join();

        response = roundTrip(response, ApiKeys.SASL_HANDSHAKE, version);

        assertThat(Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.UNSUPPORTED_SASL_MECHANISM);
        assertThat(response.data().mechanisms()).containsExactly("PLAIN");
        assertThat(connection.shouldClose()).isTrue();
        assertThat(closeAfterResponse).isTrue();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testHandshakeOnPlaintextConnectionReturnsIllegalState(short version) {
        SaslHandshakeHandler handler = new SaslHandshakeHandler();
        KafkaSaslConnection connection = KafkaSaslConnection.plaintext();
        AtomicBoolean closeAfterResponse = new AtomicBoolean();

        SaslHandshakeResponse response =
                handler.handle(
                                connection,
                                "KAFKA",
                                new InetSocketAddress("127.0.0.1", 9092),
                                handshake("PLAIN", version),
                                () -> closeAfterResponse.set(true))
                        .join();

        response = roundTrip(response, ApiKeys.SASL_HANDSHAKE, version);

        assertThat(Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.ILLEGAL_SASL_STATE);
        assertThat(connection.shouldClose()).isTrue();
        assertThat(closeAfterResponse).isTrue();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testRepeatedHandshakeReturnsIllegalState(short version) {
        SaslHandshakeHandler handler = new SaslHandshakeHandler();
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(TestingServerAuthenticator::new);
        InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.1", 9092);
        handler.handle(connection, "KAFKA", remoteAddress, handshake("PLAIN", version)).join();
        AtomicBoolean closeAfterResponse = new AtomicBoolean();

        SaslHandshakeResponse response =
                handler.handle(
                                connection,
                                "KAFKA",
                                remoteAddress,
                                handshake("PLAIN", version),
                                () -> closeAfterResponse.set(true))
                        .join();

        response = roundTrip(response, ApiKeys.SASL_HANDSHAKE, version);

        assertThat(Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.ILLEGAL_SASL_STATE);
        assertThat(connection.shouldClose()).isTrue();
        assertThat(closeAfterResponse).isTrue();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2})
    public void testAuthenticateSupportsV0ThroughV2AndReturnsPrincipal(short version) {
        SaslAuthenticateHandler handler = new SaslAuthenticateHandler();
        assertThat(handler.apiSpec().minVersion()).isZero();
        assertThat(handler.apiSpec().maxVersion()).isEqualTo((short) 2);

        KafkaSaslConnection connection = authenticatedHandshakeConnection();
        SaslAuthenticateResponse response =
                handler.handle(connection, authenticate("valid-token", version)).join();

        response = roundTrip(response, ApiKeys.SASL_AUTHENTICATE, version);

        assertThat(Errors.forCode(response.data().errorCode())).isEqualTo(Errors.NONE);
        assertThat(response.data().errorMessage()).isNull();
        assertThat(response.data().authBytes()).isEmpty();
        assertThat(response.data().sessionLifetimeMs()).isZero();
        assertThat(connection.isReady()).isTrue();
        assertThat(connection.principal()).isEqualTo(new FlussPrincipal("alice", "User"));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2})
    public void testFramedAuthenticateAfterV0HandshakeIsRejected(short version) {
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(TestingServerAuthenticator::new);
        new SaslHandshakeHandler()
                .handle(connection, "KAFKA", null, handshake("PLAIN", (short) 0))
                .join();
        AtomicBoolean closeAfterResponse = new AtomicBoolean();

        SaslAuthenticateResponse response =
                new SaslAuthenticateHandler()
                        .handle(
                                connection,
                                authenticate("valid-token", version),
                                () -> closeAfterResponse.set(true))
                        .join();

        response = roundTrip(response, ApiKeys.SASL_AUTHENTICATE, version);

        assertThat(Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.ILLEGAL_SASL_STATE);
        assertThat(connection.shouldClose()).isTrue();
        assertThat(connection.isAuthenticatingWithRawTokens()).isFalse();
        assertThat(closeAfterResponse).isTrue();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2})
    public void testBadCredentialsReturnSafeFailureAndCloseAfterResponse(short version) {
        SaslAuthenticateHandler handler = new SaslAuthenticateHandler();
        KafkaSaslConnection connection = authenticatedHandshakeConnection();
        AtomicBoolean closeAfterResponse = new AtomicBoolean();

        SaslAuthenticateResponse response =
                handler.handle(
                                connection,
                                authenticate("secret-value", version),
                                () -> closeAfterResponse.set(true))
                        .join();

        response = roundTrip(response, ApiKeys.SASL_AUTHENTICATE, version);

        assertThat(Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED);
        assertThat(response.data().errorMessage())
                .doesNotContain("secret-value")
                .contains("invalid credentials");
        assertThat(response.data().authBytes()).isEmpty();
        assertThat(response.data().sessionLifetimeMs()).isZero();
        assertThat(connection.shouldClose()).isTrue();
        assertThat(closeAfterResponse).isTrue();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2})
    public void testAuthenticateBeforeHandshakeReturnsIllegalState(short version) {
        SaslAuthenticateHandler handler = new SaslAuthenticateHandler();
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(TestingServerAuthenticator::new);
        AtomicBoolean closeAfterResponse = new AtomicBoolean();

        SaslAuthenticateResponse response =
                handler.handle(
                                connection,
                                authenticate("valid-token", version),
                                () -> closeAfterResponse.set(true))
                        .join();

        response = roundTrip(response, ApiKeys.SASL_AUTHENTICATE, version);

        assertThat(Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.ILLEGAL_SASL_STATE);
        assertThat(connection.shouldClose()).isTrue();
        assertThat(closeAfterResponse).isTrue();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2})
    public void testAuthenticateAfterCompletionReturnsIllegalState(short version) {
        SaslAuthenticateHandler handler = new SaslAuthenticateHandler();
        KafkaSaslConnection connection = authenticatedHandshakeConnection();
        handler.handle(connection, authenticate("valid-token", version)).join();
        AtomicBoolean closeAfterResponse = new AtomicBoolean();

        SaslAuthenticateResponse response =
                handler.handle(
                                connection,
                                authenticate("valid-token", version),
                                () -> closeAfterResponse.set(true))
                        .join();

        response = roundTrip(response, ApiKeys.SASL_AUTHENTICATE, version);

        assertThat(Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.ILLEGAL_SASL_STATE);
        assertThat(connection.shouldClose()).isTrue();
        assertThat(closeAfterResponse).isTrue();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testHandshakeInitializationFailureClosesAuthenticator(short version)
            throws Exception {
        ServerAuthenticator authenticator = mock(ServerAuthenticator.class);
        doThrow(new AuthenticationException("internal initialization details"))
                .when(authenticator)
                .initialize(any());
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        AtomicBoolean closeAfterResponse = new AtomicBoolean();

        SaslHandshakeResponse response =
                new SaslHandshakeHandler()
                        .handle(
                                connection,
                                "KAFKA",
                                new InetSocketAddress("127.0.0.1", 9092),
                                handshake("PLAIN", version),
                                () -> closeAfterResponse.set(true))
                        .join();
        response = roundTrip(response, ApiKeys.SASL_HANDSHAKE, version);

        assertThat(Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED);
        assertThat(response.data().mechanisms()).containsExactly("PLAIN");
        assertThat(connection.isAuthenticating()).isFalse();
        assertThat(connection.principal()).isEqualTo(FlussPrincipal.ANONYMOUS);
        assertThat(connection.shouldClose()).isTrue();
        assertThat(closeAfterResponse).isTrue();
        verify(authenticator).close();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2})
    public void testAuthenticateOnPlaintextConnectionReturnsIllegalState(short version) {
        KafkaSaslConnection connection = KafkaSaslConnection.plaintext();
        AtomicBoolean closeAfterResponse = new AtomicBoolean();

        SaslAuthenticateResponse response =
                new SaslAuthenticateHandler()
                        .handle(
                                connection,
                                authenticate("valid-token", version),
                                () -> closeAfterResponse.set(true))
                        .join();
        response = roundTrip(response, ApiKeys.SASL_AUTHENTICATE, version);

        assertThat(Errors.forCode(response.data().errorCode()))
                .isEqualTo(Errors.ILLEGAL_SASL_STATE);
        assertThat(response.data().authBytes()).isEmpty();
        assertThat(connection.shouldClose()).isTrue();
        assertThat(closeAfterResponse).isTrue();
    }

    @SuppressWarnings("unchecked")
    private static <T extends AbstractResponse> T roundTrip(
            T response, ApiKeys apiKey, short version) {
        RequestHeader requestHeader = new RequestHeader(apiKey, version, "sasl-test", 37);
        ResponseHeader responseHeader = requestHeader.toResponseHeader();
        // Parse the bytes as a client would, including version-specific headers/correlation IDs.
        return (T)
                AbstractResponse.parseResponse(
                        RequestUtils.serialize(
                                responseHeader.data(),
                                responseHeader.headerVersion(),
                                response.data(),
                                version),
                        requestHeader);
    }

    private static KafkaSaslConnection authenticatedHandshakeConnection() {
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(TestingServerAuthenticator::new);
        connection.beginAuthentication("PLAIN", "KAFKA", new InetSocketAddress("127.0.0.1", 9092));
        return connection;
    }

    private static SaslHandshakeRequest handshake(String mechanism, short version) {
        return new SaslHandshakeRequest(
                new SaslHandshakeRequestData().setMechanism(mechanism), version);
    }

    private static SaslAuthenticateRequest authenticate(String token, short version) {
        return new SaslAuthenticateRequest(
                new SaslAuthenticateRequestData()
                        .setAuthBytes(token.getBytes(StandardCharsets.UTF_8)),
                version);
    }

    private static final class TestingServerAuthenticator implements ServerAuthenticator {
        private boolean completed;

        @Override
        public String protocol() {
            return "sasl";
        }

        @Override
        public byte[] evaluateResponse(byte[] token) {
            if (!java.util.Arrays.equals(token, "valid-token".getBytes(StandardCharsets.UTF_8))) {
                throw new AuthenticationException("Rejected token contents");
            }
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
