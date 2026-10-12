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

package org.apache.fluss.kafka.matrix;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Transparent bootstrap proxy recording actual SASL request versions, never token contents. */
public final class SaslWireProbe implements AutoCloseable {
    private final ServerSocket listener;
    private final ExecutorService workers = Executors.newCachedThreadPool();
    private final List<Socket> sockets = Collections.synchronizedList(new ArrayList<>());
    private final List<String> events = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger sequence = new AtomicInteger();
    private final boolean sasl;

    /** Starts forwarding bootstrap connections; broker metadata still advertises real endpoints. */
    public SaslWireProbe(String host, int port, boolean sasl) throws IOException {
        this.sasl = sasl;
        listener = new ServerSocket(0, 50, InetAddress.getByName("localhost"));
        workers.submit(
                () -> {
                    while (!listener.isClosed()) {
                        try {
                            Socket client = listener.accept();
                            sockets.add(client);
                            Socket broker = new Socket(host, port);
                            sockets.add(broker);
                            int connection = sequence.incrementAndGet();
                            workers.submit(() -> forwardRequests(client, broker, connection));
                            workers.submit(() -> forwardResponses(broker, client));
                        } catch (IOException failure) {
                            if (!listener.isClosed()) {
                                events.add("ACCEPT_FAILED " + failure.getClass().getSimpleName());
                            }
                        }
                    }
                });
    }

    /** Returns the bootstrap address for the child JVM. */
    public String address() {
        return "localhost:" + listener.getLocalPort();
    }

    private void forwardRequests(Socket client, Socket broker, int connection) {
        boolean rawToken = false;
        try {
            DataInputStream input = new DataInputStream(client.getInputStream());
            DataOutputStream output = new DataOutputStream(broker.getOutputStream());
            while (!client.isClosed()) {
                int size = input.readInt();
                if (size < 0 || size > 16 * 1024 * 1024) {
                    throw new IOException("Invalid frame length " + size);
                }
                byte[] frame = new byte[size];
                input.readFully(frame);
                if (sasl && rawToken) {
                    events.add("connection=" + connection + " RAW_SASL_TOKEN bytes=" + size);
                    rawToken = false;
                } else if (sasl && size >= 8) {
                    ByteBuffer header = ByteBuffer.wrap(frame);
                    short api = header.getShort();
                    short version = header.getShort();
                    if (api == 17) {
                        events.add("connection=" + connection + " SASL_HANDSHAKE v=" + version);
                        rawToken = version == 0;
                    } else if (api == 36) {
                        events.add("connection=" + connection + " SASL_AUTHENTICATE v=" + version);
                    }
                }
                output.writeInt(size);
                output.write(frame);
                output.flush();
            }
        } catch (EOFException ignored) {
            // Clients close bootstrap connections once metadata supplies real broker addresses.
        } catch (IOException failure) {
            if (!listener.isClosed()) {
                events.add(
                        "connection="
                                + connection
                                + " FORWARD_CLOSED "
                                + failure.getClass().getSimpleName());
            }
        } finally {
            closeSocket(broker);
            closeSocket(client);
        }
    }

    private void forwardResponses(Socket broker, Socket client) {
        try {
            InputStream input = broker.getInputStream();
            OutputStream output = client.getOutputStream();
            byte[] buffer = new byte[8192];
            int length;
            while ((length = input.read(buffer)) >= 0) {
                output.write(buffer, 0, length);
                output.flush();
            }
        } catch (IOException ignored) {
            // Request-side errors and missing authentication evidence fail the owning test.
        } finally {
            closeSocket(client);
            closeSocket(broker);
        }
    }

    /** Checks wire evidence independently of the declared client release. */
    public void assertAuthentication() {
        if (sasl) {
            List<String> captured = events();
            assertThat(captured).anyMatch(event -> event.contains("SASL_HANDSHAKE v="));
            for (String handshake : captured) {
                if (handshake.contains("SASL_HANDSHAKE v=")) {
                    String connection = handshake.substring(0, handshake.indexOf(' '));
                    assertThat(captured)
                            .anyMatch(
                                    event ->
                                            event.startsWith(connection + " ")
                                                    && event.contains(
                                                            handshake.endsWith("v=0")
                                                                    ? "RAW_SASL_TOKEN"
                                                                    : "SASL_AUTHENTICATE v="));
                }
            }
        }
    }

    /** Returns non-sensitive wire observations for the per-mode, per-version artifact. */
    public List<String> events() {
        synchronized (events) {
            return new ArrayList<>(events);
        }
    }

    @Override
    public void close() throws Exception {
        listener.close();
        workers.shutdownNow();
        synchronized (sockets) {
            for (Socket socket : sockets) {
                closeSocket(socket);
            }
        }
        assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    private static void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // All remaining sockets are closed by the owning probe.
        }
    }
}
