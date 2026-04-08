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

package org.apache.fluss.e2e;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Shared client context for Java bridge commands. */
public final class E2eClientContext implements AutoCloseable {

    private final Configuration configuration;
    private final Connection connection;
    private final Admin admin;

    private E2eClientContext(Configuration configuration, Connection connection, Admin admin) {
        this.configuration = configuration;
        this.connection = connection;
        this.admin = admin;
    }

    public static E2eClientContext open(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw CommandException.invalidArgument(
                    "Missing bootstrap servers. Provide --bootstrap-servers or FLUSS_BOOTSTRAP_SERVERS.");
        }

        List<String> endpoints =
                Arrays.stream(bootstrapServers.split(","))
                        .map(String::trim)
                        .filter(value -> !value.isEmpty())
                        .collect(Collectors.toList());
        if (endpoints.isEmpty()) {
            throw CommandException.invalidArgument(
                    "No valid bootstrap server endpoints were provided.");
        }

        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.BOOTSTRAP_SERVERS, endpoints);
        Connection connection = ConnectionFactory.createConnection(configuration);
        return new E2eClientContext(configuration, connection, connection.getAdmin());
    }

    public Configuration configuration() {
        return configuration;
    }

    public Connection connection() {
        return connection;
    }

    public Admin admin() {
        return admin;
    }

    @Override
    public void close() throws Exception {
        try {
            admin.close();
        } finally {
            connection.close();
        }
    }
}
