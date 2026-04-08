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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** JSON helpers for the Java bridge. */
public final class JsonOutput {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonOutput() {}

    public static void print(Map<String, Object> payload) {
        print(payload, System.out);
    }

    static void print(Map<String, Object> payload, PrintStream output) {
        try {
            output.println(OBJECT_MAPPER.writeValueAsString(payload));
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize JSON payload.", e);
        }
    }

    public static Map<String, Object> success(String command) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("version", "1.0");
        payload.put("timestamp", Instant.now().toString());
        payload.put("command", command);
        payload.put("ok", true);
        return payload;
    }

    public static Map<String, Object> failure(String command, CommandException exception) {
        Map<String, Object> payload = success(command);
        payload.put("ok", false);
        payload.put("error", exception.errorPayload());
        return payload;
    }

    public static Map<String, Object> unexpectedFailure(String command, Throwable throwable) {
        CommandException exception =
                new CommandException(
                        "UnexpectedFailure",
                        throwable.getMessage() == null
                                ? throwable.getClass().getName()
                                : throwable.getMessage(),
                        1,
                        Map.of("cause", throwable.getClass().getName()));
        return failure(command, exception);
    }
}
