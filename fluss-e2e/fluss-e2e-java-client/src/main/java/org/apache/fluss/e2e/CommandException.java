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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Exception type used to report structured command failures. */
public class CommandException extends RuntimeException {

    private final String errorType;
    private final int exitCode;
    private final Map<String, Object> details;

    CommandException(String errorType, String message, int exitCode) {
        this(errorType, message, exitCode, Collections.emptyMap());
    }

    CommandException(String errorType, String message, int exitCode, Map<String, Object> details) {
        super(message);
        this.errorType = errorType;
        this.exitCode = exitCode;
        this.details = details == null ? Collections.emptyMap() : details;
    }

    public String errorType() {
        return errorType;
    }

    public int exitCode() {
        return exitCode;
    }

    public Map<String, Object> errorPayload() {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("type", errorType);
        error.put("message", getMessage());
        if (!details.isEmpty()) {
            error.put("details", details);
        }
        return error;
    }

    static CommandException invalidArgument(String message) {
        return new CommandException("InvalidArguments", message, 2);
    }

    static CommandException runtimeFailure(String message, Throwable cause) {
        Map<String, Object> details = new LinkedHashMap<>();
        if (cause != null) {
            details.put("cause", cause.getClass().getSimpleName());
        }
        return new CommandException("RuntimeFailure", message, 1, details);
    }
}
