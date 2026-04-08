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

package org.apache.fluss.e2e.perf.config;

import java.util.Locale;

/** Supported workload phase types. */
public enum PhaseType {
    WRITE("write"),
    LOOKUP("lookup"),
    PREFIX_LOOKUP("prefix-lookup"),
    SCAN("scan"),
    MIXED("mixed");

    private final String value;

    PhaseType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /** Parses a phase type string (case-insensitive). */
    public static PhaseType fromString(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Phase type must not be null");
        }
        String lower = s.toLowerCase(Locale.ROOT);
        for (PhaseType type : values()) {
            if (type.value.equals(lower)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown phase type: " + s);
    }
}
