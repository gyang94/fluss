/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.format.json;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

/** Parser lifecycle helpers for converter tests. */
final class JsonConverterTestUtils {
    private static final JsonFactory FACTORY = new JsonFactory();

    private JsonConverterTestUtils() {}

    static Object convertJson(JsonToFlussConverter converter, String json, String path)
            throws IOException {
        try (JsonParser parser = FACTORY.createParser(json)) {
            parser.nextToken();
            return converter.convert(parser, path);
        }
    }

    static Object convertNode(JsonToFlussConverter converter, JsonNode node, String path)
            throws IOException {
        try (JsonParser parser = node.traverse()) {
            parser.nextToken();
            return converter.convert(parser, path);
        }
    }
}
