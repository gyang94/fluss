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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

/** Collects rescue values by their structural location, independently of bounded error paths. */
final class JsonRescue {
    private JsonRescue() {}

    @Nullable
    static Consumer<JsonNode> field(@Nullable Consumer<JsonNode> parent, String name) {
        return parent == null
                ? null
                : value -> {
                    ObjectNode result = JsonNodeFactory.instance.objectNode();
                    result.set(name, value);
                    parent.accept(result);
                };
    }

    /** Accumulates all rescued array elements in one array, avoiding quadratic copying. */
    static final class ArrayCollector {
        private final Consumer<JsonNode> parent;
        private final int size;
        @Nullable private ArrayNode result;

        ArrayCollector(Consumer<JsonNode> parent, int size) {
            this.parent = parent;
            this.size = size;
        }

        Consumer<JsonNode> element(int index) {
            return value -> {
                if (result == null) {
                    result = JsonNodeFactory.instance.arrayNode(size);
                    for (int i = 0; i < size; i++) {
                        result.addNull();
                    }
                }
                result.set(index, merge(result.get(index), value));
            };
        }

        void finish() {
            if (result != null) {
                parent.accept(result);
            }
        }
    }

    static JsonNode merge(JsonNode first, JsonNode second) {
        if (first == null || first.isNull()) {
            return second;
        }
        if (second == null || second.isNull()) {
            return first;
        }
        if (first.isObject() && second.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = second.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                ((ObjectNode) first)
                        .set(field.getKey(), merge(first.get(field.getKey()), field.getValue()));
            }
            return first;
        }
        if (first.isArray() && second.isArray()) {
            for (int i = 0; i < second.size(); i++) {
                ((ArrayNode) first).set(i, merge(first.get(i), second.get(i)));
            }
            return first;
        }
        return second;
    }
}
