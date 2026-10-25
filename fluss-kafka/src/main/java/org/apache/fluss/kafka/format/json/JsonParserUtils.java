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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.DecimalNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.DoubleNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;

/** Reads JSON values only when their original structure must be retained. */
final class JsonParserUtils {
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
    private static final JsonNode NEGATIVE_ZERO = new NegativeZeroNode();

    private JsonParserUtils() {}

    static JsonNode readValue(JsonParser parser) throws IOException {
        if (parser.currentToken() == null) {
            throw new IOException("Unexpected end of JSON value.");
        }
        switch (parser.currentToken()) {
            case START_OBJECT:
                ObjectNode object = NODES.objectNode();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    requireField(parser);
                    String name = parser.currentName();
                    parser.nextToken();
                    object.set(name, readValue(parser));
                }
                return object;
            case START_ARRAY:
                ArrayNode array = NODES.arrayNode();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    array.add(readValue(parser));
                }
                return array;
            case VALUE_STRING:
                return NODES.textNode(parser.getText());
            case VALUE_NUMBER_INT:
                if ("-0".equals(parser.getText())) {
                    return NEGATIVE_ZERO;
                }
                switch (parser.getNumberType()) {
                    case INT:
                        return NODES.numberNode(parser.getIntValue());
                    case LONG:
                        return NODES.numberNode(parser.getLongValue());
                    default:
                        return NODES.numberNode(parser.getBigIntegerValue());
                }
            case VALUE_NUMBER_FLOAT:
                BigDecimal number = parser.getDecimalValue();
                return isNegativeZero(parser, number)
                        ? NEGATIVE_ZERO
                        : DecimalNode.valueOf(number.stripTrailingZeros());
            case VALUE_TRUE:
                return NODES.booleanNode(true);
            case VALUE_FALSE:
                return NODES.booleanNode(false);
            case VALUE_NULL:
                return NODES.nullNode();
            default:
                throw new IOException("Expected a JSON value.");
        }
    }

    static String scalarText(JsonParser parser) throws IOException {
        if (parser.currentToken() == JsonToken.START_OBJECT
                || parser.currentToken() == JsonToken.START_ARRAY) {
            return "";
        }
        if (parser.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
            BigDecimal number = parser.getDecimalValue();
            return isNegativeZero(parser, number) ? "-0" : number.stripTrailingZeros().toString();
        }
        return parser.getText();
    }

    static void skipValue(JsonParser parser) throws IOException {
        // Visit tokens instead of skipChildren so the decoder still validates ignored strings.
        int depth = 0;
        do {
            JsonToken token = parser.currentToken();
            if (token == null) {
                throw new IOException("Unexpected end of JSON value.");
            }
            if (token.isStructStart()) {
                depth++;
            } else if (token.isStructEnd()) {
                depth--;
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                parser.getDecimalValue();
            }
            if (depth > 0) {
                parser.nextToken();
            }
        } while (depth > 0);
    }

    static void requireField(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.FIELD_NAME) {
            throw new IOException("Expected a JSON object member.");
        }
    }

    private static boolean isNegativeZero(JsonParser parser, BigDecimal number) throws IOException {
        return number.signum() == 0 && parser.getText().charAt(0) == '-';
    }

    /** Keeps the existing textual and serialized representations of signed zero. */
    private static final class NegativeZeroNode extends DoubleNode {
        private NegativeZeroNode() {
            super(-0.0d);
        }

        @Override
        public String asText() {
            return "-0";
        }
    }
}
