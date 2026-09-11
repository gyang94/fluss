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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.schema.KafkaFieldProjection;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.DoubleNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.FloatNode;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** Decodes a strict UTF-8 JSON object according to the projected Fluss fields. */
@Internal
public final class JsonKafkaFieldDecoder implements KafkaFieldDecoder {
    static final int MAX_JSON_NESTING_DEPTH = 64;
    static final int MAX_CONTAINER_ELEMENTS = JsonToFlussConverters.MAX_CONTAINER_ELEMENTS;
    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private final KafkaFieldProjection projection;
    private final Map<String, Integer> projectedFields = new HashMap<>();
    private final JsonToFlussConverter[] converters;

    /** Creates a JSON decoder and validates every projected field type. */
    public JsonKafkaFieldDecoder(KafkaFieldProjection projection) {
        this.projection = projection;
        converters = new JsonToFlussConverter[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            if (projectedFields.put(projection.nameAt(i), i) != null) {
                throw new KafkaTopicSchemaException(
                        "Duplicate Kafka JSON field '" + projection.nameAt(i) + "'.");
            }
            converters[i] = JsonToFlussConverters.create(projection.dataTypeAt(i));
        }
    }

    @Override
    public Object[] decode(@Nullable byte[] bytes) {
        if (bytes == null) {
            return nullValues();
        }
        try (Reader reader =
                        new InputStreamReader(
                                new ByteArrayInputStream(bytes),
                                StandardCharsets.UTF_8
                                        .newDecoder()
                                        .onMalformedInput(CodingErrorAction.REPORT)
                                        .onUnmappableCharacter(CodingErrorAction.REPORT));
                JsonParser parser = OBJECT_MAPPER.getFactory().createParser(reader)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new KafkaRecordEncodingException(
                        "Kafka JSON record value must have an object root.");
            }
            Object[] values = new Object[projection.size()];
            boolean[] present = new boolean[projection.size()];
            int fieldCount = 0;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (++fieldCount > MAX_CONTAINER_ELEMENTS) {
                    throw new KafkaRecordEncodingException(
                            "Kafka JSON object exceeds the maximum field count of "
                                    + MAX_CONTAINER_ELEMENTS
                                    + ".");
                }
                String fieldName = parser.currentName();
                String path = JsonPath.field(JsonPath.ROOT, fieldName);
                Integer position = projectedFields.get(fieldName);
                if (position == null) {
                    throw new KafkaRecordEncodingException(
                            "Invalid Kafka record value at " + path + ": unknown field.");
                }
                parser.nextToken();
                JsonNode value = readValue(parser, projection.dataTypeAt(position), path);
                values[position] = converters[position].convert(value, path);
                present[position] = true;
            }
            if (parser.nextToken() != null) {
                throw new KafkaRecordEncodingException(
                        "Kafka record value is not valid strict UTF-8 JSON: trailing content.");
            }
            for (int i = 0; i < projection.size(); i++) {
                if (!present[i]) {
                    values[i] =
                            converters[i].convert(
                                    null, JsonPath.field(JsonPath.ROOT, projection.nameAt(i)));
                }
            }
            return values;
        } catch (KafkaRecordEncodingException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new KafkaRecordEncodingException(
                    "Kafka record value is not valid strict UTF-8 JSON.", e);
        }
    }

    private static JsonNode readValue(JsonParser parser, DataType dataType, String path)
            throws IOException {
        if (parser.currentToken() == JsonToken.START_OBJECT
                || parser.currentToken() == JsonToken.START_ARRAY) {
            switch (dataType.getTypeRoot()) {
                case ROW:
                case ARRAY:
                case MAP:
                    break;
                default:
                    throw new KafkaRecordEncodingException(
                            "Invalid Kafka record value at " + path + ": expected a JSON scalar.");
            }
        }
        if (parser.currentToken().isNumeric()) {
            // Parse floating-point targets from the original token: BigDecimal loses negative
            // zero, and parsing FLOAT through a double can round twice. Other numeric targets
            // retain exact Jackson integer/BigDecimal nodes and their strict conversion rules.
            switch (dataType.getTypeRoot()) {
                case FLOAT:
                    return FloatNode.valueOf(Float.parseFloat(parser.getText()));
                case DOUBLE:
                    return DoubleNode.valueOf(Double.parseDouble(parser.getText()));
                default:
                    break;
            }
        }
        return OBJECT_MAPPER.readTree(parser);
    }

    private Object[] nullValues() {
        Object[] values = new Object[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            DataType dataType = projection.dataTypeAt(i);
            if (!dataType.isNullable()) {
                throw new KafkaRecordEncodingException(
                        "Kafka null value cannot populate NOT NULL field '"
                                + projection.nameAt(i)
                                + "'.");
            }
        }
        return values;
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder()
                                .maxNestingDepth(MAX_JSON_NESTING_DEPTH)
                                .maxNumberLength(StreamReadConstraints.DEFAULT_MAX_NUM_LEN)
                                .maxStringLength(StreamReadConstraints.DEFAULT_MAX_STRING_LEN)
                                .build());
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        // Trailing content is checked after the root object, not after individual field values.
        return mapper;
    }
}
