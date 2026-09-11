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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/** Decodes a strict UTF-8 JSON object according to the projected Fluss fields. */
@Internal
public final class JsonKafkaFieldDecoder implements KafkaFieldDecoder {
    static final int MAX_JSON_NESTING_DEPTH = 64;
    static final int MAX_CONTAINER_ELEMENTS = 10_000;
    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private final KafkaFieldProjection projection;
    private final Set<String> projectedFieldNames = new HashSet<>();
    private final JsonToFlussConverter[] converters;

    /** Creates a JSON decoder and validates every projected field type. */
    public JsonKafkaFieldDecoder(KafkaFieldProjection projection) {
        this.projection = projection;
        converters = new JsonToFlussConverter[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            if (!projectedFieldNames.add(projection.nameAt(i))) {
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
        final JsonNode root;
        try {
            String json =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(bytes))
                            .toString();
            root = OBJECT_MAPPER.readTree(json);
        } catch (IOException | RuntimeException e) {
            throw new KafkaRecordEncodingException(
                    "Kafka record value is not valid strict UTF-8 JSON.", e);
        }
        if (root == null || !root.isObject()) {
            throw new KafkaRecordEncodingException(
                    "Kafka JSON record value must have an object root.");
        }
        if (root.size() > MAX_CONTAINER_ELEMENTS) {
            throw new KafkaRecordEncodingException(
                    "Kafka JSON object exceeds the maximum field count of "
                            + MAX_CONTAINER_ELEMENTS
                            + ".");
        }
        Iterator<String> fieldNames = root.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            if (!projectedFieldNames.contains(fieldName)) {
                throw new KafkaRecordEncodingException(
                        "Invalid Kafka record value at "
                                + JsonPath.field(JsonPath.ROOT, fieldName)
                                + ": unknown field.");
            }
        }
        Object[] values = new Object[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            String name = projection.nameAt(i);
            values[i] = converters[i].convert(root.get(name), JsonPath.field(JsonPath.ROOT, name));
        }
        return values;
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
        mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        return mapper;
    }
}
