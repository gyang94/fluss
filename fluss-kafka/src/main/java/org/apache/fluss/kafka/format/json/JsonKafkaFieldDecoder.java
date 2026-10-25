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
import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaFieldProjection;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.DoubleNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.FloatNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Decodes a strict UTF-8 JSON object according to the projected Fluss fields. */
@Internal
public final class JsonKafkaFieldDecoder implements KafkaFieldDecoder {
    // The JSON root and a container at business depth zero add two levels to schema depth.
    // The nested physical ROW represents the JSON root and adds no extra level.
    static final int MAX_JSON_NESTING_DEPTH = KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH + 2;
    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private final KafkaFieldProjection projection;
    private final @Nullable RowType nestedType;
    private final Map<String, Integer> projectedFields = new HashMap<>();
    private final JsonToFlussConverter[] converters;

    /** Creates a JSON decoder and validates every projected field type. */
    public JsonKafkaFieldDecoder(KafkaFieldProjection projection) {
        this(projection, KafkaValueConversionLayout.FLAT);
    }

    /** Creates a decoder whose nested physical ROW represents the JSON object root. */
    public JsonKafkaFieldDecoder(
            KafkaFieldProjection projection, @Nullable KafkaValueConversionLayout layout) {
        if (layout == KafkaValueConversionLayout.NESTED) {
            if (projection.size() != 1
                    || !KafkaTableSchema.VALUE_COLUMN.equals(projection.nameAt(0))
                    || !(projection.dataTypeAt(0) instanceof RowType)) {
                throw new KafkaTopicSchemaException(
                        "Nested Kafka JSON requires exactly one _kafka_value ROW.");
            }
            nestedType = (RowType) projection.dataTypeAt(0);
            List<Integer> positions = new ArrayList<>();
            for (int i = 0; i < nestedType.getFieldCount(); i++) {
                positions.add(i);
            }
            this.projection = new KafkaFieldProjection(nestedType, positions);
        } else {
            nestedType = null;
            this.projection = projection;
        }
        converters = new JsonToFlussConverter[this.projection.size()];
        for (int i = 0; i < this.projection.size(); i++) {
            String name = this.projection.nameAt(i);
            if (KafkaTableSchema.isReservedName(name)) {
                throw new KafkaTopicSchemaException(
                        "Reserved name '" + name + "' cannot be a Kafka JSON business field.");
            }
            if (projectedFields.put(name, i) != null) {
                throw new KafkaTopicSchemaException("Duplicate Kafka JSON field '" + name + "'.");
            }
            converters[i] = JsonToFlussConverters.create(this.projection.dataTypeAt(i));
        }
    }

    @Override
    public Object[] decode(@Nullable ByteBuffer bytes) {
        if (bytes == null) {
            return nullValues();
        }
        try (Reader reader =
                        new InputStreamReader(
                                new ByteBufferBackedInputStream(bytes.duplicate()),
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
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.currentName();
                String path = JsonPath.field(JsonPath.ROOT, fieldName);
                if (KafkaTableSchema.isReservedName(fieldName)) {
                    throw new KafkaRecordEncodingException(
                            "Invalid Kafka record value at " + path + ": reserved field.");
                }
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
            return nestedType == null ? values : new Object[] {GenericRow.of(values)};
        } catch (KafkaRecordEncodingException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new KafkaRecordEncodingException(
                    "Kafka record value is not valid strict UTF-8 JSON.", e);
        }
    }

    private static JsonNode readValue(JsonParser parser, DataType dataType, String path)
            throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT
                && parser.currentToken() != JsonToken.START_ARRAY) {
            return readScalar(parser, dataType);
        }
        switch (dataType.getTypeRoot()) {
            case ARRAY:
                return readArray(parser, (ArrayType) dataType, path);
            case ROW:
            case MAP:
                return readObject(parser, dataType, path);
            default:
                throw new KafkaRecordEncodingException(
                        "Invalid Kafka record value at " + path + ": expected a JSON scalar.");
        }
    }

    private static ArrayNode readArray(JsonParser parser, ArrayType dataType, String path)
            throws IOException {
        if (parser.currentToken() != JsonToken.START_ARRAY) {
            throw new KafkaRecordEncodingException(
                    "Invalid Kafka record value at " + path + ": expected a JSON array.");
        }
        DataType elementType = dataType.getElementType();
        ArrayNode array = OBJECT_MAPPER.createArrayNode();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            array.add(readValue(parser, elementType, JsonPath.index(path, array.size())));
        }
        return array;
    }

    private static ObjectNode readObject(JsonParser parser, DataType dataType, String path)
            throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw new KafkaRecordEncodingException(
                    "Invalid Kafka record value at " + path + ": expected a JSON object.");
        }
        Map<String, DataType> rowFields = new HashMap<>();
        if (dataType instanceof RowType) {
            for (DataField field : ((RowType) dataType).getFields()) {
                rowFields.put(field.getName(), field.getType());
            }
        }
        ObjectNode object = OBJECT_MAPPER.createObjectNode();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String fieldName = parser.currentName();
            String fieldPath = JsonPath.field(path, fieldName);
            DataType fieldType =
                    dataType instanceof MapType
                            ? ((MapType) dataType).getValueType()
                            : rowFields.get(fieldName);
            if (fieldType == null) {
                throw new KafkaRecordEncodingException(
                        "Invalid Kafka record value at " + fieldPath + ": unknown field.");
            }
            parser.nextToken();
            object.set(fieldName, readValue(parser, fieldType, fieldPath));
        }
        return object;
    }

    private static JsonNode readScalar(JsonParser parser, DataType dataType) throws IOException {
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
        if (nestedType != null) {
            if (!nestedType.isNullable()) {
                throw new KafkaRecordEncodingException(
                        "Kafka null value cannot populate NOT NULL field '_kafka_value'.");
            }
            return new Object[] {null};
        }
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
