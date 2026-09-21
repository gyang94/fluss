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
import org.apache.fluss.row.BinaryString;
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
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
    private final int rescueProjectionPosition;

    /** Creates a JSON decoder and validates every projected field type. */
    public JsonKafkaFieldDecoder(KafkaFieldProjection projection) {
        this(projection, null);
    }

    /** Creates a JSON decoder with an optional nullable STRING rescue column. */
    public JsonKafkaFieldDecoder(
            KafkaFieldProjection projection, @Nullable String valueRescueColumn) {
        this.projection = projection;
        converters = new JsonToFlussConverter[projection.size()];
        int resolvedRescuePosition = -1;
        for (int i = 0; i < projection.size(); i++) {
            if (projectedFields.put(projection.nameAt(i), i) != null) {
                throw new KafkaTopicSchemaException(
                        "Duplicate Kafka JSON field '" + projection.nameAt(i) + "'.");
            }
            if (projection.nameAt(i).equals(valueRescueColumn)) {
                DataType rescueType = projection.dataTypeAt(i);
                if (!(rescueType instanceof StringType) || !rescueType.isNullable()) {
                    throw new KafkaTopicSchemaException(
                            "Kafka value rescue column '"
                                    + valueRescueColumn
                                    + "' must be nullable STRING.");
                }
                resolvedRescuePosition = i;
                continue;
            }
            converters[i] = JsonToFlussConverters.create(projection.dataTypeAt(i));
        }
        if (valueRescueColumn != null && resolvedRescuePosition < 0) {
            throw new KafkaTopicSchemaException(
                    "Kafka value rescue column '"
                            + valueRescueColumn
                            + "' is not in the value projection.");
        }
        rescueProjectionPosition = resolvedRescuePosition;
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
            ObjectNode rescuedFields =
                    rescueProjectionPosition < 0 ? null : OBJECT_MAPPER.createObjectNode();
            JsonNode[] nestedRescues =
                    rescueProjectionPosition < 0 ? null : new JsonNode[projection.size()];
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
                validateUnicode(fieldName, path);
                Integer position = projectedFields.get(fieldName);
                if (position == null) {
                    if (rescuedFields == null) {
                        throw new KafkaRecordEncodingException(
                                "Invalid Kafka record value at " + path + ": unknown field.");
                    }
                    parser.nextToken();
                    rescuedFields.set(fieldName, readBoundedTree(parser, path));
                    continue;
                }
                parser.nextToken();
                if (position == rescueProjectionPosition) {
                    if (parser.currentToken() != JsonToken.VALUE_NULL) {
                        throw new KafkaRecordEncodingException(
                                "Invalid Kafka record value at "
                                        + path
                                        + ": the configured rescue column is reserved and must be null or absent.");
                    }
                    continue;
                }
                JsonNode value =
                        readValue(
                                parser,
                                projection.dataTypeAt(position),
                                path,
                                rescuedFields != null);
                if (nestedRescues != null) {
                    nestedRescues[position] =
                            extractNestedUnknownFields(
                                    value, projection.dataTypeAt(position), path);
                }
                values[position] =
                        converters[position].convert(
                                value,
                                path,
                                nestedRescues == null
                                        ? null
                                        : rescued ->
                                                nestedRescues[position] =
                                                        JsonRescue.merge(
                                                                nestedRescues[position], rescued));
                present[position] = true;
            }
            if (parser.nextToken() != null) {
                throw new KafkaRecordEncodingException(
                        "Kafka record value is not valid strict UTF-8 JSON: trailing content.");
            }
            for (int i = 0; i < projection.size(); i++) {
                if (i != rescueProjectionPosition && !present[i]) {
                    values[i] =
                            converters[i].convert(
                                    null, JsonPath.field(JsonPath.ROOT, projection.nameAt(i)));
                }
            }
            if (rescuedFields != null) {
                // Keep root unknown fields first, then nested fields in projection order.
                for (int i = 0; i < projection.size(); i++) {
                    if (nestedRescues[i] != null) {
                        rescuedFields.set(projection.nameAt(i), nestedRescues[i]);
                    }
                }
                values[rescueProjectionPosition] =
                        rescuedFields.isEmpty()
                                ? null
                                : BinaryString.fromString(rescuedFields.toString());
            }
            return values;
        } catch (KafkaRecordEncodingException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new KafkaRecordEncodingException(
                    "Kafka record value is not valid strict UTF-8 JSON.", e);
        }
    }

    private static JsonNode readValue(
            JsonParser parser, DataType dataType, String path, boolean rescueEnabled)
            throws IOException {
        if (parser.currentToken() == JsonToken.START_OBJECT
                || parser.currentToken() == JsonToken.START_ARRAY) {
            switch (dataType.getTypeRoot()) {
                case ROW:
                case ARRAY:
                case MAP:
                    break;
                default:
                    if (rescueEnabled && dataType.isNullable()) {
                        break;
                    }
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
                    float floatValue = Float.parseFloat(parser.getText());
                    if (rescueEnabled && dataType.isNullable() && !Float.isFinite(floatValue)) {
                        return readBoundedTree(parser, path);
                    }
                    return FloatNode.valueOf(floatValue);
                case DOUBLE:
                    double doubleValue = Double.parseDouble(parser.getText());
                    if (rescueEnabled && dataType.isNullable() && !Double.isFinite(doubleValue)) {
                        return readBoundedTree(parser, path);
                    }
                    return DoubleNode.valueOf(doubleValue);
                default:
                    break;
            }
        }
        return readBoundedTree(parser, path);
    }

    private static JsonNode readBoundedTree(JsonParser parser, String path) throws IOException {
        // Bound every container while consuming tokens, before allocating excess children.
        // The parser independently enforces nesting depth and rejects duplicate object fields.
        switch (parser.currentToken()) {
            case START_ARRAY:
                ArrayNode array = OBJECT_MAPPER.createArrayNode();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    checkContainerSize(array.size() + 1, path);
                    array.add(readBoundedTree(parser, JsonPath.index(path, array.size())));
                }
                return array;
            case START_OBJECT:
                ObjectNode object = OBJECT_MAPPER.createObjectNode();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    checkContainerSize(object.size() + 1, path);
                    String fieldName = parser.currentName();
                    String fieldPath = JsonPath.field(path, fieldName);
                    validateUnicode(fieldName, fieldPath);
                    parser.nextToken();
                    object.set(fieldName, readBoundedTree(parser, fieldPath));
                }
                return object;
            default:
                JsonNode value = OBJECT_MAPPER.readTree(parser);
                if (value.isTextual()) {
                    validateUnicode(value.textValue(), path);
                }
                // Jackson may turn an overflowing numeric token into a non-finite DoubleNode
                // even with USE_BIG_DECIMAL_FOR_FLOATS. Rescue must not serialize it as a string.
                if ((value.isDouble() || value.isFloat())
                        && !Double.isFinite(value.doubleValue())) {
                    throw new KafkaRecordEncodingException(
                            "Invalid Kafka record value at "
                                    + path
                                    + ": non-finite or overflowing number.");
                }
                return value;
        }
    }

    private static void validateUnicode(String value, String path) {
        if (JsonToFlussConverters.hasUnpairedSurrogate(value)) {
            throw new KafkaRecordEncodingException(
                    "Invalid Kafka record value at " + path + ": unpaired UTF-16 surrogate.");
        }
    }

    private static void checkContainerSize(int size, String path) {
        if (size > MAX_CONTAINER_ELEMENTS) {
            throw new KafkaRecordEncodingException(
                    "Invalid Kafka record value at "
                            + path
                            + ": container size exceeds "
                            + MAX_CONTAINER_ELEMENTS
                            + ".");
        }
    }

    private static JsonNode extractNestedUnknownFields(
            JsonNode node, DataType dataType, String path) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (dataType instanceof RowType && node.isObject()) {
            return extractRowUnknownFields((ObjectNode) node, (RowType) dataType, path);
        }
        if (dataType instanceof ArrayType && node.isArray()) {
            return extractArrayUnknownFields((ArrayNode) node, (ArrayType) dataType, path);
        }
        if (dataType instanceof MapType && node.isObject()) {
            return extractMapValueUnknownFields((ObjectNode) node, (MapType) dataType, path);
        }
        return null;
    }

    private static JsonNode extractRowUnknownFields(ObjectNode node, RowType rowType, String path) {
        validateContainerSize(node.size(), path);
        ObjectNode rescuedFields = OBJECT_MAPPER.createObjectNode();
        List<String> fieldsToRemove = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            int fieldPosition = rowType.getFieldIndex(field.getKey());
            if (fieldPosition < 0) {
                rescuedFields.set(field.getKey(), field.getValue());
                fieldsToRemove.add(field.getKey());
            } else {
                JsonNode nestedRescue =
                        extractNestedUnknownFields(
                                field.getValue(),
                                rowType.getTypeAt(fieldPosition),
                                JsonPath.field(path, field.getKey()));
                if (nestedRescue != null) {
                    rescuedFields.set(field.getKey(), nestedRescue);
                }
            }
        }
        for (String fieldName : fieldsToRemove) {
            node.remove(fieldName);
        }
        return rescuedFields.isEmpty() ? null : rescuedFields;
    }

    private static JsonNode extractArrayUnknownFields(
            ArrayNode node, ArrayType arrayType, String path) {
        validateContainerSize(node.size(), path);
        ArrayNode rescuedElements = OBJECT_MAPPER.createArrayNode();
        boolean hasRescuedElement = false;
        int elementPosition = 0;
        for (JsonNode element : node) {
            JsonNode rescuedElement =
                    extractNestedUnknownFields(
                            element,
                            arrayType.getElementType(),
                            JsonPath.index(path, elementPosition));
            if (rescuedElement == null) {
                rescuedElements.addNull();
            } else {
                rescuedElements.add(rescuedElement);
                hasRescuedElement = true;
            }
            elementPosition++;
        }
        return hasRescuedElement ? rescuedElements : null;
    }

    private static JsonNode extractMapValueUnknownFields(
            ObjectNode node, MapType mapType, String path) {
        validateContainerSize(node.size(), path);
        ObjectNode rescuedValues = OBJECT_MAPPER.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode rescuedValue =
                    extractNestedUnknownFields(
                            field.getValue(),
                            mapType.getValueType(),
                            JsonPath.field(path, field.getKey()));
            if (rescuedValue != null) {
                rescuedValues.set(field.getKey(), rescuedValue);
            }
        }
        return rescuedValues.isEmpty() ? null : rescuedValues;
    }

    private static void validateContainerSize(int size, String path) {
        if (size > JsonToFlussConverters.MAX_CONTAINER_ELEMENTS) {
            throw new KafkaRecordEncodingException(
                    "Kafka JSON container at "
                            + path
                            + " exceeds the maximum element count of "
                            + JsonToFlussConverters.MAX_CONTAINER_ELEMENTS
                            + ".");
        }
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
