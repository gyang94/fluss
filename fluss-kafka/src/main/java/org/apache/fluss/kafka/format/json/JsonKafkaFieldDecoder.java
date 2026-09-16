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
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException.Reason;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/** Decodes a plain JSON object according to the projected Fluss fields. */
@Internal
public final class JsonKafkaFieldDecoder implements KafkaFieldDecoder {

    static final int MAX_JSON_NESTING_DEPTH = 64;

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private final KafkaFieldProjection projection;
    private final Set<String> projectedFieldNames;
    private final JsonToFlussConverter[] converters;
    private final String[] fieldPaths;
    private final int rescueProjectionPosition;

    /** Creates a JSON decoder and precompiles converters for every projected field. */
    public JsonKafkaFieldDecoder(KafkaFieldProjection projection) {
        this(projection, null);
    }

    /** Creates a JSON decoder with an optional nullable STRING rescue column. */
    public JsonKafkaFieldDecoder(
            KafkaFieldProjection projection, @Nullable String valueRescueColumn) {
        this.projection = projection;
        this.projectedFieldNames = new HashSet<>();
        this.converters = new JsonToFlussConverter[projection.size()];
        this.fieldPaths = new String[projection.size()];
        int resolvedRescuePosition = -1;
        for (int i = 0; i < projection.size(); i++) {
            fieldPaths[i] = JsonPath.field(JsonPath.ROOT, projection.nameAt(i));
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
            projectedFieldNames.add(projection.nameAt(i));
            converters[i] = JsonToFlussConverters.create(projection.dataTypeAt(i));
        }
        if (valueRescueColumn != null && resolvedRescuePosition < 0) {
            throw new KafkaTopicSchemaException(
                    "Kafka value rescue column '"
                            + valueRescueColumn
                            + "' is not in the value projection.");
        }
        this.rescueProjectionPosition = resolvedRescuePosition;
    }

    @Override
    public Object[] decode(@Nullable byte[] bytes) {
        return decode(bytes, ignored -> {});
    }

    @Override
    public Object[] decode(@Nullable byte[] bytes, Consumer<Reason> rescuedErrorObserver) {
        if (bytes == null) {
            return nullValues();
        }
        final JsonNode root;
        try {
            root = OBJECT_MAPPER.readTree(bytes);
        } catch (IOException | RuntimeException e) {
            throw new KafkaRecordEncodingException(
                    Reason.JSON_SYNTAX, "Kafka record value is not valid strict UTF-8 JSON.", e);
        }
        if (root == null || !root.isObject()) {
            throw new KafkaRecordEncodingException(
                    Reason.JSON_SYNTAX, "Kafka JSON record value must have an object root.");
        }
        JsonNode unknownFields = validateAndExtractUnknownFields(root);
        if (unknownFields != null) {
            rescuedErrorObserver.accept(Reason.UNKNOWN_FIELD);
        }
        ObjectNode rescuedFields =
                rescueProjectionPosition < 0
                        ? null
                        : unknownFields == null
                                ? OBJECT_MAPPER.createObjectNode()
                                : (ObjectNode) unknownFields;
        Object[] values = new Object[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            if (i == rescueProjectionPosition) {
                continue;
            }
            String fieldName = projection.nameAt(i);
            values[i] =
                    converters[i].convert(
                            root.get(fieldName),
                            fieldPaths[i],
                            rescuedFields == null
                                    ? null
                                    : node -> {
                                        rescuedErrorObserver.accept(Reason.NULLABLE_TYPE);
                                        rescuedFields.set(
                                                fieldName,
                                                JsonRescue.merge(
                                                        rescuedFields.get(fieldName), node));
                                    });
        }
        if (rescuedFields != null && !rescuedFields.isEmpty()) {
            values[rescueProjectionPosition] =
                    JsonToFlussConverters.encodeString(rescuedFields.toString());
        }
        return values;
    }

    private JsonNode validateAndExtractUnknownFields(JsonNode root) {
        if (root.size() > JsonToFlussConverters.MAX_CONTAINER_ELEMENTS) {
            throw new KafkaRecordEncodingException(
                    Reason.RESOURCE_LIMIT,
                    "Kafka JSON object at "
                            + JsonPath.ROOT
                            + " exceeds the maximum field count of "
                            + JsonToFlussConverters.MAX_CONTAINER_ELEMENTS
                            + ".");
        }
        if (rescueProjectionPosition < 0) {
            validateProjectedFields(root);
            return null;
        }

        String rescueColumn = projection.nameAt(rescueProjectionPosition);
        JsonNode suppliedRescueValue = root.get(rescueColumn);
        if (suppliedRescueValue != null && !suppliedRescueValue.isNull()) {
            throw new KafkaRecordEncodingException(
                    "Invalid Kafka record value at "
                            + JsonPath.field(JsonPath.ROOT, rescueColumn)
                            + ": the configured rescue column is reserved and must be null or absent.");
        }

        ObjectNode rescuedFields = OBJECT_MAPPER.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (field.getKey().equals(rescueColumn)) {
                fields.remove();
            } else if (!projectedFieldNames.contains(field.getKey())) {
                rescuedFields.set(field.getKey(), field.getValue());
                fields.remove();
            }
        }

        for (int i = 0; i < projection.size(); i++) {
            if (i == rescueProjectionPosition) {
                continue;
            }
            String fieldName = projection.nameAt(i);
            JsonNode nestedRescue =
                    extractNestedUnknownFields(
                            root.get(fieldName), projection.dataTypeAt(i), fieldPaths[i]);
            if (nestedRescue != null) {
                rescuedFields.set(fieldName, nestedRescue);
            }
        }
        return rescuedFields.isEmpty() ? null : rescuedFields;
    }

    private void validateProjectedFields(JsonNode root) {
        Iterator<String> fieldNames = root.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            if (!projectedFieldNames.contains(fieldName)) {
                throw new KafkaRecordEncodingException(
                        Reason.UNKNOWN_FIELD,
                        "Invalid Kafka record value at "
                                + JsonPath.field(JsonPath.ROOT, fieldName)
                                + ": unknown field.");
            }
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
        ObjectNode rescuedFields = null;
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            int fieldPosition = rowType.getFieldIndex(field.getKey());
            if (fieldPosition < 0) {
                if (rescuedFields == null) {
                    rescuedFields = OBJECT_MAPPER.createObjectNode();
                }
                rescuedFields.set(field.getKey(), field.getValue());
                fields.remove();
            } else {
                DataType fieldType = rowType.getTypeAt(fieldPosition);
                if (!isContainerType(fieldType)) {
                    continue;
                }
                JsonNode nestedRescue =
                        extractNestedUnknownFields(
                                field.getValue(), fieldType, JsonPath.field(path, field.getKey()));
                if (nestedRescue != null) {
                    if (rescuedFields == null) {
                        rescuedFields = OBJECT_MAPPER.createObjectNode();
                    }
                    rescuedFields.set(field.getKey(), nestedRescue);
                }
            }
        }
        return rescuedFields;
    }

    private static JsonNode extractArrayUnknownFields(
            ArrayNode node, ArrayType arrayType, String path) {
        validateContainerSize(node.size(), path);
        if (!isContainerType(arrayType.getElementType())) {
            return null;
        }
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
        if (!isContainerType(mapType.getValueType())) {
            return null;
        }
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

    // Scalar types cannot contain schema-declared fields, so this unknown-field pass must not
    // construct paths or rescue containers for them. The converter still checks their values.
    private static boolean isContainerType(DataType dataType) {
        return dataType instanceof RowType
                || dataType instanceof ArrayType
                || dataType instanceof MapType;
    }

    private static void validateContainerSize(int size, String path) {
        if (size > JsonToFlussConverters.MAX_CONTAINER_ELEMENTS) {
            throw new KafkaRecordEncodingException(
                    Reason.RESOURCE_LIMIT,
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
                        Reason.NOT_NULL_MISSING,
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
        mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        return mapper;
    }
}
