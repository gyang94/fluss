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

import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.Base64Variants;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.fluss.utils.DateTimeUtils.timestampToMicros;
import static org.apache.fluss.utils.DateTimeUtils.timestampToNanos;

/**
 * JSON-to-Fluss converter construction.
 *
 * <p>Scalar coercion and recursive conversion follow Apache Flink 1.20's {@code
 * JsonToRowDataConverters}, adapted to Fluss internal values and schema nullability. Fluss
 * additionally normalizes temporal precision and fixed binary widths, supports timestamp offsets
 * and local timestamps, and reports numeric overflow instead of storing non-finite numbers or a
 * null decimal.
 */
final class JsonToFlussConverters {

    private JsonToFlussConverters() {}

    /**
     * Creates a converter for one top-level JSON business field, excluding the JSON root container.
     * The shared schema contract validates the entire type before recursive converter construction.
     */
    static JsonToFlussConverter create(DataType dataType) {
        try {
            KafkaTableSchema.validateJsonType(dataType);
        } catch (InvalidTableException e) {
            throw new KafkaTopicSchemaException(e.getMessage());
        }
        return createValidated(dataType);
    }

    private static JsonToFlussConverter createValidated(DataType dataType) {
        final JsonToFlussConverter notNullConverter = createNotNull(dataType);
        return (parser, path, rescue) -> {
            if (parser == null
                    || parser.currentToken() == JsonToken.VALUE_NULL
                    || parser.currentToken() == JsonToken.NOT_AVAILABLE) {
                if (!dataType.isNullable()) {
                    throw invalid(path, dataType, "field is missing or null").recordException();
                }
                return null;
            }
            try {
                return notNullConverter.convert(parser, path, rescue);
            } catch (KafkaRecordEncodingException e) {
                throw e;
            } catch (ConversionException | NumberFormatException e) {
                if (rescue != null && dataType.isNullable()) {
                    rescue.accept(JsonParserUtils.readValue(parser));
                    return null;
                }
                throw e instanceof ConversionException
                        ? ((ConversionException) e).recordException()
                        : invalid(path, dataType, "value cannot be converted", e).recordException();
            } catch (RuntimeException e) {
                // Internal errors must not silently turn valid data into null.
                throw invalid(path, dataType, "value cannot be converted", e).recordException();
            }
        };
    }

    private static JsonToFlussConverter createNotNull(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return booleanConverter();
            case TINYINT:
                return tinyIntConverter();
            case SMALLINT:
                return smallIntConverter();
            case INTEGER:
                return intConverter();
            case BIGINT:
                return bigIntConverter();
            case FLOAT:
                return floatConverter(dataType);
            case DOUBLE:
                return doubleConverter(dataType);
            case DECIMAL:
                return decimalConverter((DecimalType) dataType);
            case CHAR:
            case STRING:
                return stringConverter();
            case BINARY:
                return binaryConverter((BinaryType) dataType);
            case BYTES:
                return bytesConverter(dataType);
            case DATE:
                return dateConverter(dataType);
            case TIME_WITHOUT_TIME_ZONE:
                return timeConverter((TimeType) dataType);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampConverter((TimestampType) dataType);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampLtzConverter((LocalZonedTimestampType) dataType);
            case ARRAY:
                return arrayConverter((ArrayType) dataType);
            case MAP:
                return mapConverter((MapType) dataType);
            case ROW:
                return rowConverter((RowType) dataType);
            default:
                throw new KafkaTopicSchemaException(
                        "Kafka JSON format does not support Fluss type "
                                + dataType.asSummaryString()
                                + ".");
        }
    }

    private static JsonToFlussConverter booleanConverter() {
        return (parser, path, rescue) -> {
            // Containers have empty scalar text, which Boolean.parseBoolean treats as false.
            boolean value = Boolean.parseBoolean(JsonParserUtils.scalarText(parser).trim());
            JsonParserUtils.skipValue(parser);
            return value;
        };
    }

    private static JsonToFlussConverter tinyIntConverter() {
        return (parser, path, rescue) -> Byte.parseByte(JsonParserUtils.scalarText(parser).trim());
    }

    private static JsonToFlussConverter smallIntConverter() {
        return (parser, path, rescue) ->
                Short.parseShort(JsonParserUtils.scalarText(parser).trim());
    }

    private static JsonToFlussConverter intConverter() {
        return (parser, path, rescue) -> {
            if (parser.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
                BigDecimal value = parser.getDecimalValue();
                if (value.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0
                        && value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0) {
                    return value.intValue();
                }
            }
            return Integer.parseInt(JsonParserUtils.scalarText(parser).trim());
        };
    }

    private static JsonToFlussConverter bigIntConverter() {
        return (parser, path, rescue) -> {
            if (parser.currentToken() == JsonToken.VALUE_NUMBER_FLOAT) {
                BigDecimal value = parser.getDecimalValue();
                if (value.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0
                        && value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0) {
                    return value.longValue();
                }
            }
            return Long.parseLong(JsonParserUtils.scalarText(parser).trim());
        };
    }

    private static JsonToFlussConverter floatConverter(DataType dataType) {
        return (parser, path, rescue) -> {
            // Parse decimal text directly to float to avoid an intermediate double rounding.
            float value = Float.parseFloat(JsonParserUtils.scalarText(parser).trim());
            require(Float.isFinite(value), path, dataType, "non-finite or overflowing number");
            return value;
        };
    }

    private static JsonToFlussConverter doubleConverter(DataType dataType) {
        return (parser, path, rescue) -> {
            double value = Double.parseDouble(JsonParserUtils.scalarText(parser).trim());
            require(Double.isFinite(value), path, dataType, "non-finite or overflowing number");
            return value;
        };
    }

    private static JsonToFlussConverter decimalConverter(DecimalType dataType) {
        return (parser, path, rescue) -> {
            BigDecimal value =
                    parser.currentToken().isNumeric()
                            ? parser.getDecimalValue()
                            : new BigDecimal(JsonParserUtils.scalarText(parser));
            if (value.signum() == 0) {
                return Decimal.zero(dataType.getPrecision(), dataType.getScale());
            }
            // Check the resulting digits before setScale can expand a large JSON exponent.
            // Use long arithmetic because BigDecimal scales may span the entire int range.
            long scaledPrecision = (long) value.precision() - value.scale() + dataType.getScale();
            require(
                    scaledPrecision <= dataType.getPrecision(),
                    path,
                    dataType,
                    "decimal precision exceeds target");
            // Values below a tenth of the target unit round to zero. Avoid constructing a huge
            // power of ten when the input has an extreme negative exponent.
            if (scaledPrecision < 0) {
                return Decimal.zero(dataType.getPrecision(), dataType.getScale());
            }
            // Decimal uses HALF_UP and checks precision again after a possible rounding carry.
            Decimal decimal =
                    Decimal.fromBigDecimal(value, dataType.getPrecision(), dataType.getScale());
            if (decimal == null) {
                throw invalid(path, dataType, "decimal precision exceeds target");
            }
            return decimal;
        };
    }

    private static JsonToFlussConverter stringConverter() {
        return (parser, path, rescue) ->
                BinaryString.fromString(
                        parser.currentToken().isStructStart()
                                ? JsonParserUtils.readValue(parser).toString()
                                : JsonParserUtils.scalarText(parser));
    }

    private static JsonToFlussConverter binaryConverter(BinaryType dataType) {
        return (parser, path, rescue) -> {
            byte[] value = decodeBase64(parser, path, dataType);
            // Arrow uses FixedSizeBinaryVector: zero-pad short values and truncate long ones.
            return value.length == dataType.getLength()
                    ? value
                    : Arrays.copyOf(value, dataType.getLength());
        };
    }

    private static JsonToFlussConverter bytesConverter(DataType dataType) {
        return (parser, path, rescue) -> decodeBase64(parser, path, dataType);
    }

    private static JsonToFlussConverter dateConverter(DataType dataType) {
        return (parser, path, rescue) -> {
            String value = textualValue(parser, path, dataType);
            try {
                return Math.toIntExact(LocalDate.parse(value).toEpochDay());
            } catch (DateTimeException | ArithmeticException e) {
                throw invalid(path, dataType, "invalid ISO-8601 date", e);
            }
        };
    }

    private static JsonToFlussConverter timeConverter(TimeType dataType) {
        final int nanosPerUnit = nanosPerUnit(Math.min(dataType.getPrecision(), 3));
        return (parser, path, rescue) -> {
            String value = textualValue(parser, path, dataType);
            final LocalTime time;
            try {
                time = LocalTime.parse(value);
            } catch (DateTimeParseException e) {
                throw invalid(path, dataType, "invalid ISO-8601 local time", e);
            }
            return time.toSecondOfDay() * 1_000
                    + (time.getNano() / nanosPerUnit * nanosPerUnit) / 1_000_000;
        };
    }

    private static JsonToFlussConverter timestampConverter(TimestampType dataType) {
        final int nanosPerUnit = nanosPerUnit(dataType.getPrecision());
        return (parser, path, rescue) -> {
            String value = textualValue(parser, path, dataType).replace(' ', 'T');
            final LocalDateTime timestamp;
            try {
                timestamp = LocalDateTime.parse(value);
            } catch (DateTimeParseException e) {
                throw invalid(path, dataType, "invalid ISO-8601 local timestamp", e);
            }
            LocalDateTime normalized =
                    timestamp.withNano(timestamp.getNano() / nanosPerUnit * nanosPerUnit);
            long millis =
                    timestampMillis(
                            normalized.toInstant(ZoneOffset.UTC),
                            dataType.getPrecision(),
                            path,
                            dataType);
            return TimestampNtz.fromMillis(millis, normalized.getNano() % 1_000_000);
        };
    }

    private static JsonToFlussConverter timestampLtzConverter(LocalZonedTimestampType dataType) {
        final int nanosPerUnit = nanosPerUnit(dataType.getPrecision());
        final ZoneId localZone = ZoneId.systemDefault();
        return (parser, path, rescue) -> {
            String value = textualValue(parser, path, dataType).replace(' ', 'T');
            Instant timestamp;
            try {
                timestamp = OffsetDateTime.parse(value).toInstant();
            } catch (DateTimeParseException e) {
                try {
                    timestamp = LocalDateTime.parse(value).atZone(localZone).toInstant();
                } catch (DateTimeParseException localError) {
                    throw invalid(path, dataType, "invalid timestamp", localError);
                }
            }
            Instant normalized =
                    Instant.ofEpochSecond(
                            timestamp.getEpochSecond(),
                            timestamp.getNano() / nanosPerUnit * nanosPerUnit);
            long millis = timestampMillis(normalized, dataType.getPrecision(), path, dataType);
            return TimestampLtz.fromEpochMillis(millis, normalized.getNano() % 1_000_000);
        };
    }

    private static JsonToFlussConverter arrayConverter(ArrayType dataType) {
        JsonToFlussConverter elementConverter = createValidated(dataType.getElementType());
        return (parser, path, rescue) -> {
            require(
                    parser.currentToken() == JsonToken.START_ARRAY,
                    path,
                    dataType,
                    "expected a JSON array");
            RescueCollector collector = rescue == null ? null : new RescueCollector(rescue);
            List<Object> elements = new ArrayList<>();
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                int index = elements.size();
                elements.add(
                        elementConverter.convert(
                                parser,
                                JsonPath.index(path, index),
                                collector == null ? null : collector.element(index)));
            }
            if (collector != null) {
                collector.finishArray(elements.size());
            }
            return new GenericArray(elements.toArray());
        };
    }

    private static JsonToFlussConverter mapConverter(MapType dataType) {
        JsonToFlussConverter valueConverter = createValidated(dataType.getValueType());
        return (parser, path, rescue) -> {
            require(
                    parser.currentToken() == JsonToken.START_OBJECT,
                    path,
                    dataType,
                    "expected a JSON object");
            RescueCollector collector = rescue == null ? null : new RescueCollector(rescue);
            Map<BinaryString, Object> values = new LinkedHashMap<>();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                JsonParserUtils.requireField(parser);
                String key = parser.currentName();
                parser.nextToken();
                values.put(
                        BinaryString.fromString(key),
                        valueConverter.convert(
                                parser,
                                JsonPath.field(path, key),
                                collector == null ? null : collector.field(key)));
            }
            if (collector != null) {
                collector.finish();
            }
            return new GenericMap(values);
        };
    }

    private static JsonToFlussConverter rowConverter(RowType dataType) {
        List<DataField> fields = dataType.getFields();
        String[] fieldNames = new String[fields.size()];
        JsonToFlussConverter[] fieldConverters = new JsonToFlussConverter[fields.size()];
        Map<String, Integer> positions = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            fieldNames[i] = field.getName();
            fieldConverters[i] = createValidated(field.getType());
            positions.put(field.getName(), i);
        }
        return (parser, path, rescue) -> {
            require(
                    parser.currentToken() == JsonToken.START_OBJECT,
                    path,
                    dataType,
                    "expected a JSON object");
            RescueCollector collector = rescue == null ? null : new RescueCollector(rescue);
            JsonNode[] fieldRescues = rescue == null ? null : new JsonNode[fieldNames.length];
            boolean[] present = new boolean[fieldNames.length];
            GenericRow row = new GenericRow(fieldNames.length);
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                JsonParserUtils.requireField(parser);
                String name = parser.currentName();
                Integer position = positions.get(name);
                parser.nextToken();
                if (position == null) {
                    if (collector == null) {
                        JsonParserUtils.skipValue(parser);
                    } else {
                        collector.addField(name, JsonParserUtils.readValue(parser));
                    }
                } else {
                    row.setField(
                            position,
                            fieldConverters[position].convert(
                                    parser,
                                    JsonPath.field(path, name),
                                    collector == null
                                            ? null
                                            : value -> fieldRescues[position] = value));
                    present[position] = true;
                }
            }
            for (int i = 0; i < fieldNames.length; i++) {
                if (!present[i]) {
                    row.setField(
                            i,
                            fieldConverters[i].convert(null, JsonPath.field(path, fieldNames[i])));
                }
                // Preserve unknown-input order followed by declared-field order in rescue JSON.
                if (collector != null && fieldRescues[i] != null) {
                    collector.addField(fieldNames[i], fieldRescues[i]);
                }
            }
            if (collector != null) {
                collector.finish();
            }
            return row;
        };
    }

    /** Collects a container's unknown members and failed values without changing the input tree. */
    private static final class RescueCollector {
        private final Consumer<JsonNode> sink;
        @Nullable private JsonNode rescued;

        private RescueCollector(Consumer<JsonNode> sink) {
            this.sink = sink;
        }

        private Consumer<JsonNode> field(String name) {
            return value -> addField(name, value);
        }

        private void addField(String name, JsonNode value) {
            if (rescued == null) {
                rescued = JsonNodeFactory.instance.objectNode();
            }
            ((ObjectNode) rescued).set(name, value);
        }

        private Consumer<JsonNode> element(int index) {
            return value -> {
                if (rescued == null) {
                    rescued = JsonNodeFactory.instance.arrayNode();
                }
                ArrayNode array = (ArrayNode) rescued;
                while (array.size() <= index) {
                    array.addNull();
                }
                array.set(index, value);
            };
        }

        private void finishArray(int size) {
            if (rescued != null) {
                ArrayNode array = (ArrayNode) rescued;
                while (array.size() < size) {
                    array.addNull();
                }
            }
            finish();
        }

        private void finish() {
            if (rescued != null) {
                sink.accept(rescued);
            }
        }
    }

    private static long timestampMillis(
            Instant timestamp, int precision, String path, DataType dataType) {
        try {
            long millis = timestamp.toEpochMilli();
            int nanos = timestamp.getNano() % 1_000_000;
            // Arrow stores precision 4-6 in microseconds and precision 7-9 in nanoseconds.
            // Check here as well so Kafka reports the offending JSON field before encoding.
            if (precision > 6) {
                timestampToNanos(millis, nanos);
            } else if (precision > 3) {
                timestampToMicros(millis, nanos);
            }
            return millis;
        } catch (ArithmeticException e) {
            throw invalid(path, dataType, "timestamp exceeds storage range", e);
        }
    }

    private static String textualValue(JsonParser parser, String path, DataType dataType)
            throws IOException {
        require(
                parser.currentToken() == JsonToken.VALUE_STRING,
                path,
                dataType,
                "expected a JSON string");
        return parser.getText();
    }

    private static byte[] decodeBase64(JsonParser parser, String path, DataType dataType)
            throws IOException {
        require(
                parser.currentToken() == JsonToken.VALUE_STRING
                        || parser.currentToken() == JsonToken.VALUE_EMBEDDED_OBJECT,
                path,
                dataType,
                "expected a Base64 string or binary node");
        if (parser.currentToken() == JsonToken.VALUE_EMBEDDED_OBJECT) {
            return parser.getBinaryValue();
        }
        // Materialize text before decoding: getBinaryValue may partially consume an invalid token.
        String value = parser.getText();
        try {
            return Base64Variants.getDefaultVariant().decode(value);
        } catch (IllegalArgumentException e) {
            throw invalid(path, dataType, "invalid Base64 string", e);
        }
    }

    private static int nanosPerUnit(int precision) {
        int unit = 1;
        for (int i = precision; i < 9; i++) {
            unit *= 10;
        }
        return unit;
    }

    private static void require(boolean condition, String path, DataType dataType, String reason) {
        if (!condition) {
            throw invalid(path, dataType, reason);
        }
    }

    private static ConversionException invalid(String path, DataType dataType, String reason) {
        return invalid(path, dataType, reason, null);
    }

    private static ConversionException invalid(
            String path, DataType dataType, String reason, @Nullable Throwable cause) {
        return new ConversionException(
                "Invalid Kafka record value at "
                        + path
                        + " for "
                        + dataType.asSummaryString()
                        + ": "
                        + reason
                        + ".",
                cause);
    }

    /** Distinguishes expected data conversion failures from internal errors. */
    private static final class ConversionException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ConversionException(String message, @Nullable Throwable cause) {
            super(message, cause);
        }

        private KafkaRecordEncodingException recordException() {
            return new KafkaRecordEncodingException(getMessage(), getCause());
        }
    }
}
