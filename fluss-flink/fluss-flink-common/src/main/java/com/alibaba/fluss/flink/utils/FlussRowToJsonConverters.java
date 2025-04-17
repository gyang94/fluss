/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.MapType;
import com.alibaba.fluss.types.RowType;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static com.alibaba.fluss.flink.utils.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static com.alibaba.fluss.flink.utils.TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static com.alibaba.fluss.flink.utils.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static com.alibaba.fluss.flink.utils.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static com.alibaba.fluss.flink.utils.TimeFormats.SQL_TIME_FORMAT;
import static com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

/** A converter to convert Fluss's {@link com.alibaba.fluss.row.InternalRow} to {@link JsonNode}. */
public class FlussRowToJsonConverters {

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    public FlussRowToJsonConverters(TimestampFormat timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    /**
     * Runtime converter that converts objects of Fluss data structures to corresponding {@link
     * JsonNode}s.
     */
    @FunctionalInterface
    public interface FlussRowDataToJsonConverter extends Serializable {
        JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);
    }

    public FlussRowDataToJsonConverter createConverter(DataType flussDataType) {
        return wrapIntoNullableConverter(createNotNullConverter(flussDataType));
    }

    private FlussRowDataToJsonConverter createNotNullConverter(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                return ((mapper, reuse, value) ->
                        mapper.getNodeFactory().textNode(value.toString()));
            case BOOLEAN:
                return (mapper, reuse, value) ->
                        mapper.getNodeFactory().booleanNode((boolean) value);
            case BINARY:
            case BYTES:
                return ((mapper, reuse, value) ->
                        mapper.getNodeFactory().binaryNode((byte[]) value));
            case DECIMAL:
                return createDecimalConverter();
            case TINYINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((byte) value);
            case SMALLINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((short) value);
            case INTEGER:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((int) value);
            case BIGINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((long) value);
            case FLOAT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((float) value);
            case DOUBLE:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((double) value);
            case DATE:
                return createDateConverter();
            case TIME_WITHOUT_TIME_ZONE:
                return createTimeConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return createTimestampConverter();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return createTimestampWithLocalZone();
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return createMapConverter(
                        mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case ROW:
                return createRowConverter((RowType) type);
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private FlussRowDataToJsonConverter createDecimalConverter() {
        return (mapper, reuse, value) -> {
            BigDecimal bd = ((DecimalData) value).toBigDecimal();
            return mapper.getNodeFactory()
                    .numberNode(
                            mapper.isEnabled(WRITE_BIGDECIMAL_AS_PLAIN)
                                    ? bd
                                    : bd.stripTrailingZeros());
        };
    }

    private FlussRowDataToJsonConverter createDateConverter() {
        return ((mapper, reuse, value) -> {
            int days = (int) value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format(date));
        });
    }

    private FlussRowDataToJsonConverter createTimeConverter() {
        return (mapper, reuse, value) -> {
            int millisecond = (int) value;
            LocalTime time = LocalTime.ofSecondOfDay(millisecond / 1000L);
            return mapper.getNodeFactory().textNode(SQL_TIME_FORMAT.format(time));
        };
    }

    private FlussRowDataToJsonConverter createTimestampConverter() {
        switch (timestampFormat) {
            case ISO_8601:
                return (mapper, reuse, value) -> {
                    TimestampNtz timestamp = (TimestampNtz) value;
                    return mapper.getNodeFactory()
                            .textNode(ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            case SQL:
                return (mapper, reuse, value) -> {
                    TimestampNtz timestamp = (TimestampNtz) value;
                    return mapper.getNodeFactory()
                            .textNode(SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private FlussRowDataToJsonConverter createTimestampWithLocalZone() {
        switch (timestampFormat) {
            case ISO_8601:
                return (mapper, reuse, value) -> {
                    TimestampNtz timestampWithLocalZone = (TimestampNtz) value;
                    return mapper.getNodeFactory()
                            .textNode(
                                    ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                            timestampWithLocalZone
                                                    .toInstant()
                                                    .atOffset(ZoneOffset.UTC)));
                };
            case SQL:
                return (mapper, reuse, value) -> {
                    TimestampNtz timestampWithLocalZone = (TimestampNtz) value;
                    return mapper.getNodeFactory()
                            .textNode(
                                    SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                            timestampWithLocalZone
                                                    .toInstant()
                                                    .atOffset(ZoneOffset.UTC)));
                };
            default:
                throw new TableException(
                        "Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private FlussRowDataToJsonConverter createArrayConverter(ArrayType type) {
        // TODO
        return null;
    }

    private FlussRowDataToJsonConverter createMapConverter(
            String typeSummary, DataType keyType, DataType valueType) {
        // TODO
        return null;
    }

    private FlussRowDataToJsonConverter createRowConverter(RowType type) {
        // TODO
        return null;
    }

    private FlussRowDataToJsonConverter wrapIntoNullableConverter(
            FlussRowDataToJsonConverter flussRowDataToJsonConverter) {
        return ((mapper, reuse, value) -> {
            if (value == null) {
                return mapper.getNodeFactory().nullNode();
            } else {
                return flussRowDataToJsonConverter.convert(mapper, reuse, value);
            }
        });
    }
}
