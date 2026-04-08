/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.e2e;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Helpers for default table schema, deterministic rows, and checksums. */
public final class Rows {

    private static final LocalDateTime BASE_TIME = LocalDateTime.of(2026, 1, 1, 0, 0, 0);

    private Rows() {}

    public static Schema defaultSchema() {
        return Schema.newBuilder()
                .primaryKey("id")
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("value", DataTypes.BIGINT())
                .column("ts", DataTypes.TIMESTAMP(3))
                .build();
    }

    public static TableDescriptor defaultTableDescriptor(int buckets) {
        return defaultTableDescriptor(buckets, null);
    }

    public static TableDescriptor defaultTableDescriptor(int buckets, Integer replicationFactor) {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(defaultSchema())
                        .distributedBy(buckets, "id")
                        .build();
        if (replicationFactor != null) {
            descriptor = descriptor.withReplicationFactor(replicationFactor);
        }
        return descriptor;
    }

    public static GenericRow defaultRow(int id) {
        return GenericRow.of(
                id,
                BinaryString.fromString("test_row_" + id),
                id * 1000L,
                TimestampNtz.fromLocalDateTime(BASE_TIME.plusSeconds(id)));
    }

    public static List<Map<String, Object>> generatedRows(int count) {
        return generatedRows(0, count);
    }

    public static List<Map<String, Object>> generatedRows(int startId, int count) {
        List<Map<String, Object>> rows = new ArrayList<>(count);
        RowType rowType = defaultSchema().getRowType();
        for (int id = startId; id < startId + count; id++) {
            rows.add(toJsonRow(defaultRow(id), rowType));
        }
        return rows;
    }

    public static Map<String, Object> toJsonRow(InternalRow row, RowType rowType) {
        Map<String, Object> values = new LinkedHashMap<>();
        InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(rowType);
        for (int index = 0; index < rowType.getFieldCount(); index++) {
            Object value = getters[index].getFieldOrNull(row);
            values.put(
                    rowType.getFieldNames().get(index),
                    convertValue(value, rowType.getTypeAt(index)));
        }
        return values;
    }

    private static Object convertValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case CHAR:
            case STRING:
                return value.toString();
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return value;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampNtz) value).toLocalDateTime().toString();
            default:
                return value.toString();
        }
    }

    public static String expectedChecksum(int count) {
        return expectedChecksum(0, count);
    }

    public static String expectedChecksum(int startId, int count) {
        return checksum(generatedRows(startId, count));
    }

    public static String checksum(List<Map<String, Object>> rows) {
        List<Map<String, Object>> orderedRows = new ArrayList<>(rows);
        orderedRows.sort(Comparator.comparingInt(row -> ((Number) row.get("id")).intValue()));
        StringBuilder builder = new StringBuilder();
        for (Map<String, Object> row : orderedRows) {
            builder.append(row.get("id"))
                    .append('|')
                    .append(row.get("name"))
                    .append('|')
                    .append(row.get("value"))
                    .append('|')
                    .append(row.get("ts"))
                    .append('\n');
        }
        return sha256(builder.toString());
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(hash.length * 2);
            for (byte element : hash) {
                builder.append(String.format("%02x", element));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Missing SHA-256 digest implementation.", e);
        }
    }
}
