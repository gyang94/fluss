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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.RecordHeader;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.metadata.KafkaFieldProjection;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.MapType;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Assembles decoded values, raw keys and headers using the shared fixed-column contract. */
@Internal
public final class KafkaRowAssembler {

    private final KafkaTableSchema topicSchema;

    /** Creates an assembler for the resolved topic schema. */
    public KafkaRowAssembler(KafkaTableSchema topicSchema) {
        this.topicSchema = checkNotNull(topicSchema);
        if (topicSchema.metadataPosition() >= 0) {
            throw new KafkaTopicSchemaException(
                    "_kafka_metadata conversion is not supported by this transcoder yet.");
        }
    }

    /**
     * Assembles one Fluss row for immediate synchronous encoding.
     *
     * <p>The row borrows decoded values and header payloads; it must not modify them or retain them
     * after conversion.
     */
    public GenericRow assemble(
            Object[] valueValues, @Nullable byte[] key, List<RecordHeader> headers) {
        GenericRow row = new GenericRow(topicSchema.rowType().getFieldCount());
        setProjectedFields(row, topicSchema.valueProjection(), valueValues);
        if (topicSchema.keyPosition() >= 0) {
            int position = topicSchema.keyPosition();
            if (key == null && !topicSchema.rowType().getTypeAt(position).isNullable()) {
                throw new KafkaRecordEncodingException(
                        "Null Kafka key cannot populate NOT NULL _kafka_key.");
            }
            row.setField(position, key);
        }
        if (topicSchema.headersPosition() >= 0) {
            int position = topicSchema.headersPosition();
            row.setField(
                    position,
                    toHeaders(headers, (MapType) topicSchema.rowType().getTypeAt(position)));
        }
        // A declared _kafka_rescue stays null for binary/string; omitted columns do not exist.
        return row;
    }

    private static void setProjectedFields(
            GenericRow row, KafkaFieldProjection projection, Object[] values) {
        if (values.length != projection.size()) {
            throw new IllegalArgumentException(
                    "Kafka decoder returned "
                            + values.length
                            + " fields for a projection of "
                            + projection.size()
                            + ".");
        }
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value == null && !projection.dataTypeAt(i).isNullable()) {
                throw new KafkaRecordEncodingException(
                        "Kafka record cannot populate NOT NULL field '"
                                + projection.nameAt(i)
                                + "' with null.");
            }
            row.setField(projection.positionAt(i), value);
        }
    }

    private static GenericMap toHeaders(List<RecordHeader> headers, MapType type) {
        Map<BinaryString, byte[]> values = new LinkedHashMap<>();
        for (RecordHeader header : headers) {
            values.put(BinaryString.fromString(header.name()), header.borrowedValue());
        }
        // Validate the final map: an overwritten null must not reject a non-null last value.
        for (Map.Entry<BinaryString, byte[]> entry : values.entrySet()) {
            if (entry.getKey() == null && !type.getKeyType().isNullable()) {
                throw new KafkaRecordEncodingException(
                        "Null Kafka header name cannot populate NOT NULL _kafka_headers map keys.");
            }
            if (entry.getValue() == null && !type.getValueType().isNullable()) {
                throw new KafkaRecordEncodingException(
                        "Null Kafka header value cannot populate NOT NULL _kafka_headers map values.");
            }
        }
        return new GenericMap(values);
    }
}
