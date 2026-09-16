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

package org.apache.fluss.kafka.format;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.schema.KafkaFieldProjection;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.shaded.guava32.com.google.common.base.Utf8;
import org.apache.fluss.types.StringType;

import javax.annotation.Nullable;

/** Factory for strict UTF-8 Kafka string decoding. */
@Internal
public final class StringKafkaFormatFactory implements KafkaFormatFactory {

    @Override
    public KafkaDataFormat format() {
        return KafkaDataFormat.STRING;
    }

    @Override
    public KafkaFieldDecoder createDecoder(
            KafkaFieldProjection projection, @Nullable String valueRescueColumn) {
        if (valueRescueColumn != null) {
            throw new KafkaTopicSchemaException(
                    "Kafka value rescue column is only supported for JSON format.");
        }
        if (projection.size() != 1 || !(projection.dataTypeAt(0) instanceof StringType)) {
            throw new KafkaTopicSchemaException(
                    "Kafka string format requires exactly one STRING field.");
        }
        final String fieldName = projection.nameAt(0);
        return bytes -> new Object[] {decode(bytes, fieldName)};
    }

    private static BinaryString decode(byte[] bytes, String fieldName) {
        if (bytes == null) {
            return null;
        }
        if (!Utf8.isWellFormed(bytes)) {
            throw new KafkaRecordEncodingException(
                    "Kafka field '" + fieldName + "' is not valid UTF-8.");
        }
        // The command owns these bytes until the synchronous Arrow/KV encoder has copied them.
        // Retain UTF-8 directly instead of allocating and re-encoding a UTF-16 String.
        return BinaryString.fromBytes(bytes);
    }
}
