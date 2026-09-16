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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;

/** Indicates that Kafka record bytes cannot be decoded using the configured data format. */
@Internal
public final class KafkaRecordEncodingException extends IllegalArgumentException {

    /** Bounded message-level conversion categories, independent of exception text. */
    public enum Reason {
        JSON_SYNTAX,
        NOT_NULL_MISSING,
        NOT_NULL_TYPE,
        NULLABLE_TYPE,
        UNKNOWN_FIELD,
        RESOURCE_LIMIT,
        INVALID_ENCODING;

        /** Returns the bit used when one message encounters several categories. */
        public int mask() {
            return 1 << ordinal();
        }
    }

    private final Reason reason;

    /** Creates an uncategorized record encoding exception. */
    public KafkaRecordEncodingException(String message) {
        this(Reason.INVALID_ENCODING, message, null);
    }

    /** Creates an uncategorized record encoding exception with a cause. */
    public KafkaRecordEncodingException(String message, Throwable cause) {
        this(Reason.INVALID_ENCODING, message, cause);
    }

    /** Creates a categorized record encoding exception. */
    public KafkaRecordEncodingException(Reason reason, String message) {
        this(reason, message, null);
    }

    /** Creates a categorized record encoding exception with a cause. */
    public KafkaRecordEncodingException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    /** Returns the stable message error category. */
    public Reason reason() {
        return reason;
    }
}
