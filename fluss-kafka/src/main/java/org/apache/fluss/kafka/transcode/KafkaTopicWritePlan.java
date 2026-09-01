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
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.schema.KafkaTopicSchema;
import org.apache.fluss.metadata.TableInfo;

import javax.annotation.concurrent.ThreadSafe;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Immutable, shareable conversion plan compiled from one version of a Kafka topic table.
 *
 * <p>The plan contains only read-only schema, decoder, and row-assembly state. Arrow writers and
 * allocators are deliberately not part of this object because those resources require exclusive
 * leases and an explicit lifecycle.
 */
@Internal
@ThreadSafe
public final class KafkaTopicWritePlan {

    private final TableInfo tableInfo;
    private final KafkaTopicSchema topicSchema;
    private final KafkaFieldDecoder keyDecoder;
    private final KafkaFieldDecoder valueDecoder;
    private final KafkaRowAssembler rowAssembler;

    KafkaTopicWritePlan(
            TableInfo tableInfo,
            KafkaTopicSchema topicSchema,
            KafkaFieldDecoder keyDecoder,
            KafkaFieldDecoder valueDecoder,
            KafkaRowAssembler rowAssembler) {
        this.tableInfo = checkNotNull(tableInfo);
        this.topicSchema = checkNotNull(topicSchema);
        this.keyDecoder = checkNotNull(keyDecoder);
        this.valueDecoder = checkNotNull(valueDecoder);
        this.rowAssembler = checkNotNull(rowAssembler);
    }

    TableInfo tableInfo() {
        return tableInfo;
    }

    KafkaTopicSchema topicSchema() {
        return topicSchema;
    }

    KafkaFieldDecoder keyDecoder() {
        return keyDecoder;
    }

    KafkaFieldDecoder valueDecoder() {
        return valueDecoder;
    }

    KafkaRowAssembler rowAssembler() {
        return rowAssembler;
    }
}
