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
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.bytesview.BytesView;

import java.util.List;

/** Converts borrowed Kafka records into the native Fluss log representation. */
@Internal
public interface KafkaRecordTranscoder {
    /**
     * Validates an authoritative table snapshot and prepares a plan that partitions can share.
     *
     * <p>Callers must treat the snapshot as immutable and supply a new instance whenever the table
     * schema, properties or identity changes. Implementations may cache plans for unchanged
     * snapshots. A prepared plan describes that snapshot only; it does not refresh metadata.
     */
    default KafkaTopicWritePlan prepare(TableInfo tableInfo) {
        return new KafkaTopicWritePlan(
                tableInfo, new KafkaTopicSchemaResolver().resolve(tableInfo.toTableDescriptor()));
    }

    /**
     * Transcodes one requested partition using an already prepared plan. The borrowed input and
     * independently owned output follow the same lifetime contract as {@link #transcode(int, List,
     * TableInfo)}.
     */
    default BytesView transcode(int partitionId, List<Record> records, KafkaTopicWritePlan plan)
            throws Exception {
        return transcode(partitionId, records, plan.tableInfo());
    }

    /**
     * Transcodes records from the requested Kafka partition according to the target Fluss table
     * schema and log format.
     *
     * <p>The table snapshot must follow the immutability contract of {@link #prepare(TableInfo)}.
     *
     * <p>The caller keeps input storage alive and unchanged until this method returns or throws.
     * The returned bytes own their storage and remain valid after the input buffers are released.
     */
    BytesView transcode(int partitionId, List<Record> records, TableInfo tableInfo)
            throws Exception;
}
