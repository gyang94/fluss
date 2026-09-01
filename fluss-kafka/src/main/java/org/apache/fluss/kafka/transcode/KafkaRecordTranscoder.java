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
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.bytesview.BytesView;

import java.util.List;

/** Converts copied Kafka records into the native Fluss log representation. */
@Internal
public interface KafkaRecordTranscoder {
    /** Resolves or reuses the immutable conversion plan for the target table version. */
    KafkaTopicWritePlan prepare(TableInfo tableInfo);

    /** Transcodes records using a previously prepared immutable write plan. */
    default BytesView transcode(List<Record> records, KafkaTopicWritePlan writePlan)
            throws Exception {
        return transcode(records, writePlan, KafkaOutputMemoryBudget.UNBOUNDED);
    }

    /**
     * Transcodes records while reserving converted output storage before physical allocation.
     *
     * <p>Every implementation must explicitly account for retained output through the supplied
     * budget before physical allocation. Implementations that retain no output may ignore the
     * budget, but must still implement this method so a future transcoder cannot silently bypass
     * production admission.
     */
    BytesView transcode(
            List<Record> records,
            KafkaTopicWritePlan writePlan,
            KafkaOutputMemoryBudget outputMemoryBudget)
            throws Exception;

    /**
     * Prepares and transcodes records in one call.
     *
     * <p>This convenience method is retained for component callers. Produce backends should prepare
     * once per topic and share the returned plan across all partitions.
     */
    default BytesView transcode(List<Record> records, TableInfo tableInfo) throws Exception {
        return transcode(records, prepare(tableInfo));
    }

    /** Invalidates a compiled plan when an authoritative metadata notification is available. */
    default void invalidate(TableInfo tableInfo) {}
}
