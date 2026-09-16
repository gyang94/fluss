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

import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.metrics.KafkaProduceMetrics;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException.Reason;
import org.apache.fluss.row.GenericRow;

import java.util.function.Consumer;

/** Decodes one message and observes only errors actually encountered during conversion. */
final class KafkaRowDecoder {
    private KafkaRowDecoder() {}

    static GenericRow decode(Record record, KafkaTopicWritePlan plan, KafkaProduceMetrics metrics) {
        int[] errors = new int[1];
        Consumer<Reason> observer = reason -> errors[0] |= reason.mask();
        boolean invalid = false;
        boolean rescued = false;
        try {
            Object[] keys = plan.keyDecoder().decode(record.borrowedKey(), observer);
            Object[] values = plan.valueDecoder().decode(record.borrowedValue(), observer);
            GenericRow row =
                    plan.rowAssembler()
                            .assemble(keys, values, record.timestamp(), record.headers());
            rescued = errors[0] != 0;
            return row;
        } catch (KafkaRecordEncodingException failure) {
            errors[0] |= failure.reason().mask();
            invalid = true;
            throw failure;
        } finally {
            try {
                metrics.recordConversion(errors[0], rescued, invalid);
            } catch (Throwable ignored) {
                // Observational metrics must not affect row conversion or buffer ownership.
            }
        }
    }
}
