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

package org.apache.fluss.server.coordinator.group;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.BytesUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ReplicaManagerKvWriter}. */
class ReplicaManagerKvWriterTest {

    private static final Schema CONSUMER_OFFSETS_SCHEMA =
            Schema.newBuilder()
                    .column("offset_key", DataTypes.BYTES())
                    .column("offset_value", DataTypes.BYTES())
                    .primaryKey("offset_key")
                    .build();

    @Test
    void testBuildKvRecordBatchWithSingleRecord() throws Exception {
        ReplicaManagerKvWriter writer = new ReplicaManagerKvWriter(null, 1L);

        byte[] key = new byte[] {1, 2, 3};
        byte[] value = new byte[] {4, 5, 6};
        List<CoordinatorResult.KvRecord> records =
                Collections.singletonList(new CoordinatorResult.KvRecord(key, value));

        KvRecordBatch batch = writer.buildKvRecordBatch(records);

        assertThat(batch.getRecordCount()).isEqualTo(1);
        batch.ensureValid();

        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(0, CONSUMER_OFFSETS_SCHEMA));

        List<KvRecord> readRecords = new ArrayList<>();
        for (KvRecord record : batch.records(readContext)) {
            readRecords.add(record);
        }
        assertThat(readRecords).hasSize(1);
        assertThat(BytesUtils.toArray(readRecords.get(0).getKey())).isEqualTo(key);

        BinaryRow row = readRecords.get(0).getRow();
        assertThat(row).isNotNull();
        assertThat(row.getBytes(0)).isEqualTo(value);
    }

    @Test
    void testBuildKvRecordBatchWithMultipleRecords() throws Exception {
        ReplicaManagerKvWriter writer = new ReplicaManagerKvWriter(null, 1L);

        byte[] key1 = new byte[] {10, 20};
        byte[] value1 = new byte[] {30, 40};
        byte[] key2 = new byte[] {50, 60};
        byte[] value2 = new byte[] {70, 80};

        List<CoordinatorResult.KvRecord> records =
                Arrays.asList(
                        new CoordinatorResult.KvRecord(key1, value1),
                        new CoordinatorResult.KvRecord(key2, value2));

        KvRecordBatch batch = writer.buildKvRecordBatch(records);

        assertThat(batch.getRecordCount()).isEqualTo(2);
        batch.ensureValid();

        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(0, CONSUMER_OFFSETS_SCHEMA));

        List<KvRecord> readRecords = new ArrayList<>();
        for (KvRecord record : batch.records(readContext)) {
            readRecords.add(record);
        }
        assertThat(readRecords).hasSize(2);

        assertThat(BytesUtils.toArray(readRecords.get(0).getKey())).isEqualTo(key1);
        assertThat(readRecords.get(0).getRow()).isNotNull();
        assertThat(readRecords.get(0).getRow().getBytes(0)).isEqualTo(value1);

        assertThat(BytesUtils.toArray(readRecords.get(1).getKey())).isEqualTo(key2);
        assertThat(readRecords.get(1).getRow()).isNotNull();
        assertThat(readRecords.get(1).getRow().getBytes(0)).isEqualTo(value2);
    }

    @Test
    void testBuildKvRecordBatchWithTombstone() throws Exception {
        ReplicaManagerKvWriter writer = new ReplicaManagerKvWriter(null, 1L);

        byte[] key = new byte[] {1, 2, 3};
        List<CoordinatorResult.KvRecord> records =
                Collections.singletonList(new CoordinatorResult.KvRecord(key, null));

        KvRecordBatch batch = writer.buildKvRecordBatch(records);

        assertThat(batch.getRecordCount()).isEqualTo(1);
        batch.ensureValid();

        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(0, CONSUMER_OFFSETS_SCHEMA));

        List<KvRecord> readRecords = new ArrayList<>();
        for (KvRecord record : batch.records(readContext)) {
            readRecords.add(record);
        }
        assertThat(readRecords).hasSize(1);
        assertThat(BytesUtils.toArray(readRecords.get(0).getKey())).isEqualTo(key);
        assertThat(readRecords.get(0).getRow()).isNull();
    }

    @Test
    void testBuildKvRecordBatchWithEmptyRecords() throws Exception {
        ReplicaManagerKvWriter writer = new ReplicaManagerKvWriter(null, 1L);

        List<CoordinatorResult.KvRecord> records = Collections.emptyList();

        KvRecordBatch batch = writer.buildKvRecordBatch(records);

        assertThat(batch.getRecordCount()).isEqualTo(0);
        batch.ensureValid();
    }
}
