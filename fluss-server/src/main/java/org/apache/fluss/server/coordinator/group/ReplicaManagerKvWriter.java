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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.types.DataTypes;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Bridges {@link CoordinatorRuntime} write operations to {@link ReplicaManager}'s KV write path.
 *
 * <p>Converts raw key-value byte pairs (from {@link OffsetKeyValueCodec}) into {@link
 * KvRecordBatch} and writes via {@link ReplicaManager#putRecordsToKv} with requiredAcks=-1 for
 * HWM-deferred completion.
 *
 * <p>The sys.consumer_offsets table schema has a single value column: offset_value BYTES. The
 * encoded key from OffsetKeyValueCodec is used as the KV primary key, and the encoded value is
 * stored as a CompactedRow with a single BYTES column.
 */
@Internal
public class ReplicaManagerKvWriter implements CoordinatorRuntime.KvWriter {

    private static final int REQUIRED_ACKS = -1;
    private static final int TIMEOUT_MS = 30000;
    private static final short SCHEMA_ID = 0;
    private static final int PAGE_SIZE = 1024 * 64;

    private final ReplicaManager replicaManager;
    private final long consumerOffsetsTableId;

    public ReplicaManagerKvWriter(ReplicaManager replicaManager, long consumerOffsetsTableId) {
        this.replicaManager = replicaManager;
        this.consumerOffsetsTableId = consumerOffsetsTableId;
    }

    @Override
    public CompletableFuture<Void> writeAndWaitForHwm(
            int bucketId, List<CoordinatorResult.KvRecord> records) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            TableBucket tableBucket = new TableBucket(consumerOffsetsTableId, bucketId);
            KvRecordBatch batch = buildKvRecordBatch(records);

            replicaManager.putRecordsToKv(
                    TIMEOUT_MS,
                    REQUIRED_ACKS,
                    Collections.singletonMap(tableBucket, batch),
                    null,
                    MergeMode.DEFAULT,
                    (short) 0,
                    bucketResults -> {
                        for (PutKvResultForBucket result : bucketResults) {
                            if (result.failed()) {
                                future.completeExceptionally(result.getError().exception());
                                return;
                            }
                        }
                        future.complete(null);
                    });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * Builds a {@link KvRecordBatch} from raw key-value byte pairs.
     *
     * <p>Each record's key is used as the KV primary key, and the value is encoded as a {@link
     * CompactedRow} with a single BYTES column. Null values (tombstones) pass null as the row.
     */
    KvRecordBatch buildKvRecordBatch(List<CoordinatorResult.KvRecord> records) throws Exception {
        UnmanagedPagedOutputView outputView = new UnmanagedPagedOutputView(PAGE_SIZE);
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        SCHEMA_ID, Integer.MAX_VALUE, outputView, KvFormat.COMPACTED);

        CompactedRow reusableRow =
                new CompactedRow(new org.apache.fluss.types.DataType[] {DataTypes.BYTES()});
        CompactedRowWriter rowWriter = new CompactedRowWriter(1);

        for (CoordinatorResult.KvRecord record : records) {
            if (record.value() == null) {
                builder.append(record.key(), null);
            } else {
                rowWriter.reset();
                rowWriter.writeBytes(record.value());
                reusableRow.pointTo(rowWriter.segment(), 0, rowWriter.position());
                builder.append(record.key(), reusableRow);
            }
        }

        return DefaultKvRecordBatch.pointToBytesView(builder.build());
    }
}
