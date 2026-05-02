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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.types.DataTypes;

import javax.annotation.Nullable;

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

    private static final TablePath CONSUMER_OFFSETS_TABLE_PATH =
            TablePath.of("sys", "consumer_offsets");
    private static final int REQUIRED_ACKS = -1;
    private static final int TIMEOUT_MS = 30000;
    private static final int PAGE_SIZE = 1024 * 64;

    private final ReplicaManager replicaManager;
    @Nullable private final TabletServerMetadataCache metadataCache;
    @Nullable private final MetadataManager metadataManager;
    @Nullable private volatile Long consumerOffsetsTableId;
    private volatile short consumerOffsetsSchemaId;
    @Nullable private volatile Schema consumerOffsetsSchema;

    public ReplicaManagerKvWriter(ReplicaManager replicaManager, long consumerOffsetsTableId) {
        this(replicaManager, null, null, Long.valueOf(consumerOffsetsTableId), (short) 0, null);
    }

    public ReplicaManagerKvWriter(
            ReplicaManager replicaManager,
            MetadataManager metadataManager,
            @Nullable Long consumerOffsetsTableId) {
        this(replicaManager, null, metadataManager, consumerOffsetsTableId, (short) 0, null);
    }

    public ReplicaManagerKvWriter(
            ReplicaManager replicaManager,
            TabletServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            @Nullable TableInfo consumerOffsetsTableInfo) {
        this(
                replicaManager,
                metadataCache,
                metadataManager,
                consumerOffsetsTableInfo == null
                        ? null
                        : Long.valueOf(consumerOffsetsTableInfo.getTableId()),
                consumerOffsetsTableInfo == null
                        ? (short) 0
                        : (short) consumerOffsetsTableInfo.getSchemaId(),
                consumerOffsetsTableInfo == null ? null : consumerOffsetsTableInfo.getSchema());
    }

    private ReplicaManagerKvWriter(
            ReplicaManager replicaManager,
            @Nullable TabletServerMetadataCache metadataCache,
            @Nullable MetadataManager metadataManager,
            @Nullable Long consumerOffsetsTableId,
            short consumerOffsetsSchemaId,
            @Nullable Schema consumerOffsetsSchema) {
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.consumerOffsetsTableId = consumerOffsetsTableId;
        this.metadataManager = metadataManager;
        this.consumerOffsetsSchemaId = consumerOffsetsSchemaId;
        this.consumerOffsetsSchema = consumerOffsetsSchema;
    }

    @Override
    public CompletableFuture<Void> writeAndWaitForHwm(
            int bucketId, List<CoordinatorResult.KvRecord> records) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            ConsumerOffsetsTableMetadata tableMetadata = resolveConsumerOffsetsTableMetadata();
            if (metadataCache != null) {
                metadataCache.updateLatestSchema(
                        tableMetadata.tableId(),
                        new SchemaInfo(tableMetadata.schema(), tableMetadata.schemaId()));
            }
            TableBucket tableBucket = new TableBucket(tableMetadata.tableId(), bucketId);
            KvRecordBatch batch = buildKvRecordBatch(records, tableMetadata.schemaId());

            replicaManager.putRecordsToKv(
                    TIMEOUT_MS,
                    REQUIRED_ACKS,
                    Collections.singletonMap(tableBucket, batch),
                    null,
                    MergeMode.OVERWRITE,
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

    private ConsumerOffsetsTableMetadata resolveConsumerOffsetsTableMetadata() {
        Long tableId = consumerOffsetsTableId;
        Schema schema = consumerOffsetsSchema;
        if (tableId != null && schema != null) {
            return new ConsumerOffsetsTableMetadata(
                    tableId.longValue(), consumerOffsetsSchemaId, schema);
        }

        if (metadataManager == null) {
            throw new IllegalStateException(
                    "sys.consumer_offsets table metadata is unavailable and metadata manager is not configured.");
        }

        synchronized (this) {
            if (consumerOffsetsTableId == null || consumerOffsetsSchema == null) {
                try {
                    TableInfo tableInfo = metadataManager.getTable(CONSUMER_OFFSETS_TABLE_PATH);
                    consumerOffsetsTableId = tableInfo.getTableId();
                    consumerOffsetsSchemaId = (short) tableInfo.getSchemaId();
                    consumerOffsetsSchema = tableInfo.getSchema();
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Failed to resolve sys.consumer_offsets table metadata from metadata.",
                            e);
                }
            }

            return new ConsumerOffsetsTableMetadata(
                    consumerOffsetsTableId.longValue(),
                    consumerOffsetsSchemaId,
                    consumerOffsetsSchema);
        }
    }

    /**
     * Builds a {@link KvRecordBatch} from raw key-value byte pairs.
     *
     * <p>Each record's key is used as the KV primary key, and the value is encoded as a {@link
     * CompactedRow} with a single BYTES column. Null values (tombstones) pass null as the row.
     */
    KvRecordBatch buildKvRecordBatch(List<CoordinatorResult.KvRecord> records, short schemaId)
            throws Exception {
        UnmanagedPagedOutputView outputView = new UnmanagedPagedOutputView(PAGE_SIZE);
        KvRecordBatchBuilder builder =
                KvRecordBatchBuilder.builder(
                        schemaId, Integer.MAX_VALUE, outputView, KvFormat.COMPACTED);

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

    KvRecordBatch buildKvRecordBatch(List<CoordinatorResult.KvRecord> records) throws Exception {
        return buildKvRecordBatch(records, (short) 0);
    }

    private static final class ConsumerOffsetsTableMetadata {
        private final long tableId;
        private final short schemaId;
        private final Schema schema;

        private ConsumerOffsetsTableMetadata(long tableId, short schemaId, Schema schema) {
            this.tableId = tableId;
            this.schemaId = schemaId;
            this.schema = schema;
        }

        private long tableId() {
            return tableId;
        }

        private short schemaId() {
            return schemaId;
        }

        private Schema schema() {
            return schema;
        }
    }
}
