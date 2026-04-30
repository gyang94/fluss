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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.CommitOffsetsRequest;
import org.apache.fluss.rpc.messages.CommitOffsetsResponse;
import org.apache.fluss.rpc.messages.FetchOffsetsRequest;
import org.apache.fluss.rpc.messages.FetchOffsetsResponse;
import org.apache.fluss.rpc.messages.PbCommitOffsetEntry;
import org.apache.fluss.rpc.messages.PbCommitOffsetResultEntry;
import org.apache.fluss.rpc.messages.PbFetchOffsetEntry;
import org.apache.fluss.rpc.messages.PbFetchOffsetResultEntry;
import org.apache.fluss.rpc.protocol.Errors;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages the in-memory cache of consumer group offsets and provides commit/fetch/replay
 * operations.
 *
 * <p>Offsets are organized by group ID and {@link TableBucket}. The {@link #commitOffset} method
 * builds KV records for persistence but does not write to storage directly. The {@link #replay}
 * method applies committed records into the in-memory cache.
 */
@Internal
public class OffsetMetadataManager {

    /** groupId -> { TableBucket -> OffsetAndMetadata }. */
    private final Map<String, Map<TableBucket, OffsetAndMetadata>> offsets;

    public OffsetMetadataManager() {
        this.offsets = new HashMap<>();
    }

    /**
     * Builds KV records for an offset commit request. Does NOT write to storage.
     *
     * @param request the commit offsets request
     * @return a result containing the KV records and the response
     */
    public CoordinatorResult<CommitOffsetsResponse> commitOffset(CommitOffsetsRequest request) {
        List<CoordinatorResult.KvRecord> records = new ArrayList<>();
        CommitOffsetsResponse response = new CommitOffsetsResponse();
        String groupId = request.getGroupId();

        for (int i = 0; i < request.getOffsetsCount(); i++) {
            PbCommitOffsetEntry entry = request.getOffsetAt(i);
            long tableId = entry.getTableId();
            Long partitionId = entry.hasPartitionId() ? entry.getPartitionId() : null;
            int bucketId = entry.getBucketId();

            PbCommitOffsetResultEntry resultEntry = response.addResult();
            resultEntry.setTableId(tableId);
            if (partitionId != null) {
                resultEntry.setPartitionId(partitionId);
            }
            resultEntry.setBucketId(bucketId);

            if (entry.getOffset() < 0) {
                resultEntry.setErrorCode(Errors.INVALID_COMMIT_OFFSET_EXCEPTION.code());
                continue;
            }

            byte[] key = OffsetKeyValueCodec.encodeKey(groupId, tableId, partitionId, bucketId);
            OffsetAndMetadata oam =
                    new OffsetAndMetadata(
                            entry.getOffset(),
                            entry.getLeaderEpoch(),
                            entry.hasMetadata() ? entry.getMetadata() : "",
                            System.currentTimeMillis());
            byte[] value = OffsetKeyValueCodec.encodeValue(oam);
            records.add(new CoordinatorResult.KvRecord(key, value));

            resultEntry.setErrorCode(Errors.NONE.code());
        }

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Fetches offsets from the in-memory cache for a given group.
     *
     * <p>If the request has no entries, all offsets for the group are returned. Otherwise, only the
     * requested buckets are returned.
     *
     * @param groupId the consumer group ID
     * @param request the fetch offsets request
     * @return the response with offset information
     */
    public FetchOffsetsResponse fetchOffsets(String groupId, FetchOffsetsRequest request) {
        FetchOffsetsResponse response = new FetchOffsetsResponse();
        Map<TableBucket, OffsetAndMetadata> groupOffsets =
                offsets.getOrDefault(
                        groupId, Collections.<TableBucket, OffsetAndMetadata>emptyMap());

        if (request.getEntriesCount() == 0) {
            for (Map.Entry<TableBucket, OffsetAndMetadata> mapEntry : groupOffsets.entrySet()) {
                TableBucket tb = mapEntry.getKey();
                OffsetAndMetadata oam = mapEntry.getValue();
                PbFetchOffsetResultEntry resultEntry = response.addResult();
                resultEntry.setTableId(tb.getTableId());
                if (tb.getPartitionId() != null) {
                    resultEntry.setPartitionId(tb.getPartitionId());
                }
                resultEntry.setBucketId(tb.getBucket());
                resultEntry.setOffset(oam.offset());
                resultEntry.setLeaderEpoch(oam.leaderEpoch());
                resultEntry.setMetadata(oam.metadata());
                resultEntry.setErrorCode(Errors.NONE.code());
            }
        } else {
            for (int i = 0; i < request.getEntriesCount(); i++) {
                PbFetchOffsetEntry entry = request.getEntryAt(i);
                long tableId = entry.getTableId();
                Long partitionId = entry.hasPartitionId() ? entry.getPartitionId() : null;
                int bucketId = entry.getBucketId();

                TableBucket tb = new TableBucket(tableId, partitionId, bucketId);
                OffsetAndMetadata oam = groupOffsets.get(tb);

                PbFetchOffsetResultEntry resultEntry = response.addResult();
                resultEntry.setTableId(tableId);
                if (partitionId != null) {
                    resultEntry.setPartitionId(partitionId);
                }
                resultEntry.setBucketId(bucketId);

                if (oam != null) {
                    resultEntry.setOffset(oam.offset());
                    resultEntry.setLeaderEpoch(oam.leaderEpoch());
                    resultEntry.setMetadata(oam.metadata());
                } else {
                    resultEntry.setOffset(OffsetAndMetadata.EMPTY.offset());
                    resultEntry.setLeaderEpoch(OffsetAndMetadata.EMPTY.leaderEpoch());
                    resultEntry.setMetadata(OffsetAndMetadata.EMPTY.metadata());
                }
                resultEntry.setErrorCode(Errors.NONE.code());
            }
        }

        return response;
    }

    /**
     * Replays a KV record into the in-memory cache.
     *
     * <p>This is called during loading from storage and after successful writes. If the value is
     * {@code null} (tombstone), the offset is removed from the cache.
     *
     * @param key the encoded offset key
     * @param value the encoded offset value, or {@code null} for a tombstone
     */
    public void replay(byte[] key, @Nullable byte[] value) {
        OffsetKeyValueCodec.DecodedKey decodedKey = OffsetKeyValueCodec.decodeKey(key);
        String groupId = decodedKey.groupId();
        TableBucket tableBucket = decodedKey.tableBucket();

        if (value == null) {
            Map<TableBucket, OffsetAndMetadata> groupOffsets = offsets.get(groupId);
            if (groupOffsets != null) {
                groupOffsets.remove(tableBucket);
                if (groupOffsets.isEmpty()) {
                    offsets.remove(groupId);
                }
            }
        } else {
            OffsetAndMetadata oam = OffsetKeyValueCodec.decodeValue(value);
            offsets.computeIfAbsent(groupId, k -> new HashMap<>()).put(tableBucket, oam);
        }
    }
}
