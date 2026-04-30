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

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Codec for encoding and decoding keys and values of the {@code sys.consumer_offsets} KV table.
 *
 * <p>Key format: {@code [version:2][groupIdLen:2][groupId:N][tableId:8][hasPartition:1]
 * [partitionId:8 if present][bucketId:4]}
 *
 * <p>Value format: {@code [version:2][offset:8][leaderEpoch:4][commitTimestamp:8]
 * [metadataLen:2][metadata:N]}
 */
@Internal
public final class OffsetKeyValueCodec {

    private static final short KEY_VERSION = 0;
    private static final short VALUE_VERSION = 0;

    private OffsetKeyValueCodec() {}

    // ------------------------------------------------------------------------
    //  Key encoding / decoding
    // ------------------------------------------------------------------------

    /** Encodes a consumer offset key into a byte array. */
    public static byte[] encodeKey(
            String groupId, long tableId, @Nullable Long partitionId, int bucketId) {
        byte[] groupIdBytes = groupId.getBytes(StandardCharsets.UTF_8);
        boolean hasPartition = partitionId != null;

        // version(2) + groupIdLen(2) + groupId(N) + tableId(8) + hasPartition(1)
        // + partitionId(8 if present) + bucketId(4)
        int size = 2 + 2 + groupIdBytes.length + 8 + 1 + (hasPartition ? 8 : 0) + 4;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putShort(KEY_VERSION);
        buffer.putShort((short) groupIdBytes.length);
        buffer.put(groupIdBytes);
        buffer.putLong(tableId);
        buffer.put((byte) (hasPartition ? 1 : 0));
        if (hasPartition) {
            buffer.putLong(partitionId);
        }
        buffer.putInt(bucketId);

        return buffer.array();
    }

    /** Decodes a consumer offset key from a byte array. */
    public static DecodedKey decodeKey(byte[] key) {
        ByteBuffer buffer = ByteBuffer.wrap(key);

        // version — reserved for future format changes
        buffer.getShort();

        short groupIdLen = buffer.getShort();
        byte[] groupIdBytes = new byte[groupIdLen];
        buffer.get(groupIdBytes);
        String groupId = new String(groupIdBytes, StandardCharsets.UTF_8);

        long tableId = buffer.getLong();

        boolean hasPartition = buffer.get() != 0;
        Long partitionId = hasPartition ? buffer.getLong() : null;

        int bucketId = buffer.getInt();

        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
        return new DecodedKey(groupId, tableBucket);
    }

    // ------------------------------------------------------------------------
    //  Value encoding / decoding
    // ------------------------------------------------------------------------

    /** Encodes an {@link OffsetAndMetadata} into a byte array. */
    public static byte[] encodeValue(OffsetAndMetadata oam) {
        byte[] metadataBytes = oam.metadata().getBytes(StandardCharsets.UTF_8);

        // version(2) + offset(8) + leaderEpoch(4) + commitTimestamp(8) + metadataLen(2) +
        // metadata(N)
        int size = 2 + 8 + 4 + 8 + 2 + metadataBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putShort(VALUE_VERSION);
        buffer.putLong(oam.offset());
        buffer.putInt(oam.leaderEpoch());
        buffer.putLong(oam.commitTimestampMs());
        buffer.putShort((short) metadataBytes.length);
        buffer.put(metadataBytes);

        return buffer.array();
    }

    /** Decodes a byte array into an {@link OffsetAndMetadata}. */
    public static OffsetAndMetadata decodeValue(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value);

        // version — reserved for future format changes
        buffer.getShort();

        long offset = buffer.getLong();
        int leaderEpoch = buffer.getInt();
        long commitTimestampMs = buffer.getLong();

        short metadataLen = buffer.getShort();
        byte[] metadataBytes = new byte[metadataLen];
        buffer.get(metadataBytes);
        String metadata = new String(metadataBytes, StandardCharsets.UTF_8);

        return new OffsetAndMetadata(offset, leaderEpoch, metadata, commitTimestampMs);
    }

    // ------------------------------------------------------------------------
    //  Inner classes
    // ------------------------------------------------------------------------

    /** Decoded representation of a consumer offset key. */
    public static final class DecodedKey {

        private final String groupId;
        private final TableBucket tableBucket;

        DecodedKey(String groupId, TableBucket tableBucket) {
            this.groupId = groupId;
            this.tableBucket = tableBucket;
        }

        public String groupId() {
            return groupId;
        }

        public TableBucket tableBucket() {
            return tableBucket;
        }

        @Override
        public String toString() {
            return "DecodedKey{groupId='" + groupId + "', tableBucket=" + tableBucket + '}';
        }
    }
}
