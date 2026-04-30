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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OffsetKeyValueCodec}. */
class OffsetKeyValueCodecTest {

    @Test
    void testKeyRoundTrip() {
        byte[] key = OffsetKeyValueCodec.encodeKey("my-group", 42L, null, 7);
        OffsetKeyValueCodec.DecodedKey decoded = OffsetKeyValueCodec.decodeKey(key);
        assertThat(decoded.groupId()).isEqualTo("my-group");
        assertThat(decoded.tableBucket().getTableId()).isEqualTo(42L);
        assertThat(decoded.tableBucket().getPartitionId()).isNull();
        assertThat(decoded.tableBucket().getBucket()).isEqualTo(7);
    }

    @Test
    void testKeyRoundTripWithPartition() {
        byte[] key = OffsetKeyValueCodec.encodeKey("group-2", 100L, 55L, 3);
        OffsetKeyValueCodec.DecodedKey decoded = OffsetKeyValueCodec.decodeKey(key);
        assertThat(decoded.groupId()).isEqualTo("group-2");
        assertThat(decoded.tableBucket().getTableId()).isEqualTo(100L);
        assertThat(decoded.tableBucket().getPartitionId()).isEqualTo(55L);
        assertThat(decoded.tableBucket().getBucket()).isEqualTo(3);
    }

    @Test
    void testValueRoundTrip() {
        OffsetAndMetadata original = new OffsetAndMetadata(999L, 3, "user-meta", 1234567890L);
        byte[] value = OffsetKeyValueCodec.encodeValue(original);
        OffsetAndMetadata decoded = OffsetKeyValueCodec.decodeValue(value);
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void testValueRoundTripNoMetadata() {
        OffsetAndMetadata original = new OffsetAndMetadata(0L, -1, "", 9999L);
        byte[] value = OffsetKeyValueCodec.encodeValue(original);
        OffsetAndMetadata decoded = OffsetKeyValueCodec.decodeValue(value);
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void testKeyWithEmptyGroupId() {
        byte[] key = OffsetKeyValueCodec.encodeKey("", 1L, null, 0);
        OffsetKeyValueCodec.DecodedKey decoded = OffsetKeyValueCodec.decodeKey(key);
        assertThat(decoded.groupId()).isEmpty();
    }

    @Test
    void testKeyWithUnicodeGroupId() {
        byte[] key = OffsetKeyValueCodec.encodeKey("组-group-日本語", 10L, null, 2);
        OffsetKeyValueCodec.DecodedKey decoded = OffsetKeyValueCodec.decodeKey(key);
        assertThat(decoded.groupId()).isEqualTo("组-group-日本語");
    }
}
