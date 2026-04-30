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

/** Tests for {@link OffsetAndMetadata}. */
class OffsetAndMetadataTest {

    @Test
    void testBasicProperties() {
        OffsetAndMetadata oam = new OffsetAndMetadata(100L, 5, "meta", 1234567890L);
        assertThat(oam.offset()).isEqualTo(100L);
        assertThat(oam.leaderEpoch()).isEqualTo(5);
        assertThat(oam.metadata()).isEqualTo("meta");
        assertThat(oam.commitTimestampMs()).isEqualTo(1234567890L);
    }

    @Test
    void testEmptySentinel() {
        assertThat(OffsetAndMetadata.EMPTY.offset()).isEqualTo(-1L);
        assertThat(OffsetAndMetadata.EMPTY.leaderEpoch()).isEqualTo(-1);
        assertThat(OffsetAndMetadata.EMPTY.metadata()).isEmpty();
        assertThat(OffsetAndMetadata.EMPTY.commitTimestampMs()).isEqualTo(-1L);
    }

    @Test
    void testEquality() {
        OffsetAndMetadata oam1 = new OffsetAndMetadata(100L, 5, "meta", 1234567890L);
        OffsetAndMetadata oam2 = new OffsetAndMetadata(100L, 5, "meta", 1234567890L);
        OffsetAndMetadata oam3 = new OffsetAndMetadata(200L, 5, "meta", 1234567890L);
        assertThat(oam1).isEqualTo(oam2);
        assertThat(oam1).isNotEqualTo(oam3);
        assertThat(oam1.hashCode()).isEqualTo(oam2.hashCode());
    }

    @Test
    void testToString() {
        OffsetAndMetadata oam = new OffsetAndMetadata(100L, 5, "meta", 1234567890L);
        assertThat(oam.toString()).contains("100").contains("5").contains("meta");
    }
}
