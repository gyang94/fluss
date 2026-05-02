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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Unit test for {@link LogScannerStatus}. */
class LogScannerStatusTest {

    @Test
    void testAllCommittableOffsetsOnlyIncludesValidPositions() {
        LogScannerStatus status = new LogScannerStatus();
        TableBucket bucket0 = new TableBucket(DATA1_TABLE_ID, 0);
        TableBucket bucket1 = new TableBucket(DATA1_TABLE_ID, 1);
        TableBucket bucket2 = new TableBucket(DATA1_TABLE_ID, 2);

        Map<TableBucket, Long> assignedOffsets = new LinkedHashMap<>();
        assignedOffsets.put(bucket0, LogScanner.EARLIEST_OFFSET);
        assignedOffsets.put(bucket1, 5L);
        assignedOffsets.put(bucket2, 8L);
        status.assignScanBuckets(assignedOffsets);

        assertThat(status.allCommittableOffsets())
                .containsOnly(entry(bucket1, 5L), entry(bucket2, 8L));

        status.updateOffset(bucket2, -1L);
        assertThat(status.allCommittableOffsets()).containsOnly(entry(bucket1, 5L));

        status.unassignScanBuckets(Arrays.asList(bucket1));
        assertThat(status.allCommittableOffsets()).isEmpty();
    }
}
