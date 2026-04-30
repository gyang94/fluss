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

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoordinatorResult}. */
class CoordinatorResultTest {

    @Test
    void testWithRecordsAndResponse() {
        CoordinatorResult.KvRecord record =
                new CoordinatorResult.KvRecord(new byte[] {1, 2}, new byte[] {3, 4});
        CoordinatorResult<String> result =
                new CoordinatorResult<>(Collections.singletonList(record), "ok");
        assertThat(result.records()).hasSize(1);
        assertThat(result.records().get(0).key()).containsExactly(1, 2);
        assertThat(result.records().get(0).value()).containsExactly(3, 4);
        assertThat(result.response()).isEqualTo("ok");
    }

    @Test
    void testEmptyRecords() {
        CoordinatorResult<Integer> result = new CoordinatorResult<>(Collections.emptyList(), 42);
        assertThat(result.records()).isEmpty();
        assertThat(result.response()).isEqualTo(42);
    }

    @Test
    void testMultipleRecords() {
        CoordinatorResult.KvRecord r1 =
                new CoordinatorResult.KvRecord(new byte[] {1}, new byte[] {2});
        CoordinatorResult.KvRecord r2 = new CoordinatorResult.KvRecord(new byte[] {3}, null);
        CoordinatorResult<String> result = new CoordinatorResult<>(Arrays.asList(r1, r2), "done");
        assertThat(result.records()).hasSize(2);
        assertThat(result.records().get(1).value()).isNull();
    }
}
