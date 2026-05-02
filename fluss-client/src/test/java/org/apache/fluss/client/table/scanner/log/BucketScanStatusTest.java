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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link BucketScanStatus}. */
class BucketScanStatusTest {

    @Test
    void testHasValidPositionTracksOffset() {
        BucketScanStatus status = new BucketScanStatus(LogScanner.EARLIEST_OFFSET);
        assertThat(status.hasValidPosition()).isFalse();

        status.setOffset(-1L);
        assertThat(status.hasValidPosition()).isFalse();

        status.setOffset(0L);
        assertThat(status.hasValidPosition()).isTrue();
        assertThat(status.getOffset()).isEqualTo(0L);

        status.setOffset(10L);
        assertThat(status.hasValidPosition()).isTrue();
        assertThat(status.getOffset()).isEqualTo(10L);
    }
}
