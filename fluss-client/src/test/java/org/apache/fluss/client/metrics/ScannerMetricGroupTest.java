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

package org.apache.fluss.client.metrics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ScannerMetricGroup}. */
class ScannerMetricGroupTest {

    @Test
    void testFetchBytesTotal() {
        ScannerMetricGroup metricGroup = TestingScannerMetricGroup.newInstance();
        assertThat(metricGroup.fetchBytesTotal().getCount()).isEqualTo(0);

        metricGroup.fetchBytesTotal().inc(1024);
        assertThat(metricGroup.fetchBytesTotal().getCount()).isEqualTo(1024);

        metricGroup.fetchBytesTotal().inc(2048);
        assertThat(metricGroup.fetchBytesTotal().getCount()).isEqualTo(3072);
    }

    @Test
    void testRecordsBytesTotal() {
        ScannerMetricGroup metricGroup = TestingScannerMetricGroup.newInstance();
        assertThat(metricGroup.recordsBytesTotal().getCount()).isEqualTo(0);

        metricGroup.recordsBytesTotal().inc(512);
        assertThat(metricGroup.recordsBytesTotal().getCount()).isEqualTo(512);

        metricGroup.recordsBytesTotal().inc(256);
        assertThat(metricGroup.recordsBytesTotal().getCount()).isEqualTo(768);
    }
}
