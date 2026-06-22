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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metrics.ScannerMetricGroup;

/**
 * Aggregates fetch-level metrics reported by {@link CompletedFetch} instances and forwards them to
 * the {@link ScannerMetricGroup}.
 *
 * <p>A {@link CompletedFetch} accumulates the bytes of the record batches it consumes locally and,
 * once it is drained (i.e. fully consumed), reports the aggregated value here in a single call,
 * rather than mutating a metric counter on every batch. This keeps {@link CompletedFetch} decoupled
 * from the concrete metric primitives.
 */
@Internal
class FetchLogMetricsAggregator {

    private final ScannerMetricGroup scannerMetricGroup;

    FetchLogMetricsAggregator(ScannerMetricGroup scannerMetricGroup) {
        this.scannerMetricGroup = scannerMetricGroup;
    }

    /** Records the total bytes of record batches consumed by a drained {@link CompletedFetch}. */
    void recordBytesConsumed(long bytes) {
        scannerMetricGroup.recordsBytesTotal().inc(bytes);
    }
}
