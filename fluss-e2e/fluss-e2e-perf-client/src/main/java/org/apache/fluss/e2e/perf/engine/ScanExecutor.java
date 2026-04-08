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

package org.apache.fluss.e2e.perf.engine;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.e2e.perf.config.DataConfig;
import org.apache.fluss.e2e.perf.config.TableConfig;
import org.apache.fluss.e2e.perf.config.WorkloadPhaseConfig;
import org.apache.fluss.e2e.perf.stats.LatencyRecorder;
import org.apache.fluss.e2e.perf.stats.ThroughputCounter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Executor for scan workload phases. Creates a LogScanner, subscribes to all buckets from the
 * configured offset, and polls in a loop.
 */
public class ScanExecutor implements PhaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ScanExecutor.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final int threadIndex;

    /** Creates a ScanExecutor for single-threaded use (thread index 0). */
    public ScanExecutor() {
        this(0);
    }

    /**
     * Creates a ScanExecutor with an explicit thread index for bucket partitioning.
     *
     * @param threadIndex the zero-based index of this thread among all scan threads
     */
    public ScanExecutor(int threadIndex) {
        this.threadIndex = threadIndex;
    }

    @Override
    public void execute(
            Connection conn,
            TableConfig tableConfig,
            DataConfig dataConfig,
            WorkloadPhaseConfig phaseConfig,
            LatencyRecorder latencyRecorder,
            ThroughputCounter throughputCounter,
            long startIndex,
            long endIndex)
            throws Exception {

        long totalRecords = endIndex - startIndex;
        long warmupOps = ExecutorUtils.parseWarmupOps(phaseConfig.warmup(), totalRecords);
        Duration duration = ExecutorUtils.parseDuration(phaseConfig.duration());

        long fromOffset = parseFromOffset(phaseConfig.fromOffset());

        // Pre-compute estimated row size from column types for throughput calculation
        int estimatedRowBytes = ExecutorUtils.estimateRowBytesFromSchema(tableConfig.columns());

        TablePath tablePath = TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name());

        try (Table table = conn.getTable(tablePath)) {
            TableInfo tableInfo = table.getTableInfo();
            int numBuckets = tableInfo.getNumBuckets();

            try (LogScanner scanner = table.newScan().createLogScanner()) {
                // Partition buckets across threads using the configured thread count and
                // the explicit threadIndex assigned at construction time.
                int totalThreads = phaseConfig.threads() != null ? phaseConfig.threads() : 1;

                int bucketsAssigned = 0;
                for (int bucket = 0; bucket < numBuckets; bucket++) {
                    if (bucket % totalThreads == threadIndex) {
                        scanner.subscribe(bucket, fromOffset);
                        bucketsAssigned++;
                    }
                }
                LOG.info(
                        "Scan thread {} subscribed to {}/{} buckets",
                        threadIndex,
                        bucketsAssigned,
                        numBuckets);

                if (bucketsAssigned == 0) {
                    // More threads than buckets — this thread has nothing to scan.
                    return;
                }

                long startTime = System.nanoTime();
                long opsCount = 0;
                int consecutiveEmptyPolls = 0;
                final int maxEmptyPolls = 20; // Stop after 20 consecutive empty polls (10s)

                while (opsCount < totalRecords) {
                    if (duration != null && ExecutorUtils.isExpired(startTime, duration)) {
                        break;
                    }

                    long pollStart = System.nanoTime();
                    ScanRecords records = scanner.poll(POLL_TIMEOUT);
                    long pollLatency = System.nanoTime() - pollStart;

                    if (records.isEmpty()) {
                        consecutiveEmptyPolls++;
                        if (consecutiveEmptyPolls >= maxEmptyPolls) {
                            LOG.warn(
                                    "No data after {} consecutive polls ({}ms), stopping early",
                                    consecutiveEmptyPolls,
                                    consecutiveEmptyPolls * POLL_TIMEOUT.toMillis());
                            break;
                        }
                        continue;
                    }

                    // Reset counter on successful poll
                    consecutiveEmptyPolls = 0;

                    // Count records in this batch and estimate bytes
                    int batchSize = 0;
                    for (ScanRecord ignored : records) {
                        batchSize++;
                        opsCount++;
                        if (opsCount >= totalRecords) {
                            break;
                        }
                    }

                    // Record poll latency as batch-level metric (not amortized per record).
                    // This represents the true poll-to-delivery latency for each batch.
                    if (opsCount > warmupOps && batchSize > 0) {
                        latencyRecorder.record(pollLatency);
                        throughputCounter.record((long) batchSize * estimatedRowBytes);
                    }
                }
            }
        }
    }

    static long parseFromOffset(String fromOffset) {
        if (fromOffset == null || fromOffset.isEmpty() || "earliest".equalsIgnoreCase(fromOffset)) {
            return LogScanner.EARLIEST_OFFSET;
        }
        if ("latest".equalsIgnoreCase(fromOffset)) {
            throw new IllegalArgumentException(
                    "from-offset=latest is not supported by LogScanner. "
                            + "Use 'earliest' or an explicit numeric offset.");
        }
        return Long.parseLong(fromOffset);
    }
}
