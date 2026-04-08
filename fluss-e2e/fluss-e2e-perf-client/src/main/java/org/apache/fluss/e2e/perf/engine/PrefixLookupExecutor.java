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
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.e2e.perf.config.ColumnConfig;
import org.apache.fluss.e2e.perf.config.DataConfig;
import org.apache.fluss.e2e.perf.config.TableConfig;
import org.apache.fluss.e2e.perf.config.WorkloadPhaseConfig;
import org.apache.fluss.e2e.perf.datagen.FieldGenerator;
import org.apache.fluss.e2e.perf.stats.LatencyRecorder;
import org.apache.fluss.e2e.perf.stats.ThroughputCounter;
import org.apache.fluss.metadata.TablePath;

import java.time.Duration;
import java.util.List;
import java.util.Random;

/**
 * Executor for prefix-lookup workload phases. Creates a prefix lookuper where {@code
 * key-prefix-length} determines how many PK columns to use as the lookup prefix. Uses async
 * pipelined lookups via {@link LookupExecutor#asyncLookupLoop}.
 */
public class PrefixLookupExecutor implements PhaseExecutor {

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

        List<ColumnConfig> columns = tableConfig.columns();
        List<String> pkColumns = tableConfig.primaryKey();
        if (!tableConfig.hasPrimaryKey()) {
            throw new IllegalArgumentException("Prefix lookup requires a primary key table");
        }

        int prefixLength =
                phaseConfig.keyPrefixLength() != null
                        ? phaseConfig.keyPrefixLength()
                        : pkColumns.size();
        if (prefixLength > pkColumns.size()) {
            throw new IllegalArgumentException(
                    "key-prefix-length ("
                            + prefixLength
                            + ") exceeds primary key column count ("
                            + pkColumns.size()
                            + ")");
        }

        FieldGenerator[] generators = ExecutorUtils.buildGenerators(columns, dataConfig);

        List<String> prefixColumns = pkColumns.subList(0, prefixLength);
        int[] prefixIndexes = ExecutorUtils.resolvePkIndexes(prefixColumns, columns);

        long warmupOps = ExecutorUtils.parseWarmupOps(phaseConfig.warmup(), endIndex - startIndex);
        Long rateLimit = phaseConfig.rateLimit();
        Duration duration = ExecutorUtils.parseDuration(phaseConfig.duration());

        long keyMin = ExecutorUtils.resolveKeyMin(phaseConfig.keyRange(), startIndex);
        long keyMax = ExecutorUtils.resolveKeyMax(phaseConfig.keyRange(), endIndex);

        Random random = ExecutorUtils.createSeededRandom(dataConfig, startIndex);

        TablePath tablePath = TablePath.of(ExecutorUtils.DEFAULT_DATABASE, tableConfig.name());

        try (Table table = conn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().lookupBy(prefixColumns).createLookuper();
            int keyBytes = ExecutorUtils.estimateKeyBytes(columns, prefixIndexes);

            LookupExecutor.LookupContext ctx =
                    new LookupExecutor.LookupContext(
                            generators,
                            columns,
                            prefixIndexes,
                            latencyRecorder,
                            throughputCounter,
                            startIndex,
                            endIndex,
                            warmupOps,
                            rateLimit,
                            duration,
                            keyMin,
                            keyMax,
                            keyBytes,
                            random);
            LookupExecutor.asyncLookupLoop(lookuper, ctx);
        }
    }
}
