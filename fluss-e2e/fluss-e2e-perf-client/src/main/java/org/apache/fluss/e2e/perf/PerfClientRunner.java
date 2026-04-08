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

package org.apache.fluss.e2e.perf;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.e2e.perf.config.ColumnConfig;
import org.apache.fluss.e2e.perf.config.DataConfig;
import org.apache.fluss.e2e.perf.config.DataTypeParser;
import org.apache.fluss.e2e.perf.config.PhaseType;
import org.apache.fluss.e2e.perf.config.ScenarioConfig;
import org.apache.fluss.e2e.perf.config.ScenarioValidator;
import org.apache.fluss.e2e.perf.config.TableConfig;
import org.apache.fluss.e2e.perf.config.WorkloadPhaseConfig;
import org.apache.fluss.e2e.perf.engine.LookupExecutor;
import org.apache.fluss.e2e.perf.engine.MixedExecutor;
import org.apache.fluss.e2e.perf.engine.PhaseExecutor;
import org.apache.fluss.e2e.perf.engine.PrefixLookupExecutor;
import org.apache.fluss.e2e.perf.engine.ScanExecutor;
import org.apache.fluss.e2e.perf.engine.WriteExecutor;
import org.apache.fluss.e2e.perf.stats.LatencyRecorder;
import org.apache.fluss.e2e.perf.stats.ThroughputCounter;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.fluss.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Entry point for the perf-client Docker container.
 *
 * <p>Reads a scenario YAML from a mounted volume, connects to the Fluss cluster via {@code
 * BOOTSTRAP_SERVERS} env var, creates the table, runs workload phases sequentially, and writes
 * results JSON to the output directory.
 *
 * <p>This is a simplified version of the original {@code YamlDrivenEngine} — cluster management,
 * resource sampling, and report generation are handled externally by Docker Compose, Prometheus,
 * and the Python CLI respectively.
 */
public class PerfClientRunner {

    private static final Logger LOG = LoggerFactory.getLogger(PerfClientRunner.class);
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final String DEFAULT_DATABASE = "fluss";
    private static final String DEFAULT_SCENARIO_FILE = "/perf/scenario.yaml";
    private static final String DEFAULT_OUTPUT_DIR = "/perf/output";

    public static void main(String[] args) {
        try {
            int exitCode = run();
            System.exit(exitCode);
        } catch (Exception e) {
            LOG.error("Fatal error in PerfClientRunner", e);
            System.exit(ExitCode.EXECUTION_ERROR.code());
        }
    }

    static int run() throws Exception {
        // 1. Resolve scenario file path
        String scenarioFile =
                System.getenv("SCENARIO_FILE") != null
                        ? System.getenv("SCENARIO_FILE")
                        : DEFAULT_SCENARIO_FILE;
        Path scenarioPath = Paths.get(scenarioFile);
        if (!Files.exists(scenarioPath)) {
            LOG.error("Scenario file not found: {}", scenarioPath);
            return ExitCode.CONFIG_ERROR.code();
        }

        // 2. Parse and validate scenario
        String yaml = new String(Files.readAllBytes(scenarioPath), StandardCharsets.UTF_8);
        ScenarioConfig config = ScenarioConfig.parse(yaml);
        List<String> errors = ScenarioValidator.validate(config);
        if (!errors.isEmpty()) {
            LOG.error("Scenario validation failed:");
            for (String err : errors) {
                LOG.error("  - {}", err);
            }
            return ExitCode.CONFIG_ERROR.code();
        }

        // 3. Resolve bootstrap servers
        String bootstrapServers =
                System.getenv("BOOTSTRAP_SERVERS") != null
                        ? System.getenv("BOOTSTRAP_SERVERS")
                        : "coordinator-server:9123";
        LOG.info(
                "Starting PerfClientRunner: scenario={}, bootstrap={}",
                scenarioPath,
                bootstrapServers);

        // 4. Create connection
        Configuration clientConf = new Configuration();
        clientConf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
        if (config.client() != null && config.client().properties() != null) {
            for (Map.Entry<String, String> entry : config.client().properties().entrySet()) {
                clientConf.setString(entry.getKey(), entry.getValue());
            }
        }

        PrintStream jsonLinesOut = System.out;
        String outputDir =
                System.getenv("OUTPUT_DIR") != null
                        ? System.getenv("OUTPUT_DIR")
                        : DEFAULT_OUTPUT_DIR;

        List<Map<String, Object>> phaseResults = new ArrayList<>();
        String status = "complete";
        String errorMsg = null;
        long overallStartNanos = System.nanoTime();

        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(clientConf);

            // 5. Create table
            long tableId = createTable(conn, config);
            waitForTableReady(conn, config.table().name(), 60_000);
            LOG.info("Table created and ready (id={})", tableId);
            emitEvent(jsonLinesOut, "table_ready", null, null);

            // 6. Execute workload phases
            List<WorkloadPhaseConfig> phases = config.workload();
            if (phases != null) {
                DataConfig dataConfig = config.data() != null ? config.data() : new DataConfig();
                for (int i = 0; i < phases.size(); i++) {
                    WorkloadPhaseConfig phase = phases.get(i);
                    String phaseName = phase.phase();
                    emitEvent(jsonLinesOut, "phase_start", phaseName, null);
                    try {
                        Map<String, Object> result =
                                executePhaseWithRetry(
                                        conn, config.table(), dataConfig, phase, jsonLinesOut);
                        phaseResults.add(result);
                        emitEvent(jsonLinesOut, "phase_end", phaseName, null);
                    } catch (Exception e) {
                        LOG.error("Phase '{}' failed", phaseName, e);
                        emitEvent(jsonLinesOut, "phase_error", phaseName, e.getMessage());
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Perf run failed", e);
            status = "partial";
            errorMsg = e.getMessage();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close connection", e);
                }
            }
        }

        long overallElapsedNanos = System.nanoTime() - overallStartNanos;

        // 7. Write results JSON
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("status", status);
        results.put("elapsedMs", TimeUnit.NANOSECONDS.toMillis(overallElapsedNanos));
        results.put("phases", phaseResults);
        if (errorMsg != null) {
            results.put("error", errorMsg);
        }

        Path outputPath = Paths.get(outputDir);
        Files.createDirectories(outputPath);
        Path resultsFile = outputPath.resolve("results.json");
        String json = MAPPER.writeValueAsString(results);
        Files.write(resultsFile, json.getBytes(StandardCharsets.UTF_8));
        LOG.info("Results written to {}", resultsFile);

        emitEvent(jsonLinesOut, "complete", null, null);

        return "partial".equals(status) ? ExitCode.EXECUTION_ERROR.code() : ExitCode.SUCCESS.code();
    }

    // -------------------------------------------------------------------------
    //  Table creation
    // -------------------------------------------------------------------------

    private static long createTable(Connection conn, ScenarioConfig config) throws Exception {
        TableConfig tableConfig = config.table();
        try (Admin admin = conn.getAdmin()) {
            admin.createDatabase(DEFAULT_DATABASE, DatabaseDescriptor.EMPTY, true).get();

            Schema.Builder schemaBuilder = Schema.newBuilder();
            for (ColumnConfig col : tableConfig.columns()) {
                DataType dataType = DataTypeParser.parse(col.type());
                if (col.agg() != null && col.agg().function() != null) {
                    AggFunctionType aggType = AggFunctionType.fromString(col.agg().function());
                    if (aggType == null) {
                        throw new IllegalArgumentException(
                                "Unknown aggregation function: " + col.agg().function());
                    }
                    AggFunction aggFunction = AggFunctions.of(aggType, col.agg().args());
                    schemaBuilder.column(col.name(), dataType, aggFunction);
                } else {
                    schemaBuilder.column(col.name(), dataType);
                }
            }
            if (tableConfig.hasPrimaryKey()) {
                schemaBuilder.primaryKey(tableConfig.primaryKey());
            }
            Schema schema = schemaBuilder.build();

            TableDescriptor.Builder descriptorBuilder = TableDescriptor.builder().schema(schema);
            if (tableConfig.buckets() != null) {
                List<String> bucketKeys =
                        tableConfig.bucketKeys() != null
                                ? tableConfig.bucketKeys()
                                : Collections.emptyList();
                descriptorBuilder.distributedBy(tableConfig.buckets(), bucketKeys);
            }
            if (tableConfig.mergeEngine() != null && !tableConfig.mergeEngine().isEmpty()) {
                MergeEngineType mergeEngineType =
                        MergeEngineType.fromString(tableConfig.mergeEngine());
                descriptorBuilder.property(ConfigOptions.TABLE_MERGE_ENGINE, mergeEngineType);
            }
            if (tableConfig.properties() != null) {
                descriptorBuilder.properties(tableConfig.properties());
            }
            if (tableConfig.logFormat() != null && !tableConfig.logFormat().isEmpty()) {
                descriptorBuilder.property(
                        ConfigOptions.TABLE_LOG_FORMAT,
                        LogFormat.valueOf(tableConfig.logFormat().toUpperCase()));
            }
            if (tableConfig.kvFormat() != null && !tableConfig.kvFormat().isEmpty()) {
                descriptorBuilder.property(
                        ConfigOptions.TABLE_KV_FORMAT,
                        KvFormat.valueOf(tableConfig.kvFormat().toUpperCase()));
            }

            TableDescriptor descriptor = descriptorBuilder.build();
            TablePath tablePath = TablePath.of(DEFAULT_DATABASE, tableConfig.name());
            admin.createTable(tablePath, descriptor, false).get();
            LOG.info("Created table {}", tablePath);

            return admin.getTableInfo(tablePath).get().getTableId();
        }
    }

    private static void waitForTableReady(Connection conn, String tableName, long timeoutMs)
            throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DATABASE, tableName);
        long deadline = System.currentTimeMillis() + timeoutMs;

        try (Admin admin = conn.getAdmin()) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    TableInfo info = admin.getTableInfo(tablePath).get(1, TimeUnit.SECONDS);
                    if (info.getNumBuckets() > 0) {
                        LOG.info("Table {} ready with {} buckets", tablePath, info.getNumBuckets());
                        return;
                    }
                } catch (Exception e) {
                    LOG.debug("Waiting for table ready: {}", e.getMessage());
                }
                Thread.sleep(500);
            }
        }
        throw new TimeoutException("Table " + tablePath + " not ready after " + timeoutMs + "ms");
    }

    // -------------------------------------------------------------------------
    //  Phase execution
    // -------------------------------------------------------------------------

    private static Map<String, Object> executePhaseWithRetry(
            Connection conn,
            TableConfig tableConfig,
            DataConfig dataConfig,
            WorkloadPhaseConfig phase,
            PrintStream jsonLinesOut)
            throws Exception {
        int maxRetries = phase.maxRetries() != null ? phase.maxRetries() : 0;
        for (int attempt = 1; ; attempt++) {
            try {
                return executePhase(conn, tableConfig, dataConfig, phase, jsonLinesOut);
            } catch (Exception e) {
                if (attempt > maxRetries) {
                    throw e;
                }
                LOG.warn(
                        "Phase '{}' failed on attempt {}/{}, retrying: {}",
                        phase.phase(),
                        attempt,
                        maxRetries,
                        e.getMessage());
                Thread.sleep(2000);
            }
        }
    }

    private static Map<String, Object> executePhase(
            Connection conn,
            TableConfig tableConfig,
            DataConfig dataConfig,
            WorkloadPhaseConfig phase,
            PrintStream jsonLinesOut)
            throws Exception {

        int threads = phase.threads() != null ? phase.threads() : 1;
        long totalRecords = phase.records() != null ? phase.records() : Long.MAX_VALUE;

        LatencyRecorder latencyRecorder = new LatencyRecorder();
        ThroughputCounter throughputCounter = new ThroughputCounter();

        PhaseExecutor executor = createExecutor(phase.phase());

        // Start periodic progress reporter (1 event/sec via json-lines)
        ScheduledExecutorService progressReporter =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "progress-reporter");
                            t.setDaemon(true);
                            return t;
                        });
        final long phaseStartNanos = System.nanoTime();
        progressReporter.scheduleAtFixedRate(
                () ->
                        emitProgress(
                                jsonLinesOut,
                                phase.phase(),
                                latencyRecorder,
                                throughputCounter,
                                phaseStartNanos),
                1,
                1,
                TimeUnit.SECONDS);

        long startNanos = System.nanoTime();

        if (threads == 1) {
            executor.execute(
                    conn,
                    tableConfig,
                    dataConfig,
                    phase,
                    latencyRecorder,
                    throughputCounter,
                    0,
                    totalRecords);
        } else {
            boolean isScan = PhaseType.fromString(phase.phase()) == PhaseType.SCAN;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                long perThread = totalRecords / threads;
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    long start = t * perThread;
                    long end = (t == threads - 1) ? totalRecords : start + perThread;
                    PhaseExecutor threadExecutor = isScan ? new ScanExecutor(t) : executor;
                    futures.add(
                            pool.submit(
                                    () -> {
                                        try {
                                            threadExecutor.execute(
                                                    conn,
                                                    tableConfig,
                                                    dataConfig,
                                                    phase,
                                                    latencyRecorder,
                                                    throughputCounter,
                                                    start,
                                                    end);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                        return null;
                                    }));
                }
                Duration phaseDuration = parseDuration(phase.duration());
                long futureTimeoutMs =
                        phaseDuration != null
                                ? phaseDuration.toMillis() + 60_000
                                : TimeUnit.HOURS.toMillis(1);
                for (Future<?> f : futures) {
                    try {
                        f.get(futureTimeoutMs, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        LOG.warn(
                                "Phase '{}' thread timed out after {}ms",
                                phase.phase(),
                                futureTimeoutMs);
                    } catch (ExecutionException e) {
                        LOG.warn(
                                "Phase '{}' thread failed: {}",
                                phase.phase(),
                                e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                    }
                }
            } finally {
                pool.shutdown();
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    LOG.warn("Thread pool did not terminate within 60s, forcing shutdown");
                    pool.shutdownNow();
                    pool.awaitTermination(10, TimeUnit.SECONDS);
                }
            }
        }

        long elapsedNanos = System.nanoTime() - startNanos;

        // Stop progress reporter
        progressReporter.shutdownNow();
        try {
            if (!progressReporter.awaitTermination(2, TimeUnit.SECONDS)) {
                LOG.warn("Progress reporter did not terminate within 2s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Build phase result map
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("phase", phase.phase());
        result.put("elapsedMs", elapsedMs);
        result.put("totalOps", throughputCounter.totalOps());
        result.put("totalBytes", throughputCounter.totalBytes());
        result.put(
                "opsPerSec", String.format("%.1f", throughputCounter.opsPerSecond(elapsedNanos)));
        result.put(
                "bytesPerSec",
                String.format("%.1f", throughputCounter.bytesPerSecond(elapsedNanos)));
        result.put("p50Ms", String.format("%.2f", latencyRecorder.p50Nanos() / 1e6));
        result.put("p95Ms", String.format("%.2f", latencyRecorder.p95Nanos() / 1e6));
        result.put("p99Ms", String.format("%.2f", latencyRecorder.p99Nanos() / 1e6));
        result.put("p999Ms", String.format("%.2f", latencyRecorder.p999Nanos() / 1e6));
        result.put("maxMs", String.format("%.2f", latencyRecorder.maxNanos() / 1e6));

        return result;
    }

    // -------------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------------

    private static PhaseExecutor createExecutor(String phaseType) {
        PhaseType type = PhaseType.fromString(phaseType);
        switch (type) {
            case WRITE:
                return new WriteExecutor();
            case LOOKUP:
                return new LookupExecutor();
            case PREFIX_LOOKUP:
                return new PrefixLookupExecutor();
            case SCAN:
                return new ScanExecutor();
            case MIXED:
                return new MixedExecutor();
            default:
                throw new IllegalArgumentException("Unknown phase type: " + phaseType);
        }
    }

    private static Duration parseDuration(String durationStr) {
        if (durationStr == null || durationStr.isEmpty()) {
            return null;
        }
        return org.apache.fluss.utils.TimeUtils.parseDuration(durationStr.trim());
    }

    private static void emitProgress(
            PrintStream out,
            String phase,
            LatencyRecorder latency,
            ThroughputCounter throughput,
            long phaseStartNanos) {
        try {
            long elapsedNanos = System.nanoTime() - phaseStartNanos;
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("event", "progress");
            map.put("timestamp", System.currentTimeMillis());
            map.put("phase", phase);
            map.put("elapsedSec", String.format("%.1f", elapsedNanos / 1e9));
            map.put("totalOps", throughput.totalOps());
            map.put("totalBytes", throughput.totalBytes());
            map.put("opsPerSec", String.format("%.1f", throughput.opsPerSecond(elapsedNanos)));
            map.put("bytesPerSec", String.format("%.1f", throughput.bytesPerSecond(elapsedNanos)));
            map.put("p50Ms", String.format("%.2f", latency.p50Nanos() / 1e6));
            map.put("p99Ms", String.format("%.2f", latency.p99Nanos() / 1e6));
            map.put("maxMs", String.format("%.2f", latency.maxNanos() / 1e6));
            out.println(MAPPER.writeValueAsString(map));
            out.flush();
        } catch (Exception e) {
            LOG.debug("Failed to emit progress event", e);
        }
    }

    private static void emitEvent(PrintStream out, String event, String phase, String message) {
        try {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("event", event);
            map.put("timestamp", System.currentTimeMillis());
            if (phase != null) {
                map.put("phase", phase);
            }
            if (message != null) {
                map.put("message", message);
            }
            out.println(MAPPER.writeValueAsString(map));
            out.flush();
        } catch (Exception e) {
            LOG.warn("Failed to emit json-lines event", e);
        }
    }
}
