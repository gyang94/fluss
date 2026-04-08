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

package org.apache.fluss.e2e;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Entry point for the Fluss E2E Java bridge. */
public final class ClientRunner {

    private static final String DEFAULT_DATABASE = "e2e";
    private static final int DEFAULT_BUCKETS = 4;
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    private ClientRunner() {}

    public static void main(String[] args) {
        int exitCode = run(args, ClientRunner::dispatch, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    @FunctionalInterface
    interface Dispatcher {
        Map<String, Object> dispatch(String[] args) throws Exception;
    }

    static int run(String[] args, Dispatcher dispatcher, PrintStream stdout, PrintStream stderr) {
        String command = commandLabel(args);
        PrintStream originalOut = System.out;
        ByteArrayOutputStream capturedStdout = new ByteArrayOutputStream();
        Map<String, Object> payload;
        int exitCode = 0;

        try (PrintStream interceptedOut =
                new PrintStream(capturedStdout, true, StandardCharsets.UTF_8)) {
            System.setOut(interceptedOut);
            try {
                payload = dispatcher.dispatch(args);
            } catch (CommandException exception) {
                payload = JsonOutput.failure(command, exception);
                exitCode = exception.exitCode();
            } catch (Throwable throwable) {
                payload = JsonOutput.unexpectedFailure(command, throwable);
                exitCode = 1;
            }
        } finally {
            System.setOut(originalOut);
        }

        String bufferedStdout = capturedStdout.toString(StandardCharsets.UTF_8);
        if (!bufferedStdout.isBlank()) {
            stderr.print(bufferedStdout);
        }
        JsonOutput.print(payload, stdout);
        return exitCode;
    }

    static String commandLabel(String[] args) {
        if (args == null || args.length == 0) {
            return "unknown";
        }
        if (args.length > 1
                && ("cluster".equals(args[0])
                        || "admin".equals(args[0])
                        || "flink".equals(args[0]))) {
            return args[0] + " " + args[1];
        }
        return args[0];
    }

    static Map<String, Object> dispatch(String[] args) throws Exception {
        if (args.length == 0) {
            throw CommandException.invalidArgument("No command was provided.");
        }

        switch (args[0]) {
            case "cluster":
                return handleCluster(Arrays.copyOfRange(args, 1, args.length));
            case "admin":
                return handleAdmin(Arrays.copyOfRange(args, 1, args.length));
            case "write":
                return handleWrite(Arrays.copyOfRange(args, 1, args.length));
            case "scan":
                return handleScan(Arrays.copyOfRange(args, 1, args.length));
            case "flink":
                return handleFlink(Arrays.copyOfRange(args, 1, args.length));
            default:
                throw CommandException.invalidArgument("Unknown command: " + args[0]);
        }
    }

    private static Map<String, Object> handleCluster(String[] args) throws Exception {
        if (args.length == 0 || !"ping".equals(args[0])) {
            throw CommandException.invalidArgument("Expected `cluster ping`.");
        }

        ParsedArgs parsed = ParsedArgs.parse(Arrays.copyOfRange(args, 1, args.length));
        String bootstrapServers = parsed.bootstrapServers();
        int timeoutSeconds = parsed.intOption("timeout-seconds", DEFAULT_TIMEOUT_SECONDS);

        try (E2eClientContext context = E2eClientContext.open(bootstrapServers)) {
            List<ServerNode> nodes =
                    context.admin().getServerNodes().get(timeoutSeconds, TimeUnit.SECONDS);
            if (nodes.isEmpty()) {
                throw new CommandException(
                        "ClusterUnavailable", "No server nodes were discovered.", 1);
            }

            Map<String, Object> payload = JsonOutput.success("cluster ping");
            List<Map<String, Object>> serverPayload = new ArrayList<>();
            for (ServerNode node : nodes) {
                Map<String, Object> server = new LinkedHashMap<>();
                server.put("uid", node.uid());
                server.put("id", node.id());
                server.put("type", node.serverType().name());
                server.put("host", node.host());
                server.put("port", node.port());
                if (node.rack() != null) {
                    server.put("rack", node.rack());
                }
                serverPayload.add(server);
            }
            payload.put("servers", serverPayload);
            payload.put("server_count", serverPayload.size());
            return payload;
        }
    }

    private static Map<String, Object> handleAdmin(String[] args) throws Exception {
        if (args.length == 0) {
            throw CommandException.invalidArgument("Missing admin subcommand.");
        }

        ParsedArgs parsed = ParsedArgs.parse(Arrays.copyOfRange(args, 1, args.length));
        String bootstrapServers = parsed.bootstrapServers();
        String database = parsed.stringOption("database", DEFAULT_DATABASE);
        String tableName = parsed.stringOption("table", null);
        int timeoutSeconds = parsed.intOption("timeout-seconds", DEFAULT_TIMEOUT_SECONDS);

        try (E2eClientContext context = E2eClientContext.open(bootstrapServers)) {
            switch (args[0]) {
                case "list-tables":
                    Map<String, Object> listPayload = JsonOutput.success("admin list-tables");
                    listPayload.put(
                            "tables",
                            context.admin()
                                    .listTables(database)
                                    .get(timeoutSeconds, TimeUnit.SECONDS));
                    listPayload.put("database", database);
                    return listPayload;
                case "create-table":
                    String createTableName = required(tableName, "Missing --table.");
                    int buckets = parsed.intOption("buckets", DEFAULT_BUCKETS);
                    Integer replicationFactor =
                            parsed.hasOption("replication-factor")
                                    ? parsed.intOption("replication-factor", 0)
                                    : null;
                    validatePositive(
                            replicationFactor,
                            "replication-factor",
                            "Option --replication-factor expects a positive integer.");
                    context.admin()
                            .createDatabase(database, DatabaseDescriptor.EMPTY, true)
                            .get(timeoutSeconds, TimeUnit.SECONDS);
                    TablePath createTablePath = TablePath.of(database, createTableName);
                    context.admin()
                            .createTable(
                                    createTablePath,
                                    Rows.defaultTableDescriptor(buckets, replicationFactor),
                                    true)
                            .get(timeoutSeconds, TimeUnit.SECONDS);
                    TableInfo createdTable =
                            context.admin()
                                    .getTableInfo(createTablePath)
                                    .get(timeoutSeconds, TimeUnit.SECONDS);
                    Map<String, Object> createPayload = JsonOutput.success("admin create-table");
                    createPayload.put("database", database);
                    createPayload.put("table", createTableName);
                    createPayload.put("table_id", createdTable.getTableId());
                    createPayload.put("bucket_count", createdTable.getNumBuckets());
                    createPayload.put("schema_id", createdTable.getSchemaId());
                    if (createdTable
                            .getProperties()
                            .contains(ConfigOptions.TABLE_REPLICATION_FACTOR)) {
                        createPayload.put(
                                "replication_factor",
                                createdTable.getTableConfig().getReplicationFactor());
                    }
                    return createPayload;
                case "drop-table":
                    String dropTableName = required(tableName, "Missing --table.");
                    TablePath dropPath = TablePath.of(database, dropTableName);
                    context.admin().dropTable(dropPath, true).get(timeoutSeconds, TimeUnit.SECONDS);
                    Map<String, Object> dropPayload = JsonOutput.success("admin drop-table");
                    dropPayload.put("database", database);
                    dropPayload.put("table", dropTableName);
                    return dropPayload;
                case "get-table":
                    String getTableName = required(tableName, "Missing --table.");
                    TableInfo tableInfo =
                            context.admin()
                                    .getTableInfo(TablePath.of(database, getTableName))
                                    .get(timeoutSeconds, TimeUnit.SECONDS);
                    Map<String, Object> getPayload = JsonOutput.success("admin get-table");
                    getPayload.putAll(tablePayload(tableInfo));
                    return getPayload;
                case "alter-table":
                    String alterTableName = required(tableName, "Missing --table.");
                    String addColumnName =
                            required(
                                    parsed.stringOption("add-column", null),
                                    "Missing --add-column.");
                    String columnTypeText =
                            required(
                                    parsed.stringOption("column-type", null),
                                    "Missing --column-type.");
                    DataType columnType = parseColumnType(columnTypeText);
                    TablePath alterPath = TablePath.of(database, alterTableName);
                    context.admin()
                            .alterTable(
                                    alterPath,
                                    List.of(
                                            TableChange.addColumn(
                                                    addColumnName,
                                                    columnType,
                                                    null,
                                                    TableChange.ColumnPosition.last())),
                                    false)
                            .get(timeoutSeconds, TimeUnit.SECONDS);
                    TableInfo alteredTableInfo =
                            context.admin()
                                    .getTableInfo(alterPath)
                                    .get(timeoutSeconds, TimeUnit.SECONDS);
                    Map<String, Object> alterPayload = JsonOutput.success("admin alter-table");
                    alterPayload.putAll(tablePayload(alteredTableInfo));
                    alterPayload.put("added_column", addColumnName);
                    alterPayload.put("added_column_type", columnType.asSerializableString());
                    return alterPayload;
                case "rebalance":
                    List<GoalType> goals = parseGoalTypes(parsed.stringOption("goals", ""));
                    String rebalanceId =
                            context.admin().rebalance(goals).get(timeoutSeconds, TimeUnit.SECONDS);
                    Map<String, Object> rebalancePayload = JsonOutput.success("admin rebalance");
                    rebalancePayload.put("rebalance_id", rebalanceId);
                    rebalancePayload.put("goals", goalNames(goals));
                    return rebalancePayload;
                case "list-rebalance-progress":
                    String rebalanceProgressId = parsed.stringOption("rebalance-id", null);
                    Optional<RebalanceProgress> progress =
                            context.admin()
                                    .listRebalanceProgress(rebalanceProgressId)
                                    .get(timeoutSeconds, TimeUnit.SECONDS);
                    Map<String, Object> progressPayload =
                            JsonOutput.success("admin list-rebalance-progress");
                    progressPayload.put("present", progress.isPresent());
                    if (rebalanceProgressId != null) {
                        progressPayload.put("requested_rebalance_id", rebalanceProgressId);
                    }
                    if (progress.isPresent()) {
                        progressPayload.putAll(rebalanceProgressPayload(progress.get()));
                    }
                    return progressPayload;
                default:
                    throw CommandException.invalidArgument("Unknown admin subcommand: " + args[0]);
            }
        }
    }

    private static Map<String, Object> handleWrite(String[] args) throws Exception {
        ParsedArgs parsed = ParsedArgs.parse(args);
        String bootstrapServers = parsed.bootstrapServers();
        String database = parsed.stringOption("database", DEFAULT_DATABASE);
        String tableName = required(parsed.stringOption("table", null), "Missing --table.");
        int count = parsed.intOption("count", 1000);
        int startId = parsed.intOption("start-id", 0);
        int timeoutSeconds = parsed.intOption("timeout-seconds", DEFAULT_TIMEOUT_SECONDS);
        validateNonNegative(count, "count", "Option --count expects a non-negative integer.");
        validateNonNegative(
                startId, "start-id", "Option --start-id expects a non-negative integer.");
        long startedNanos = System.nanoTime();

        TablePath tablePath = TablePath.of(database, tableName);
        try (E2eClientContext context = E2eClientContext.open(bootstrapServers);
                Table table = context.connection().getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int id = startId; id < startId + count; id++) {
                writer.upsert(Rows.defaultRow(id));
            }
            writer.flush();

            Map<String, Object> payload = JsonOutput.success("write");
            payload.put("database", database);
            payload.put("table", tableName);
            payload.put("count", count);
            payload.put("start_id", startId);
            payload.put("end_id_exclusive", startId + count);
            payload.put("checksum", Rows.expectedChecksum(startId, count));
            payload.put(
                    "row_count",
                    context.admin()
                            .getTableStats(tablePath)
                            .get(timeoutSeconds, TimeUnit.SECONDS)
                            .getRowCount());
            payload.put("latency_ms", elapsedMillis(startedNanos, System.nanoTime()));
            return payload;
        }
    }

    private static Map<String, Object> handleScan(String[] args) throws Exception {
        ParsedArgs parsed = ParsedArgs.parse(args);
        String bootstrapServers = parsed.bootstrapServers();
        String database = parsed.stringOption("database", DEFAULT_DATABASE);
        String tableName = required(parsed.stringOption("table", null), "Missing --table.");
        int timeoutSeconds = parsed.intOption("timeout-seconds", DEFAULT_TIMEOUT_SECONDS);
        long startedNanos = System.nanoTime();

        TablePath tablePath = TablePath.of(database, tableName);
        try (E2eClientContext context = E2eClientContext.open(bootstrapServers)) {
            TableInfo tableInfo =
                    context.admin().getTableInfo(tablePath).get(timeoutSeconds, TimeUnit.SECONDS);
            long rowCount =
                    context.admin()
                            .getTableStats(tablePath)
                            .get(timeoutSeconds, TimeUnit.SECONDS)
                            .getRowCount();
            int limit =
                    parsed.hasOption("limit")
                            ? parsed.intOption("limit", 0)
                            : Math.toIntExact(rowCount);

            try (Table table = context.connection().getTable(tablePath);
                    BatchScanner scanner = table.newScan().limit(limit).createBatchScanner()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                CloseableIterator<InternalRow> batch;
                while ((batch = scanner.pollBatch(Duration.ofSeconds(1))) != null) {
                    try (CloseableIterator<InternalRow> closeableBatch = batch) {
                        while (closeableBatch.hasNext()) {
                            rows.add(Rows.toJsonRow(closeableBatch.next(), tableInfo.getRowType()));
                        }
                    }
                }

                Map<String, Object> payload = JsonOutput.success("scan");
                payload.put("database", database);
                payload.put("table", tableName);
                payload.put("count", rows.size());
                payload.put("rows", rows);
                payload.put("checksum", Rows.checksum(rows));
                payload.put("latency_ms", elapsedMillis(startedNanos, System.nanoTime()));
                return payload;
            }
        }
    }

    private static final String FLINK_REST_BASE_URL = "http://localhost:8081";

    private static Map<String, Object> handleFlink(String[] args) throws Exception {
        if (args.length == 0) {
            throw CommandException.invalidArgument("Missing flink subcommand.");
        }

        switch (args[0]) {
            case "submit":
                return handleFlinkSubmit(Arrays.copyOfRange(args, 1, args.length));
            case "status":
                return handleFlinkStatus(Arrays.copyOfRange(args, 1, args.length));
            case "wait":
                return handleFlinkWait(Arrays.copyOfRange(args, 1, args.length));
            default:
                throw CommandException.invalidArgument("Unknown flink subcommand: " + args[0]);
        }
    }

    private static Map<String, Object> handleFlinkSubmit(String[] args) throws Exception {
        ParsedArgs parsed = ParsedArgs.parse(args);
        String jarPath = required(parsed.stringOption("jar", null), "Missing --jar.");
        String entryClass =
                required(parsed.stringOption("entry-class", null), "Missing --entry-class.");
        String programArgs = parsed.stringOption("args", null);

        HttpClient httpClient =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        // Step 1: Upload JAR
        Path jarFile = Path.of(jarPath);
        if (!Files.exists(jarFile)) {
            throw CommandException.invalidArgument("JAR file not found: " + jarPath);
        }

        String boundary = UUID.randomUUID().toString();
        byte[] jarBytes = Files.readAllBytes(jarFile);
        String fileName = jarFile.getFileName().toString();

        byte[] multipartBody = buildMultipartBody(boundary, fileName, jarBytes);

        HttpRequest uploadRequest =
                HttpRequest.newBuilder()
                        .uri(URI.create(FLINK_REST_BASE_URL + "/jars/upload"))
                        .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(multipartBody))
                        .timeout(Duration.ofSeconds(60))
                        .build();

        HttpResponse<String> uploadResponse =
                httpClient.send(uploadRequest, HttpResponse.BodyHandlers.ofString());
        if (uploadResponse.statusCode() != 200) {
            throw CommandException.runtimeFailure(
                    "Flink JAR upload failed with status "
                            + uploadResponse.statusCode()
                            + ": "
                            + uploadResponse.body(),
                    null);
        }

        Map<String, Object> uploadPayload = parseJsonResponse(uploadResponse.body());
        String uploadedFilename = (String) uploadPayload.get("filename");
        if (uploadedFilename == null) {
            throw CommandException.runtimeFailure(
                    "Flink JAR upload response missing filename field.", null);
        }
        String jarId = uploadedFilename;
        if (jarId.contains("/")) {
            jarId = jarId.substring(jarId.lastIndexOf('/') + 1);
        }

        // Step 2: Run JAR
        Map<String, Object> runBody = new LinkedHashMap<>();
        runBody.put("entryClass", entryClass);
        if (programArgs != null && !programArgs.isEmpty()) {
            runBody.put("programArgs", programArgs);
        }
        String runBodyJson =
                new org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper()
                        .writeValueAsString(runBody);

        HttpRequest runRequest =
                HttpRequest.newBuilder()
                        .uri(URI.create(FLINK_REST_BASE_URL + "/jars/" + jarId + "/run"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(runBodyJson))
                        .timeout(Duration.ofSeconds(60))
                        .build();

        HttpResponse<String> runResponse =
                httpClient.send(runRequest, HttpResponse.BodyHandlers.ofString());
        if (runResponse.statusCode() != 200) {
            throw CommandException.runtimeFailure(
                    "Flink job run failed with status "
                            + runResponse.statusCode()
                            + ": "
                            + runResponse.body(),
                    null);
        }

        Map<String, Object> runPayload = parseJsonResponse(runResponse.body());
        String jobId = (String) runPayload.get("jobid");
        if (jobId == null) {
            throw CommandException.runtimeFailure(
                    "Flink job run response missing jobid field.", null);
        }

        Map<String, Object> payload = JsonOutput.success("flink submit");
        payload.put("job_id", jobId);
        payload.put("jar_id", jarId);
        payload.put("entry_class", entryClass);
        if (programArgs != null) {
            payload.put("program_args", programArgs);
        }
        return payload;
    }

    private static Map<String, Object> handleFlinkStatus(String[] args) throws Exception {
        ParsedArgs parsed = ParsedArgs.parse(args);
        String jobId = required(parsed.stringOption("job-id", null), "Missing --job-id.");

        HttpClient httpClient =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(FLINK_REST_BASE_URL + "/jobs/" + jobId))
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw CommandException.runtimeFailure(
                    "Flink job status request failed with status "
                            + response.statusCode()
                            + ": "
                            + response.body(),
                    null);
        }

        Map<String, Object> jobPayload = parseJsonResponse(response.body());
        String state = (String) jobPayload.get("state");

        Map<String, Object> payload = JsonOutput.success("flink status");
        payload.put("job_id", jobId);
        payload.put("state", state);
        payload.put("name", jobPayload.get("name"));
        return payload;
    }

    private static Map<String, Object> handleFlinkWait(String[] args) throws Exception {
        ParsedArgs parsed = ParsedArgs.parse(args);
        String jobId = required(parsed.stringOption("job-id", null), "Missing --job-id.");
        int timeoutSeconds = parsed.intOption("timeout-seconds", 180);

        HttpClient httpClient =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        long startedNanos = System.nanoTime();
        long deadlineMillis = System.currentTimeMillis() + (long) timeoutSeconds * 1000;
        String lastState = "UNKNOWN";

        while (System.currentTimeMillis() < deadlineMillis) {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(FLINK_REST_BASE_URL + "/jobs/" + jobId))
                            .GET()
                            .timeout(Duration.ofSeconds(10))
                            .build();

            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                Map<String, Object> jobPayload = parseJsonResponse(response.body());
                lastState = (String) jobPayload.get("state");
                if (isTerminalState(lastState)) {
                    Map<String, Object> payload = JsonOutput.success("flink wait");
                    payload.put("job_id", jobId);
                    payload.put("state", lastState);
                    payload.put("duration_ms", elapsedMillis(startedNanos, System.nanoTime()));
                    return payload;
                }
            }
            Thread.sleep(2000);
        }

        throw CommandException.runtimeFailure(
                String.format(
                        "Flink job %s did not reach a terminal state within %ds. Last state: %s",
                        jobId, timeoutSeconds, lastState),
                null);
    }

    private static boolean isTerminalState(String state) {
        return "FINISHED".equals(state)
                || "FAILED".equals(state)
                || "CANCELED".equals(state)
                || "CANCELLED".equals(state);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseJsonResponse(String body) {
        try {
            return new org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind
                            .ObjectMapper()
                    .readValue(body, Map.class);
        } catch (IOException exception) {
            throw CommandException.runtimeFailure(
                    "Failed to parse Flink REST API response: " + exception.getMessage(),
                    exception);
        }
    }

    private static byte[] buildMultipartBody(String boundary, String fileName, byte[] fileBytes)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String lineEnd = "\r\n";
        baos.write(("--" + boundary + lineEnd).getBytes(StandardCharsets.UTF_8));
        baos.write(
                ("Content-Disposition: form-data; name=\"jarfile\"; filename=\""
                                + fileName
                                + "\""
                                + lineEnd)
                        .getBytes(StandardCharsets.UTF_8));
        baos.write(
                ("Content-Type: application/java-archive" + lineEnd)
                        .getBytes(StandardCharsets.UTF_8));
        baos.write(lineEnd.getBytes(StandardCharsets.UTF_8));
        baos.write(fileBytes);
        baos.write(lineEnd.getBytes(StandardCharsets.UTF_8));
        baos.write(("--" + boundary + "--" + lineEnd).getBytes(StandardCharsets.UTF_8));
        return baos.toByteArray();
    }

    static long elapsedMillis(long startedNanos, long finishedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(finishedNanos - startedNanos);
    }

    static Map<String, Object> tablePayload(TableInfo tableInfo) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("database", tableInfo.getTablePath().getDatabaseName());
        payload.put("table", tableInfo.getTablePath().getTableName());
        payload.put("table_id", tableInfo.getTableId());
        payload.put("schema_id", tableInfo.getSchemaId());
        payload.put("bucket_count", tableInfo.getNumBuckets());
        if (tableInfo.getProperties().contains(ConfigOptions.TABLE_REPLICATION_FACTOR)) {
            payload.put("replication_factor", tableInfo.getTableConfig().getReplicationFactor());
        }
        payload.put("primary_keys", tableInfo.getPrimaryKeys());
        payload.put("bucket_keys", tableInfo.getBucketKeys());

        List<Map<String, Object>> columns = new ArrayList<>();
        for (Schema.Column column : tableInfo.getSchema().getColumns()) {
            Map<String, Object> columnPayload = new LinkedHashMap<>();
            columnPayload.put("name", column.getName());
            columnPayload.put("type", column.getDataType().asSerializableString());
            columnPayload.put("comment", column.getComment().orElse(null));
            columns.add(columnPayload);
        }
        payload.put("columns", columns);
        return payload;
    }

    static List<GoalType> parseGoalTypes(String goalText) {
        if (goalText == null || goalText.trim().isEmpty()) {
            return List.of();
        }

        List<GoalType> goals = new ArrayList<>();
        for (String rawToken : goalText.split(",")) {
            String token = rawToken.trim();
            if (token.isEmpty()) {
                continue;
            }

            try {
                goals.add(GoalType.fromName(token.replace('-', '_')));
            } catch (IllegalArgumentException exception) {
                throw CommandException.invalidArgument(
                        String.format(
                                "Unsupported --goals value `%s`. Expected comma-separated values from %s.",
                                token, Arrays.toString(GoalType.values())));
            }
        }
        return goals;
    }

    static Map<String, Object> rebalanceProgressPayload(RebalanceProgress progress) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("rebalance_id", progress.rebalanceId());
        payload.put("rebalance_status", progress.status().name());
        payload.put("progress", progress.formatAsPercentage());

        List<Map<String, Object>> bucketPayloads = new ArrayList<>();
        for (RebalanceResultForBucket bucketProgress : progress.progressForBucketMap().values()) {
            Map<String, Object> bucketPayload = new LinkedHashMap<>();
            bucketPayload.put("table_id", bucketProgress.tableBucket().getTableId());
            bucketPayload.put("bucket_id", bucketProgress.tableBucket().getBucket());
            if (bucketProgress.tableBucket().getPartitionId() != null) {
                bucketPayload.put("partition_id", bucketProgress.tableBucket().getPartitionId());
            }
            bucketPayload.put("original_leader", bucketProgress.plan().getOriginalLeader());
            bucketPayload.put("new_leader", bucketProgress.plan().getNewLeader());
            bucketPayload.put("origin_replicas", bucketProgress.plan().getOriginReplicas());
            bucketPayload.put("new_replicas", bucketProgress.plan().getNewReplicas());
            bucketPayload.put("rebalance_status", bucketProgress.status().name());
            bucketPayloads.add(bucketPayload);
        }
        payload.put("progress_for_buckets", bucketPayloads);
        return payload;
    }

    static DataType parseColumnType(String columnTypeText) {
        try {
            return DataTypes.parse(columnTypeText);
        } catch (RuntimeException exception) {
            throw CommandException.invalidArgument(
                    String.format(
                            "Unsupported --column-type value `%s`: %s",
                            columnTypeText, exception.getMessage()));
        }
    }

    private static String required(String value, String message) {
        if (value == null || value.trim().isEmpty()) {
            throw CommandException.invalidArgument(message);
        }
        return value;
    }

    private static List<String> goalNames(List<GoalType> goals) {
        List<String> names = new ArrayList<>(goals.size());
        for (GoalType goal : goals) {
            names.add(goal.name().toLowerCase(Locale.ROOT));
        }
        return names;
    }

    private static void validatePositive(Integer value, String optionName, String message) {
        if (value != null && value <= 0) {
            throw CommandException.invalidArgument(
                    String.format("%s Received --%s=%s.", message, optionName, value));
        }
    }

    private static void validateNonNegative(int value, String optionName, String message) {
        if (value < 0) {
            throw CommandException.invalidArgument(
                    String.format("%s Received --%s=%s.", message, optionName, value));
        }
    }

    static final class ParsedArgs {
        private final Map<String, String> options;

        private ParsedArgs(Map<String, String> options) {
            this.options = options;
        }

        static ParsedArgs parse(String[] args) {
            Map<String, String> options = new LinkedHashMap<>();
            for (int index = 0; index < args.length; index++) {
                String token = args[index];
                if (!token.startsWith("--")) {
                    throw CommandException.invalidArgument(
                            "Unexpected positional argument: " + token);
                }

                if (index + 1 >= args.length || args[index + 1].startsWith("--")) {
                    throw CommandException.invalidArgument("Missing value for option: " + token);
                }

                options.put(token.substring(2), args[index + 1]);
                index++;
            }
            return new ParsedArgs(options);
        }

        boolean hasOption(String name) {
            return options.containsKey(name);
        }

        String stringOption(String name, String defaultValue) {
            return options.getOrDefault(name, defaultValue);
        }

        int intOption(String name, int defaultValue) {
            String value = options.get(name);
            if (value == null) {
                return defaultValue;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException exception) {
                throw CommandException.invalidArgument(
                        String.format("Option --%s expects an integer but got `%s`.", name, value));
            }
        }

        String bootstrapServers() {
            String value = stringOption("bootstrap-servers", null);
            if (value == null || value.trim().isEmpty()) {
                value = System.getenv("FLUSS_BOOTSTRAP_SERVERS");
            }
            return value;
        }
    }
}
