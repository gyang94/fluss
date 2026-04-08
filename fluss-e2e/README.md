# Fluss E2E Testing Framework

End-to-end testing framework for Apache Fluss. Provides both functional correctness tests and performance benchmarks, all driven by a single Python CLI (`fluss-e2e`).

## Repository Layout

```
fluss-e2e/
├── fluss-e2e-cli/            # Python CLI and scenario definitions (this README)
│   ├── fluss_e2e/            # CLI source: commands, cluster manager, reporters
│   └── scenarios/            # YAML scenario files + per-scenario docker-compose.yml
├── fluss-e2e-java-client/    # Java bridge JAR (table admin & data I/O over Fluss RPC)
└── fluss-e2e-perf-client/    # Java perf-client container image (throughput driver)
```

## Quick Start

### Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.10+ |
| Java (build + runtime) | 11+ |
| Docker + Docker Compose v2 | latest |
| Maven (or `./mvnw`) | 3.8.6+ |

### Install the CLI

From the repository root:

```bash
pip install -e fluss-e2e/fluss-e2e-cli
```

Verify:

```bash
fluss-e2e --help
```

### Run a Functional Test (pre-built image)

```bash
fluss-e2e functional test \
  --scenario write-read \
  --no-build \
  --fluss-image fluss-local:latest
```

### Run a Performance Test (pre-built image)

```bash
fluss-e2e perf test \
  --scenario perf-log-append \
  --fluss-image fluss-local:latest
```

---

## Command Reference

### `functional` — Correctness Tests

Uses the Java bridge client to exercise Fluss table operations and verify data integrity.

```
fluss-e2e functional <subcommand> [options]
```

| Subcommand | Description |
|---|---|
| `build <source_dir>` | Build the Java bridge JAR and Fluss Docker image from source |
| `test --scenario <name>` | Run a scenario end-to-end (build → cluster → test → teardown) |
| `cluster up / down / logs` | Manual cluster lifecycle management |
| `cluster restart --service <name>` | Restart a specific container |
| `list` | List all registered functional scenarios |

#### Common `functional test` flags

| Flag | Default | Description |
|---|---|---|
| `--scenario <name>` | _(required)_ | Scenario to run |
| `--no-build` | _(build enabled)_ | Skip Maven build; requires a pre-built JAR and image |
| `--fluss-image <img>` | `fluss-local:latest` | Docker image to use when `--no-build` |
| `--fluss-source <dir>` | — | Path to Fluss source checkout (required when `--build`) |
| `--rows N` | `1000` | Row count for `write-read` scenario |
| `--tables N` | `5` | Table count for `table-ops` scenario |
| `--keep-cluster` | `false` | Leave containers running after the test |
| `--timeout <sec>` | `120` | Cluster startup timeout |
| `--output <path>` | `docs/<scenario>-<ts>` | Report output directory |

---

### `perf` — Performance Benchmarks

Launches a Docker Compose cluster plus a containerised perf-client, streams live progress, and evaluates pass/fail thresholds.

```
fluss-e2e perf <subcommand> [options]
```

| Subcommand | Description |
|---|---|
| `build <source_dir>` | Build the Fluss runtime image and perf-client image |
| `test --scenario <name>` | Run a perf scenario and evaluate thresholds |
| `cluster up / down / logs` | Manual cluster lifecycle management |
| `list` | List all available perf scenarios |

#### Common `perf test` flags

| Flag | Default | Description |
|---|---|---|
| `--scenario <name>` | _(required)_ | Perf scenario to run |
| `--fluss-image <img>` | `fluss-local:latest` | Fluss server Docker image |
| `--param key=value` | — | Override any YAML scenario value (repeatable) |
| `--formats <list>` | `json,csv,html` | Report formats (comma-separated) |
| `--keep-cluster` | `false` | Leave containers running after the test |
| `--grafana-port <port>` | `3000` | Host port for the Grafana UI |
| `--timeout <sec>` | — | Overall timeout in seconds |
| `--output <path>` | `docs/<scenario>-<ts>` | Report output directory |

---

## Scenarios

### Functional Scenarios

| Scenario | Tags | Description |
|---|---|---|
| `write-read` | pk, write, scan | Create a PK table, write deterministic rows, scan back, verify row count and checksum |
| `table-ops` | admin, ddl, schema | Create N tables, alter a schema column, validate, then drop all |

**Usage examples:**

```bash
# write-read: 1000 rows (default), using existing image
fluss-e2e functional test --scenario write-read --no-build --fluss-image fluss-local:latest

# table-ops: 3 tables, build from source
fluss-e2e functional test --scenario table-ops --tables 3 --fluss-source /path/to/fluss

# Keep cluster running for manual inspection
fluss-e2e functional test --scenario write-read --no-build --fluss-image fluss-local:latest --keep-cluster
```

### Performance Scenarios

| Scenario | Tags | Description |
|---|---|---|
| `perf-log-append` | log, throughput | Log table: sustained append (2M × 512B records) then concurrent scan |
| `perf-kv-upsert` | kv, upsert, rocksdb | KV table: bulk upsert → sustained write → point-get → mixed write/lookup |
| `perf-kv-agg` | kv, aggregation | KV table with SUM/MAX/MIN/BOOL_OR merge engine: write-heavy then read-heavy mixed phases |

**Usage examples:**

```bash
# Quick log-append benchmark
fluss-e2e perf test --scenario perf-log-append

# KV upsert stress test with custom image
fluss-e2e perf test --scenario perf-kv-upsert --fluss-image fluss-local:latest

# Override a scenario parameter at runtime
fluss-e2e perf test --scenario perf-kv-agg --param threads=8

# Keep Grafana/Prometheus up for dashboard inspection
fluss-e2e perf test --scenario perf-kv-agg --keep-cluster
```

#### Performance Scenario YAML Structure

Each perf scenario is a YAML file under `fluss-e2e-cli/scenarios/<name>/`:

```yaml
meta:
  name: my-scenario
  description: "What this measures"
  tags: [kv, perf]

table:            # Table schema and engine settings
  name: my_table
  columns: [...]
  primary-key: [id]
  buckets: 12

data:             # Data generator configuration
  seed: 42
  generators:
    id: { type: sequential, start: 0, end: 1000000 }

workload:         # One or more phases
  - phase: write
    records: 1000000
    threads: 8
  - phase: mixed
    duration: 2m
    threads: 8
    mix: { write: 70, lookup: 30 }

thresholds:       # Pass/fail criteria
  write-tps:
    min: 50000
  p99-ms:
    max: 2000
```

Supported phase types: `write`, `lookup`, `scan`, `mixed`.

---

## Output and Reports

All test runs write output under `docs/<scenario>-<YYYYMMDD-HHmmss>/`:

```
docs/perf-kv-upsert-20260408-120000/
├── summary.json          # Threshold evaluation result
├── report.html           # Human-readable HTML report
├── report.csv            # Raw per-phase metrics
└── perf-kv-upsert-20260408-120000-artifacts/
    ├── coordinator-server.log
    ├── tablet-server-0.log
    ├── tablet-server-1.log
    └── tablet-server-2.log
```

Functional tests write a single `summary.json` plus an `-artifacts/` directory containing server logs.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `FLUSS_E2E_REPO_ROOT` | _(auto-detected)_ | Override repository root detection |
| `FLUSS_E2E_BUILD_TIMEOUT` | `600` | Maven build timeout (seconds) |
| `FLUSS_E2E_CLUSTER_STARTUP_TIMEOUT` | `120` | Cluster health-check timeout (seconds) |
| `FLUSS_E2E_SCENARIO_TIMEOUT` | `300` | Default scenario execution timeout (seconds) |
| `FLUSS_E2E_HEALTH_CHECK_INTERVAL` | `5` | Interval between cluster health checks (seconds) |
| `FLUSS_E2E_PROMETHEUS_URL` | `http://localhost:9090` | Prometheus endpoint for metrics collection |
| `FLUSS_E2E_MAVEN_COMMAND` | `./mvnw` | Maven executable path |
| `FLUSS_E2E_JAVA_COMMAND` | `java` | Java executable path |

---

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | All checks passed |
| `1` | Scenario failure (data mismatch, threshold violated) |
| `2` | Build failure (Maven error, JAR not found) |
| `3` | Cluster failure (startup timeout, connectivity error) |
| `4` | Configuration error (invalid arguments, missing files) |

---

## Architecture

```
fluss-e2e
├── CLI (Python)
│   ├── functional test ──► Java bridge JAR ──► Fluss cluster (Docker Compose)
│   │                          (table admin, write, scan, verify)
│   └── perf test ───────► perf-client container ──► Fluss cluster (Docker Compose)
│                              (throughput driver, live progress stream)
├── Java bridge (fluss-e2e-java-client)
│   └── Thin wrapper over the Fluss Java client; speaks JSON over stdout
└── Perf client (fluss-e2e-perf-client)
    └── Multi-threaded driver; emits JSON progress events; evaluates thresholds
```

The CLI manages the full Docker Compose lifecycle — it brings the cluster up, waits for health checks, runs the scenario, collects logs, and tears down. Each scenario ships its own `docker-compose.yml` inside `scenarios/<name>/` to allow per-scenario resource tuning (e.g. larger `tmpfs` for RocksDB-heavy KV workloads).

---

## Troubleshooting

**`ConfigError: --fluss-source is required`**
Add `--no-build --fluss-image <image>` to reuse an existing Docker image instead of building from source.

**Cluster fails to start / port conflicts**
Check for stale containers (`docker ps -a`) and verify ports 2181, 9123–9126, and 9090 are free.

**Tablet servers crash during KV perf test (tmpfs full)**
RocksDB workloads require more shared storage. Increase the `size` in the scenario's `docker-compose.yml` volumes section (e.g. `size=8g`).

**Java bridge returns invalid JSON**
The JAR is outdated. Rebuild with `fluss-e2e functional build <source_dir>`.

**Maven build fails**
Ensure `JAVA_HOME` points to JDK 11+. The CLI falls back to the system `mvn` if `./mvnw` is unavailable.
