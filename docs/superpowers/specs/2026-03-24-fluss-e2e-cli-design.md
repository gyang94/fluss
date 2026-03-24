# Fluss E2E CLI Design

**Date:** 2026-03-24
**Status:** Approved
**Author:** Design session

## Overview

A Python CLI tool for locally running end-to-end tests for Fluss cluster and clients. Designed for both human developers and AI agents to quickly verify code changes work correctly.

**Primary Workflow:** Build JARs → Start Cluster → Run Test → Stop Cluster → Output JSON Results

## Goals

- **AI agent-friendly:** Structured JSON output, predictable exit codes, programmatic interface
- **Developer-friendly:** Clear progress output, readable logs, intuitive commands
- **Extensible:** Easy to add new test scenarios
- **Isolated:** Docker Compose provides clean cluster isolation

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Docker | 20.0+ | Docker Engine with Compose v2 |
| Python | 3.10+ | For running the CLI |
| Java | 11+ | For Fluss client JARs |
| Maven | 3.8+ | Via `./mvnw` wrapper |

**Working Directory:** Run CLI from Fluss project root (contains `pom.xml` and `build-target/`).

## Non-Goals

- Production deployment testing (use Helm for that)
- Performance benchmarking (use JMH for that)
- CI/CD integration (can be added later)

## High-Level Architecture

```
+-------------------------------------------------------------+
|                      fluss-e2e CLI                          |
|  +---------+  +---------+  +---------+  +---------------+  |
|  |  build  |--| cluster |--|scenario |--|   reporter    |  |
|  |   cmd   |  |   cmd   |  |   cmd   |  | (JSON output) |  |
|  +---------+  +---------+  +---------+  +---------------+  |
+-------------------------------------------------------------+
        |              |              |
        v              v              v
+------------+ +------------+ +------------------------------+
| Maven Build| | Docker     | | Test Scenarios               |
| (./mvnw)   | | Compose    | | * write-read                 |
|            | |            | | * table-ops                  |
|            | | * ZooKeeper| | * failover                   |
|            | | * Coord    | | * flink-integration          |
|            | | * TabletSvr| |                              |
|            | |            | | Uses: fluss-client JAR       |
+------------+ +------------+ +------------------------------+
```

## CLI Interface

### Main Command

```bash
fluss-e2e run --scenario write-read --rows 1000
```

### Subcommands

| Command | Description |
|---------|-------------|
| `run` | Full workflow: build, start cluster, run test, stop cluster |
| `build` | Build JARs only |
| `cluster up` | Start cluster only |
| `cluster down` | Stop cluster only |
| `cluster logs` | View cluster logs |
| `test` | Run scenario against running cluster |
| `scenarios list` | List available scenarios |

### Run Command Options

```
fluss-e2e run [OPTIONS]

Options:
  --scenario NAME       Test scenario (required)
                        Values: write-read, table-ops, failover, flink-integration
  --rows N              Rows for write-read scenario (default: 1000)
  --tables N            Tables for table-ops scenario (default: 5)
  --flink-version VER   Flink version for flink-integration (default: 1.20)
  --build / --no-build  Rebuild JARs (default: --build)
  --keep-cluster        Don't stop cluster after test
  --output PATH         Write JSON to file (default: stdout)
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success - all tests passed |
| 1 | Test failure - scenario assertions failed |
| 2 | Build failure - Maven build failed |
| 3 | Cluster failure - cluster didn't start or crashed |
| 4 | Configuration error - invalid arguments |

## Docker Compose Cluster

### Services

```yaml
services:
  zookeeper:
    image: zookeeper:3.8
    ports: ["2181:2181"]
    volumes: [zk-data:/data, zk-logs:/datalog]
    healthcheck:
      test: ["CMD", "zkServer.sh", "status"]
      interval: 5s
      timeout: 10s
      retries: 10
      start_period: 10s

  coordinator:
    build: {context: ../, dockerfile: docker/Dockerfile.coordinator}
    ports: ["9123:9123", "9124:9124"]
    depends_on:
      zookeeper: {condition: service_healthy}
    volumes: [coordinator-logs:/opt/fluss/logs, ../build-target:/opt/fluss/lib]
    environment: [FLUSS_ZOOKEEPER_CONNECT=zookeeper:2181]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9124/health"]
      interval: 5s
      timeout: 10s
      retries: 12
      start_period: 30s

  tablet-server:
    build: {context: ../, dockerfile: docker/Dockerfile.tablet}
    depends_on:
      coordinator: {condition: service_healthy}
    volumes: [tablet-logs:/opt/fluss/logs, ../build-target:/opt/fluss/lib]
    environment:
      - FLUSS_ZOOKEEPER_CONNECT=zookeeper:2181
      - FLUSS_COORDINATOR_HOST=coordinator
    deploy: {replicas: 2}
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9125/health"]
      interval: 5s
      timeout: 10s
      retries: 10
      start_period: 20s

  prometheus:
    image: prom/prometheus:latest
    ports: ["9090:9090"]
    volumes: [./prometheus.yml:/etc/prometheus/prometheus.yml]

  flink-jobmanager:
    image: flink:1.20-scala_2.12
    ports: ["8081:8081", "6123:6123"]
    command: jobmanager
    environment:
      - FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager
    volumes: [../build-target:/opt/flink/usrlib]
    profiles: [flink]  # Only started for flink-integration scenario
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/overview"]
      interval: 5s
      timeout: 10s
      retries: 10
      start_period: 30s

  flink-taskmanager:
    image: flink:1.20-scala_2.12
    depends_on:
      flink-jobmanager: {condition: service_healthy}
    command: taskmanager
    environment:
      - FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager
    volumes: [../build-target:/opt/flink/usrlib]
    profiles: [flink]
    deploy: {replicas: 2}
```

### Key Design Decisions

1. **Volume mount for JARs** - `build-target/` mounted to `/opt/fluss/lib` for immediate JAR updates without image rebuilds
2. **Log volumes** - Named volumes per service for post-test collection
3. **2 tablet servers** - Enables failover scenario testing
4. **Prometheus** - Pre-configured for metrics scraping
5. **Health checks** - Each service has health check for readiness detection

## Test Scenarios

### Base Class

```python
class Scenario(ABC):
    name: str
    description: str

    @abstractmethod
    def setup(self, cluster: ClusterClient) -> None: pass

    @abstractmethod
    def execute(self, cluster: ClusterClient) -> ScenarioResult: pass

    @abstractmethod
    def validate(self, result: ScenarioResult) -> ValidationResult: pass

    def teardown(self, cluster: ClusterClient) -> None: pass
```

### ClusterClient Interface

```python
class ClusterClient:
    """High-level client for scenario interactions with Fluss cluster.

    Wraps Java client JAR via subprocess. All methods return dataclasses
    with operation results.
    """

    def __init__(self, host: str, port: int, client_jar: Path):
        self._host = host
        self._port = port
        self._client_jar = client_jar

    # Admin operations
    def create_table(self, name: str, schema: TableSchema) -> None:
        """Create a table with the given schema."""

    def drop_table(self, name: str) -> None:
        """Drop a table."""

    def list_tables(self) -> List[str]:
        """List all tables in the default database."""

    def alter_table(self, name: str, add_column: str = None) -> None:
        """Alter table schema."""

    # Write operations
    def write_rows(self, table: str, rows: List[dict] = None,
                   count: int = None, schema: TableSchema = None) -> WriteResult:
        """Write rows to table. Either rows or count must be provided.
        If count, generates deterministic test data."""

    # Read operations
    def scan_table(self, table: str, limit: int = None) -> ScanResult:
        """Scan entire table, return all rows."""

    def lookup_row(self, table: str, key: dict) -> Optional[dict]:
        """Point lookup by primary key."""

    # Cluster operations (for failover scenario)
    def kill_tablet_server(self, replica: int) -> None:
        """Kill specific tablet server replica."""

    def start_tablet_server(self, replica: int) -> None:
        """Restart killed tablet server."""

    def wait_for_rebalance(self, timeout: int = 30) -> None:
        """Wait for cluster to rebalance after failover."""

    # Flink operations (for flink-integration scenario)
    def submit_flink_job(self, jar: str, args: List[str]) -> FlinkJobResult:
        """Submit Flink job and wait for completion."""


@dataclass
class WriteResult:
    count: int
    latency_ms: float
    checksum: str  # SHA-256 of written data


@dataclass
class ScanResult:
    rows: List[dict]
    count: int
    checksum: str
    latency_ms: float


@dataclass
class TableSchema:
    columns: List[ColumnDef]
    primary_key: List[str] = None

    DEFAULT = TableSchema(
        columns=[
            ColumnDef("id", "INT", primary=True),
            ColumnDef("name", "STRING"),
            ColumnDef("value", "BIGINT"),
            ColumnDef("ts", "TIMESTAMP"),
        ],
        primary_key=["id"]
    )
```

### Java Client Invocation

The `ClusterClient` invokes Java code via subprocess. The `java-client/` module provides:

```java
// ClientRunner.java - Main entry point
public class ClientRunner {
    public static void main(String[] args) {
        String command = args[0];  // write, scan, admin, flink
        String host = System.getenv("FLUSS_HOST");
        int port = Integer.parseInt(System.getenv("FLUSS_PORT"));

        switch (command) {
            case "write":
                WriteRows.execute(host, port, args[1], Integer.parseInt(args[2]));
                break;
            case "scan":
                ScanTable.execute(host, port, args[1]);
                break;
            case "admin":
                TableOps.execute(host, port, args[1], args[2..]);
                break;
            case "flink":
                FlinkJob.execute(args[1..]);
                break;
        }
    }
}
```

**Communication Protocol:**

1. Python CLI sets environment variables: `FLUSS_HOST`, `FLUSS_PORT`
2. Python CLI invokes: `java -jar java-client.jar <command> <args>`
3. Java client outputs JSON to stdout
4. Python CLI parses JSON response

**Example invocation:**
```bash
java -jar java-client.jar write test_table 1000
# Output: {"count": 1000, "latency_ms": 234.5, "checksum": "abc123"}
```

### Scenario: write-read

Write N rows, read them back, verify data integrity.

- Setup: Create test table with known schema
- Execute: Write N rows, scan table, calculate checksum
- Validate: Row counts match, checksum matches expected

**Test Table Schema:**
```sql
CREATE TABLE test_table (
    id INT PRIMARY KEY,
    name STRING,
    value BIGINT,
    ts TIMESTAMP
) WITH (
    'bucket-num' = '4'
);
```

**Test Data Generation:**
- `id`: Sequential integers 0 to N-1
- `name`: `"test_row_{id}"` (deterministic string)
- `value`: `id * 1000L` (deterministic bigint)
- `ts`: `2026-01-01 00:00:00 + id seconds`

**Checksum Calculation:** SHA-256 of concatenated row values (sorted by primary key)

### Scenario: table-ops

Create, alter, list, drop tables.

- Execute: Create N tables, list tables, alter one table, drop all tables
- Validate: All operations succeed, table counts correct

### Scenario: failover

Kill tablet server, verify recovery and data consistency.

- Execute: Write initial data, kill tablet server, wait for rebalance, write more data, restart server, verify data
- Validate: No data loss, recovery succeeds

### Scenario: flink-integration

Test Flink source/sink connectors.

- Requires: `--profile flink` in Docker Compose (starts Flink JobManager + TaskManagers)
- Execute: Submit Flink write job, submit Flink read job
- Validate: Both jobs complete successfully, row counts match

## Timeout Configuration

| Timeout | Default | Description |
|---------|---------|-------------|
| `build_timeout` | 600s | Maven build timeout |
| `cluster_startup_timeout` | 120s | Wait for all services healthy |
| `scenario_timeout` | 300s | Maximum scenario execution time |
| `health_check_interval` | 5s | Interval between health checks |
| `health_check_retries` | 10-12 | Retries before declaring unhealthy |
| `flink_job_timeout` | 180s | Flink job completion wait |

Timeouts can be overridden via environment variables:
```bash
export FLUSS_E2E_BUILD_TIMEOUT=900
export FLUSS_E2E_CLUSTER_STARTUP_TIMEOUT=180
```

## Log and Metrics Collection

### Collection Timing

1. **During execution:** Prometheus metrics scraped every 5s to memory
2. **After scenario completion:**
   - Docker logs captured via `docker logs <container>`
   - Log files copied from Docker volumes to local temp directory
   - Prometheus snapshot exported to JSON file

### Log Collection

```python
class LogCollector:
    def collect_all(self, output_dir: Path) -> Dict[str, LogInfo]:
        """Collect logs from all containers after test."""
        logs = {}
        for container in ["coordinator", "tablet-server-1", "tablet-server-2"]:
            # Capture stdout/stderr
            stdout = docker_client.logs(container, stdout=True, stderr=True)

            # Copy log files from volume
            docker_client.cp(f"{container}:/opt/fluss/logs", output_dir / container)

            # Parse for errors/warnings
            errors = self._extract_lines(stdout, "ERROR")
            warnings = self._extract_lines(stdout, "WARN")

            logs[container] = LogInfo(
                path=str(output_dir / container / "fluss.log"),
                lines=stdout.count('\n'),
                errors=errors,
                warnings=warnings
            )
        return logs
```

### Metrics Collection

```python
class MetricsCollector:
    def __init__(self, prometheus_url: str):
        self._url = prometheus_url
        self._snapshots = []

    def scrape(self) -> Dict[str, float]:
        """Scrape current metrics from Prometheus. Called periodically."""
        metrics = requests.get(f"{self._url}/api/v1/query", params={
            "query": "fluss_*"
        }).json()
        self._snapshots.append(metrics)
        return metrics

    def export_summary(self, output_path: Path) -> MetricsSummary:
        """Export aggregated metrics after test."""
        # Calculate aggregations (p99, avg, etc.) from snapshots
        summary = self._aggregate_snapshots()

        # Write full snapshot to file
        with open(output_path, 'w') as f:
            json.dump(self._snapshots, f)

        return summary
```

## JSON Output Format

### Success Example

```json
{
  "version": "1.0",
  "timestamp": "2026-03-24T12:34:56.789Z",
  "command": "run",
  "scenario": "write-read",
  "status": "passed",
  "exit_code": 0,
  "duration_ms": 45678,

  "build": {
    "status": "success",
    "duration_ms": 12000,
    "modules_built": ["fluss-common", "fluss-client", "fluss-server"],
    "jar_paths": ["build-target/fluss-client-0.10-SNAPSHOT.jar"]
  },

  "cluster": {
    "status": "running",
    "startup_duration_ms": 8500,
    "services": {
      "zookeeper": {"status": "healthy", "port": 2181},
      "coordinator": {"status": "healthy", "port": 9123},
      "tablet-server-1": {"status": "healthy", "port": 9125},
      "tablet-server-2": {"status": "healthy", "port": 9126}
    }
  },

  "scenario": {
    "name": "write-read",
    "status": "passed",
    "duration_ms": 15000,
    "result": {
      "rows_written": 1000,
      "rows_read": 1000,
      "write_latency_ms": 234,
      "read_latency_ms": 156,
      "checksum": "abc123..."
    },
    "validation": {
      "passed": true,
      "checks": [
        {"name": "row_count_match", "passed": true, "expected": 1000, "actual": 1000},
        {"name": "data_integrity", "passed": true}
      ]
    }
  },

  "logs": {
    "coordinator": {
      "path": "/tmp/fluss-e2e/logs/coordinator.log",
      "lines": 1234,
      "errors": [],
      "warnings": ["WARN: Connection timeout, retrying..."]
    }
  },

  "metrics": {
    "prometheus_url": "http://localhost:9090",
    "snapshot_path": "/tmp/fluss-e2e/metrics/snapshot.json",
    "summary": {
      "write_ops_total": 1000,
      "read_ops_total": 1000,
      "write_latency_p99_ms": 45,
      "read_latency_p99_ms": 32
    }
  },

  "error": null
}
```

### Failure Example

```json
{
  "version": "1.0",
  "timestamp": "2026-03-24T12:34:56.789Z",
  "command": "run",
  "scenario": "write-read",
  "status": "failed",
  "exit_code": 1,

  "scenario": {
    "name": "write-read",
    "status": "failed",
    "validation": {
      "passed": false,
      "checks": [
        {"name": "row_count_match", "passed": true},
        {"name": "data_integrity", "passed": false, "error": "Checksum mismatch"}
      ]
    }
  },

  "error": {
    "type": "ValidationError",
    "message": "Data integrity check failed: checksum mismatch"
  }
}
```

## Project Structure

```
fluss-e2e-cli/
+-- pyproject.toml
+-- README.md
+-- requirements.txt
|
+-- fluss_e2e/
|   +-- cli.py                     # CLI entry point
|   +-- builder/
|   |   +-- maven.py               # Maven build orchestration
|   +-- cluster/
|   |   +-- docker_compose.py      # Docker Compose management
|   |   +-- health_check.py        # Cluster readiness
|   |   +-- client.py              # ClusterClient helper
|   +-- scenarios/
|   |   +-- base.py                # Scenario abstract class
|   |   +-- registry.py            # Scenario registry
|   |   +-- write_read.py
|   |   +-- table_ops.py
|   |   +-- failover.py
|   |   +-- flink_integration.py
|   +-- collector/
|   |   +-- logs.py                # Log collection
|   |   +-- metrics.py             # Prometheus scraping
|   +-- reporter/
|       +-- json_reporter.py       # JSON output
|       +-- models.py              # Pydantic models
|
+-- docker/
|   +-- docker-compose.yaml
|   +-- Dockerfile.coordinator
|   +-- Dockerfile.tablet
|   +-- prometheus.yml
|   +-- entrypoint.sh
|
+-- java-client/
|   +-- src/main/java/org/apache/fluss/e2e/
|   |   +-- ClientRunner.java      # Java client entry
|   |   +-- WriteRows.java
|   |   +-- ScanTable.java
|   |   +-- TableOps.java
|   +-- pom.xml
|
+-- tests/
    +-- conftest.py
    +-- test_builder.py
    +-- test_cluster.py
    +-- test_scenarios.py
```

## Dependencies

```toml
[project]
dependencies = [
    "click>=8.0",
    "docker>=7.0",
    "pydantic>=2.0",
    "pyyaml>=6.0",
    "requests>=2.28",
    "rich>=13.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-cov>=4.0",
]
```

## Future Extensions

- Additional test scenarios (Kafka protocol, Spark connector, lake tiering)
- CI/CD integration templates
- Performance regression detection
- Multi-cluster testing
- Web dashboard for test history
