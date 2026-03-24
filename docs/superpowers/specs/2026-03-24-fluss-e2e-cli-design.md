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

  coordinator:
    build: {context: ../, dockerfile: docker/Dockerfile.coordinator}
    ports: ["9123:9123", "9124:9124"]
    depends_on: [zookeeper]
    volumes: [coordinator-logs:/opt/fluss/logs, ../build-target:/opt/fluss/lib]
    environment: [FLUSS_ZOOKEEPER_CONNECT=zookeeper:2181]

  tablet-server:
    build: {context: ../, dockerfile: docker/Dockerfile.tablet}
    depends_on: [coordinator]
    volumes: [tablet-logs:/opt/fluss/logs, ../build-target:/opt/fluss/lib]
    environment:
      - FLUSS_ZOOKEEPER_CONNECT=zookeeper:2181
      - FLUSS_COORDINATOR_HOST=coordinator
    deploy: {replicas: 2}

  prometheus:
    image: prom/prometheus:latest
    ports: ["9090:9090"]
    volumes: [./prometheus.yml:/etc/prometheus/prometheus.yml]
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

### Scenario: write-read

Write N rows, read them back, verify data integrity.

- Setup: Create test table with known schema
- Execute: Write N rows, scan table, calculate checksum
- Validate: Row counts match, checksum matches expected

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

- Execute: Submit Flink write job, submit Flink read job
- Validate: Both jobs complete successfully, row counts match

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
