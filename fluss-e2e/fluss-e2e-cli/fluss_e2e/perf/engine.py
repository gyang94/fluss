# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""PerfEngine — orchestrates perf test lifecycle via Docker Compose."""

from __future__ import annotations

import json
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ..cluster.client import ClusterClient
from ..cluster.compose import ComposeClusterManager, ServicePort
from ..collector.metrics import MetricsCollector
from ..config import RuntimeConfig, _resolve_cli_root
from ..errors import ClusterError, ConfigError, ScenarioFailure
from ..subprocess_runner import CommandTimeout, failure_details, run_command, timeout_details
from .loader import load_perf_scenario, perf_scenarios_dir
from .schema import PerfScenarioConfig

_PROMETHEUS_RANGE_QUERIES = {
    "write_tps": "sum(fluss_tabletserver_messagesInPerSecond)",
    "write_bytes_per_sec": "sum(fluss_tabletserver_bytesInPerSecond)",
    "log_flush_latency_ms": "max(fluss_tabletserver_logFlushLatencyMs)",
    "kv_flush_latency_ms": "max(fluss_tabletserver_kvFlushLatencyMs)",
    "request_process_time_ms": "max(fluss_tabletserver_request_requestProcessTimeMs)",
    "request_total_time_ms": "max(fluss_tabletserver_request_totalTimeMs)",
    "jvm_heap_bytes": "sum(fluss_tabletserver_status_JVM_memory_heap_used)",
    "gc_young_ms_per_sec": (
        "sum(fluss_tabletserver_status_JVM_GC_G1_Young_Generation_timeMsPerSecond)"
    ),
    "gc_old_ms_per_sec": (
        "sum(fluss_tabletserver_status_JVM_GC_G1_Old_Generation_timeMsPerSecond)"
    ),
    "rocksdb_memory_bytes": "sum(fluss_tabletserver_rocksdbMemoryUsageTotal)",
}


@dataclass
class PerfRunResult:
    """Result of a perf test run."""

    status: str  # "passed", "failed", "partial"
    scenario_name: str
    elapsed_ms: int = 0
    phases: list[dict[str, Any]] = field(default_factory=list)
    client_results: dict[str, Any] | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": self.status,
            "scenario_name": self.scenario_name,
            "elapsed_ms": self.elapsed_ms,
            "phases": self.phases,
        }
        if self.client_results is not None:
            result["client_results"] = self.client_results
        if self.metrics:
            result["metrics"] = self.metrics
        if self.error is not None:
            result["error"] = self.error
        return result


@dataclass(frozen=True)
class PerfRuntimeStack:
    """Generated runtime compose stack for a perf scenario."""

    compose_file: Path
    compose_override_file: Path | None
    service_ports: tuple[ServicePort, ...]
    controllable_services: frozenset[str]


class _PerfClientLogStream:
    """Streams perf-client container logs to stdout and an artifact file."""

    def __init__(self, process: subprocess.Popen[str], artifact_path: Path) -> None:
        self._process = process
        self._artifact_path = artifact_path
        self._thread = threading.Thread(
            target=self._pump,
            name="perf-client-log-stream",
            daemon=True,
        )
        self._thread.start()

    def _pump(self) -> None:
        stdout = self._process.stdout
        if stdout is None:
            return
        with self._artifact_path.open("a", encoding="utf-8") as artifact:
            for line in stdout:
                artifact.write(line)
                artifact.flush()
                print(line, end="", flush=True)

    def stop(self) -> None:
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
        self._thread.join(timeout=5)


class PerfEngine:
    """Orchestrates the full perf test lifecycle.

    1. Parse perf YAML scenario
    2. Start Docker Compose cluster (Fluss + Prometheus + Grafana + perf-client)
    3. Wait for cluster ready
    4. Wait for perf-client container to complete
    5. Read perf-client results JSON
    6. Tear down (unless keep_cluster=True)
    """

    def __init__(
        self,
        config: RuntimeConfig,
        *,
        fluss_image: str | None = None,
        keep_cluster: bool = False,
        grafana_port: int = 3000,
        timeout_s: int | None = None,
    ) -> None:
        self._config = config
        self._fluss_image = fluss_image
        self._keep_cluster = keep_cluster
        self._grafana_port = grafana_port
        self._timeout_s = timeout_s

    def run_scenario(
        self,
        scenario_name: str,
        *,
        scenario: PerfScenarioConfig | None = None,
        output_dir: Path | None = None,
        perf_client_image: str | None = None,
    ) -> PerfRunResult:
        """Run a perf scenario by name."""
        started = time.monotonic()

        # 1. Load scenario
        if scenario is None:
            scenario = load_perf_scenario(scenario_name)
        scenario_dir = perf_scenarios_dir() / scenario_name

        # 2. Prepare output directory
        if output_dir is None:
            import datetime as _dt
            ts = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
            output_dir = Path("docs") / f"{scenario_name}-{ts}"
        output_dir = output_dir.resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        # 3. Write scenario YAML to temp location for mounting into container
        scenario_yaml_path = output_dir / "scenario.yaml"
        if scenario.raw_yaml:
            scenario_yaml_path.write_text(scenario.raw_yaml, encoding="utf-8")
        else:
            # Fall back to the original file
            original = scenario_dir / f"{scenario_name}.yaml"
            if original.exists():
                shutil.copy2(original, scenario_yaml_path)
            else:
                raise ConfigError(
                    f"Cannot find scenario YAML for `{scenario_name}`.",
                    details={"scenario_dir": str(scenario_dir)},
                )

        # 4. Generate runtime stack and compose env overrides
        runtime_stack = self._prepare_runtime_stack(
            scenario,
            scenario_dir=scenario_dir,
            output_dir=output_dir,
        )
        env_overrides = self._build_compose_env(
            scenario_yaml_path=scenario_yaml_path,
            output_dir=output_dir,
            perf_client_image=perf_client_image,
        )

        # 5. Start Docker Compose
        cluster = self._create_cluster_manager(runtime_stack, env_overrides)
        client = ClusterClient(self._config)
        cluster_attempted = False
        log_stream: _PerfClientLogStream | None = None
        try:
            cluster_attempted = True
            self._compose_up(cluster, client)
            # Allow extra time for CoordinatorEventProcessor to fully initialize
            # after ports are reachable (TCP-open doesn't mean the server is ready)
            time.sleep(5)
            metrics_window_start_s = int(time.time())
            self._start_perf_client(cluster)
            log_stream = self._start_perf_client_log_stream(cluster, output_dir)

            # 6. Wait for perf-client to complete
            client_results = self._wait_for_perf_client(cluster, output_dir)
            metrics_window_end_s = int(time.time())
            metrics = self._collect_prometheus_metrics(
                scenario,
                start_s=metrics_window_start_s,
                end_s=metrics_window_end_s,
            )

            elapsed_ms = int((time.monotonic() - started) * 1000)
            phases = client_results.get("phases", []) if client_results else []
            client_status = (
                client_results.get("status", "unknown") if client_results else "unknown"
            )

            if client_status == "complete":
                return PerfRunResult(
                    status="passed",
                    scenario_name=scenario_name,
                    elapsed_ms=elapsed_ms,
                    phases=phases,
                    client_results=client_results,
                    metrics=metrics,
                )
            else:
                return PerfRunResult(
                    status="failed",
                    scenario_name=scenario_name,
                    elapsed_ms=elapsed_ms,
                    phases=phases,
                    client_results=client_results,
                    metrics=metrics,
                    error=client_results.get("error") if client_results else None,
                )

        except Exception as exc:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            error_msg = str(exc)
            return PerfRunResult(
                status="failed",
                scenario_name=scenario_name,
                elapsed_ms=elapsed_ms,
                error=error_msg,
            )
        finally:
            if log_stream is not None:
                log_stream.stop()
            if cluster_attempted and not self._keep_cluster:
                try:
                    self._compose_down(cluster)
                except Exception:  # noqa: BLE001
                    pass

    def _resolve_compose_file(self, scenario_dir: Path) -> Path:
        """Resolve compose file: scenario-specific or default perf compose."""
        scenario_compose = scenario_dir / "docker-compose.yml"
        if scenario_compose.exists():
            return scenario_compose
        # Fall back to the perf-specific compose
        from ..config import _resolve_cli_root

        cli_root = _resolve_cli_root(self._config.repo_root)
        return cli_root / "docker" / "docker-compose.perf.yml"

    def _prepare_runtime_stack(
        self,
        scenario: PerfScenarioConfig,
        *,
        scenario_dir: Path,
        output_dir: Path,
    ) -> PerfRuntimeStack:
        # Use the scenario's own docker-compose.yml as the base compose file.
        compose_file = self._resolve_compose_file(scenario_dir)
        if not compose_file.exists():
            raise ConfigError(
                "No docker-compose.yml found for perf scenario.",
                details={"scenario_dir": str(scenario_dir)},
            )

        tablet_servers = self._tablet_server_count(scenario)
        cli_root = _resolve_cli_root(self._config.repo_root)

        # Write the Prometheus config for this run into the output dir.
        prometheus_config_path = output_dir / "prometheus-perf.generated.yml"
        prometheus_config_path.write_text(
            self._render_prometheus_config(tablet_servers),
            encoding="utf-8",
        )

        # Build a minimal compose override that patches only the runtime-dynamic
        # parts: FLUSS_PROPERTIES (advertised.listeners must use Docker service
        # names, not localhost), prometheus config path, grafana asset paths,
        # optional JVM args, and the perf-client depends_on list.
        grafana_root = cli_root / "docker" / "grafana"
        override_services: dict[str, Any] = {}

        # Coordinator override
        coord_env: dict[str, str] = {
            "FLUSS_PROPERTIES": self._coordinator_properties(scenario),
        }
        jvm_cs = self._cluster_jvm_args(scenario)
        if jvm_cs:
            coord_env["FLUSS_ENV_JAVA_OPTS_CS"] = jvm_cs
        override_services["coordinator-server"] = {"environment": coord_env}

        # Tablet server overrides
        for tablet_id in range(tablet_servers):
            ts_env: dict[str, str] = {
                "FLUSS_PROPERTIES": self._tablet_properties(scenario, tablet_id),
            }
            jvm_ts = self._cluster_jvm_args(scenario)
            if jvm_ts:
                ts_env["FLUSS_ENV_JAVA_OPTS_TS"] = jvm_ts
            override_services["tablet-server-%s" % tablet_id] = {"environment": ts_env}

        # Prometheus override: point at the generated config file
        override_services["prometheus"] = {
            "volumes": [
                "%s:/etc/prometheus/prometheus.yml:ro"
                % prometheus_config_path.resolve(),
            ],
        }

        # Grafana override: point at the cli-root grafana assets
        override_services["grafana"] = {
            "volumes": [
                "%s:/etc/grafana/provisioning/datasources:ro"
                % (grafana_root / "datasources").resolve(),
                "%s:/etc/grafana/provisioning/dashboards:ro"
                % (grafana_root / "dashboards").resolve(),
            ],
        }

        # perf-client depends_on: cover all tablet servers in this scenario
        perf_depends = ["coordinator-server"]
        perf_depends.extend(
            "tablet-server-%s" % tablet_id for tablet_id in range(tablet_servers)
        )
        override_services["perf-client"] = {"depends_on": perf_depends}

        override_doc: dict[str, Any] = {"services": override_services}

        override_file = (output_dir / "docker-compose.perf.override.yml").resolve()
        override_file.write_text(
            yaml.safe_dump(override_doc, sort_keys=False),
            encoding="utf-8",
        )

        service_ports = self._build_service_ports(tablet_servers)
        controllable_services = frozenset(
            service.name
            for service in service_ports
            if service.name.startswith("tablet-server-")
        )
        return PerfRuntimeStack(
            compose_file=compose_file,
            compose_override_file=override_file,
            service_ports=service_ports,
            controllable_services=controllable_services,
        )

    @staticmethod
    def _tablet_server_count(scenario: PerfScenarioConfig) -> int:
        tablet_servers = scenario.cluster.tablet_servers if scenario.cluster else 3
        if tablet_servers < 1:
            raise ConfigError(
                "Perf scenario `cluster.tablet-servers` must be at least 1.",
                details={"tablet_servers": tablet_servers},
            )
        return tablet_servers

    @staticmethod
    def _cluster_jvm_args(scenario: PerfScenarioConfig) -> str | None:
        if scenario.cluster is None or not scenario.cluster.jvm_args:
            return None
        args = [arg.strip() for arg in scenario.cluster.jvm_args if arg.strip()]
        if not args:
            return None
        return " ".join(args)

    def _cluster_config_overrides(self, scenario: PerfScenarioConfig) -> dict[str, str]:
        if scenario.cluster is None:
            return {}
        return dict(scenario.cluster.config_overrides)

    def _coordinator_properties(self, scenario: PerfScenarioConfig) -> str:
        properties = self._cluster_config_overrides(scenario)
        properties.update(
            {
                "zookeeper.address": "zookeeper:2181",
                "bind.listeners": (
                    "INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123"
                ),
                "advertised.listeners": "CLIENT://coordinator-server:9123",
                "internal.listener.name": "INTERNAL",
                "remote.data.dir": "/tmp/fluss/remote-data",
                "metrics.reporters": "prometheus",
                "metrics.reporter.prometheus.port": "9249",
            }
        )
        return self._format_fluss_properties(properties)

    def _tablet_properties(
        self,
        scenario: PerfScenarioConfig,
        tablet_id: int,
    ) -> str:
        properties = self._cluster_config_overrides(scenario)
        service_name = "tablet-server-%s" % tablet_id
        properties.update(
            {
                "zookeeper.address": "zookeeper:2181",
                "bind.listeners": (
                    "INTERNAL://%s:0, CLIENT://%s:9123" % (service_name, service_name)
                ),
                "advertised.listeners": "CLIENT://%s:9123" % service_name,
                "internal.listener.name": "INTERNAL",
                "tablet-server.id": str(tablet_id),
                "kv.snapshot.interval": "0s",
                "data.dir": "/tmp/fluss/data/%s" % service_name,
                "remote.data.dir": "/tmp/fluss/remote-data",
                "metrics.reporters": "prometheus",
                "metrics.reporter.prometheus.port": "9249",
            }
        )
        return self._format_fluss_properties(properties)

    @staticmethod
    def _format_fluss_properties(properties: dict[str, str]) -> str:
        return "\n".join(
            "%s: %s" % (key, value)
            for key, value in properties.items()
        )

    @staticmethod
    def _build_service_ports(tablet_servers: int) -> tuple[ServicePort, ...]:
        ports = [
            ServicePort("zookeeper", 2181),
            ServicePort("coordinator-server", 9123),
        ]
        for tablet_id in range(tablet_servers):
            ports.append(ServicePort("tablet-server-%s" % tablet_id, 9124 + tablet_id))
        return tuple(ports)

    @staticmethod
    def _render_prometheus_config(tablet_servers: int) -> str:
        tablet_targets = [
            "tablet-server-%s:9249" % tablet_id for tablet_id in range(tablet_servers)
        ]
        payload = {
            "global": {
                "scrape_interval": "5s",
                "evaluation_interval": "5s",
            },
            "scrape_configs": [
                {
                    "job_name": "fluss-coordinator",
                    "static_configs": [
                        {
                            "targets": ["coordinator-server:9249"],
                            "labels": {"service": "coordinator-server"},
                        }
                    ],
                },
                {
                    "job_name": "fluss-tablet-servers",
                    "static_configs": [
                        {
                            "targets": tablet_targets,
                            "labels": {"service": "tablet-server"},
                        }
                    ],
                },
            ],
        }
        return yaml.safe_dump(payload, sort_keys=False)

    def _collect_prometheus_metrics(
        self,
        scenario: PerfScenarioConfig,
        *,
        start_s: int,
        end_s: int,
    ) -> dict[str, Any]:
        collector = MetricsCollector(self._config.prometheus_url)
        if not collector.is_available():
            return {}

        step = scenario.sampling.interval if scenario.sampling else "5s"
        normalized_end_s = max(end_s, start_s + 1)
        metrics: dict[str, Any] = {}
        for metric_name, query in _PROMETHEUS_RANGE_QUERIES.items():
            response = collector.query_range(
                query,
                start_s=start_s,
                end_s=normalized_end_s,
                step=step,
            )
            if response.get("status") != "success":
                continue
            series = self._normalize_prometheus_series(
                response.get("data", {}).get("result", [])
            )
            if series:
                metrics[metric_name] = {
                    "query": query,
                    "series": series,
                }
        return metrics

    @staticmethod
    def _normalize_prometheus_series(raw_series: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_series, list):
            return []

        normalized: list[dict[str, Any]] = []
        for item in raw_series:
            if not isinstance(item, dict):
                continue
            metric = item.get("metric", {})
            values = item.get("values", [])
            if not isinstance(metric, dict) or not isinstance(values, list):
                continue

            points: list[dict[str, float]] = []
            for value in values:
                if not isinstance(value, list) or len(value) != 2:
                    continue
                try:
                    timestamp = float(value[0])
                    point_value = float(value[1])
                except (TypeError, ValueError):
                    continue
                points.append(
                    {
                        "timestamp": timestamp,
                        "value": point_value,
                    }
                )
            if points:
                normalized.append(
                    {
                        "metric": dict(metric),
                        "points": points,
                    }
                )
        return normalized

    def _create_cluster_manager(
        self,
        runtime_stack: PerfRuntimeStack,
        env_overrides: dict[str, str],
    ) -> ComposeClusterManager:
        return ComposeClusterManager(
            self._config,
            compose_file=runtime_stack.compose_file,
            compose_override_file=runtime_stack.compose_override_file,
            fluss_image=self._fluss_image,
            service_ports=runtime_stack.service_ports,
            controllable_services=runtime_stack.controllable_services,
            env_overrides=env_overrides,
        )

    def _build_compose_env(
        self,
        *,
        scenario_yaml_path: Path,
        output_dir: Path,
        perf_client_image: str | None = None,
    ) -> dict[str, str]:
        env: dict[str, str] = {}
        env["PERF_SCENARIO_FILE"] = str(scenario_yaml_path)
        env["PERF_OUTPUT_DIR"] = str(output_dir)
        if perf_client_image:
            env["PERF_CLIENT_IMAGE"] = perf_client_image
        if self._grafana_port != 3000:
            env["GRAFANA_PORT"] = str(self._grafana_port)
        return env

    def _compose_up(
        self,
        cluster: ComposeClusterManager,
        client: ClusterClient,
    ) -> None:
        """Start the compose stack and wait for ports to become reachable.

        Unlike functional tests where the Java bridge client runs on the host,
        the perf cluster advertises Docker-internal service names in its
        listeners.  A host-side ``client.ping()`` would fail because it cannot
        resolve those names.  We therefore only wait for the mapped host ports
        to become reachable (port polling) which is sufficient to confirm all
        services are accepting connections.
        """
        timeout = (
            self._timeout_s
            if self._timeout_s is not None
            else self._config.cluster_startup_timeout_s
        )
        # Run docker compose up
        command = cluster._compose_command("up", "-d")
        try:
            result = run_command(
                command,
                cwd=str(self._config.repo_root),
                env=cluster._compose_env(),
                timeout_s=timeout,
            )
        except CommandTimeout as exc:
            raise ClusterError(
                "docker compose up timed out.",
                details=timeout_details(
                    exc.command,
                    exc.timeout_s,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    duration_ms=exc.duration_ms,
                ),
            ) from exc
        if result.returncode != 0:
            raise ClusterError(
                "docker compose up failed.",
                details=failure_details(result),
            )
        # Wait for host-mapped ports to become reachable
        self._wait_for_ports(cluster, timeout_s=timeout)

    def _wait_for_ports(
        self,
        cluster: ComposeClusterManager,
        *,
        timeout_s: int,
    ) -> None:
        """Poll host-mapped ports until all are reachable."""
        import socket

        deadline = time.monotonic() + timeout_s
        service_ports = cluster._effective_service_ports
        while time.monotonic() < deadline:
            all_open = True
            for sp in service_ports:
                try:
                    with socket.create_connection(("127.0.0.1", sp.port), timeout=1):
                        pass
                except OSError:
                    all_open = False
                    break
            if all_open:
                return
            time.sleep(self._config.health_check_interval_s)
        raise ClusterError(
            "Perf cluster ports did not become reachable before timeout.",
            details={
                "timeout_seconds": timeout_s,
                "ports": [
                    {"name": sp.name, "port": sp.port} for sp in service_ports
                ],
            },
        )

    def _start_perf_client(self, cluster: ComposeClusterManager) -> None:
        """Start the profiled perf-client container after the cluster is ready."""
        command = cluster._compose_command("up", "-d", "perf-client")
        timeout = self._timeout_s or 300
        try:
            result = run_command(
                command,
                cwd=str(self._config.repo_root),
                env=cluster._compose_env(),
                timeout_s=timeout,
            )
        except CommandTimeout as exc:
            raise ClusterError(
                "docker compose up timed out for perf-client.",
                details=timeout_details(
                    exc.command,
                    exc.timeout_s,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    duration_ms=exc.duration_ms,
                ),
            ) from exc
        if result.returncode != 0:
            raise ClusterError(
                "docker compose up failed for perf-client.",
                details=failure_details(result),
            )

    def _start_perf_client_log_stream(
        self,
        cluster: ComposeClusterManager,
        output_dir: Path,
    ) -> _PerfClientLogStream:
        command = cluster._compose_command("logs", "--no-color", "--follow", "perf-client")
        process = subprocess.Popen(
            command,
            cwd=str(self._config.repo_root),
            env=cluster._compose_env(),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        return _PerfClientLogStream(process, output_dir / "perf-client.log")

    def _compose_down(
        self,
        cluster: ComposeClusterManager,
    ) -> None:
        """Tear down the compose stack."""
        cluster.down()

    def _wait_for_perf_client(
        self,
        cluster: ComposeClusterManager,
        output_dir: Path,
    ) -> dict[str, Any] | None:
        """Wait for the perf-client container to exit, then read results."""
        timeout = self._timeout_s or 3600  # default 1 hour
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            # Check perf-client container status
            try:
                command = cluster._compose_command(
                    "ps", "--all", "--format", "json", "perf-client"
                )
                result = run_command(
                    command,
                    cwd=str(self._config.repo_root),
                    env=cluster._compose_env(),
                    timeout_s=30,
                )
                if result.returncode == 0 and result.stdout.strip():
                    rows = ComposeClusterManager._parse_ps_rows(result.stdout)
                    for row in rows:
                        service = str(row.get("Service") or row.get("service") or "").strip()
                        if service == "perf-client":
                            state = ComposeClusterManager._normalize_service_state(
                                raw_state=str(row.get("State") or row.get("state") or "").strip(),
                                status_text=str(row.get("Status") or row.get("status") or "").strip(),
                            )
                            if state in ("exited", "dead", "stopped"):
                                client_results = self._read_results(output_dir)
                                if client_results is not None:
                                    return client_results
                                exit_code = row.get("ExitCode")
                                if exit_code in (None, ""):
                                    exit_code = row.get("exitCode")
                                if exit_code in (None, ""):
                                    exit_code = row.get("exit_code")
                                error = "Perf client exited without producing results.json."
                                if exit_code not in (None, ""):
                                    error = (
                                        "Perf client exited with code %s without producing results.json."
                                        % exit_code
                                    )
                                return {"status": "failed", "error": error, "phases": []}
            except Exception:  # noqa: BLE001
                pass

            time.sleep(5)

        raise ScenarioFailure(
            "Perf client did not complete within timeout.",
            details={"timeout_seconds": timeout},
        )

    def _read_results(self, output_dir: Path) -> dict[str, Any] | None:
        """Read results.json written by the perf-client container."""
        results_file = output_dir / "results.json"
        if not results_file.exists():
            return None
        try:
            return json.loads(results_file.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return None


def list_perf_scenarios() -> list[dict[str, Any]]:
    """List available perf scenario presets."""
    scenarios_dir = perf_scenarios_dir()
    if not scenarios_dir.is_dir():
        return []
    result: list[dict[str, Any]] = []
    for entry in sorted(scenarios_dir.iterdir()):
        if not entry.is_dir() or not entry.name.startswith("perf-"):
            continue
        yaml_file = entry / f"{entry.name}.yaml"
        if yaml_file.exists():
            try:
                scenario = load_perf_scenario(entry.name)
                result.append({
                    "name": entry.name,
                    "description": scenario.meta.description,
                    "tags": list(scenario.meta.tags),
                })
            except Exception:  # noqa: BLE001
                result.append({
                    "name": entry.name,
                    "description": "(failed to parse)",
                    "tags": [],
                })
    return result
