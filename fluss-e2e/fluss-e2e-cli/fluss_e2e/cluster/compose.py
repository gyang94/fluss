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

"""Docker Compose lifecycle for the local Fluss cluster."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import socket
import tempfile
import time
from typing import Any, Mapping

from ..config import RuntimeConfig, _resolve_cli_root
from ..errors import ConfigError, ClusterError, ServiceControlError, ServiceCrashLoopError, ServiceStartupTimeoutError
from ..subprocess_runner import CommandTimeout, failure_details, run_command, timeout_details


@dataclass(frozen=True)
class ServicePort:
    name: str
    port: int


class ComposeClusterManager:
    SERVICE_PORTS = (
        ServicePort("zookeeper", 2181),
        ServicePort("coordinator-server", 9123),
        ServicePort("tablet-server-0", 9124),
        ServicePort("tablet-server-1", 9125),
        ServicePort("tablet-server-2", 9126),
    )
    CONTROLLABLE_SERVICES = frozenset({"tablet-server-0", "tablet-server-1", "tablet-server-2"})

    def __init__(
        self,
        config: RuntimeConfig,
        *,
        compose_file: Path | None = None,
        compose_override_file: Path | None = None,
        compose_profiles: tuple[str, ...] = (),
        fluss_image: str | None = None,
        fluss_source_dir: Path | None = None,
        service_ports: tuple[ServicePort, ...] | None = None,
        controllable_services: frozenset[str] | None = None,
        env_overrides: Mapping[str, str] | None = None,
    ) -> None:
        self._config = config
        self._compose_file = compose_file or config.compose_file
        self._compose_override_file = compose_override_file
        self._compose_profiles = compose_profiles
        self._fluss_image = fluss_image
        self._service_ports = service_ports
        self._controllable_services = controllable_services
        self._env_overrides = dict(env_overrides) if env_overrides is not None else {}
        # Auto-detect fluss source from cwd if not provided
        if fluss_source_dir is None:
            cwd = Path.cwd()
            if (cwd / "fluss-dist").is_dir() and (cwd / "pom.xml").is_file():
                fluss_source_dir = cwd
        self._fluss_source_dir = fluss_source_dir

    @property
    def _effective_service_ports(self) -> tuple[ServicePort, ...]:
        return self._service_ports or self.SERVICE_PORTS

    @property
    def _effective_controllable_services(self) -> frozenset[str]:
        return self._controllable_services or self.CONTROLLABLE_SERVICES

    def up(self, client, *, timeout_s: int | None = None) -> dict[str, Any]:
        command = self._compose_command("up", "-d")
        effective_timeout = timeout_s or self._config.cluster_startup_timeout_s
        try:
            result = run_command(
                command,
                cwd=str(self._config.repo_root),
                env=self._compose_env(),
                timeout_s=effective_timeout,
            )
        except CommandTimeout as exc:
            section = self._cluster_section(
                cluster_status="failed",
                service_status="unknown",
                startup_duration_ms=exc.duration_ms,
            )
            raise ClusterError(
                "docker compose up timed out.",
                details=timeout_details(
                    exc.command,
                    exc.timeout_s,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    duration_ms=exc.duration_ms,
                ),
                section=section,
            ) from exc
        section = self._cluster_section(
            cluster_status="running" if result.returncode == 0 else "failed",
            service_status="starting" if result.returncode == 0 else "failed",
            startup_duration_ms=result.duration_ms,
        )
        if result.returncode != 0:
            raise ClusterError(
                "docker compose up failed.",
                details=failure_details(result),
                section=section,
            )
        self.wait_until_ready(client, timeout_s=effective_timeout)
        return self._cluster_section(
            cluster_status="running",
            service_status="healthy",
            startup_duration_ms=result.duration_ms,
        )

    def down(self) -> dict[str, Any]:
        command = self._compose_command("down", "--remove-orphans")
        try:
            result = run_command(
                command,
                cwd=str(self._config.repo_root),
                env=self._compose_env(),
                timeout_s=60,
            )
        except CommandTimeout as exc:
            section = self._cluster_section(
                cluster_status="failed",
                service_status="unknown",
                startup_duration_ms=exc.duration_ms,
            )
            raise ClusterError(
                "docker compose down timed out.",
                details=timeout_details(
                    exc.command,
                    exc.timeout_s,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    duration_ms=exc.duration_ms,
                ),
                section=section,
            ) from exc
        section = self._cluster_section(
            cluster_status="stopped",
            service_status="stopped",
            startup_duration_ms=result.duration_ms,
        )
        if result.returncode != 0:
            raise ClusterError(
                "docker compose down failed.",
                details=failure_details(result),
                section=section,
            )
        return section

    def logs(
        self,
        services: list[str] | None = None,
        *,
        artifact_dir: Path | None = None,
        strict: bool = True,
    ) -> dict[str, Any]:
        requested_services = services or [service.name for service in self._effective_service_ports]
        target_dir = self._resolve_artifact_dir(artifact_dir)
        summaries: dict[str, Any] = {}
        captured_services = 0

        for service in requested_services:
            command = self._compose_command("logs", "--no-color", service)
            try:
                result = run_command(
                    command,
                    cwd=str(self._config.repo_root),
                    env=self._compose_env(),
                    timeout_s=60,
                )
            except CommandTimeout as exc:
                if strict:
                    raise ClusterError(
                        f"docker compose logs timed out for service `{service}`.",
                        details=timeout_details(
                            exc.command,
                            exc.timeout_s,
                            stdout=exc.stdout,
                            stderr=exc.stderr,
                            duration_ms=exc.duration_ms,
                        ),
                    ) from exc
                summaries[service] = {
                    "status": "failed",
                    "error": timeout_details(
                        exc.command,
                        exc.timeout_s,
                        stdout=exc.stdout,
                        stderr=exc.stderr,
                        duration_ms=exc.duration_ms,
                    ),
                }
                continue
            if result.returncode != 0:
                if strict:
                    raise ClusterError(
                        f"docker compose logs failed for service `{service}`.",
                        details=failure_details(result),
                    )
                summaries[service] = {
                    "status": "failed",
                    "error": failure_details(result),
                }
                continue

            artifact_path = target_dir / f"{service}.log"
            artifact_path.write_text(result.stdout, encoding="utf-8")
            summaries[service] = self._summarize_log(result.stdout, artifact_path)
            captured_services += 1

        if captured_services == len(requested_services):
            status = "captured"
        elif captured_services > 0:
            status = "partial"
        else:
            status = "failed"

        return {
            "status": status,
            "artifact_dir": str(target_dir),
            "services": summaries,
        }

    def service_status(self, service: str) -> dict[str, Any]:
        self._validate_service_name(service)
        result = self._compose_ps(services=[service])
        services = result["services"]
        if service not in services:
            return {
                "service": service,
                "state": "missing",
                "status_text": "not created",
                "container_name": None,
                "health": None,
                "exit_code": None,
            }
        return services[service]

    def stop_service(self, service: str) -> dict[str, Any]:
        self._validate_service_name(service)
        before = self.service_status(service)
        started = time.monotonic()
        command = self._compose_command("stop", service)
        try:
            result = run_command(
                command,
                cwd=str(self._config.repo_root),
                env=self._compose_env(),
                timeout_s=60,
            )
        except CommandTimeout as exc:
            raise ServiceControlError(
                f"docker compose stop timed out for service `{service}`.",
                details=timeout_details(
                    exc.command,
                    exc.timeout_s,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    duration_ms=exc.duration_ms,
                ),
                section=self._service_action_section(
                    action="stop",
                    service=service,
                    status="failed",
                    before=before,
                    after=before,
                    duration_ms=int((time.monotonic() - started) * 1000),
                ),
            ) from exc
        if result.returncode != 0:
            raise ServiceControlError(
                f"docker compose stop failed for service `{service}`.",
                details=failure_details(result),
                section=self._service_action_section(
                    action="stop",
                    service=service,
                    status="failed",
                    before=before,
                    after=before,
                    duration_ms=int((time.monotonic() - started) * 1000),
                ),
            )
        after = self._wait_for_service_state(
            service,
            expected_states={"exited", "stopped", "dead"},
            timeout_s=60,
            action="stop",
        )
        return self._service_action_section(
            action="stop",
            service=service,
            status="stopped",
            before=before,
            after=after,
            duration_ms=int((time.monotonic() - started) * 1000),
        )

    def start_service(self, service: str, client, *, timeout_s: int | None = None) -> dict[str, Any]:
        return self._bring_service_online(
            action="start",
            command_args=("start", service),
            service=service,
            client=client,
            timeout_s=timeout_s or self._config.cluster_startup_timeout_s,
        )

    def restart_service(
        self,
        service: str,
        client,
        *,
        timeout_s: int | None = None,
    ) -> dict[str, Any]:
        return self._bring_service_online(
            action="restart",
            command_args=("restart", service),
            service=service,
            client=client,
            timeout_s=timeout_s or self._config.cluster_startup_timeout_s,
        )

    def wait_until_ready(self, client, *, timeout_s: int | None = None) -> None:
        effective_timeout = timeout_s or self._config.cluster_startup_timeout_s
        deadline = time.monotonic() + effective_timeout
        # Each ping attempt should be short so we can retry within the deadline.
        ping_timeout_s = min(10, max(2, effective_timeout // 10))
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if all(self._port_open(port.port) for port in self._effective_service_ports[1:]):
                try:
                    client.ping(timeout_s=ping_timeout_s)
                    return
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
            time.sleep(self._config.health_check_interval_s)
        raise ClusterError(
            "Cluster did not become ready before the startup timeout elapsed.",
            details={
                "timeout_seconds": effective_timeout,
                "last_error": str(last_error) if last_error else None,
            },
            section=self._cluster_section(
                cluster_status="failed",
                service_status="unhealthy",
                startup_duration_ms=0,
            ),
        )

    COMPOSE_PROJECT_NAME = "fluss-e2e"

    def _compose_command(self, *args: str) -> list[str]:
        command = [
            "docker",
            "compose",
            "-p",
            self.COMPOSE_PROJECT_NAME,
            "-f",
            str(self._compose_file),
        ]
        if self._compose_override_file is not None:
            command.extend(["-f", str(self._compose_override_file)])
        for profile in self._compose_profiles:
            command.extend(["--profile", profile])
        command.extend(args)
        return command

    def _compose_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env["FLUSS_E2E_REPO_ROOT"] = str(self._config.repo_root)
        env["FLUSS_E2E_BUILD_TARGET"] = str(self._resolve_build_target())
        env["FLUSS_SOURCE_ROOT"] = str(self._config.fluss_source_root)
        cli_root = _resolve_cli_root(self._config.repo_root)
        env["FLUSS_E2E_CLI_ROOT"] = str(cli_root)
        if self._fluss_image:
            env["FLUSS_IMAGE"] = self._fluss_image
        if self._fluss_source_dir:
            resolved = self._fluss_source_dir.resolve()
            env["FLUSS_SOURCE_ROOT"] = str(resolved)
            build_target = self._find_build_target(resolved)
            if build_target is not None:
                env["FLUSS_E2E_BUILD_TARGET"] = str(build_target)
        env.update(self._env_overrides)
        return env

    def _resolve_build_target(self) -> Path:
        """Find a build-target directory with an actual lib/ containing jars."""
        # Try the configured build_target_dir first
        candidate = self._find_build_target(self._config.repo_root)
        if candidate is not None:
            return candidate
        # Fall back to config default even if empty
        return self._config.build_target_dir

    @staticmethod
    def _find_build_target(root: Path) -> Path | None:
        """Search common locations for a build-target with a populated lib/."""
        candidates = [
            root / "build-target",
        ]
        # Also check fluss-dist Maven output
        dist_target = root / "fluss-dist" / "target"
        if dist_target.is_dir():
            for child in sorted(dist_target.iterdir()):
                if child.is_dir() and child.name.endswith("-bin"):
                    inner = next(child.iterdir(), None) if child.is_dir() else None
                    if inner is not None and inner.is_dir():
                        candidates.append(inner)
        for bt in candidates:
            lib_dir = bt / "lib"
            if lib_dir.is_dir() and any(lib_dir.glob("*.jar")):
                return bt.resolve()
        return None

    def _compose_ps(self, *, services: list[str] | None = None) -> dict[str, Any]:
        command = ["ps", "--all"]
        if services:
            command.extend(services)
        command.extend(["--format", "json"])
        try:
            result = run_command(
                self._compose_command(*command),
                cwd=str(self._config.repo_root),
                env=self._compose_env(),
                timeout_s=30,
            )
        except CommandTimeout as exc:
            raise ServiceControlError(
                "docker compose ps timed out.",
                details=timeout_details(
                    exc.command,
                    exc.timeout_s,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    duration_ms=exc.duration_ms,
                ),
            ) from exc
        if result.returncode != 0:
            raise ServiceControlError(
                "docker compose ps failed.",
                details=failure_details(result),
            )

        rows = self._parse_ps_rows(result.stdout)
        services_payload: dict[str, Any] = {}
        for row in rows:
            service = str(row.get("Service") or row.get("service") or "").strip()
            if not service:
                continue
            status_text = str(row.get("Status") or row.get("status") or "").strip()
            state = self._normalize_service_state(
                raw_state=str(row.get("State") or row.get("state") or "").strip(),
                status_text=status_text,
            )
            exit_code = row.get("ExitCode")
            if exit_code in (None, ""):
                exit_code = row.get("exitCode")
            if exit_code in (None, ""):
                exit_code = row.get("exit_code")
            services_payload[service] = {
                "service": service,
                "state": state,
                "status_text": status_text or state,
                "container_name": row.get("Name") or row.get("name"),
                "health": row.get("Health") or row.get("health"),
                "exit_code": int(exit_code) if exit_code not in (None, "") else None,
            }

        overall_status = "running"
        if services and services_payload:
            states = {service_state["state"] for service_state in services_payload.values()}
            if states <= {"running"}:
                overall_status = "running"
            elif states & {"missing", "dead", "exited", "stopped"}:
                overall_status = "degraded"
        elif not services_payload:
            overall_status = "stopped"

        return {
            "status": overall_status,
            "services": services_payload,
        }

    def _bring_service_online(
        self,
        *,
        action: str,
        command_args: tuple[str, ...],
        service: str,
        client,
        timeout_s: int,
    ) -> dict[str, Any]:
        self._validate_service_name(service)
        before = self.service_status(service)
        started = time.monotonic()
        try:
            result = run_command(
                self._compose_command(*command_args),
                cwd=str(self._config.repo_root),
                env=self._compose_env(),
                timeout_s=timeout_s,
            )
        except CommandTimeout as exc:
            raise ServiceStartupTimeoutError(
                f"docker compose {action} timed out for service `{service}`.",
                details=timeout_details(
                    exc.command,
                    exc.timeout_s,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    duration_ms=exc.duration_ms,
                ),
                section=self._service_action_section(
                    action=action,
                    service=service,
                    status="failed",
                    before=before,
                    after=before,
                    duration_ms=int((time.monotonic() - started) * 1000),
                ),
            ) from exc
        if result.returncode != 0:
            raise ServiceControlError(
                f"docker compose {action} failed for service `{service}`.",
                details=failure_details(result),
                section=self._service_action_section(
                    action=action,
                    service=service,
                    status="failed",
                    before=before,
                    after=before,
                    duration_ms=int((time.monotonic() - started) * 1000),
                ),
            )

        after_state = self._wait_for_service_state(
            service,
            expected_states={"running"},
            timeout_s=timeout_s,
            action=action,
        )
        try:
            self.wait_until_ready(client)
        except ClusterError as exc:
            latest = self.service_status(service)
            raise ServiceStartupTimeoutError(
                f"Service `{service}` did not re-enter the ready state after `{action}`.",
                details={
                    "service": service,
                    "action": action,
                    "service_state": latest,
                    "cluster_error": exc.to_error_block(),
                    **({"cluster_details": exc.details} if exc.details else {}),
                },
                section=self._service_action_section(
                    action=action,
                    service=service,
                    status="failed",
                    before=before,
                    after=latest,
                    duration_ms=int((time.monotonic() - started) * 1000),
                ),
            ) from exc
        latest = self.service_status(service)
        return self._service_action_section(
            action=action,
            service=service,
            status="running",
            before=before,
            after=latest,
            duration_ms=int((time.monotonic() - started) * 1000),
        )

    def _wait_for_service_state(
        self,
        service: str,
        *,
        expected_states: set[str],
        timeout_s: int,
        action: str,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        last_state = self.service_status(service)
        while True:
            if last_state["state"] in expected_states:
                return last_state
            if expected_states == {"running"} and last_state["state"] in {"dead", "exited"}:
                raise ServiceCrashLoopError(
                    f"Service `{service}` entered `{last_state['state']}` after `{action}`.",
                    details={
                        "service": service,
                        "action": action,
                        "service_state": last_state,
                    },
                    section=self._service_action_section(
                        action=action,
                        service=service,
                        status="failed",
                        before=None,
                        after=last_state,
                    ),
                )
            if time.monotonic() >= deadline:
                break
            time.sleep(self._config.health_check_interval_s)
            last_state = self.service_status(service)

        error_class = ServiceStartupTimeoutError if expected_states == {"running"} else ServiceControlError
        raise error_class(
            f"Service `{service}` did not reach the expected state after `{action}`.",
            details={
                "service": service,
                "action": action,
                "expected_states": sorted(expected_states),
                "service_state": last_state,
                "timeout_seconds": timeout_s,
            },
            section=self._service_action_section(
                action=action,
                service=service,
                status="failed",
                before=None,
                after=last_state,
            ),
        )

    def _service_action_section(
        self,
        *,
        action: str,
        service: str,
        status: str,
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
        duration_ms: int | None = None,
    ) -> dict[str, Any]:
        section = {
            "status": status,
            "action": action,
            "service": service,
            "before": before,
            "after": after,
        }
        if duration_ms is not None:
            section["duration_ms"] = duration_ms
        return section

    def _cluster_section(
        self,
        *,
        cluster_status: str,
        service_status: str,
        startup_duration_ms: int,
    ) -> dict[str, Any]:
        return {
            "status": cluster_status,
            "startup_duration_ms": startup_duration_ms,
            "services": {
                service.name: {"status": service_status, "port": service.port}
                for service in self._effective_service_ports
            },
        }

    @staticmethod
    def _port_open(port: int) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            return False

    @staticmethod
    def _resolve_artifact_dir(artifact_dir: Path | None) -> Path:
        if artifact_dir is None:
            return Path(tempfile.mkdtemp(prefix="fluss-e2e-logs-"))
        artifact_dir.mkdir(parents=True, exist_ok=True)
        return artifact_dir

    def _validate_service_name(self, service: str) -> None:
        supported_services = self._effective_controllable_services
        if service not in supported_services:
            raise ConfigError(
                f"Service `{service}` is not a supported explicit tablet-server target.",
                details={
                    "service": service,
                    "supported_services": sorted(supported_services),
                },
            )

    @staticmethod
    def _parse_ps_rows(stdout: str) -> list[dict[str, Any]]:
        text = stdout.strip()
        if not text:
            return []
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            rows = []
            for line in text.splitlines():
                candidate = line.strip()
                if not candidate:
                    continue
                item = json.loads(candidate)
                if isinstance(item, dict):
                    rows.append(item)
            return rows

        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            return [payload]
        return []

    @staticmethod
    def _normalize_service_state(*, raw_state: str, status_text: str) -> str:
        state = raw_state.strip().lower()
        if state:
            return state

        status = status_text.strip().lower()
        if status.startswith("up"):
            return "running"
        if status.startswith("exited"):
            return "exited"
        if status.startswith("created"):
            return "created"
        if status.startswith("dead"):
            return "dead"
        if status.startswith("restarting"):
            return "restarting"
        return "unknown"

    @staticmethod
    def _summarize_log(stdout: str, artifact_path: Path) -> dict[str, Any]:
        lines = stdout.splitlines()
        warn_lines = [line for line in lines if " WARN " in line or line.startswith("WARN")]
        error_lines = [line for line in lines if " ERROR " in line or line.startswith("ERROR")]
        return {
            "status": "captured",
            "artifact_path": str(artifact_path),
            "line_count": len(lines),
            "warn_count": len(warn_lines),
            "error_count": len(error_lines),
            "warn_samples": warn_lines[:5],
            "error_samples": error_lines[:5],
        }
