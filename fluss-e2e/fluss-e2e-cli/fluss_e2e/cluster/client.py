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

"""Python adapter around the Java bridge."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..config import RuntimeConfig
from ..errors import ClusterError, ConfigError
from ..subprocess_runner import CommandTimeout, failure_details, run_command, timeout_details
from ..builder.maven import resolve_java_client_jar


def _parse_json_payload(stdout: str) -> dict[str, Any]:
    text = stdout.strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError as exc:
        last_error = exc

    for line in reversed(text.splitlines()):
        candidate = line.strip()
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError as exc:
            last_error = exc
            continue
        if isinstance(payload, dict):
            return payload

    raise last_error


class ClusterClient:
    def __init__(
        self,
        config: RuntimeConfig,
        *,
        bootstrap_servers: str = "localhost:9123",
        jar_path: Path | None = None,
        command_timeout_s: int | None = None,
    ) -> None:
        self._config = config
        self._bootstrap_servers = bootstrap_servers
        self._jar_path = jar_path
        self._command_timeout_s = command_timeout_s

    @property
    def jar_path(self) -> Path:
        if self._jar_path is None:
            try:
                self._jar_path = resolve_java_client_jar(self._config)
            except ClusterError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise ConfigError(
                    "The Fluss E2E Java client JAR is missing. Run `fluss-e2e build` first.",
                    details={"target_dir": str(self._config.java_client_target_dir)},
                ) from exc
        return self._jar_path

    def ping(self, timeout_s: int | None = None) -> dict[str, Any]:
        return self._invoke(["cluster", "ping"], timeout_s=timeout_s)

    def list_tables(self, database: str = "e2e") -> list[str]:
        payload = self._invoke(["admin", "list-tables", "--database", database])
        return list(payload["tables"])

    def create_table(
        self,
        name: str,
        database: str = "e2e",
        buckets: int = 4,
        *,
        replication_factor: int | None = None,
    ) -> dict[str, Any]:
        command = [
            "admin",
            "create-table",
            "--database",
            database,
            "--table",
            name,
            "--buckets",
            str(buckets),
        ]
        if replication_factor is not None:
            command.extend(["--replication-factor", str(replication_factor)])
        return self._invoke(command)

    def drop_table(self, name: str, database: str = "e2e") -> dict[str, Any]:
        return self._invoke(
            ["admin", "drop-table", "--database", database, "--table", name]
        )

    def get_table(self, name: str, database: str = "e2e") -> dict[str, Any]:
        return self._invoke(
            ["admin", "get-table", "--database", database, "--table", name]
        )

    def alter_table_add_column(
        self,
        name: str,
        *,
        column_name: str,
        column_type: str,
        database: str = "e2e",
    ) -> dict[str, Any]:
        return self._invoke(
            [
                "admin",
                "alter-table",
                "--database",
                database,
                "--table",
                name,
                "--add-column",
                column_name,
                "--column-type",
                column_type,
            ]
        )

    def write_rows(
        self,
        table: str,
        count: int,
        database: str = "e2e",
        *,
        start_id: int = 0,
    ) -> dict[str, Any]:
        command = [
            "write",
            "--database",
            database,
            "--table",
            table,
            "--count",
            str(count),
        ]
        if start_id != 0:
            command.extend(["--start-id", str(start_id)])
        return self._invoke(command)

    def scan_table(
        self,
        table: str,
        *,
        database: str = "e2e",
        limit: int | None = None,
    ) -> dict[str, Any]:
        command = ["scan", "--database", database, "--table", table]
        if limit is not None:
            command.extend(["--limit", str(limit)])
        return self._invoke(command)

    def rebalance(self, goals: list[str] | None = None) -> dict[str, Any]:
        command = ["admin", "rebalance"]
        if goals:
            command.extend(["--goals", ",".join(goals)])
        return self._invoke(command)

    def list_rebalance_progress(
        self,
        *,
        rebalance_id: str | None = None,
    ) -> dict[str, Any]:
        command = ["admin", "list-rebalance-progress"]
        if rebalance_id is not None:
            command.extend(["--rebalance-id", rebalance_id])
        return self._invoke(command)

    def _invoke(self, args: list[str], timeout_s: int | None = None) -> dict[str, Any]:
        if not self.jar_path.exists():
            raise ConfigError(
                "The Fluss E2E Java client JAR is missing. Run `fluss-e2e build` first.",
                details={"jar_path": str(self.jar_path)},
            )

        timeout_s = timeout_s or self._command_timeout_s or self._config.scenario_timeout_s
        command = [
            self._config.java_command,
            "-jar",
            str(self.jar_path),
            *args,
            "--bootstrap-servers",
            self._bootstrap_servers,
        ]
        try:
            result = run_command(command, cwd=str(self._config.repo_root), timeout_s=timeout_s)
        except CommandTimeout as exc:
            raise ClusterError(
                "Java bridge command timed out.",
                details=timeout_details(
                    exc.command,
                    exc.timeout_s,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    duration_ms=exc.duration_ms,
                ),
            ) from exc

        payload: dict[str, Any]
        try:
            payload = _parse_json_payload(result.stdout)
        except json.JSONDecodeError as exc:
            raise ClusterError(
                "Java bridge returned invalid JSON.",
                details={**failure_details(result), "json_error": str(exc)},
            ) from exc

        if result.returncode != 0 or not payload.get("ok", False):
            error = payload.get("error", {})
            raise ClusterError(
                error.get("message", "Java bridge command failed."),
                details={**failure_details(result), "payload": payload},
            )
        return payload
