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

"""YAML-driven scenario execution engine."""

from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import logging
import subprocess
import sys
import time
from typing import Any
import urllib.error
import urllib.request
from ..errors import (
    ConfigError,
    ScenarioFailure,
)
from .resolver import evaluate_assertion, resolve_value
from .schema import ScenarioConfig, StepConfig, TeardownStep
from .tracer import Tracer, TraceConfig, _now_iso
from .verifier import run_verify

logger = logging.getLogger(__name__)

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0)


def _row_timestamp_text(row_id: int) -> str:
    value = BASE_TIME + timedelta(seconds=row_id)
    if value.second == 0 and value.microsecond == 0:
        return value.isoformat(timespec="minutes")
    return value.isoformat(timespec="seconds")


def _expected_checksum(start_id: int, count: int) -> str:
    digest = hashlib.sha256()
    for row_id in range(start_id, start_id + count):
        digest.update(
            (
                f"{row_id}|test_row_{row_id}|{row_id * 1000}|{_row_timestamp_text(row_id)}\n"
            ).encode("utf-8")
        )
    return digest.hexdigest()


class ScenarioEngine:
    """Executes a YAML-defined scenario against a client and cluster manager."""

    def __init__(
        self,
        config: ScenarioConfig,
        *,
        client: Any,
        cluster_manager: ComposeClusterManager | None = None,
        cli_params: dict[str, Any] | None = None,
        tracer: Tracer | None = None,
    ) -> None:
        self._config = config
        self._client = client
        self._cluster_manager = cluster_manager
        self._tracer = tracer

        self._params = dict(config.params)
        if cli_params:
            self._params.update(cli_params)

        self._context: dict[str, Any] = {
            "params": self._params,
            "expected_checksum": _expected_checksum,
        }

        self._table_name: str | None = None
        self._table_created = False
        self._created_tables: list[str] = []
        self._checks: list[dict[str, Any]] = []
        self._started: float = 0.0

    @property
    def context(self) -> dict[str, Any]:
        return self._context

    def execute(self) -> dict[str, Any]:
        started = time.monotonic()
        self._started = started
        scenario_name = self._config.meta.name
        self._table_name = f"{self._config.table.name_prefix}_{int(time.time() * 1000)}"
        self._context["table_name"] = self._table_name

        if self._tracer is not None:
            self._tracer.start_run(
                scenario_name=scenario_name,
                params=self._params,
                table_name=self._table_name,
            )

        try:
            for step_index, step in enumerate(self._config.steps, start=1):
                self._execute_step(step, step_number=step_index)

            # Run verify rules if any
            verify_results = run_verify(self._config.verify, self._context)
            verify_passed = all(r.passed for r in verify_results)
            verify_section = {
                "passed": verify_passed,
                "results": [
                    {"type": r.rule_type, "passed": r.passed, "details": list(r.details)}
                    for r in verify_results
                ],
            }

            if self._tracer is not None:
                self._tracer.record_verify(
                    status="passed" if verify_passed else "failed",
                    rules=verify_section["results"],
                )

            checks_passed = all(check["passed"] for check in self._checks) if self._checks else True
            overall_passed = checks_passed and verify_passed

            result = self._build_result(
                name=scenario_name,
                status="passed" if overall_passed else "failed",
                started=started,
            )
            result["verify"] = verify_section

            if self._tracer is not None:
                self._tracer.finalize(
                    status="passed" if overall_passed else "failed",
                    context=self._context,
                )

            if not overall_passed:
                raise ScenarioFailure(
                    f"{scenario_name} validation failed.",
                    details={"checks": self._checks, "verify": verify_section},
                    section=result,
                )
            return result
        except ScenarioFailure:
            if self._tracer is not None:
                self._tracer.finalize(
                    status="failed",
                    error=str(sys.exc_info()[1]),
                    context=self._context,
                )
            raise
        except Exception as exc:
            if self._tracer is not None:
                self._tracer.record_error(exc)
                self._tracer.finalize(
                    status="failed",
                    error=str(exc),
                    context=self._context,
                )
            raise
        finally:
            self._run_teardown()

    def _execute_step(self, step: StepConfig, *, step_number: int = 0) -> None:
        action = step.action
        resolved_args = {
            key: resolve_value(val, self._context)
            for key, val in step.args.items()
        }

        step_id = step.id or step.save_as or action
        step_started_at = _now_iso()
        step_started = time.monotonic()

        handler = self._get_handler(action)
        try:
            result = handler(step, resolved_args)
        except Exception as exc:
            step_duration_ms = int((time.monotonic() - step_started) * 1000)
            if self._tracer is not None:
                self._tracer.record_step(
                    step_number=step_number,
                    step_id=step_id,
                    action=action,
                    args=resolved_args,
                    status="failed",
                    started_at=step_started_at,
                    finished_at=_now_iso(),
                    duration_ms=step_duration_ms,
                    error=str(exc),
                )
            if step.on_error == "continue":
                step_key = step.save_as or step.id or action
                self._context[step_key] = {"error": True, "action": action}
                return
            raise

        step_key = step.save_as or step.id or action
        if result is not None:
            self._context[step_key] = result

        step_duration_ms = int((time.monotonic() - step_started) * 1000)
        if self._tracer is not None:
            self._tracer.record_step(
                step_number=step_number,
                step_id=step_id,
                action=action,
                args=resolved_args,
                status="passed",
                started_at=step_started_at,
                finished_at=_now_iso(),
                duration_ms=step_duration_ms,
                result=result,
                saved_as=step_key if result is not None else None,
            )

    def _get_handler(self, action: str):
        handlers = {
            "create-table": self._action_create_table,
            "create-tables": self._action_create_tables,
            "write": self._action_write,
            "scan": self._action_scan,
            "drop-table": self._action_drop_table,
            "drop-tables": self._action_drop_tables,
            "ping": self._action_ping,
            "stop-service": self._action_stop_service,
            "restart-service": self._action_restart_service,
            "list-tables": self._action_list_tables,
            "get-table": self._action_get_table,
            "alter-table": self._action_alter_table,
            "validate": self._action_validate,
            "shell": self._action_shell,
            "http": self._action_http,
        }
        handler = handlers.get(action)
        if handler is None:
            raise ConfigError(
                f"Unknown action `{action}` in scenario steps.",
                details={"action": action},
            )
        return handler

    def _action_create_table(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        table_name = self._table_name
        database = self._config.table.database
        buckets = args.get("buckets", self._config.table.buckets)
        replication_factor = (
            self._config.table.properties.get("replication_factor")
            or args.get("replication_factor")
        )

        kwargs: dict[str, Any] = {"database": database, "buckets": buckets}
        if replication_factor is not None:
            kwargs["replication_factor"] = int(replication_factor)

        result = self._client.create_table(table_name, **kwargs)
        self._table_created = True
        return result

    def _action_create_tables(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        """Create multiple tables with indexed names."""
        count = int(args.get("count", self._params.get("tables", 5)))
        database = self._config.table.database
        buckets = args.get("buckets", self._config.table.buckets)
        base_name = self._table_name

        created: list[str] = []
        for index in range(count):
            table_name = f"{base_name}_{index}"
            self._client.create_table(table_name, database=database, buckets=buckets)
            created.append(table_name)

        self._created_tables = created
        self._context["created_tables"] = created
        return {"tables_created": len(created), "table_names": created}

    def _action_write(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        count = int(args.get("count", self._params.get("rows", 1000)))
        start_id = int(args.get("start-id", args.get("start_id", 0)))
        database = self._config.table.database
        return self._client.write_rows(
            self._table_name,
            count,
            database=database,
            start_id=start_id,
        )

    def _action_scan(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        limit_raw = args.get("limit", self._params.get("rows"))
        limit = int(limit_raw) if limit_raw is not None else None
        database = self._config.table.database
        return self._client.scan_table(
            self._table_name,
            database=database,
            limit=limit,
        )

    def _action_drop_table(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        database = self._config.table.database
        result = self._client.drop_table(self._table_name, database=database)
        self._table_created = False
        return result

    def _action_drop_tables(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        """Drop all tables created by create-tables."""
        database = self._config.table.database
        dropped: list[str] = []
        for table_name in list(self._created_tables):
            self._client.drop_table(table_name, database=database)
            dropped.append(table_name)
        self._created_tables.clear()
        return {"tables_dropped": len(dropped), "table_names": dropped}

    def _action_ping(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        return self._client.ping()

    def _action_stop_service(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        if self._cluster_manager is None:
            raise ConfigError("stop-service requires a cluster manager.")
        service = str(args["service"])
        result = self._cluster_manager.stop_service(service)
        return {
            **result,
            "state": result["after"]["state"],
        }

    def _action_restart_service(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        if self._cluster_manager is None:
            raise ConfigError("restart-service requires a cluster manager.")
        service = str(args["service"])
        timeout_s = int(args.get("timeout-s", args.get("timeout_s", 120)))
        return self._cluster_manager.restart_service(
            service,
            self._client,
            timeout_s=timeout_s,
        )

    def _action_list_tables(self, step: StepConfig, args: dict[str, Any]) -> list[str]:
        database = args.get("database", self._config.table.database)
        return self._client.list_tables(database=database)

    def _action_get_table(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        table_name = args.get("table", self._table_name)
        database = args.get("database", self._config.table.database)
        return self._client.get_table(table_name, database=database)

    def _action_alter_table(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        table_name = args.get("table", self._table_name)
        database = args.get("database", self._config.table.database)
        column_name = args["column-name"]
        column_type = args["column-type"]
        return self._client.alter_table_add_column(
            table_name,
            column_name=column_name,
            column_type=column_type,
            database=database,
        )

    def _action_validate(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        for check in step.checks:
            passed, expected, actual = evaluate_assertion(
                check.assert_expr, self._context
            )
            self._checks.append({
                "name": check.name,
                "passed": passed,
                "expected": expected,
                "actual": actual,
            })
        return {"validated": len(step.checks)}

    def _action_shell(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        command = step.command
        if command is None:
            raise ConfigError("shell action requires a `command` field.")
        resolved_command = resolve_value(command, self._context)
        result = subprocess.run(
            resolved_command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=self._config.params.get("timeout_override_s", 300),
        )
        if result.returncode != 0:
            raise ScenarioFailure(
                f"Shell command failed with exit code {result.returncode}.",
                details={
                    "command": resolved_command,
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
            )
        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }

    def _action_http(self, step: StepConfig, args: dict[str, Any]) -> dict[str, Any]:
        url = step.url
        method = step.method or "GET"
        if url is None:
            raise ConfigError("http action requires a `url` field.")
        resolved_url = resolve_value(url, self._context)
        headers = {
            k: resolve_value(v, self._context) for k, v in step.headers.items()
        }
        body_data = None
        if step.body is not None:
            body_data = resolve_value(step.body, self._context).encode("utf-8")

        request = urllib.request.Request(
            resolved_url, method=method, headers=headers, data=body_data,
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response_body = response.read().decode("utf-8")
                return {
                    "status": response.status,
                    "body": response_body,
                }
        except urllib.error.HTTPError as exc:
            return {
                "status": exc.code,
                "body": exc.read().decode("utf-8", errors="replace"),
            }
        except (urllib.error.URLError, OSError) as exc:
            raise ScenarioFailure(
                f"HTTP request failed: {exc}",
                details={"url": resolved_url, "method": method, "error": str(exc)},
            ) from exc

    def _run_teardown(self) -> None:
        teardown_actions: list[dict[str, Any]] = []
        teardown_status = "completed"

        for teardown_step in self._config.teardown:
            try:
                self._execute_teardown_step(teardown_step)
                teardown_actions.append({
                    "action": teardown_step.action,
                    "status": "ok",
                    "error": None,
                })
            except Exception as exc:  # noqa: BLE001
                teardown_actions.append({
                    "action": teardown_step.action,
                    "status": "error",
                    "error": str(exc),
                })
                if teardown_step.on_error != "ignore":
                    teardown_status = "failed"
                    raise

        if self._table_created and not any(
            t.action in ("drop-table", "drop-tables") for t in self._config.teardown
        ):
            try:
                self._client.drop_table(
                    self._table_name,
                    database=self._config.table.database,
                )
                teardown_actions.append({
                    "action": "auto-drop-table",
                    "status": "ok",
                    "error": None,
                })
            except Exception:  # noqa: BLE001
                teardown_actions.append({
                    "action": "auto-drop-table",
                    "status": "error",
                    "error": "failed to auto-drop primary table",
                })

        for table_name in self._created_tables:
            try:
                self._client.drop_table(
                    table_name,
                    database=self._config.table.database,
                )
                teardown_actions.append({
                    "action": f"auto-drop-batch-table:{table_name}",
                    "status": "ok",
                    "error": None,
                })
            except Exception:  # noqa: BLE001
                teardown_actions.append({
                    "action": f"auto-drop-batch-table:{table_name}",
                    "status": "error",
                    "error": f"failed to auto-drop batch table {table_name}",
                })

        if self._tracer is not None:
            self._tracer.record_teardown(
                status=teardown_status,
                actions=teardown_actions,
            )

    def _execute_teardown_step(self, step: TeardownStep) -> None:
        resolved_args = {
            key: resolve_value(val, self._context)
            for key, val in step.args.items()
        }
        handler = self._get_handler(step.action)
        fake_step = StepConfig(action=step.action, args=resolved_args, on_error=step.on_error)
        handler(fake_step, resolved_args)

    def _build_result(
        self,
        *,
        name: str,
        status: str,
        started: float,
    ) -> dict[str, Any]:
        return {
            "name": name,
            "status": status,
            "duration_ms": int(max(0, time.monotonic() - started) * 1000),
            "result": {
                key: value
                for key, value in self._context.items()
                if key not in ("params", "expected_checksum", "table_name")
            },
            "validation": {
                "passed": all(c["passed"] for c in self._checks) if self._checks else status == "passed",
                "checks": list(self._checks),
            },
        }

