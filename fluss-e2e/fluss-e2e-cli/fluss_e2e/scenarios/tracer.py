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

"""Scenario execution tracer that writes per-step JSON trace files."""

from __future__ import annotations

import json
import logging
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MAX_VALUE_LENGTH = 10_000


@dataclass(frozen=True)
class TraceConfig:
    enabled: bool = True
    base_dir: Path | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")


def _truncate(value: Any) -> Any:
    """Truncate large string values to keep trace files readable."""
    if isinstance(value, str) and len(value) > _MAX_VALUE_LENGTH:
        return value[:_MAX_VALUE_LENGTH] + f"... (truncated, {len(value)} chars total)"
    if isinstance(value, dict):
        return {k: _truncate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_truncate(item) for item in value]
    return value


def _safe_serialize(obj: Any) -> Any:
    """Make objects JSON-serializable by converting non-standard types."""
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    if isinstance(obj, dict):
        return {str(k): _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_serialize(item) for item in obj]
    if callable(obj):
        return f"<function {getattr(obj, '__name__', repr(obj))}>"
    return str(obj)


def _write_json(path: Path, data: Any) -> None:
    """Write data as pretty-printed JSON, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable = _safe_serialize(data)
    path.write_text(
        json.dumps(serializable, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


class Tracer:
    """Records scenario execution into a timestamped trace folder.

    Trace folder layout::

        traces/<run_id>/
            summary.json
            steps/
                01_create-table.json
                02_write.json
                ...
            verify.json
            teardown.json
            context.json
    """

    def __init__(self, config: TraceConfig) -> None:
        self._config = config
        self._run_id: str = ""
        self._trace_dir: Path | None = None
        self._scenario_name: str = ""
        self._started_at: str = ""
        self._step_count: int = 0
        self._steps_passed: int = 0
        self._steps_failed: int = 0
        self._verify_passed: bool | None = None
        self._params: dict[str, Any] = {}
        self._table_name: str = ""

    @property
    def enabled(self) -> bool:
        return self._config.enabled and self._config.base_dir is not None

    @property
    def trace_dir(self) -> Path | None:
        return self._trace_dir

    def start_run(
        self,
        *,
        scenario_name: str,
        params: dict[str, Any],
        table_name: str,
    ) -> None:
        if not self.enabled:
            return

        self._scenario_name = scenario_name
        self._params = dict(params)
        self._table_name = table_name
        self._run_id = _run_id()
        self._started_at = _now_iso()

        assert self._config.base_dir is not None
        self._trace_dir = self._config.base_dir / "traces" / self._run_id
        self._trace_dir.mkdir(parents=True, exist_ok=True)
        (self._trace_dir / "steps").mkdir(exist_ok=True)

        logger.info("Trace started: %s", self._trace_dir)

    def record_step(
        self,
        *,
        step_number: int,
        step_id: str,
        action: str,
        args: dict[str, Any],
        status: str,
        started_at: str,
        finished_at: str,
        duration_ms: int,
        result: Any = None,
        saved_as: str | None = None,
        error: str | None = None,
    ) -> None:
        if not self.enabled or self._trace_dir is None:
            return

        self._step_count += 1
        if status == "passed":
            self._steps_passed += 1
        elif status == "failed":
            self._steps_failed += 1

        step_data = {
            "step_number": step_number,
            "step_id": step_id,
            "action": action,
            "args": _truncate(args),
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "result": _truncate(result),
            "saved_as": saved_as,
            "error": error,
        }

        filename = f"{step_number:02d}_{action}.json"
        _write_json(self._trace_dir / "steps" / filename, step_data)

    def record_verify(
        self,
        *,
        status: str,
        rules: list[dict[str, Any]],
    ) -> None:
        if not self.enabled or self._trace_dir is None:
            return

        self._verify_passed = status == "passed"

        verify_data = {
            "status": status,
            "rules": rules,
        }
        _write_json(self._trace_dir / "verify.json", verify_data)

    def record_teardown(
        self,
        *,
        status: str,
        actions: list[dict[str, Any]],
    ) -> None:
        if not self.enabled or self._trace_dir is None:
            return

        teardown_data = {
            "status": status,
            "actions": actions,
        }
        _write_json(self._trace_dir / "teardown.json", teardown_data)

    def finalize(
        self,
        *,
        status: str,
        error: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if not self.enabled or self._trace_dir is None:
            return

        finished_at = _now_iso()

        summary = {
            "schema_version": "1.0",
            "scenario": self._scenario_name,
            "run_id": self._run_id,
            "status": status,
            "started_at": self._started_at,
            "finished_at": finished_at,
            "total_steps": self._step_count,
            "steps_passed": self._steps_passed,
            "steps_failed": self._steps_failed,
            "verify_passed": self._verify_passed,
            "params": self._params,
            "table_name": self._table_name,
            "error": error,
        }
        _write_json(self._trace_dir / "summary.json", summary)

        if context is not None:
            context_snapshot = {
                k: _truncate(v)
                for k, v in context.items()
                if k != "expected_checksum"
            }
            _write_json(self._trace_dir / "context.json", context_snapshot)

        logger.info("Trace finalized: %s", self._trace_dir)

    def record_error(self, error: Exception) -> None:
        """Record an unexpected error that aborted the run."""
        if not self.enabled or self._trace_dir is None:
            return

        error_data = {
            "type": type(error).__name__,
            "message": str(error),
            "traceback": traceback.format_exception(error),
        }
        _write_json(self._trace_dir / "error.json", error_data)
