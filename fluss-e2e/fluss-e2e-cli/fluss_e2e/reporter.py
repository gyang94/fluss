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

"""JSON report helpers."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from .errors import E2EError


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_report(command: str, scenario_name: str | None = None) -> dict[str, Any]:
    return {
        "version": "1.0",
        "timestamp": _utc_now(),
        "command": command,
        "scenario_name": scenario_name,
        "status": "running",
        "exit_code": 0,
        "duration_ms": 0,
        "build": None,
        "cluster": None,
        "scenario": None,
        "logs": None,
        "metrics": None,
        "error": None,
    }


def finalize_report(
    report: dict[str, Any],
    *,
    status: str,
    exit_code: int,
    duration_ms: int,
) -> dict[str, Any]:
    report["status"] = status
    report["exit_code"] = exit_code
    report["duration_ms"] = duration_ms
    return report


def attach_error(report: dict[str, Any], error: E2EError) -> None:
    report["error"] = error.to_error_block()
    if error.section is not None:
        if error.exit_code == 2:
            report["build"] = error.section
        elif error.exit_code == 3:
            report["cluster"] = error.section


def write_report(report: dict[str, Any], output_path: str | None) -> None:
    payload = json.dumps(report, indent=2, sort_keys=False)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + "\n", encoding="utf-8")
        return
    print(payload)
