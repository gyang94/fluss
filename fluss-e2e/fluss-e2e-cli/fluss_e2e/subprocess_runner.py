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

"""Shared subprocess helpers."""

from __future__ import annotations

from dataclasses import dataclass
import subprocess
import time
from typing import Mapping

from .errors import E2EError


@dataclass
class CommandResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    duration_ms: int


@dataclass
class CommandTimeout(Exception):
    command: list[str]
    timeout_s: int
    stdout: str = ""
    stderr: str = ""
    duration_ms: int = 0

    def __post_init__(self) -> None:
        super().__init__(f"Command timed out after {self.timeout_s} seconds.")


def _normalize_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def run_command(
    command: list[str],
    *,
    cwd: str,
    env: Mapping[str, str] | None = None,
    timeout_s: int,
) -> CommandResult:
    start = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=dict(env) if env is not None else None,
            text=True,
            capture_output=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        duration_ms = int((time.monotonic() - start) * 1000)
        raise CommandTimeout(
            command=command,
            timeout_s=timeout_s,
            stdout=_normalize_output(exc.stdout),
            stderr=_normalize_output(exc.stderr),
            duration_ms=duration_ms,
        ) from exc
    duration_ms = int((time.monotonic() - start) * 1000)
    return CommandResult(
        command=command,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        duration_ms=duration_ms,
    )


def failure_details(result: CommandResult) -> dict[str, object]:
    return {
        "command": result.command,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "duration_ms": result.duration_ms,
    }


def timeout_details(
    command: list[str],
    timeout_s: int,
    *,
    stdout: str = "",
    stderr: str = "",
    duration_ms: int = 0,
) -> dict[str, object]:
    details: dict[str, object] = {"command": command, "timeout_seconds": timeout_s}
    if stdout:
        details["stdout"] = stdout
    if stderr:
        details["stderr"] = stderr
    if duration_ms:
        details["duration_ms"] = duration_ms
    return details


def ensure_success(result: CommandResult, error: E2EError) -> CommandResult:
    if result.returncode != 0:
        raise error
    return result
