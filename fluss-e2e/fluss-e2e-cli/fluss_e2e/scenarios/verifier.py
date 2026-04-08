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

"""Pluggable verification for scenario results."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
import logging
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

from .resolver import evaluate_assertion, resolve_value
from .schema import (
    DataVerifyRule,
    HealthVerifyRule,
    LogVerifyRule,
    MetricsVerifyRule,
    VerifyRule,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VerifyResult:
    """Outcome of a single verification rule."""

    rule_type: str
    passed: bool
    details: tuple[dict[str, Any], ...] = ()


class Verifier(ABC):
    @abstractmethod
    def verify(self, rule: VerifyRule, context: dict[str, Any]) -> VerifyResult:
        raise NotImplementedError


class DataVerifier(Verifier):
    """Evaluates ${...} assertions against step results in context."""

    def verify(self, rule: VerifyRule, context: dict[str, Any]) -> VerifyResult:
        if not isinstance(rule, DataVerifyRule):
            raise TypeError(f"Expected DataVerifyRule, got {type(rule).__name__}")
        details: list[dict[str, Any]] = []
        all_passed = True
        for expr in rule.assert_exprs:
            passed, expected, actual = evaluate_assertion(expr, context)
            details.append({
                "expression": expr,
                "passed": passed,
                "expected": expected,
                "actual": actual,
            })
            if not passed:
                all_passed = False
        return VerifyResult(
            rule_type="data",
            passed=all_passed,
            details=tuple(details),
        )


class MetricsVerifier(Verifier):
    """Queries Prometheus and evaluates metric assertions."""

    def verify(self, rule: VerifyRule, context: dict[str, Any]) -> VerifyResult:
        if not isinstance(rule, MetricsVerifyRule):
            raise TypeError(f"Expected MetricsVerifyRule, got {type(rule).__name__}")
        details: list[dict[str, Any]] = []
        all_passed = True

        for query in rule.queries:
            resolved_labels = {
                k: resolve_value(v, context) for k, v in query.labels.items()
            }
            label_selectors = ",".join(
                f'{k}="{v}"' for k, v in resolved_labels.items()
            )
            prom_query = f'{query.metric}{{{label_selectors}}}'
            value = self._query_prometheus(rule.source, prom_query)
            if value is None:
                details.append({
                    "metric": query.metric,
                    "passed": False,
                    "error": "no data returned",
                })
                all_passed = False
                continue

            passed = self._evaluate_threshold(value, query.assert_expr)
            details.append({
                "metric": query.metric,
                "passed": passed,
                "value": value,
                "assertion": query.assert_expr,
            })
            if not passed:
                all_passed = False

        return VerifyResult(
            rule_type="metrics",
            passed=all_passed,
            details=tuple(details),
        )

    @staticmethod
    def _query_prometheus(source: str, query: str) -> float | None:
        params = urllib.parse.urlencode({"query": query})
        url = f"{source.rstrip('/')}/api/v1/query?{params}"
        try:
            request = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(request, timeout=10) as response:
                body = response.read().decode("utf-8")
                payload = json.loads(body)
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            logger.warning("Prometheus query failed: %s", exc)
            return None

        results = payload.get("data", {}).get("result", [])
        if not results:
            return None
        raw_value = results[0].get("value", [None, None])
        if len(raw_value) < 2:
            return None
        try:
            return float(raw_value[1])
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _evaluate_threshold(value: float, assert_expr: str) -> bool:
        expr = assert_expr.strip()
        for op_str, op_fn in [
            (">=", lambda a, b: a >= b),
            ("<=", lambda a, b: a <= b),
            ("!=", lambda a, b: a != b),
            ("==", lambda a, b: a == b),
            (">", lambda a, b: a > b),
            ("<", lambda a, b: a < b),
        ]:
            if expr.startswith(op_str):
                threshold = float(expr[len(op_str):].strip())
                return op_fn(value, threshold)
        return False


class LogVerifier(Verifier):
    """Searches Docker container logs for pattern presence/absence."""

    def __init__(
        self,
        *,
        log_fetcher: Callable[[str], str] | None = None,
    ) -> None:
        self._log_fetcher = log_fetcher

    def verify(self, rule: VerifyRule, context: dict[str, Any]) -> VerifyResult:
        if not isinstance(rule, LogVerifyRule):
            raise TypeError(f"Expected LogVerifyRule, got {type(rule).__name__}")
        container = (
            resolve_value(rule.container, context)
            if "${" in rule.container
            else rule.container
        )

        if self._log_fetcher is not None:
            logs = self._log_fetcher(container)
        else:
            logs = self._fetch_docker_logs(container)

        details: list[dict[str, Any]] = []
        all_passed = True

        for log_assert in rule.asserts:
            found = bool(re.search(log_assert.pattern, logs))
            passed = found == log_assert.exists
            details.append({
                "pattern": log_assert.pattern,
                "exists_expected": log_assert.exists,
                "found": found,
                "passed": passed,
            })
            if not passed:
                all_passed = False

        return VerifyResult(
            rule_type="logs",
            passed=all_passed,
            details=tuple(details),
        )

    @staticmethod
    def _fetch_docker_logs(container: str) -> str:
        result = subprocess.run(
            ["docker", "logs", container],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.stdout + result.stderr


class HealthVerifier(Verifier):
    """Makes HTTP calls and checks status codes."""

    def verify(self, rule: VerifyRule, context: dict[str, Any]) -> VerifyResult:
        if not isinstance(rule, HealthVerifyRule):
            raise TypeError(f"Expected HealthVerifyRule, got {type(rule).__name__}")
        details: list[dict[str, Any]] = []
        all_passed = True

        for check in rule.checks:
            url = (
                resolve_value(check.url, context)
                if "${" in check.url
                else check.url
            )
            try:
                request = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(request, timeout=10) as response:
                    actual_status = response.status
            except urllib.error.HTTPError as exc:
                actual_status = exc.code
            except (urllib.error.URLError, OSError):
                actual_status = 0

            passed = actual_status == check.status
            details.append({
                "url": url,
                "expected_status": check.status,
                "actual_status": actual_status,
                "passed": passed,
            })
            if not passed:
                all_passed = False

        return VerifyResult(
            rule_type="health",
            passed=all_passed,
            details=tuple(details),
        )


_VERIFIER_REGISTRY: dict[str, Callable[..., Verifier]] = {
    "data": lambda **kw: DataVerifier(),
    "metrics": lambda **kw: MetricsVerifier(),
    "logs": lambda **kw: LogVerifier(**kw),
    "health": lambda **kw: HealthVerifier(),
}


def run_verify(
    rules: tuple[VerifyRule, ...],
    context: dict[str, Any],
    *,
    log_fetcher: Callable[[str], str] | None = None,
) -> list[VerifyResult]:
    """Run all verification rules and return results (never short-circuits)."""
    results: list[VerifyResult] = []
    for rule in rules:
        factory = _VERIFIER_REGISTRY.get(rule.type)
        if factory is None:
            results.append(VerifyResult(
                rule_type=rule.type,
                passed=False,
                details=({"error": f"unknown verify type: {rule.type}"},),
            ))
            continue
        kwargs: dict[str, Any] = {}
        if rule.type == "logs" and log_fetcher is not None:
            kwargs["log_fetcher"] = log_fetcher
        verifier = factory(**kwargs)
        results.append(verifier.verify(rule, context))
    return results
