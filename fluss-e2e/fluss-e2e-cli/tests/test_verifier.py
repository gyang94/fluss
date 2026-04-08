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

"""Tests for the verifier module."""

from __future__ import annotations

import json
import unittest
from unittest.mock import patch, MagicMock

from fluss_e2e.scenarios.schema import (
    DataVerifyRule,
    HealthCheck,
    HealthVerifyRule,
    LogAssert,
    LogVerifyRule,
    MetricsQuery,
    MetricsVerifyRule,
)
from fluss_e2e.scenarios.verifier import (
    DataVerifier,
    HealthVerifier,
    LogVerifier,
    MetricsVerifier,
    VerifyResult,
    run_verify,
)


class DataVerifierTest(unittest.TestCase):
    def test_passing_assertion(self) -> None:
        rule = DataVerifyRule(assert_exprs=("${scan.count} == ${expected}",))
        context = {"scan": {"count": 10}, "expected": 10}
        verifier = DataVerifier()
        result = verifier.verify(rule, context)
        self.assertTrue(result.passed)
        self.assertEqual(len(result.details), 1)

    def test_failing_assertion(self) -> None:
        rule = DataVerifyRule(assert_exprs=("${scan.count} == ${expected}",))
        context = {"scan": {"count": 5}, "expected": 10}
        verifier = DataVerifier()
        result = verifier.verify(rule, context)
        self.assertFalse(result.passed)

    def test_multiple_assertions_all_must_pass(self) -> None:
        rule = DataVerifyRule(
            assert_exprs=(
                "${scan.count} == ${expected_count}",
                "${scan.checksum} == abc",
            ),
        )
        context = {"scan": {"count": 10, "checksum": "abc"}, "expected_count": 10}
        verifier = DataVerifier()
        result = verifier.verify(rule, context)
        self.assertTrue(result.passed)
        self.assertEqual(len(result.details), 2)

    def test_one_failing_makes_result_fail(self) -> None:
        rule = DataVerifyRule(
            assert_exprs=(
                "${scan.count} == ${expected_count}",
                "${scan.checksum} == wrong",
            ),
        )
        context = {"scan": {"count": 10, "checksum": "abc"}, "expected_count": 10}
        verifier = DataVerifier()
        result = verifier.verify(rule, context)
        self.assertFalse(result.passed)


class LogVerifierTest(unittest.TestCase):
    def test_pattern_found_when_expected(self) -> None:
        rule = LogVerifyRule(
            container="coordinator-server",
            asserts=(LogAssert(pattern="Table .* created", exists=True),),
        )
        fake_logs = "2026-01-01 Table foo created successfully\nINFO ready"
        verifier = LogVerifier(log_fetcher=lambda container: fake_logs)
        result = verifier.verify(rule, {})
        self.assertTrue(result.passed)

    def test_pattern_not_found_when_expected_fails(self) -> None:
        rule = LogVerifyRule(
            container="coordinator-server",
            asserts=(LogAssert(pattern="FATAL", exists=True),),
        )
        fake_logs = "INFO all good"
        verifier = LogVerifier(log_fetcher=lambda container: fake_logs)
        result = verifier.verify(rule, {})
        self.assertFalse(result.passed)

    def test_pattern_found_when_not_expected_fails(self) -> None:
        rule = LogVerifyRule(
            container="coordinator-server",
            asserts=(LogAssert(pattern="OutOfMemoryError", exists=False),),
        )
        fake_logs = "ERROR OutOfMemoryError occurred"
        verifier = LogVerifier(log_fetcher=lambda container: fake_logs)
        result = verifier.verify(rule, {})
        self.assertFalse(result.passed)

    def test_pattern_not_found_when_not_expected_passes(self) -> None:
        rule = LogVerifyRule(
            container="coordinator-server",
            asserts=(LogAssert(pattern="FATAL", exists=False),),
        )
        fake_logs = "INFO all good"
        verifier = LogVerifier(log_fetcher=lambda container: fake_logs)
        result = verifier.verify(rule, {})
        self.assertTrue(result.passed)


class HealthVerifierTest(unittest.TestCase):
    @patch("fluss_e2e.scenarios.verifier.urllib.request.urlopen")
    def test_healthy_endpoint(self, mock_urlopen) -> None:
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        rule = HealthVerifyRule(
            checks=(HealthCheck(url="http://localhost:9123/health", status=200),),
        )
        verifier = HealthVerifier()
        result = verifier.verify(rule, {})
        self.assertTrue(result.passed)

    @patch("fluss_e2e.scenarios.verifier.urllib.request.urlopen")
    def test_unhealthy_endpoint(self, mock_urlopen) -> None:
        mock_response = MagicMock()
        mock_response.status = 503
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        rule = HealthVerifyRule(
            checks=(HealthCheck(url="http://localhost:9123/health", status=200),),
        )
        verifier = HealthVerifier()
        result = verifier.verify(rule, {})
        self.assertFalse(result.passed)


class MetricsVerifierTest(unittest.TestCase):
    @patch("fluss_e2e.scenarios.verifier.urllib.request.urlopen")
    def test_metric_above_threshold(self, mock_urlopen) -> None:
        prometheus_response = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {"__name__": "fluss_table_record_count", "table": "t1"},
                        "value": [1234567890, "42"],
                    }
                ],
            },
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(prometheus_response).encode()
        mock_response.status = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        rule = MetricsVerifyRule(
            source="http://localhost:9090",
            queries=(
                MetricsQuery(
                    metric="fluss_table_record_count",
                    labels={"table": "t1"},
                    assert_expr="> 0",
                ),
            ),
        )
        verifier = MetricsVerifier()
        result = verifier.verify(rule, {})
        self.assertTrue(result.passed)


class RunVerifyTest(unittest.TestCase):
    def test_run_verify_aggregates_results(self) -> None:
        rule1 = DataVerifyRule(assert_exprs=("${x} == ${ex}",))
        rule2 = DataVerifyRule(assert_exprs=("${y} == ${ey}",))
        context = {"x": 1, "y": 2, "ex": 1, "ey": 2}
        results = run_verify((rule1, rule2), context)
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.passed for r in results))

    def test_run_verify_collects_all_even_on_failure(self) -> None:
        rule1 = DataVerifyRule(assert_exprs=("${x} == ${ex}",))
        rule2 = DataVerifyRule(assert_exprs=("${y} == ${ey}",))
        context = {"x": 1, "y": 2, "ex": 1, "ey": 999}
        results = run_verify((rule1, rule2), context)
        self.assertEqual(len(results), 2)
        self.assertTrue(results[0].passed)
        self.assertFalse(results[1].passed)

    def test_run_verify_unknown_rule_type(self) -> None:
        from fluss_e2e.scenarios.schema import VerifyRule
        rule = VerifyRule(type="unknown")
        results = run_verify((rule,), {})
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].passed)


if __name__ == "__main__":
    unittest.main()
