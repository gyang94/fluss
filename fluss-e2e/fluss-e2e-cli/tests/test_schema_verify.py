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

"""Tests for verify dataclasses in schema.py."""

from __future__ import annotations

import unittest

from fluss_e2e.scenarios.schema import (
    DataVerifyRule,
    HealthCheck,
    HealthVerifyRule,
    LogAssert,
    LogVerifyRule,
    MetricsQuery,
    MetricsVerifyRule,
    ScenarioConfig,
    StepConfig,
    VerifyRule,
)


class TestDataVerifyRule(unittest.TestCase):
    def test_frozen(self) -> None:
        rule = DataVerifyRule(assert_exprs=("row_count > 0",))
        with self.assertRaises(AttributeError):
            rule.type = "other"  # type: ignore[misc]

    def test_default_type(self) -> None:
        rule = DataVerifyRule(assert_exprs=("row_count > 0", "checksum == expected"))
        self.assertEqual(rule.type, "data")

    def test_assert_exprs(self) -> None:
        exprs = ("row_count > 0", "checksum == expected")
        rule = DataVerifyRule(assert_exprs=exprs)
        self.assertEqual(rule.assert_exprs, exprs)

    def test_is_verify_rule(self) -> None:
        rule = DataVerifyRule(assert_exprs=())
        self.assertIsInstance(rule, VerifyRule)


class TestMetricsVerifyRule(unittest.TestCase):
    def test_with_queries(self) -> None:
        query = MetricsQuery(
            metric="fluss_records_total",
            labels={"table": "test"},
            assert_expr="value > 0",
        )
        rule = MetricsVerifyRule(source="prometheus", queries=(query,))
        self.assertEqual(rule.type, "metrics")
        self.assertEqual(rule.source, "prometheus")
        self.assertEqual(len(rule.queries), 1)
        self.assertEqual(rule.queries[0].metric, "fluss_records_total")
        self.assertEqual(rule.queries[0].labels, {"table": "test"})
        self.assertEqual(rule.queries[0].assert_expr, "value > 0")

    def test_is_verify_rule(self) -> None:
        rule = MetricsVerifyRule(source="prometheus", queries=())
        self.assertIsInstance(rule, VerifyRule)

    def test_frozen(self) -> None:
        rule = MetricsVerifyRule(source="prometheus", queries=())
        with self.assertRaises(AttributeError):
            rule.source = "other"  # type: ignore[misc]


class TestLogVerifyRule(unittest.TestCase):
    def test_with_asserts(self) -> None:
        log_assert = LogAssert(pattern="ERROR", exists=False)
        rule = LogVerifyRule(container="coordinator-server", asserts=(log_assert,))
        self.assertEqual(rule.type, "logs")
        self.assertEqual(rule.container, "coordinator-server")
        self.assertEqual(len(rule.asserts), 1)
        self.assertFalse(rule.asserts[0].exists)

    def test_log_assert_defaults(self) -> None:
        la = LogAssert(pattern="Started")
        self.assertTrue(la.exists)

    def test_is_verify_rule(self) -> None:
        rule = LogVerifyRule(container="ts-0", asserts=())
        self.assertIsInstance(rule, VerifyRule)


class TestHealthVerifyRule(unittest.TestCase):
    def test_with_checks(self) -> None:
        check = HealthCheck(url="http://localhost:9123/status")
        rule = HealthVerifyRule(checks=(check,))
        self.assertEqual(rule.type, "health")
        self.assertEqual(rule.checks[0].url, "http://localhost:9123/status")
        self.assertEqual(rule.checks[0].status, 200)

    def test_custom_status(self) -> None:
        check = HealthCheck(url="http://localhost:8081/ready", status=204)
        self.assertEqual(check.status, 204)

    def test_is_verify_rule(self) -> None:
        rule = HealthVerifyRule(checks=())
        self.assertIsInstance(rule, VerifyRule)


class TestStepConfigNewFields(unittest.TestCase):
    def test_save_as_default_none(self) -> None:
        step = StepConfig(action="write")
        self.assertIsNone(step.save_as)

    def test_save_as_set(self) -> None:
        step = StepConfig(action="write", save_as="write_result")
        self.assertEqual(step.save_as, "write_result")

    def test_command_default_none(self) -> None:
        step = StepConfig(action="exec")
        self.assertIsNone(step.command)

    def test_method_default_none(self) -> None:
        step = StepConfig(action="http")
        self.assertIsNone(step.method)

    def test_url_default_none(self) -> None:
        step = StepConfig(action="http")
        self.assertIsNone(step.url)

    def test_headers_default_empty(self) -> None:
        step = StepConfig(action="http")
        self.assertEqual(step.headers, {})

    def test_body_default_none(self) -> None:
        step = StepConfig(action="http")
        self.assertIsNone(step.body)

    def test_http_step_all_fields(self) -> None:
        step = StepConfig(
            action="http",
            method="POST",
            url="http://localhost:8081/submit",
            headers={"Content-Type": "application/json"},
            body='{"key": "value"}',
            save_as="response",
        )
        self.assertEqual(step.method, "POST")
        self.assertEqual(step.url, "http://localhost:8081/submit")
        self.assertEqual(step.headers, {"Content-Type": "application/json"})
        self.assertEqual(step.body, '{"key": "value"}')
        self.assertEqual(step.save_as, "response")


class TestScenarioConfigVerify(unittest.TestCase):
    def test_verify_default_empty(self) -> None:
        from fluss_e2e.scenarios.schema import (
            ClusterConfig,
            MetaConfig,
            TableConfig,
        )

        config = ScenarioConfig(
            meta=MetaConfig(name="test", description="test"),
            cluster=ClusterConfig(),
            table=TableConfig(name_prefix="t"),
            params={},
            steps=(),
        )
        self.assertEqual(config.verify, ())

    def test_verify_with_rules(self) -> None:
        from fluss_e2e.scenarios.schema import (
            ClusterConfig,
            MetaConfig,
            TableConfig,
        )

        rule = DataVerifyRule(assert_exprs=("row_count > 0",))
        config = ScenarioConfig(
            meta=MetaConfig(name="test", description="test"),
            cluster=ClusterConfig(),
            table=TableConfig(name_prefix="t"),
            params={},
            steps=(),
            verify=(rule,),
        )
        self.assertEqual(len(config.verify), 1)
        self.assertIsInstance(config.verify[0], DataVerifyRule)


if __name__ == "__main__":
    unittest.main()
