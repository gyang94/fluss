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

"""Tests for the expression resolver."""

from __future__ import annotations

import unittest

from fluss_e2e.scenarios.resolver import evaluate_assertion, resolve_value


class ResolveValueTest(unittest.TestCase):
    def test_resolve_simple_variable(self) -> None:
        result = resolve_value("${params.rows}", {"params": {"rows": 100}})
        self.assertEqual(result, 100)

    def test_resolve_nested_dict_access(self) -> None:
        result = resolve_value(
            "${scan.checksum}",
            {"scan": {"checksum": "abc123", "count": 10}},
        )
        self.assertEqual(result, "abc123")

    def test_resolve_arithmetic(self) -> None:
        result = resolve_value("${params.rows * 2}", {"params": {"rows": 50}})
        self.assertEqual(result, 100)

    def test_resolve_inline_substitution(self) -> None:
        result = resolve_value(
            "table_${params.name}_suffix",
            {"params": {"name": "test"}},
        )
        self.assertEqual(result, "table_test_suffix")

    def test_resolve_non_string_passthrough(self) -> None:
        self.assertEqual(resolve_value(42, {}), 42)
        self.assertIsNone(resolve_value(None, {}))

    def test_resolve_function_call(self) -> None:
        def double(x):
            return x * 2

        result = resolve_value("${double(5)}", {"double": double})
        self.assertEqual(result, 10)

    def test_resolve_unknown_variable_raises_key_error(self) -> None:
        with self.assertRaises(KeyError):
            resolve_value("${missing.field}", {})

    def test_resolve_string_without_expressions(self) -> None:
        result = resolve_value("hello world", {})
        self.assertEqual(result, "hello world")


class EvaluateAssertionTest(unittest.TestCase):
    def test_equality_passes(self) -> None:
        passed, expected, actual = evaluate_assertion(
            "${scan.count} == ${params.rows}",
            {"scan": {"count": 10}, "params": {"rows": 10}},
        )
        self.assertTrue(passed)
        self.assertEqual(expected, 10)
        self.assertEqual(actual, 10)

    def test_equality_fails(self) -> None:
        passed, expected, actual = evaluate_assertion(
            "${scan.count} == ${params.rows}",
            {"scan": {"count": 5}, "params": {"rows": 10}},
        )
        self.assertFalse(passed)

    def test_inequality(self) -> None:
        passed, _, _ = evaluate_assertion(
            "${a} != ${b}",
            {"a": 1, "b": 2},
        )
        self.assertTrue(passed)

    def test_greater_than(self) -> None:
        passed, _, _ = evaluate_assertion(
            "${after} > ${before}",
            {"after": 3, "before": 1},
        )
        self.assertTrue(passed)

    def test_in_operator(self) -> None:
        passed, _, _ = evaluate_assertion(
            "${state} in ['exited', 'stopped', 'dead']",
            {"state": "exited"},
        )
        self.assertTrue(passed)

    def test_in_operator_fails(self) -> None:
        passed, _, _ = evaluate_assertion(
            "${state} in ['exited', 'stopped', 'dead']",
            {"state": "running"},
        )
        self.assertFalse(passed)

    def test_arithmetic_in_assertion(self) -> None:
        passed, expected, actual = evaluate_assertion(
            "${scan.count} == ${params.rows * 2}",
            {"scan": {"count": 20}, "params": {"rows": 10}},
        )
        self.assertTrue(passed)
        self.assertEqual(expected, 20)
        self.assertEqual(actual, 20)

    def test_function_call_in_assertion(self) -> None:
        def checksum_fn(start, count):
            return f"hash_{start}_{count}"

        passed, expected, actual = evaluate_assertion(
            "${result} == ${compute(0, 10)}",
            {"result": "hash_0_10", "compute": checksum_fn},
        )
        self.assertTrue(passed)

    def test_unsupported_assertion_raises(self) -> None:
        with self.assertRaises(ValueError):
            evaluate_assertion("some random text", {})


if __name__ == "__main__":
    unittest.main()
