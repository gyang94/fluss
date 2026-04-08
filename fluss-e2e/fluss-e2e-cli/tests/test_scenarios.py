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

"""Tests for scenario behavior via the YAML engine."""

from __future__ import annotations

from types import SimpleNamespace
import unittest

from fluss_e2e.errors import ConfigError
from fluss_e2e.scenarios.base import YamlScenario
from fluss_e2e.scenarios.loader import load_scenario


class FakeClient:
    def __init__(self) -> None:
        self.created_tables: list[str] = []
        self.dropped_tables: list[str] = []

    def create_table(self, name: str, database: str = "e2e", buckets: int = 4, **kwargs) -> dict[str, object]:
        self.created_tables.append(name)
        return {"ok": True}

    def write_rows(self, table: str, count: int, database: str = "e2e", *, start_id: int = 0) -> dict[str, object]:
        return {"count": count, "checksum": "abc123", "latency_ms": 17}

    def scan_table(self, table: str, *, database: str = "e2e", limit: int | None = None) -> dict[str, object]:
        return {"count": limit, "checksum": "abc123", "latency_ms": 9}

    def drop_table(self, name: str, database: str = "e2e") -> dict[str, object]:
        self.dropped_tables.append(name)
        return {"ok": True}


class WriteReadScenarioTest(unittest.TestCase):
    def test_validate_args_rejects_non_positive_rows(self) -> None:
        config, _compose_path = load_scenario("write-read")
        scenario = YamlScenario(config)

        with self.assertRaises(ConfigError):
            scenario.validate_args(SimpleNamespace(rows=0))

    def test_run_produces_passed_result(self) -> None:
        config, _compose_path = load_scenario("write-read")
        scenario = YamlScenario(config)
        client = FakeClient()

        result = scenario.run(client=client, args=SimpleNamespace(rows=3))

        self.assertEqual(result["status"], "passed")
        self.assertEqual(len(client.created_tables), 1)
        self.assertEqual(client.created_tables, client.dropped_tables)
