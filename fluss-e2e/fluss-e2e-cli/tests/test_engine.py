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

"""Tests for the YAML scenario execution engine."""

from __future__ import annotations

from types import SimpleNamespace
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

from fluss_e2e.errors import (
    ConfigError,
    ScenarioFailure,
)
from fluss_e2e.scenarios.base import YamlScenario
from fluss_e2e.scenarios.engine import ScenarioEngine
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

    def ping(self) -> dict[str, object]:
        return {"ok": True, "server_count": 2}


def _minimal_config(
    *,
    steps: list[dict[str, Any]] | None = None,
    name: str = "test",
) -> "ScenarioConfig":
    """Build a minimal ScenarioConfig for testing."""
    from fluss_e2e.scenarios.schema import (
        ClusterConfig,
        MetaConfig,
        ScenarioConfig,
        StepConfig,
        TableConfig,
    )
    parsed_steps = []
    for raw in (steps or []):
        parsed_steps.append(StepConfig(
            action=raw["action"],
            id=raw.get("id"),
            save_as=raw.get("save_as", raw.get("save-as")),
            args=dict(raw.get("args", {})),
            on_error=raw.get("on-error", "fail"),
            command=raw.get("command"),
            method=raw.get("method"),
            url=raw.get("url"),
            headers=dict(raw.get("headers", {})),
            body=raw.get("body"),
        ))
    return ScenarioConfig(
        meta=MetaConfig(name=name, description="test"),
        cluster=ClusterConfig(),
        table=TableConfig(name_prefix="test"),
        params={},
        steps=tuple(parsed_steps),
    )


class WriteReadEngineTest(unittest.TestCase):
    def test_yaml_scenario_run_passes(self) -> None:
        config, _compose_path = load_scenario("write-read")
        scenario = YamlScenario(config)
        client = FakeClient()

        result = scenario.run(client=client, args=SimpleNamespace(rows=3))

        self.assertEqual(result["status"], "passed")
        self.assertTrue(result["validation"]["passed"])
        self.assertEqual(len(client.created_tables), 1)
        self.assertEqual(len(client.dropped_tables), 1)

    def test_yaml_scenario_validate_args_rejects_zero_rows(self) -> None:
        config, _compose_path = load_scenario("write-read")
        scenario = YamlScenario(config)

        with self.assertRaises(ConfigError):
            scenario.validate_args(SimpleNamespace(rows=0))

    def test_engine_write_read_checks(self) -> None:
        config, _compose_path = load_scenario("write-read")
        client = FakeClient()
        engine = ScenarioEngine(
            config, client=client, cli_params={"rows": 5},
        )
        result = engine.execute()

        self.assertEqual(result["status"], "passed")
        checks = result["validation"]["checks"]
        check_names = [c["name"] for c in checks]
        self.assertIn("row_count_match", check_names)
        self.assertIn("data_integrity", check_names)
        self.assertTrue(all(c["passed"] for c in checks))


class EngineContextTest(unittest.TestCase):
    def test_step_results_stored_by_id(self) -> None:
        config, _compose_path = load_scenario("write-read")
        client = FakeClient()
        engine = ScenarioEngine(config, client=client, cli_params={"rows": 5})
        engine.execute()

        self.assertIn("write", engine.context)
        self.assertIn("scan", engine.context)
        self.assertEqual(engine.context["write"]["count"], 5)

    def test_cli_params_override_yaml_defaults(self) -> None:
        config, _compose_path = load_scenario("write-read")
        client = FakeClient()
        engine = ScenarioEngine(config, client=client, cli_params={"rows": 42})

        self.assertEqual(engine.context["params"]["rows"], 42)


class FakeTableOpsClient:
    def __init__(self) -> None:
        self.tables: dict[str, dict[str, object]] = {}
        self.dropped_tables: list[str] = []
        self._next_table_id = 1

    def create_table(self, name: str, database: str = "e2e", buckets: int = 4, **kwargs) -> dict[str, object]:
        payload = {
            "database": database, "table": name,
            "table_id": self._next_table_id, "schema_id": 1,
            "bucket_count": buckets, "primary_keys": ["id"],
            "columns": [
                {"name": "id", "type": "INT", "comment": None},
                {"name": "name", "type": "STRING", "comment": None},
                {"name": "value", "type": "BIGINT", "comment": None},
                {"name": "ts", "type": "TIMESTAMP(3)", "comment": None},
            ],
        }
        self.tables[name] = payload
        self._next_table_id += 1
        return dict(payload)

    def list_tables(self, database: str = "e2e") -> list[str]:
        return sorted(self.tables)

    def get_table(self, name: str, database: str = "e2e") -> dict[str, object]:
        return dict(self.tables[name])

    def alter_table_add_column(
        self, name: str, *, column_name: str, column_type: str, database: str = "e2e",
    ) -> dict[str, object]:
        payload = dict(self.tables[name])
        payload["schema_id"] = int(payload["schema_id"]) + 1
        payload["columns"] = list(payload["columns"]) + [
            {"name": column_name, "type": column_type, "comment": None}
        ]
        payload["added_column"] = column_name
        self.tables[name] = payload
        return dict(payload)

    def drop_table(self, name: str, database: str = "e2e") -> dict[str, object]:
        self.tables.pop(name, None)
        self.dropped_tables.append(name)
        return {"ok": True}


class TableOpsEngineTest(unittest.TestCase):
    def test_table_ops_creates_and_drops_multiple_tables(self) -> None:
        config, _compose_path = load_scenario("table-ops")
        client = FakeTableOpsClient()

        engine = ScenarioEngine(config, client=client, cli_params={"tables": 3})
        result = engine.execute()

        self.assertEqual(result["status"], "passed")
        self.assertEqual(len(client.dropped_tables), 3)
        self.assertEqual(client.tables, {})

    def test_table_ops_validate_args_rejects_zero(self) -> None:
        config, _compose_path = load_scenario("table-ops")
        scenario = YamlScenario(config)

        with self.assertRaises(ConfigError):
            scenario.validate_args(SimpleNamespace(tables=0))

    def test_table_ops_alters_schema(self) -> None:
        config, _compose_path = load_scenario("table-ops")
        client = FakeTableOpsClient()

        engine = ScenarioEngine(config, client=client, cli_params={"tables": 2})
        result = engine.execute()

        checks = result["validation"]["checks"]
        alter_check = next(c for c in checks if c["name"] == "alter_increments_schema_id")
        self.assertTrue(alter_check["passed"])


class ShellActionTest(unittest.TestCase):
    @patch("fluss_e2e.scenarios.engine.subprocess.run")
    def test_shell_action_captures_stdout(self, mock_run) -> None:
        mock_run.return_value = SimpleNamespace(
            stdout="hello world\n", stderr="", returncode=0,
        )
        config = _minimal_config(
            steps=[{"action": "shell", "command": "echo hello world", "save_as": "out"}],
        )
        client = FakeClient()
        engine = ScenarioEngine(config, client=client)
        engine.execute()

        self.assertIn("out", engine.context)
        self.assertEqual(engine.context["out"]["stdout"], "hello world\n")
        self.assertEqual(engine.context["out"]["returncode"], 0)

    @patch("fluss_e2e.scenarios.engine.subprocess.run")
    def test_shell_action_nonzero_exit_raises(self, mock_run) -> None:
        mock_run.return_value = SimpleNamespace(
            stdout="", stderr="error!", returncode=1,
        )
        config = _minimal_config(
            steps=[{"action": "shell", "command": "false"}],
        )
        client = FakeClient()
        engine = ScenarioEngine(config, client=client)
        with self.assertRaises(ScenarioFailure):
            engine.execute()


class HttpActionTest(unittest.TestCase):
    @patch("fluss_e2e.scenarios.engine.urllib.request.urlopen")
    def test_http_get_action(self, mock_urlopen) -> None:
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = b'{"ok": true}'
        mock_response.getheaders.return_value = []
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        config = _minimal_config(
            steps=[{
                "action": "http",
                "method": "GET",
                "url": "http://localhost:9123/status",
                "save_as": "resp",
            }],
        )
        client = FakeClient()
        engine = ScenarioEngine(config, client=client)
        engine.execute()

        self.assertIn("resp", engine.context)
        self.assertEqual(engine.context["resp"]["status"], 200)


class EngineVerifyIntegrationTest(unittest.TestCase):
    def test_engine_runs_verify_after_steps(self) -> None:
        from fluss_e2e.scenarios.schema import DataVerifyRule, ScenarioConfig
        config = _minimal_config(
            steps=[{"action": "ping", "save_as": "ping_result"}],
        )
        # Create new config with verify rules
        config_with_verify = ScenarioConfig(
            meta=config.meta,
            cluster=config.cluster,
            table=config.table,
            params=config.params,
            steps=config.steps,
            verify=(DataVerifyRule(assert_exprs=("${ping_result.ok} == ${expected}",)),),
        )
        client = FakeClient()
        engine = ScenarioEngine(config_with_verify, client=client)
        # Store expected value in context before execution
        engine._context["expected"] = True
        result = engine.execute()
        self.assertEqual(result["status"], "passed")
        self.assertIn("verify", result)
        self.assertTrue(result["verify"]["passed"])

    def test_engine_verify_failure_raises(self) -> None:
        from fluss_e2e.scenarios.schema import DataVerifyRule, ScenarioConfig
        config = _minimal_config(
            steps=[{"action": "ping", "save_as": "ping_result"}],
        )
        config_with_verify = ScenarioConfig(
            meta=config.meta,
            cluster=config.cluster,
            table=config.table,
            params=config.params,
            steps=config.steps,
            verify=(DataVerifyRule(assert_exprs=("${ping_result.ok} == ${expected}",)),),
        )
        client = FakeClient()
        engine = ScenarioEngine(config_with_verify, client=client)
        engine._context["expected"] = False
        with self.assertRaises(ScenarioFailure):
            engine.execute()


class SaveAsTest(unittest.TestCase):
    def test_save_as_stores_result_under_given_key(self) -> None:
        config = _minimal_config(
            steps=[
                {"action": "ping", "save_as": "my_ping"},
            ],
        )
        client = FakeClient()
        engine = ScenarioEngine(config, client=client)
        engine.execute()

        self.assertIn("my_ping", engine.context)


if __name__ == "__main__":
    unittest.main()
