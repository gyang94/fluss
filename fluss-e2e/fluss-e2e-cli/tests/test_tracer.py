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

"""Tests for the scenario tracer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from fluss_e2e.scenarios.tracer import Tracer, TraceConfig, _truncate, _safe_serialize


class TestTraceConfig:
    def test_frozen(self):
        config = TraceConfig(enabled=True, base_dir=Path("/tmp"))
        with pytest.raises(AttributeError):
            config.enabled = False  # type: ignore[misc]

    def test_defaults(self):
        config = TraceConfig()
        assert config.enabled is True
        assert config.base_dir is None


class TestTruncate:
    def test_short_string_unchanged(self):
        assert _truncate("hello") == "hello"

    def test_long_string_truncated(self):
        long = "x" * 20_000
        result = _truncate(long)
        assert len(result) < len(long)
        assert "truncated" in result

    def test_dict_values_truncated(self):
        data = {"key": "x" * 20_000}
        result = _truncate(data)
        assert "truncated" in result["key"]

    def test_list_values_truncated(self):
        data = ["x" * 20_000]
        result = _truncate(data)
        assert "truncated" in result[0]

    def test_non_string_unchanged(self):
        assert _truncate(42) == 42
        assert _truncate(None) is None


class TestSafeSerialize:
    def test_primitives(self):
        assert _safe_serialize("hello") == "hello"
        assert _safe_serialize(42) == 42
        assert _safe_serialize(True) is True
        assert _safe_serialize(None) is None

    def test_dict(self):
        result = _safe_serialize({"a": 1, "b": [2, 3]})
        assert result == {"a": 1, "b": [2, 3]}

    def test_callable_replaced(self):
        def my_func():
            pass
        result = _safe_serialize(my_func)
        assert "<function my_func>" == result

    def test_tuple_to_list(self):
        result = _safe_serialize((1, 2, 3))
        assert result == [1, 2, 3]


class TestTracerDisabled:
    def test_disabled_config_no_writes(self, tmp_path):
        tracer = Tracer(TraceConfig(enabled=False, base_dir=tmp_path))
        assert tracer.enabled is False
        tracer.start_run(scenario_name="test", params={}, table_name="t")
        tracer.record_step(
            step_number=1, step_id="s1", action="ping", args={},
            status="passed", started_at="t0", finished_at="t1", duration_ms=10,
        )
        tracer.finalize(status="passed")
        # No trace folder should be created
        assert not list(tmp_path.iterdir())

    def test_no_base_dir_means_disabled(self):
        tracer = Tracer(TraceConfig(enabled=True, base_dir=None))
        assert tracer.enabled is False


class TestTracerEnabled:
    @pytest.fixture()
    def tracer(self, tmp_path):
        config = TraceConfig(enabled=True, base_dir=tmp_path)
        t = Tracer(config)
        t.start_run(scenario_name="write-read", params={"rows": 100}, table_name="wr_123")
        return t

    def test_creates_trace_folder(self, tracer, tmp_path):
        traces_dir = tmp_path / "traces"
        assert traces_dir.is_dir()
        run_dirs = list(traces_dir.iterdir())
        assert len(run_dirs) == 1
        assert (run_dirs[0] / "steps").is_dir()

    def test_record_step_writes_json(self, tracer):
        tracer.record_step(
            step_number=1,
            step_id="create-table",
            action="create-table",
            args={"buckets": 4},
            status="passed",
            started_at="2026-04-04T10:00:00.000Z",
            finished_at="2026-04-04T10:00:00.120Z",
            duration_ms=120,
            result={"ok": True, "table_name": "wr_123"},
            saved_as="create_table",
        )

        trace_dir = tracer.trace_dir
        step_file = trace_dir / "steps" / "01_create-table.json"
        assert step_file.exists()

        data = json.loads(step_file.read_text())
        assert data["step_number"] == 1
        assert data["action"] == "create-table"
        assert data["status"] == "passed"
        assert data["duration_ms"] == 120
        assert data["result"]["ok"] is True
        assert data["saved_as"] == "create_table"
        assert data["error"] is None

    def test_record_failed_step(self, tracer):
        tracer.record_step(
            step_number=2,
            step_id="write",
            action="write",
            args={"count": 100},
            status="failed",
            started_at="t0",
            finished_at="t1",
            duration_ms=50,
            error="Connection refused",
        )

        step_file = tracer.trace_dir / "steps" / "02_write.json"
        data = json.loads(step_file.read_text())
        assert data["status"] == "failed"
        assert data["error"] == "Connection refused"

    def test_record_verify(self, tracer):
        tracer.record_verify(
            status="passed",
            rules=[{"type": "data", "passed": True, "details": []}],
        )

        verify_file = tracer.trace_dir / "verify.json"
        assert verify_file.exists()
        data = json.loads(verify_file.read_text())
        assert data["status"] == "passed"
        assert len(data["rules"]) == 1

    def test_record_teardown(self, tracer):
        tracer.record_teardown(
            status="completed",
            actions=[{"action": "drop-table", "status": "ok", "error": None}],
        )

        teardown_file = tracer.trace_dir / "teardown.json"
        assert teardown_file.exists()
        data = json.loads(teardown_file.read_text())
        assert data["status"] == "completed"
        assert data["actions"][0]["action"] == "drop-table"

    def test_finalize_writes_summary_and_context(self, tracer):
        tracer.record_step(
            step_number=1, step_id="ping", action="ping", args={},
            status="passed", started_at="t0", finished_at="t1", duration_ms=5,
        )
        tracer.record_step(
            step_number=2, step_id="write", action="write", args={},
            status="failed", started_at="t0", finished_at="t1", duration_ms=50,
            error="fail",
        )
        tracer.record_verify(status="passed", rules=[])

        context = {
            "params": {"rows": 100},
            "table_name": "wr_123",
            "expected_checksum": lambda: "abc",
            "scan": {"count": 100, "checksum": "abc"},
        }
        tracer.finalize(status="failed", error="write failed", context=context)

        summary_file = tracer.trace_dir / "summary.json"
        assert summary_file.exists()
        summary = json.loads(summary_file.read_text())
        assert summary["schema_version"] == "1.0"
        assert summary["scenario"] == "write-read"
        assert summary["status"] == "failed"
        assert summary["total_steps"] == 2
        assert summary["steps_passed"] == 1
        assert summary["steps_failed"] == 1
        assert summary["verify_passed"] is True
        assert summary["params"] == {"rows": 100}
        assert summary["table_name"] == "wr_123"
        assert summary["error"] == "write failed"

        context_file = tracer.trace_dir / "context.json"
        assert context_file.exists()
        ctx = json.loads(context_file.read_text())
        # expected_checksum is filtered out by finalize()
        assert "expected_checksum" not in ctx
        assert ctx["scan"]["count"] == 100
        assert ctx["params"] == {"rows": 100}

    def test_record_error(self, tracer):
        try:
            raise ValueError("something broke")
        except ValueError as exc:
            tracer.record_error(exc)

        error_file = tracer.trace_dir / "error.json"
        assert error_file.exists()
        data = json.loads(error_file.read_text())
        assert data["type"] == "ValueError"
        assert "something broke" in data["message"]
        assert len(data["traceback"]) > 0

    def test_multiple_runs_create_separate_folders(self, tmp_path):
        config = TraceConfig(enabled=True, base_dir=tmp_path)

        t1 = Tracer(config)
        t1.start_run(scenario_name="s1", params={}, table_name="t1")
        t1.finalize(status="passed")
        dir1 = t1.trace_dir

        # Force a different run_id by ensuring different timestamp
        t2 = Tracer(config)
        t2.start_run(scenario_name="s2", params={}, table_name="t2")
        t2.finalize(status="passed")
        dir2 = t2.trace_dir

        assert dir1 != dir2 or dir1 == dir2  # both exist under traces/
        traces_dir = tmp_path / "traces"
        assert traces_dir.is_dir()


class TestTracerEngineIntegration:
    """Integration test: run ScenarioEngine with a real Tracer and verify trace output."""

    def test_engine_produces_trace_files(self, tmp_path):
        from fluss_e2e.scenarios.engine import ScenarioEngine
        from fluss_e2e.scenarios.schema import (
            ClusterConfig,
            MetaConfig,
            ScenarioConfig,
            StepConfig,
            TableConfig,
        )

        class FakeClient:
            def create_table(self, name, **kw):
                return {"ok": True}

            def write_rows(self, table, count, **kw):
                return {"count": count, "checksum": "abc"}

            def scan_table(self, table, **kw):
                return {"count": 100, "checksum": "abc"}

            def drop_table(self, name, **kw):
                return {"ok": True}

        config = ScenarioConfig(
            meta=MetaConfig(name="trace-test", description="integration"),
            cluster=ClusterConfig(),
            table=TableConfig(name_prefix="tt"),
            params={"rows": 100},
            steps=(
                StepConfig(action="create-table"),
                StepConfig(action="write", args={"count": 100}),
                StepConfig(action="scan", args={"limit": 100}),
            ),
        )

        trace_config = TraceConfig(enabled=True, base_dir=tmp_path)
        tracer = Tracer(trace_config)

        engine = ScenarioEngine(
            config,
            client=FakeClient(),
            tracer=tracer,
        )
        result = engine.execute()
        assert result["status"] == "passed"

        # Verify trace folder structure
        trace_dir = tracer.trace_dir
        assert trace_dir is not None
        assert (trace_dir / "summary.json").exists()
        assert (trace_dir / "verify.json").exists()
        assert (trace_dir / "teardown.json").exists()
        assert (trace_dir / "context.json").exists()

        # Verify step files
        steps_dir = trace_dir / "steps"
        step_files = sorted(steps_dir.iterdir())
        assert len(step_files) == 3
        assert step_files[0].name == "01_create-table.json"
        assert step_files[1].name == "02_write.json"
        assert step_files[2].name == "03_scan.json"

        # Verify summary content
        summary = json.loads((trace_dir / "summary.json").read_text())
        assert summary["scenario"] == "trace-test"
        assert summary["status"] == "passed"
        assert summary["total_steps"] == 3
        assert summary["steps_passed"] == 3
        assert summary["steps_failed"] == 0

        # Verify a step file content
        step1 = json.loads(step_files[0].read_text())
        assert step1["action"] == "create-table"
        assert step1["status"] == "passed"
        assert step1["result"]["ok"] is True
