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

"""Tests for CLI behavior."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import unittest
from unittest.mock import patch

from fluss_e2e.cli import main
from fluss_e2e.errors import ScenarioFailure
from fluss_e2e.perf.engine import PerfRunResult
from fluss_e2e.perf.loader import load_perf_scenario
from fluss_e2e.perf.schema import PerfReportConfig


class CliTest(unittest.TestCase):
    @patch("fluss_e2e.cli.write_report")
    def test_functional_list_writes_registry_metadata(self, write_report_mock) -> None:
        exit_code = main(["functional", "list"])

        self.assertEqual(exit_code, 0)
        report = write_report_mock.call_args.args[0]
        scenarios = {item["name"]: item for item in report["available_scenarios"]}
        self.assertEqual(report["command"], "functional list")
        self.assertIn("table-ops", scenarios)
        self.assertEqual(
            scenarios["table-ops"]["parameters"][0]["flag"],
            "--tables",
        )

    @patch("fluss_e2e.cli.ComposeClusterManager.up")
    @patch("fluss_e2e.cli.UnifiedBuilder.build")
    @patch("fluss_e2e.cli.write_report")
    def test_functional_test_rejects_invalid_write_read_before_build(
        self,
        write_report_mock,
        build_mock,
        cluster_up_mock,
    ) -> None:
        exit_code = main(["functional", "test", "--scenario", "write-read", "--rows", "0"])

        self.assertEqual(exit_code, 4)
        build_mock.assert_not_called()
        cluster_up_mock.assert_not_called()
        write_report_mock.assert_called_once()

    @patch("fluss_e2e.cli.ComposeClusterManager.up")
    @patch("fluss_e2e.cli.UnifiedBuilder.build")
    @patch("fluss_e2e.cli.write_report")
    def test_functional_test_rejects_incompatible_scenario_option_before_build(
        self,
        write_report_mock,
        build_mock,
        cluster_up_mock,
    ) -> None:
        exit_code = main(["functional", "test", "--scenario", "write-read", "--tables", "2"])

        self.assertEqual(exit_code, 4)
        build_mock.assert_not_called()
        cluster_up_mock.assert_not_called()
        write_report_mock.assert_called_once()

    @patch("fluss_e2e.cli.write_report")
    @patch(
        "fluss_e2e.cli._best_effort_collect_logs",
        return_value={"status": "captured", "services": {}},
    )
    @patch("fluss_e2e.cli.ComposeClusterManager.down")
    @patch("fluss_e2e.cli.ComposeClusterManager.up")
    @patch("fluss_e2e.cli._run_scenario")
    def test_functional_test_captures_logs_on_success(
        self,
        run_scenario_mock,
        cluster_up_mock,
        cluster_down_mock,
        collect_logs_mock,
        write_report_mock,
    ) -> None:
        def populate_report(args, config, report, **kwargs) -> None:
            report["scenario"] = {"name": "write-read", "status": "passed"}

        run_scenario_mock.side_effect = populate_report
        cluster_up_mock.return_value = {"status": "running"}

        exit_code = main(["functional", "test", "--no-build", "--scenario", "write-read", "--rows", "1"])

        self.assertEqual(exit_code, 0)
        cluster_up_mock.assert_called_once()
        cluster_down_mock.assert_called_once()
        run_scenario_mock.assert_called_once()
        collect_logs_mock.assert_called_once()
        report = write_report_mock.call_args.args[0]
        self.assertEqual(report["logs"]["status"], "captured")
        self.assertEqual(report["scenario"]["status"], "passed")

    @patch("fluss_e2e.cli.write_report")
    @patch(
        "fluss_e2e.cli._best_effort_collect_logs",
        return_value={"status": "captured", "services": {}},
    )
    @patch("fluss_e2e.cli.ComposeClusterManager.down")
    @patch("fluss_e2e.cli.ComposeClusterManager.up")
    @patch(
        "fluss_e2e.cli._run_scenario",
        side_effect=ScenarioFailure(
            "boom",
            section={"name": "write-read", "status": "failed"},
        ),
    )
    def test_functional_test_captures_logs_on_failure(
        self,
        run_scenario_mock,
        cluster_up_mock,
        cluster_down_mock,
        collect_logs_mock,
        write_report_mock,
    ) -> None:
        cluster_up_mock.return_value = {"status": "running"}

        exit_code = main(["functional", "test", "--no-build", "--scenario", "write-read", "--rows", "1"])

        self.assertEqual(exit_code, 1)
        cluster_up_mock.assert_called_once()
        cluster_down_mock.assert_called_once()
        run_scenario_mock.assert_called_once()
        collect_logs_mock.assert_called_once()
        report = write_report_mock.call_args.args[0]
        self.assertEqual(report["logs"]["status"], "captured")
        self.assertEqual(report["scenario"]["status"], "failed")

    @patch("fluss_e2e.cli.ComposeClusterManager.restart_service")
    @patch("fluss_e2e.cli.write_report")
    def test_functional_cluster_restart_reports_service_action(
        self,
        write_report_mock,
        restart_service_mock,
    ) -> None:
        restart_service_mock.return_value = {
            "status": "running",
            "action": "restart",
            "service": "tablet-server-0",
        }

        exit_code = main(
            [
                "functional",
                "cluster",
                "restart",
                "--service",
                "tablet-server-0",
            ]
        )

        self.assertEqual(exit_code, 0)
        restart_service_mock.assert_called_once()
        report = write_report_mock.call_args.args[0]
        self.assertEqual(report["cluster"]["action"], "restart")
        self.assertEqual(report["cluster"]["service"], "tablet-server-0")

    @patch("fluss_e2e.cli.write_report")
    @patch(
        "fluss_e2e.perf.report.evaluate_thresholds",
        return_value={"status": "failed", "checks": []},
    )
    @patch(
        "fluss_e2e.perf.report.generate_reports",
        return_value={"json": "/tmp/summary.json"},
    )
    @patch("fluss_e2e.perf.loader.load_perf_scenario")
    @patch("fluss_e2e.perf.engine.PerfEngine.run_scenario")
    def test_perf_test_fails_when_thresholds_fail(
        self,
        run_scenario_mock,
        load_perf_scenario_mock,
        generate_reports_mock,
        evaluate_thresholds_mock,
        write_report_mock,
    ) -> None:
        run_scenario_mock.return_value = PerfRunResult(
            status="passed",
            scenario_name="perf-kv-upsert",
        )
        load_perf_scenario_mock.return_value = load_perf_scenario("perf-kv-upsert")

        exit_code = main(["perf", "test", "--scenario", "perf-kv-upsert"])

        self.assertEqual(exit_code, 1)
        run_scenario_mock.assert_called_once()
        generate_reports_mock.assert_called_once()
        evaluate_thresholds_mock.assert_called_once()
        report = write_report_mock.call_args.args[0]
        self.assertEqual(report["perf_thresholds"]["status"], "failed")

    @patch("fluss_e2e.cli.write_report")
    @patch("fluss_e2e.perf.report.evaluate_thresholds", return_value=None)
    @patch(
        "fluss_e2e.perf.report.generate_reports",
        return_value={"json": "/tmp/summary.json"},
    )
    @patch("fluss_e2e.perf.loader.load_perf_scenario")
    @patch("fluss_e2e.perf.engine.PerfEngine.run_scenario")
    def test_perf_test_uses_scenario_report_defaults(
        self,
        run_scenario_mock,
        load_perf_scenario_mock,
        generate_reports_mock,
        evaluate_thresholds_mock,
        write_report_mock,
    ) -> None:
        scenario = replace(
            load_perf_scenario("perf-kv-upsert"),
            report=PerfReportConfig(
                formats=("json", "csv"),
                output_dir="scenario-output",
            ),
        )
        run_scenario_mock.return_value = PerfRunResult(
            status="passed",
            scenario_name="perf-kv-upsert",
        )
        load_perf_scenario_mock.return_value = scenario

        exit_code = main(["perf", "test", "--scenario", "perf-kv-upsert"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            run_scenario_mock.call_args.kwargs["output_dir"],
            Path("scenario-output"),
        )
        self.assertIs(run_scenario_mock.call_args.kwargs["scenario"], scenario)
        self.assertEqual(
            generate_reports_mock.call_args.kwargs["output_dir"],
            Path("scenario-output"),
        )
        self.assertEqual(
            generate_reports_mock.call_args.kwargs["formats"],
            ("json", "csv"),
        )
        evaluate_thresholds_mock.assert_called_once()
        write_report_mock.assert_called_once()
        self.assertIsNone(write_report_mock.call_args.args[1])

    @patch("fluss_e2e.cli.write_report")
    @patch("fluss_e2e.perf.report.evaluate_thresholds", return_value=None)
    @patch(
        "fluss_e2e.perf.report.generate_reports",
        return_value={"json": "/tmp/summary.json"},
    )
    @patch("fluss_e2e.perf.loader.load_perf_scenario")
    @patch("fluss_e2e.perf.engine.PerfEngine.run_scenario")
    def test_perf_test_cli_overrides_scenario_report_defaults(
        self,
        run_scenario_mock,
        load_perf_scenario_mock,
        generate_reports_mock,
        evaluate_thresholds_mock,
        write_report_mock,
    ) -> None:
        scenario = replace(
            load_perf_scenario("perf-kv-upsert"),
            report=PerfReportConfig(
                formats=("json", "csv"),
                output_dir="scenario-output",
            ),
        )
        run_scenario_mock.return_value = PerfRunResult(
            status="passed",
            scenario_name="perf-kv-upsert",
        )
        load_perf_scenario_mock.return_value = scenario

        exit_code = main(
            [
                "perf",
                "test",
                "--scenario",
                "perf-kv-upsert",
                "--output",
                "cli-output",
                "--formats",
                "html",
            ]
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            run_scenario_mock.call_args.kwargs["output_dir"],
            Path("cli-output"),
        )
        self.assertEqual(
            generate_reports_mock.call_args.kwargs["output_dir"],
            Path("cli-output"),
        )
        self.assertEqual(
            generate_reports_mock.call_args.kwargs["formats"],
            ("html",),
        )
        evaluate_thresholds_mock.assert_called_once()
        write_report_mock.assert_called_once()
        self.assertEqual(
            write_report_mock.call_args.args[1],
            str(Path("cli-output") / "cli-report.json"),
        )

    @patch("fluss_e2e.cli.write_report")
    @patch("fluss_e2e.perf.report.evaluate_thresholds", return_value=None)
    @patch(
        "fluss_e2e.perf.report.generate_reports",
        return_value={"json": "/tmp/summary.json"},
    )
    @patch("fluss_e2e.perf.engine.PerfEngine.run_scenario")
    def test_perf_test_applies_param_overrides_before_execution(
        self,
        run_scenario_mock,
        generate_reports_mock,
        evaluate_thresholds_mock,
        write_report_mock,
    ) -> None:
        run_scenario_mock.return_value = PerfRunResult(
            status="passed",
            scenario_name="perf-kv-upsert",
        )

        exit_code = main(
            [
                "perf",
                "test",
                "--scenario",
                "perf-kv-upsert",
                "--param",
                "report.output-dir=custom-output",
                "--param",
                "cluster.tablet-servers=5",
                "--param",
                "client.config.request.timeout.ms=1000",
                "--param",
                "workload[0].threads=8",
            ]
        )

        scenario = run_scenario_mock.call_args.kwargs["scenario"]

        self.assertEqual(exit_code, 0)
        self.assertEqual(scenario.report.output_dir, "custom-output")
        self.assertEqual(scenario.cluster.tablet_servers, 5)
        self.assertEqual(scenario.client.properties["request.timeout.ms"], "1000")
        self.assertEqual(scenario.workload[0].threads, 8)
        self.assertEqual(
            run_scenario_mock.call_args.kwargs["output_dir"],
            Path("custom-output"),
        )
        self.assertEqual(
            generate_reports_mock.call_args.kwargs["output_dir"],
            Path("custom-output"),
        )
        evaluate_thresholds_mock.assert_called_once()
        write_report_mock.assert_called_once()

    @patch("fluss_e2e.cli.write_report")
    def test_perf_test_rejects_invalid_param_override(self, write_report_mock) -> None:
        exit_code = main(
            [
                "perf",
                "test",
                "--scenario",
                "perf-kv-upsert",
                "--param",
                "invalid-override",
            ]
        )

        self.assertEqual(exit_code, 4)
        report = write_report_mock.call_args.args[0]
        self.assertEqual(report["error"]["type"], "ConfigError")
