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

"""Tests for perf report generation."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import json
import tempfile
import unittest

from fluss_e2e.perf.engine import PerfRunResult
from fluss_e2e.perf.loader import load_perf_scenario
from fluss_e2e.perf.report import evaluate_thresholds, generate_reports
from fluss_e2e.perf.schema import PerfThresholdsConfig, ThresholdBound


def _sample_metrics() -> dict[str, object]:
    mib = 1024.0 * 1024.0
    return {
        "write_tps": {
            "query": "sum(rate(fluss_server_log_append_total[1m]))",
            "series": [
                {
                    "metric": {},
                    "points": [
                        {"timestamp": 1711234567.0, "value": 1200.0},
                        {"timestamp": 1711234572.0, "value": 1300.0},
                    ],
                }
            ],
        },
        "jvm_heap_bytes": {
            "query": 'sum(jvm_memory_bytes_used{area="heap"})',
            "series": [
                {
                    "metric": {},
                    "points": [
                        {"timestamp": 1711234567.0, "value": 256.0 * mib},
                        {"timestamp": 1711234572.0, "value": 300.0 * mib},
                    ],
                }
            ],
        },
        "process_rss_bytes": {
            "query": "sum(process_resident_memory_bytes)",
            "series": [
                {
                    "metric": {},
                    "points": [
                        {"timestamp": 1711234567.0, "value": 900.0 * mib},
                        {"timestamp": 1711234572.0, "value": 1100.0 * mib},
                    ],
                }
            ],
        },
    }


class PerfReportTest(unittest.TestCase):
    def test_generate_reports_writes_timeseries_csv_and_metrics_json(self) -> None:
        scenario = load_perf_scenario("perf-kv-upsert")
        result = PerfRunResult(
            status="passed",
            scenario_name="perf-kv-upsert",
            elapsed_ms=1234,
            phases=[
                {
                    "phase": "write",
                    "opsPerSec": "1200.0",
                    "p99Ms": "10.0",
                }
            ],
            metrics=_sample_metrics(),
        )

        with tempfile.TemporaryDirectory() as tempdir:
            output_dir = Path(tempdir)
            generated = generate_reports(
                result,
                scenario,
                output_dir=output_dir,
                formats=("json", "csv"),
            )

            self.assertTrue(generated["csv"].endswith("timeseries.csv"))
            csv_text = (output_dir / "timeseries.csv").read_text(encoding="utf-8")
            summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))

        self.assertIn("metric,series,timestamp,iso_timestamp,value,labels", csv_text)
        self.assertIn("write_tps", csv_text)
        self.assertIn("metrics", summary)

    def test_evaluate_thresholds_uses_prometheus_memory_metrics(self) -> None:
        scenario = replace(
            load_perf_scenario("perf-kv-upsert"),
            thresholds=PerfThresholdsConfig(
                write_tps=ThresholdBound(min=1000.0),
                p99_ms=ThresholdBound(max=50.0),
                heap_mb=ThresholdBound(max=512.0),
                rss_peak_mb=ThresholdBound(max=1024.0),
            ),
        )
        result = PerfRunResult(
            status="passed",
            scenario_name="perf-kv-upsert",
            phases=[
                {
                    "phase": "write",
                    "opsPerSec": "1200.0",
                    "p99Ms": "10.0",
                }
            ],
            metrics=_sample_metrics(),
        )

        thresholds = evaluate_thresholds(result, scenario)

        self.assertEqual(thresholds["status"], "failed")
        metrics = {check["metric"]: check for check in thresholds["checks"]}
        self.assertEqual(metrics["heap_mb"]["status"], "passed")
        self.assertEqual(metrics["rss_peak_mb"]["status"], "failed")
        self.assertAlmostEqual(metrics["heap_mb"]["actual"], 300.0, places=1)
