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

"""Tests for the metrics collector."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fluss_e2e.collector.metrics import MetricsCollector


class MetricsCollectorTest(unittest.TestCase):
    def test_is_available_returns_false_when_prometheus_unreachable(self) -> None:
        collector = MetricsCollector("http://localhost:19999")

        self.assertFalse(collector.is_available())

    @patch("fluss_e2e.collector.metrics.urllib.request.urlopen")
    def test_is_available_returns_true_when_ready(self, urlopen_mock) -> None:
        response_mock = unittest.mock.MagicMock()
        response_mock.status = 200
        response_mock.__enter__ = lambda s: s
        response_mock.__exit__ = unittest.mock.MagicMock(return_value=False)
        urlopen_mock.return_value = response_mock

        collector = MetricsCollector("http://localhost:9090")

        self.assertTrue(collector.is_available())

    @patch("fluss_e2e.collector.metrics.urllib.request.urlopen")
    def test_scrape_parses_prometheus_response(self, urlopen_mock) -> None:
        prometheus_response = json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {"__name__": "fluss_write_ops_total"},
                            "value": [1711234567, "42"],
                        },
                        {
                            "metric": {"__name__": "fluss_read_ops_total"},
                            "value": [1711234567, "17"],
                        },
                    ],
                },
            }
        ).encode("utf-8")

        response_mock = unittest.mock.MagicMock()
        response_mock.read.return_value = prometheus_response
        response_mock.__enter__ = lambda s: s
        response_mock.__exit__ = unittest.mock.MagicMock(return_value=False)
        urlopen_mock.return_value = response_mock

        collector = MetricsCollector("http://localhost:9090")
        snapshot = collector.scrape()

        self.assertEqual(snapshot["status"], "success")
        self.assertEqual(len(snapshot["data"]["result"]), 2)

    @patch("fluss_e2e.collector.metrics.urllib.request.urlopen")
    def test_query_range_parses_prometheus_matrix_response(self, urlopen_mock) -> None:
        prometheus_response = json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {"__name__": "fluss_write_ops_total"},
                            "values": [
                                [1711234567, "42"],
                                [1711234572, "84"],
                            ],
                        },
                    ],
                },
            }
        ).encode("utf-8")

        response_mock = unittest.mock.MagicMock()
        response_mock.read.return_value = prometheus_response
        response_mock.__enter__ = lambda s: s
        response_mock.__exit__ = unittest.mock.MagicMock(return_value=False)
        urlopen_mock.return_value = response_mock

        collector = MetricsCollector("http://localhost:9090")
        payload = collector.query_range(
            "sum(rate(fluss_write_ops_total[1m]))",
            start_s=1711234560,
            end_s=1711234620,
            step="5s",
        )

        self.assertEqual(payload["status"], "success")
        self.assertEqual(len(payload["data"]["result"]), 1)
        self.assertEqual(len(payload["data"]["result"][0]["values"]), 2)

    def test_scrape_returns_error_dict_when_unreachable(self) -> None:
        collector = MetricsCollector("http://localhost:19999")
        snapshot = collector.scrape()

        self.assertEqual(snapshot["status"], "error")
        self.assertIn("error", snapshot)

    @patch("fluss_e2e.collector.metrics.urllib.request.urlopen")
    def test_export_summary_writes_snapshots_to_file(self, urlopen_mock) -> None:
        prometheus_response = json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {"__name__": "fluss_write_ops_total"},
                            "value": [1711234567, "42"],
                        },
                    ],
                },
            }
        ).encode("utf-8")

        response_mock = unittest.mock.MagicMock()
        response_mock.read.return_value = prometheus_response
        response_mock.__enter__ = lambda s: s
        response_mock.__exit__ = unittest.mock.MagicMock(return_value=False)
        urlopen_mock.return_value = response_mock

        collector = MetricsCollector("http://localhost:9090")
        collector.scrape()

        with tempfile.TemporaryDirectory() as tempdir:
            output_path = Path(tempdir) / "snapshots.json"
            summary = collector.export_summary(output_path)

            self.assertTrue(output_path.exists())
            self.assertEqual(summary["prometheus_url"], "http://localhost:9090")
            self.assertEqual(summary["snapshot_count"], 1)
            self.assertIn("fluss_write_ops_total", summary["summary"]["metric_names"])
            self.assertEqual(summary["summary"]["metric_count"], 1)
            self.assertEqual(summary["summary"]["total_series"], 1)

            raw = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(len(raw), 1)

    def test_export_summary_handles_empty_snapshots(self) -> None:
        collector = MetricsCollector("http://localhost:9090")

        with tempfile.TemporaryDirectory() as tempdir:
            output_path = Path(tempdir) / "snapshots.json"
            summary = collector.export_summary(output_path)

            self.assertEqual(summary["snapshot_count"], 0)
            self.assertEqual(summary["summary"]["metric_count"], 0)
            self.assertEqual(summary["summary"]["total_series"], 0)


if __name__ == "__main__":
    unittest.main()
