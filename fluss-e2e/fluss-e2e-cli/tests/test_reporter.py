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

"""Tests for report helpers."""

from __future__ import annotations

from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import tempfile
import unittest

from fluss_e2e.reporter import finalize_report, new_report, write_report


class ReporterTest(unittest.TestCase):
    def test_report_contains_expected_top_level_fields(self) -> None:
        report = new_report("run", scenario_name="write-read")
        finalize_report(report, status="passed", exit_code=0, duration_ms=42)
        self.assertEqual(report["command"], "run")
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["scenario_name"], "write-read")

    def test_write_report_persists_json(self) -> None:
        report = new_report("build")
        finalize_report(report, status="passed", exit_code=0, duration_ms=1)
        with tempfile.TemporaryDirectory() as tempdir:
            output = Path(tempdir) / "report.json"
            buffer = StringIO()
            with redirect_stdout(buffer):
                write_report(report, str(output))
            self.assertTrue(output.exists())
            self.assertIn('"command": "build"', output.read_text(encoding="utf-8"))
            self.assertEqual(buffer.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
