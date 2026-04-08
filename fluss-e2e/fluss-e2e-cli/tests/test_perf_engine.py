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

"""Tests for perf engine compose generation."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

import yaml

from fluss_e2e.config import RuntimeConfig
from fluss_e2e.perf.engine import PerfEngine
from fluss_e2e.perf.loader import load_perf_scenario, perf_scenarios_dir
from fluss_e2e.perf.schema import PerfClusterConfig


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


class PerfEngineRuntimeStackTest(unittest.TestCase):
    def test_prepare_runtime_stack_profiles_perf_client(self) -> None:
        repo_root = _repo_root()
        config = RuntimeConfig.discover(repo_root)
        engine = PerfEngine(config)
        scenario = load_perf_scenario("perf-kv-upsert")
        scenario_dir = perf_scenarios_dir() / "perf-kv-upsert"

        with tempfile.TemporaryDirectory() as tempdir:
            output_dir = Path(tempdir)
            stack = engine._prepare_runtime_stack(
                scenario,
                scenario_dir=scenario_dir,
                output_dir=output_dir,
            )
            compose = yaml.safe_load(stack.compose_file.read_text(encoding="utf-8"))

        services = compose["services"]
        self.assertEqual(services["coordinator-server"]["command"], "coordinatorServer")
        self.assertEqual(services["tablet-server-0"]["command"], "tabletServer")
        self.assertEqual(services["perf-client"]["profiles"], ["perf-client"])
        self.assertEqual(
            [port.name for port in stack.service_ports],
            [
                "zookeeper",
                "coordinator-server",
                "tablet-server-0",
                "tablet-server-1",
                "tablet-server-2",
            ],
        )

    def test_prepare_runtime_stack_honors_cluster_overrides(self) -> None:
        repo_root = _repo_root()
        config = RuntimeConfig.discover(repo_root)
        engine = PerfEngine(config)
        scenario = replace(
            load_perf_scenario("perf-kv-upsert"),
            cluster=PerfClusterConfig(
                tablet_servers=5,
                jvm_args=("-Xmx2g",),
                config_overrides={"table.exec.test": "1"},
            ),
        )
        scenario_dir = perf_scenarios_dir() / "perf-kv-upsert"

        with tempfile.TemporaryDirectory() as tempdir:
            output_dir = Path(tempdir)
            stack = engine._prepare_runtime_stack(
                scenario,
                scenario_dir=scenario_dir,
                output_dir=output_dir,
            )
            compose = yaml.safe_load(stack.compose_file.read_text(encoding="utf-8"))
            prometheus = (output_dir / "prometheus-perf.generated.yml").read_text(
                encoding="utf-8"
            )

        services = compose["services"]
        tablet_names = sorted(
            name for name in services if name.startswith("tablet-server-")
        )
        self.assertEqual(len(tablet_names), 5)
        self.assertEqual(services["tablet-server-4"]["ports"], ["9128:9123", "9254:9249"])
        self.assertEqual(
            services["tablet-server-4"]["environment"]["FLUSS_ENV_JAVA_OPTS_TS"],
            "-Xmx2g",
        )
        self.assertIn(
            "table.exec.test: 1",
            services["tablet-server-4"]["environment"]["FLUSS_PROPERTIES"],
        )
        self.assertEqual(
            services["perf-client"]["depends_on"][-1],
            "tablet-server-4",
        )
        self.assertIn("tablet-server-4:9249", prometheus)
        self.assertEqual(stack.service_ports[-1].port, 9128)
