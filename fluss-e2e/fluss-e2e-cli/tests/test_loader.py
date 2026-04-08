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

"""Tests for the folder-based YAML scenario loader."""

from __future__ import annotations

import tempfile
from pathlib import Path
import unittest

from fluss_e2e.errors import ConfigError
from fluss_e2e.scenarios.loader import (
    list_available_scenarios,
    load_scenario,
    scenarios_dir,
)


def _create_scenario_folder(
    parent: Path,
    name: str,
    *,
    yaml_content: str | None = None,
    create_compose: bool = True,
) -> Path:
    """Helper to create a scenario folder with yaml and compose file."""
    folder = parent / name
    folder.mkdir(parents=True, exist_ok=True)
    if yaml_content is not None:
        (folder / f"{name}.yaml").write_text(yaml_content, encoding="utf-8")
    if create_compose:
        (folder / "docker-compose.yml").write_text(
            "services:\n  zookeeper:\n    image: zookeeper:3.9.2\n",
            encoding="utf-8",
        )
    return folder


_MINIMAL_YAML = """\
meta:
  name: test-scenario
  description: "A test scenario"

params:
  rows: 10

steps:
  - action: ping
"""


class LoaderTest(unittest.TestCase):
    def test_scenarios_dir_points_to_scenarios_directory(self) -> None:
        directory = scenarios_dir()
        self.assertTrue(directory.is_dir(), f"{directory} is not a directory")

    def test_list_available_scenarios_discovers_folders(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            _create_scenario_folder(base, "alpha", yaml_content=_MINIMAL_YAML)
            _create_scenario_folder(base, "beta", yaml_content=_MINIMAL_YAML)
            names = list_available_scenarios(search_dir=base)
            self.assertEqual(sorted(names), ["alpha", "beta"])

    def test_list_ignores_non_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            _create_scenario_folder(base, "real", yaml_content=_MINIMAL_YAML)
            (base / "stray.yaml").write_text("meta:\n  name: stray\n")
            names = list_available_scenarios(search_dir=base)
            self.assertEqual(names, ["real"])

    def test_list_ignores_folders_missing_compose(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            _create_scenario_folder(base, "real", yaml_content=_MINIMAL_YAML)
            _create_scenario_folder(
                base,
                "perf-only",
                yaml_content=_MINIMAL_YAML,
                create_compose=False,
            )
            names = list_available_scenarios(search_dir=base)
            self.assertEqual(names, ["real"])

    def test_load_scenario_from_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            _create_scenario_folder(base, "my-test", yaml_content=_MINIMAL_YAML)
            config, compose_path = load_scenario("my-test", search_dir=base)
            self.assertEqual(config.meta.name, "test-scenario")
            self.assertTrue(compose_path.exists())
            self.assertEqual(compose_path.name, "docker-compose.yml")

    def test_load_scenario_nonexistent_raises_config_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ConfigError) as caught:
                load_scenario("nonexistent", search_dir=Path(tmpdir))
            self.assertIn("nonexistent", str(caught.exception))

    def test_load_scenario_missing_yaml_raises_config_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            folder = base / "broken"
            folder.mkdir()
            (folder / "docker-compose.yml").write_text("services: {}\n")
            with self.assertRaises(ConfigError) as caught:
                load_scenario("broken", search_dir=base)
            self.assertIn("broken.yaml", str(caught.exception))

    def test_load_scenario_missing_compose_raises_config_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            _create_scenario_folder(
                base, "no-compose", yaml_content=_MINIMAL_YAML, create_compose=False,
            )
            with self.assertRaises(ConfigError) as caught:
                load_scenario("no-compose", search_dir=base)
            self.assertIn("docker-compose.yml", str(caught.exception))

    def test_load_scenario_parses_verify_section(self) -> None:
        yaml_with_verify = """\
meta:
  name: verified
  description: "test"

steps:
  - action: ping

verify:
  - type: data
    assert:
      - "${ping.ok} == True"
  - type: logs
    container: coordinator-server
    assert:
      - pattern: "ready"
        exists: true
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            _create_scenario_folder(base, "verified", yaml_content=yaml_with_verify)
            config, _ = load_scenario("verified", search_dir=base)
            self.assertEqual(len(config.verify), 2)
            self.assertEqual(config.verify[0].type, "data")
            self.assertEqual(config.verify[1].type, "logs")

    def test_list_available_with_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            names = list_available_scenarios(search_dir=Path(tmpdir))
            self.assertEqual(names, [])


if __name__ == "__main__":
    unittest.main()
