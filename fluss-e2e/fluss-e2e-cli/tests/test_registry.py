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

"""Tests for the scenario registry."""

from __future__ import annotations

from types import SimpleNamespace
import unittest

from fluss_e2e.errors import ConfigError
from fluss_e2e.scenarios.registry import (
    list_scenarios,
    resolve_scenario,
    validate_requested_options,
)


class RegistryTest(unittest.TestCase):
    def test_list_scenarios_contains_write_read(self) -> None:
        scenarios = {item["name"]: item for item in list_scenarios()}

        self.assertIn("write-read", scenarios)
        self.assertIn("table-ops", scenarios)
        self.assertEqual(scenarios["write-read"]["supported_options"], ["rows"])
        self.assertEqual(scenarios["table-ops"]["parameters"][0]["flag"], "--tables")

    def test_validate_requested_options_rejects_incompatible_flags(self) -> None:
        definition, _compose_path = resolve_scenario("write-read")

        with self.assertRaises(ConfigError) as caught:
            validate_requested_options(
                definition,
                SimpleNamespace(_provided_scenario_options={"rows", "tables"}),
            )

        self.assertIn("--tables", str(caught.exception))

    def test_unknown_scenario_raises_config_error(self) -> None:
        with self.assertRaises(ConfigError):
            resolve_scenario("missing")


if __name__ == "__main__":
    unittest.main()
