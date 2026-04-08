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

"""Scenario registry backed by YAML definitions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..errors import ConfigError
from .base import ScenarioDefinition, ScenarioParameter, YamlScenario
from .loader import list_available_scenarios, load_scenario, scenarios_dir
from .schema import ScenarioConfig


SCENARIO_OPTION_FLAGS = {
    "rows": "--rows",
    "tables": "--tables",
}

_PARAM_DESCRIPTIONS: dict[str, str] = {
    "rows": "Number of rows to write/scan.",
    "tables": "Number of tables to create.",
    "timeout-override-s": "Override the default scenario timeout.",
    "timeout_override_s": "Override the default scenario timeout.",
}


def _config_to_definition(config: ScenarioConfig) -> ScenarioDefinition:
    """Convert a ScenarioConfig to a ScenarioDefinition."""
    parameters: list[ScenarioParameter] = []
    supported_options: set[str] = set()

    for param_name, default_value in config.params.items():
        attr_name = param_name.replace("-", "_")
        flag = f"--{param_name.replace('_', '-')}"

        if attr_name == "timeout_override_s":
            continue

        parameters.append(
            ScenarioParameter(
                name=attr_name,
                flag=flag,
                description=_PARAM_DESCRIPTIONS.get(param_name, _PARAM_DESCRIPTIONS.get(attr_name, f"Parameter: {param_name}")),
                default=default_value,
            )
        )
        supported_options.add(attr_name)

    timeout_override = config.params.get("timeout-override-s", config.params.get("timeout_override_s"))

    return ScenarioDefinition(
        name=config.meta.name,
        description=config.meta.description,
        implemented=True,
        handler_class=YamlScenario,
        parameters=tuple(parameters),
        supported_options=frozenset(supported_options),
        compose_profiles=config.cluster.compose_profiles,
        timeout_override_s=int(timeout_override) if timeout_override is not None else None,
    )


def _load_scenarios(search_dir: Path | None = None) -> dict[str, tuple[ScenarioDefinition, Path]]:
    """Load all YAML scenarios into a registry dict. Returns (definition, compose_path)."""
    result: dict[str, tuple[ScenarioDefinition, Path]] = {}
    for name in list_available_scenarios(search_dir=search_dir):
        config, compose_path = load_scenario(name, search_dir=search_dir)
        result[config.meta.name] = (_config_to_definition(config), compose_path)
    return result


def list_scenarios(*, search_dir: Path | None = None) -> list[dict[str, object]]:
    scenarios = _load_scenarios(search_dir=search_dir)
    return [defn.to_metadata() for defn, _ in scenarios.values()]


def validate_requested_options(definition: ScenarioDefinition, args) -> None:
    provided_options = getattr(args, "_provided_scenario_options", set())
    unsupported_options = sorted(provided_options - set(definition.supported_options))
    if unsupported_options:
        flags = []
        for name in unsupported_options:
            flag = SCENARIO_OPTION_FLAGS.get(name, f"--{name.replace('_', '-')}")
            flags.append(flag)
        option_flags = ", ".join(flags)
        raise ConfigError(
            f"Scenario `{definition.name}` does not support option(s): {option_flags}.",
            details={
                "scenario": definition.name,
                "unsupported_options": unsupported_options,
            },
        )


def resolve_scenario(name: str, *, search_dir: Path | None = None) -> tuple[ScenarioDefinition, Path]:
    """Resolve a scenario by name. Returns (definition, compose_path)."""
    try:
        config, compose_path = load_scenario(name, search_dir=search_dir)
        return _config_to_definition(config), compose_path
    except ConfigError as exc:
        if "available" not in exc.details:
            raise
        raise ConfigError(
            f"Unknown scenario `{name}`.",
            details={"available": exc.details["available"]},
        ) from exc
