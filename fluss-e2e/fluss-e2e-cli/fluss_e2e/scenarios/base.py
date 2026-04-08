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

"""Scenario interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ScenarioParameter:
    name: str
    flag: str
    description: str
    default: Any | None = None
    required: bool = False


@dataclass(frozen=True)
class ScenarioDefinition:
    name: str
    description: str
    implemented: bool
    handler_class: type["Scenario"] | None
    parameters: tuple[ScenarioParameter, ...] = ()
    supported_options: frozenset[str] = frozenset()
    compose_profiles: tuple[str, ...] = ()
    timeout_override_s: int | None = None

    def to_metadata(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "implemented": self.implemented,
            "supported_options": sorted(self.supported_options),
            "parameters": [
                {
                    "name": parameter.name,
                    "flag": parameter.flag,
                    "description": parameter.description,
                    "default": parameter.default,
                    "required": parameter.required,
                }
                for parameter in self.parameters
            ],
            "compose_profiles": list(self.compose_profiles),
            "timeout_override_s": self.timeout_override_s,
        }


class Scenario(ABC):
    name: str
    description: str

    def validate_args(self, args) -> None:
        return None

    @abstractmethod
    def run(self, *, client, args, cluster_manager=None, tracer=None) -> dict[str, Any]:
        raise NotImplementedError


class YamlScenario(Scenario):
    """Scenario backed by a YAML config file, executed via the ScenarioEngine."""

    def __init__(self, scenario_config, *, compose_path: Path | None = None) -> None:
        from .schema import ScenarioConfig

        self._scenario_config: ScenarioConfig = scenario_config
        self.name = scenario_config.meta.name
        self.description = scenario_config.meta.description
        self.compose_path = compose_path

    def validate_args(self, args) -> None:
        from ..errors import ConfigError

        for param_name, default_value in self._scenario_config.params.items():
            attr_name = param_name.replace("-", "_")
            value = getattr(args, attr_name, default_value)
            if isinstance(default_value, int) and isinstance(value, int):
                if param_name in ("rows", "tables") and value <= 0:
                    flag = f"--{param_name}"
                    raise ConfigError(
                        f"The `{self.name}` scenario requires `{flag}` to be greater than 0.",
                        details={param_name: value},
                    )

    def run(self, *, client, args, cluster_manager=None, tracer=None) -> dict[str, Any]:
        from .engine import ScenarioEngine

        cli_params: dict[str, Any] = {}
        for param_name in self._scenario_config.params:
            attr_name = param_name.replace("-", "_")
            value = getattr(args, attr_name, None)
            if value is not None:
                cli_params[param_name] = value
                cli_params[attr_name] = value

        engine = ScenarioEngine(
            self._scenario_config,
            client=client,
            cluster_manager=cluster_manager,
            cli_params=cli_params,
            tracer=tracer,
        )
        return engine.execute()
