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

"""Folder-based YAML scenario loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..errors import ConfigError
from .schema import (
    ClusterConfig,
    ColumnConfig,
    DataVerifyRule,
    HealthCheck,
    HealthVerifyRule,
    LogAssert,
    LogVerifyRule,
    MetaConfig,
    MetricsQuery,
    MetricsVerifyRule,
    ScenarioConfig,
    StepConfig,
    TableConfig,
    TeardownStep,
    ValidateCheck,
    VerifyRule,
)

_SCENARIOS_DIR = Path(__file__).resolve().parent.parent.parent / "scenarios"

_REQUIRED_SECTIONS = ("meta", "steps")


def scenarios_dir() -> Path:
    return _SCENARIOS_DIR


def _is_yaml_scenario_dir(path: Path) -> bool:
    return (
        path.is_dir()
        and not path.name.startswith("_")
        and (path / f"{path.name}.yaml").exists()
        and (path / "docker-compose.yml").exists()
    )


def load_scenario(
    name: str, *, search_dir: Path | None = None,
) -> tuple[ScenarioConfig, Path]:
    """Load a scenario from its folder.

    Returns (config, compose_file_path).
    """
    directory = search_dir or _SCENARIOS_DIR
    scenario_dir = directory / name

    if not scenario_dir.is_dir():
        available = list_available_scenarios(search_dir=directory)
        raise ConfigError(
            f"Scenario folder `{name}` not found in `{directory}`.",
            details={"name": name, "search_dir": str(directory), "available": available},
        )

    yaml_path = scenario_dir / f"{name}.yaml"
    if not yaml_path.exists():
        raise ConfigError(
            f"Scenario YAML `{name}.yaml` not found in `{scenario_dir}`.",
            details={"name": name, "scenario_dir": str(scenario_dir)},
        )

    compose_path = scenario_dir / "docker-compose.yml"
    if not compose_path.exists():
        raise ConfigError(
            f"docker-compose.yml not found in scenario folder `{scenario_dir}`.",
            details={"name": name, "scenario_dir": str(scenario_dir)},
        )

    raw = _read_yaml(yaml_path)
    config = _parse_config(raw, source_path=yaml_path)
    return config, compose_path


def list_available_scenarios(*, search_dir: Path | None = None) -> list[str]:
    directory = search_dir or _SCENARIOS_DIR
    if not directory.is_dir():
        return []
    return sorted(
        entry.name
        for entry in directory.iterdir()
        if _is_yaml_scenario_dir(entry)
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ConfigError(
            f"Failed to parse YAML scenario file `{path.name}`.",
            details={"path": str(path), "error": str(exc)},
        ) from exc
    if not isinstance(data, dict):
        raise ConfigError(
            f"Scenario YAML `{path.name}` must be a mapping at the top level.",
            details={"path": str(path), "type": type(data).__name__},
        )
    return data


def _parse_config(raw: dict[str, Any], *, source_path: Path) -> ScenarioConfig:
    for section in _REQUIRED_SECTIONS:
        if section not in raw:
            raise ConfigError(
                f"Scenario YAML `{source_path.name}` is missing required section `{section}`.",
                details={"path": str(source_path), "missing": section},
            )

    meta = _parse_meta(raw["meta"])
    cluster = _parse_cluster(raw.get("cluster", {}))
    table = _parse_table(raw.get("table", {}))
    params = dict(raw.get("params", {}))
    steps = tuple(_parse_step(step) for step in raw["steps"])
    teardown = tuple(_parse_teardown(step) for step in raw.get("teardown", []))
    verify = tuple(_parse_verify_rules(raw.get("verify", [])))

    return ScenarioConfig(
        meta=meta,
        cluster=cluster,
        table=table,
        params=params,
        steps=steps,
        teardown=teardown,
        verify=verify,
    )


def _parse_meta(raw: dict[str, Any]) -> MetaConfig:
    if "name" not in raw:
        raise ConfigError("Scenario meta section requires a `name` field.")
    return MetaConfig(
        name=raw["name"],
        description=raw.get("description", ""),
        tags=tuple(raw.get("tags", ())),
    )


def _parse_cluster(raw: dict[str, Any]) -> ClusterConfig:
    return ClusterConfig(
        compose_profiles=tuple(raw.get("compose-profiles", ())),
        bootstrap_servers=raw.get("bootstrap-servers", "localhost:9123"),
    )


def _parse_table(raw: dict[str, Any]) -> TableConfig:
    if not raw:
        return TableConfig(name_prefix="e2e_table")
    columns = tuple(
        ColumnConfig(name=col["name"], type=col["type"])
        for col in raw.get("columns", ())
    )
    return TableConfig(
        name_prefix=raw.get("name-prefix", "e2e_table"),
        database=raw.get("database", "e2e"),
        columns=columns,
        primary_key=tuple(raw.get("primary-key", ())),
        buckets=raw.get("buckets", 4),
        properties=dict(raw.get("properties", {})),
    )


def _parse_step(raw: dict[str, Any]) -> StepConfig:
    if "action" not in raw:
        raise ConfigError(
            "Each step must have an `action` field.",
            details={"step": raw},
        )
    checks = tuple(
        ValidateCheck(name=check["name"], assert_expr=check["assert"])
        for check in raw.get("checks", ())
    )
    return StepConfig(
        action=raw["action"],
        id=raw.get("id"),
        save_as=raw.get("save_as", raw.get("save-as")),
        args=dict(raw.get("args", {})),
        checks=checks,
        on_error=raw.get("on-error", "fail"),
        command=raw.get("command"),
        method=raw.get("method"),
        url=raw.get("url"),
        headers=dict(raw.get("headers", {})),
        body=raw.get("body"),
    )


def _parse_teardown(raw: dict[str, Any]) -> TeardownStep:
    if "action" not in raw:
        raise ConfigError(
            "Each teardown step must have an `action` field.",
            details={"step": raw},
        )
    return TeardownStep(
        action=raw["action"],
        args=dict(raw.get("args", {})),
        on_error=raw.get("on-error", "ignore"),
    )


def _parse_verify_rules(raw_list: list[dict[str, Any]]) -> list[VerifyRule]:
    rules: list[VerifyRule] = []
    for raw in raw_list:
        rule_type = raw.get("type", "")
        if rule_type == "data":
            rules.append(DataVerifyRule(
                assert_exprs=tuple(raw.get("assert", ())),
            ))
        elif rule_type == "metrics":
            queries = tuple(
                MetricsQuery(
                    metric=q["metric"],
                    labels=dict(q.get("labels", {})),
                    assert_expr=q["assert"],
                )
                for q in raw.get("queries", ())
            )
            rules.append(MetricsVerifyRule(
                source=raw.get("source", "http://localhost:9090"),
                queries=queries,
            ))
        elif rule_type == "logs":
            asserts = tuple(
                LogAssert(
                    pattern=a["pattern"],
                    exists=a.get("exists", True),
                )
                for a in raw.get("assert", ())
            )
            rules.append(LogVerifyRule(
                container=raw["container"],
                asserts=asserts,
            ))
        elif rule_type == "health":
            checks = tuple(
                HealthCheck(
                    url=c["url"],
                    status=c.get("status", 200),
                )
                for c in raw.get("checks", ())
            )
            rules.append(HealthVerifyRule(checks=checks))
        else:
            raise ConfigError(
                f"Unknown verify type `{rule_type}`.",
                details={"type": rule_type},
            )
    return rules
