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

"""YAML loader for perf scenario configurations."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from ..errors import ConfigError
from .schema import (
    PerfAggConfig,
    PerfClientConfig,
    PerfClusterConfig,
    PerfColumnConfig,
    PerfDataConfig,
    PerfGeneratorConfig,
    PerfMetaConfig,
    PerfReportConfig,
    PerfSamplingConfig,
    PerfScenarioConfig,
    PerfTableConfig,
    PerfThresholdsConfig,
    ThresholdBound,
    WorkloadPhaseConfig,
)

_SCENARIOS_DIR = Path(__file__).resolve().parent.parent.parent / "scenarios"

_REQUIRED_SECTIONS = ("meta", "table", "workload")


def perf_scenarios_dir() -> Path:
    """Return the base scenarios directory."""
    return _SCENARIOS_DIR


def load_perf_scenario(
    scenario_name: str,
    *,
    search_dir: Path | None = None,
    overrides: dict[str, str] | None = None,
) -> PerfScenarioConfig:
    """Load a perf scenario YAML file by name from scenarios/ directory."""
    directory = search_dir or _SCENARIOS_DIR
    scenario_dir = directory / scenario_name

    if not scenario_dir.is_dir():
        available = _list_available(directory)
        raise ConfigError(
            f"Perf scenario folder `{scenario_name}` not found in `{directory}`.",
            details={
                "name": scenario_name,
                "search_dir": str(directory),
                "available": available,
            },
        )

    yaml_path = scenario_dir / f"{scenario_name}.yaml"
    if not yaml_path.exists():
        raise ConfigError(
            f"Perf scenario YAML `{scenario_name}.yaml` not found in `{scenario_dir}`.",
            details={"name": scenario_name, "scenario_dir": str(scenario_dir)},
        )

    raw_yaml = yaml_path.read_text(encoding="utf-8")
    try:
        raw = yaml.safe_load(raw_yaml)
    except yaml.YAMLError as exc:
        raise ConfigError(
            f"Failed to parse perf scenario YAML `{yaml_path.name}`.",
            details={"path": str(yaml_path), "error": str(exc)},
        ) from exc

    if not isinstance(raw, dict):
        raise ConfigError(
            f"Perf scenario YAML `{yaml_path.name}` must be a mapping at the top level.",
            details={"path": str(yaml_path), "type": type(raw).__name__},
        )

    if overrides:
        raw = _apply_overrides(raw, overrides)
        raw_yaml = yaml.safe_dump(raw, sort_keys=False)

    return _parse_perf_yaml(raw, raw_yaml)


def _list_available(directory: Path) -> list[str]:
    if not directory.is_dir():
        return []
    return sorted(
        entry.name
        for entry in directory.iterdir()
        if entry.is_dir() and not entry.name.startswith("_")
    )


def _apply_overrides(raw: dict[str, Any], overrides: dict[str, str]) -> dict[str, Any]:
    updated = copy.deepcopy(raw)
    for path, value in overrides.items():
        _apply_override(updated, path, value)
    return updated


def _apply_override(target: dict[str, Any], path: str, raw_value: str) -> None:
    if path.startswith("client.config."):
        _set_string_map_value(
            target,
            ("client", "config"),
            path[len("client.config."):],
            raw_value,
        )
        return
    if path.startswith("client.properties."):
        _set_string_map_value(
            target,
            ("client", "properties"),
            path[len("client.properties."):],
            raw_value,
        )
        return
    if path.startswith("cluster.config."):
        _set_string_map_value(
            target,
            ("cluster", "config"),
            path[len("cluster.config."):],
            raw_value,
        )
        return
    if path.startswith("table.properties."):
        _set_string_map_value(
            target,
            ("table", "properties"),
            path[len("table.properties."):],
            raw_value,
        )
        return

    tokens = _tokenize_override_path(path)
    value = _parse_override_value(raw_value)
    current: Any = target

    for index, token in enumerate(tokens[:-1]):
        next_token = tokens[index + 1]
        if isinstance(token, str):
            if not isinstance(current, dict):
                raise ConfigError(
                    f"Cannot apply perf override `{path}`.",
                    details={"path": path},
                )
            key = _resolve_mapping_key(current, token)
            if key not in current or current[key] is None:
                current[key] = [] if isinstance(next_token, int) else {}
            elif isinstance(next_token, int) and not isinstance(current[key], list):
                raise ConfigError(
                    f"Perf override `{path}` expected a list at `{token}`.",
                    details={"path": path, "token": token},
                )
            elif isinstance(next_token, str) and not isinstance(current[key], dict):
                raise ConfigError(
                    f"Perf override `{path}` expected a mapping at `{token}`.",
                    details={"path": path, "token": token},
                )
            current = current[key]
            continue

        if not isinstance(current, list):
            raise ConfigError(
                f"Cannot apply perf override `{path}`.",
                details={"path": path},
            )
        _ensure_list_size(current, token)
        if current[token] is None:
            current[token] = [] if isinstance(next_token, int) else {}
        elif isinstance(next_token, int) and not isinstance(current[token], list):
            raise ConfigError(
                f"Perf override `{path}` expected a list at index {token}.",
                details={"path": path, "index": token},
            )
        elif isinstance(next_token, str) and not isinstance(current[token], dict):
            raise ConfigError(
                f"Perf override `{path}` expected a mapping at index {token}.",
                details={"path": path, "index": token},
            )
        current = current[token]

    last_token = tokens[-1]
    if isinstance(last_token, str):
        if not isinstance(current, dict):
            raise ConfigError(
                f"Cannot apply perf override `{path}`.",
                details={"path": path},
            )
        key = _resolve_mapping_key(current, last_token)
        current[key] = value
        return

    if not isinstance(current, list):
        raise ConfigError(
            f"Cannot apply perf override `{path}`.",
            details={"path": path},
        )
    _ensure_list_size(current, last_token)
    current[last_token] = value


def _set_string_map_value(
    target: dict[str, Any],
    parents: tuple[str, str],
    key: str,
    value: str,
) -> None:
    if not key:
        raise ConfigError(
            "Perf override key must not be empty.",
            details={"parents": ".".join(parents)},
        )

    current: dict[str, Any] = target
    for segment in parents:
        resolved = _resolve_mapping_key(current, segment)
        existing = current.get(resolved)
        if existing is None:
            current[resolved] = {}
            existing = current[resolved]
        if not isinstance(existing, dict):
            raise ConfigError(
                "Perf override path points to a non-mapping value.",
                details={"parents": ".".join(parents)},
            )
        current = existing
    current[key] = value


def _parse_override_value(raw_value: str) -> Any:
    if raw_value == "":
        return ""
    try:
        return yaml.safe_load(raw_value)
    except yaml.YAMLError as exc:
        raise ConfigError(
            f"Invalid perf override value `{raw_value}`.",
            details={"value": raw_value, "error": str(exc)},
        ) from exc


def _tokenize_override_path(path: str) -> list[str | int]:
    if not path:
        raise ConfigError("Perf override key must not be empty.")

    tokens: list[str | int] = []
    for segment in path.split("."):
        if not segment:
            raise ConfigError(
                f"Invalid perf override key `{path}`.",
                details={"path": path},
            )

        buffer = segment
        bracket_start = buffer.find("[")
        if bracket_start == -1:
            tokens.append(buffer)
            continue

        key = buffer[:bracket_start]
        if key:
            tokens.append(key)
        buffer = buffer[bracket_start:]

        while buffer:
            if not buffer.startswith("["):
                raise ConfigError(
                    f"Invalid perf override key `{path}`.",
                    details={"path": path},
                )
            bracket_end = buffer.find("]")
            if bracket_end == -1:
                raise ConfigError(
                    f"Invalid perf override key `{path}`.",
                    details={"path": path},
                )
            index_text = buffer[1:bracket_end]
            if not index_text.isdigit():
                raise ConfigError(
                    f"Perf override `{path}` uses an invalid list index.",
                    details={"path": path, "index": index_text},
                )
            tokens.append(int(index_text))
            buffer = buffer[bracket_end + 1:]
    return tokens


def _resolve_mapping_key(container: dict[str, Any], token: str) -> str:
    candidates = [token]
    if "_" in token:
        candidates.append(token.replace("_", "-"))
    if "-" in token:
        candidates.append(token.replace("-", "_"))

    for candidate in candidates:
        if candidate in container:
            return candidate

    if "_" in token:
        return token.replace("_", "-")
    return token


def _ensure_list_size(values: list[Any], index: int) -> None:
    while len(values) <= index:
        values.append(None)


def _parse_perf_yaml(raw: dict[str, Any], raw_yaml: str) -> PerfScenarioConfig:
    """Parse raw YAML dict into PerfScenarioConfig."""
    for section in _REQUIRED_SECTIONS:
        if section not in raw:
            raise ConfigError(
                f"Perf scenario YAML is missing required section `{section}`.",
                details={"missing": section},
            )

    meta = _parse_meta(raw["meta"])
    table = _parse_table(raw["table"])
    workload = tuple(_parse_workload_phase(phase) for phase in raw["workload"])

    cluster = _parse_cluster(raw["cluster"]) if "cluster" in raw else None
    client = _parse_client(raw["client"]) if "client" in raw else None
    data = _parse_data(raw["data"]) if "data" in raw else None
    sampling = _parse_sampling(raw["sampling"]) if "sampling" in raw else None
    report = _parse_report(raw["report"]) if "report" in raw else None
    thresholds = _parse_thresholds(raw["thresholds"]) if "thresholds" in raw else None

    return PerfScenarioConfig(
        meta=meta,
        table=table,
        workload=workload,
        cluster=cluster,
        client=client,
        data=data,
        sampling=sampling,
        report=report,
        thresholds=thresholds,
        raw_yaml=raw_yaml,
    )


def _parse_meta(raw: dict[str, Any]) -> PerfMetaConfig:
    if "name" not in raw:
        raise ConfigError("Perf scenario meta section requires a `name` field.")
    return PerfMetaConfig(
        name=raw["name"],
        description=raw.get("description"),
        tags=tuple(raw.get("tags", ())),
    )


def _parse_cluster(raw: dict[str, Any]) -> PerfClusterConfig:
    return PerfClusterConfig(
        tablet_servers=raw.get(_kebab_to_snake("tablet-servers"), raw.get("tablet-servers", 3)),
        jvm_args=tuple(raw.get(_kebab_to_snake("jvm-args"), raw.get("jvm-args", ()))),
        config_overrides=dict(raw.get("config", {})),
    )


def _parse_client(raw: dict[str, Any]) -> PerfClientConfig:
    return PerfClientConfig(
        properties=dict(raw.get("config", {})),
    )


def _parse_column(raw: dict[str, Any]) -> PerfColumnConfig:
    agg = _parse_agg(raw.get("agg")) if "agg" in raw else None
    return PerfColumnConfig(
        name=raw["name"],
        type=raw["type"],
        agg=agg,
    )


def _parse_agg(raw: Any) -> PerfAggConfig:
    """Parse agg field which can be a string or a dict with function and args."""
    if isinstance(raw, str):
        return PerfAggConfig(function=raw)
    if isinstance(raw, dict):
        return PerfAggConfig(
            function=raw["function"],
            args={str(k): str(v) for k, v in raw.get("args", {}).items()} if "args" in raw else None,
        )
    raise ConfigError(
        f"Column `agg` must be a string or mapping, got `{type(raw).__name__}`.",
        details={"agg": raw},
    )


def _parse_table(raw: dict[str, Any]) -> PerfTableConfig:
    if "name" not in raw:
        raise ConfigError("Perf scenario table section requires a `name` field.")
    columns = tuple(_parse_column(col) for col in raw.get("columns", ()))
    pk = raw.get("primary-key", raw.get(_kebab_to_snake("primary-key")))
    bucket_keys = raw.get("bucket-keys", raw.get(_kebab_to_snake("bucket-keys")))
    return PerfTableConfig(
        name=raw["name"],
        columns=columns,
        primary_key=tuple(pk) if pk is not None else None,
        buckets=raw.get("buckets"),
        bucket_keys=tuple(bucket_keys) if bucket_keys is not None else None,
        merge_engine=raw.get("merge-engine", raw.get(_kebab_to_snake("merge-engine"))),
        log_format=raw.get("log-format", raw.get(_kebab_to_snake("log-format"))),
        kv_format=raw.get("kv-format", raw.get(_kebab_to_snake("kv-format"))),
        properties=dict(raw["properties"]) if "properties" in raw else None,
    )


def _parse_generator(raw: Any) -> PerfGeneratorConfig:
    """Parse generator which can be a simple type string or a dict with type and params."""
    if isinstance(raw, str):
        return PerfGeneratorConfig(type=raw, params=None)
    if isinstance(raw, dict):
        gen_type = raw.get("type")
        if gen_type is None:
            raise ConfigError(
                "Generator config must have a `type` field.",
                details={"generator": raw},
            )
        # All keys except 'type' are treated as params
        params = {k: v for k, v in raw.items() if k != "type"}
        return PerfGeneratorConfig(
            type=gen_type,
            params=params if params else None,
        )
    raise ConfigError(
        f"Generator must be a string or mapping, got `{type(raw).__name__}`.",
        details={"generator": raw},
    )


def _parse_data(raw: dict[str, Any]) -> PerfDataConfig:
    generators = None
    if "generators" in raw:
        generators = {
            name: _parse_generator(gen_raw)
            for name, gen_raw in raw["generators"].items()
        }
    return PerfDataConfig(
        seed=raw.get("seed"),
        generators=generators,
    )


def _parse_workload_phase(raw: dict[str, Any]) -> WorkloadPhaseConfig:
    if "phase" not in raw:
        raise ConfigError(
            "Each workload phase must have a `phase` field.",
            details={"phase": raw},
        )
    key_range_raw = raw.get("key-range", raw.get(_kebab_to_snake("key-range")))
    key_range = tuple(key_range_raw) if key_range_raw is not None else None

    # warmup can be an int (record count) or a string (duration); normalize to string
    warmup_raw = raw.get("warmup")
    warmup = str(warmup_raw) if warmup_raw is not None else None

    return WorkloadPhaseConfig(
        phase=raw["phase"],
        records=raw.get("records"),
        duration=raw.get("duration"),
        threads=raw.get("threads", 1),
        warmup=warmup,
        rate_limit=raw.get("rate-limit", raw.get(_kebab_to_snake("rate-limit"))),
        key_range=key_range,
        key_prefix_length=raw.get(
            "key-prefix-length", raw.get(_kebab_to_snake("key-prefix-length")),
        ),
        from_offset=raw.get("from-offset", raw.get(_kebab_to_snake("from-offset"))),
        max_retries=raw.get("max-retries", raw.get(_kebab_to_snake("max-retries"), 0)),
        mix=dict(raw["mix"]) if "mix" in raw else None,
    )


def _parse_sampling(raw: dict[str, Any]) -> PerfSamplingConfig:
    return PerfSamplingConfig(
        interval=raw.get("interval", "1s"),
    )


def _parse_report(raw: dict[str, Any]) -> PerfReportConfig:
    formats = raw.get("formats")
    output_dir = raw.get("output-dir", raw.get(_kebab_to_snake("output-dir"), "perf-output"))
    return PerfReportConfig(
        formats=tuple(formats) if formats is not None else ("json", "html", "csv"),
        output_dir=output_dir,
    )


def _parse_threshold_bound(raw: dict[str, Any]) -> ThresholdBound:
    return ThresholdBound(
        min=raw.get("min"),
        max=raw.get("max"),
    )


def _parse_thresholds(raw: dict[str, Any]) -> PerfThresholdsConfig:
    def _bound(key: str) -> ThresholdBound | None:
        snake_key = _kebab_to_snake(key)
        value = raw.get(key, raw.get(snake_key))
        if value is None:
            return None
        return _parse_threshold_bound(value)

    return PerfThresholdsConfig(
        write_tps=_bound("write-tps"),
        p99_ms=_bound("p99-ms"),
        rss_peak_mb=_bound("rss-peak-mb"),
        heap_mb=_bound("heap-mb"),
    )


def _kebab_to_snake(s: str) -> str:
    """Convert kebab-case to snake_case."""
    return s.replace("-", "_")
