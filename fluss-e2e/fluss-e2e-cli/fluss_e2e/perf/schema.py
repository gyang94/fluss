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

"""Frozen dataclasses for perf scenario YAML configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class PerfMetaConfig:
    name: str
    description: str | None = None
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class PerfClusterConfig:
    tablet_servers: int = 3
    jvm_args: tuple[str, ...] = ()
    config_overrides: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PerfClientConfig:
    properties: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PerfAggConfig:
    function: str
    args: dict[str, str] | None = None


@dataclass(frozen=True)
class PerfColumnConfig:
    name: str
    type: str
    agg: PerfAggConfig | None = None


@dataclass(frozen=True)
class PerfTableConfig:
    name: str
    columns: tuple[PerfColumnConfig, ...]
    primary_key: tuple[str, ...] | None = None
    buckets: int | None = None
    bucket_keys: tuple[str, ...] | None = None
    merge_engine: str | None = None
    log_format: str | None = None
    kv_format: str | None = None
    properties: dict[str, str] | None = None


@dataclass(frozen=True)
class PerfGeneratorConfig:
    type: str
    params: dict[str, Any] | None = None


@dataclass(frozen=True)
class PerfDataConfig:
    seed: int | None = None
    generators: dict[str, PerfGeneratorConfig] | None = None


@dataclass(frozen=True)
class WorkloadPhaseConfig:
    phase: str  # write, lookup, prefix-lookup, scan, mixed
    records: int | None = None
    duration: str | None = None
    threads: int = 1
    warmup: str | None = None
    rate_limit: int | None = None
    key_range: tuple[int, int] | None = None
    key_prefix_length: int | None = None
    from_offset: str | None = None
    max_retries: int = 0
    mix: dict[str, int] | None = None  # for mixed phase: {"write": 50, "lookup": 50}


@dataclass(frozen=True)
class PerfSamplingConfig:
    interval: str = "1s"


@dataclass(frozen=True)
class PerfReportConfig:
    formats: tuple[str, ...] = ("json", "html", "csv")
    output_dir: str = "perf-output"


@dataclass(frozen=True)
class ThresholdBound:
    min: float | None = None
    max: float | None = None


@dataclass(frozen=True)
class PerfThresholdsConfig:
    write_tps: ThresholdBound | None = None
    p99_ms: ThresholdBound | None = None
    rss_peak_mb: ThresholdBound | None = None
    heap_mb: ThresholdBound | None = None


@dataclass(frozen=True)
class PerfScenarioConfig:
    meta: PerfMetaConfig
    table: PerfTableConfig
    workload: tuple[WorkloadPhaseConfig, ...]
    cluster: PerfClusterConfig | None = None
    client: PerfClientConfig | None = None
    data: PerfDataConfig | None = None
    sampling: PerfSamplingConfig | None = None
    report: PerfReportConfig | None = None
    thresholds: PerfThresholdsConfig | None = None
    raw_yaml: str | None = None
