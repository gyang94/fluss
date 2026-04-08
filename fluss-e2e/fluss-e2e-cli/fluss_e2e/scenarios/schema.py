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

"""Frozen dataclasses for YAML scenario configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class MetaConfig:
    name: str
    description: str
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class ClusterConfig:
    compose_profiles: tuple[str, ...] = ()
    bootstrap_servers: str = "localhost:9123"


@dataclass(frozen=True)
class ColumnConfig:
    name: str
    type: str


@dataclass(frozen=True)
class TableConfig:
    name_prefix: str
    database: str = "e2e"
    columns: tuple[ColumnConfig, ...] = ()
    primary_key: tuple[str, ...] = ()
    buckets: int = 4
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidateCheck:
    name: str
    assert_expr: str


@dataclass(frozen=True)
class StepConfig:
    action: str
    id: str | None = None
    args: dict[str, Any] = field(default_factory=dict)
    checks: tuple[ValidateCheck, ...] = ()
    on_error: str = "fail"
    save_as: str | None = None
    command: str | None = None
    method: str | None = None
    url: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    body: str | None = None


@dataclass(frozen=True)
class TeardownStep:
    action: str
    args: dict[str, Any] = field(default_factory=dict)
    on_error: str = "ignore"


@dataclass(frozen=True)
class VerifyRule:
    type: str = field(default="", kw_only=True)


@dataclass(frozen=True)
class DataVerifyRule(VerifyRule):
    assert_exprs: tuple[str, ...]
    type: str = field(default="data", kw_only=True)


@dataclass(frozen=True)
class MetricsQuery:
    metric: str
    labels: dict[str, str]
    assert_expr: str


@dataclass(frozen=True)
class MetricsVerifyRule(VerifyRule):
    source: str
    queries: tuple[MetricsQuery, ...]
    type: str = field(default="metrics", kw_only=True)


@dataclass(frozen=True)
class LogAssert:
    pattern: str
    exists: bool = True


@dataclass(frozen=True)
class LogVerifyRule(VerifyRule):
    container: str
    asserts: tuple[LogAssert, ...]
    type: str = field(default="logs", kw_only=True)


@dataclass(frozen=True)
class HealthCheck:
    url: str
    status: int = 200


@dataclass(frozen=True)
class HealthVerifyRule(VerifyRule):
    checks: tuple[HealthCheck, ...]
    type: str = field(default="health", kw_only=True)


@dataclass(frozen=True)
class ScenarioConfig:
    meta: MetaConfig
    cluster: ClusterConfig
    table: TableConfig
    params: dict[str, Any]
    steps: tuple[StepConfig, ...]
    teardown: tuple[TeardownStep, ...] = ()
    verify: tuple[VerifyRule, ...] = ()
