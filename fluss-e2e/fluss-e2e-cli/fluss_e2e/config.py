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

"""Configuration and path resolution."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def _resolve_cli_root(repo_root: Path) -> Path:
    """Find the fluss-e2e-cli directory relative to repo_root."""
    # Monorepo layout: <repo>/tools/e2e/fluss-e2e-cli/
    monorepo_path = repo_root / "tools/e2e/fluss-e2e-cli"
    if monorepo_path.is_dir():
        return monorepo_path
    # Sibling layout: <repo>/fluss-e2e/fluss-e2e-cli/
    sibling_path = repo_root / "fluss-e2e/fluss-e2e-cli"
    if sibling_path.is_dir():
        return sibling_path
    # Dev workspace layout: <workspace>/fluss-e2e-cli/
    dev_path = repo_root / "fluss-e2e-cli"
    if dev_path.is_dir():
        return dev_path
    # Fallback: assume repo_root IS the cli root
    return repo_root


@dataclass(frozen=True)
class RuntimeConfig:
    repo_root: Path
    build_timeout_s: int
    cluster_startup_timeout_s: int
    scenario_timeout_s: int
    health_check_interval_s: int
    maven_command: str
    java_command: str
    compose_file: Path
    runtime_dockerfile: Path
    build_target_dir: Path
    prometheus_url: str

    @classmethod
    def discover(cls, repo_root: Path | None = None) -> "RuntimeConfig":
        if repo_root is None:
            env_root = os.getenv("FLUSS_E2E_REPO_ROOT")
            if env_root:
                repo_root = Path(env_root)
            else:
                # Try monorepo layout: <repo>/tools/e2e/fluss-e2e-cli/fluss_e2e/config.py
                candidate = Path(__file__).resolve().parents[4]
                if (candidate / "tools/e2e/fluss-e2e-cli").is_dir():
                    repo_root = candidate
                else:
                    # Dev workspace layout: <workspace>/fluss-e2e-cli/fluss_e2e/config.py
                    # Use the workspace root (parent of fluss-e2e-cli/)
                    repo_root = Path(__file__).resolve().parents[2]
        repo_root = repo_root.resolve()
        cli_root = _resolve_cli_root(repo_root)
        return cls(
            repo_root=repo_root,
            build_timeout_s=_env_int("FLUSS_E2E_BUILD_TIMEOUT", 600),
            cluster_startup_timeout_s=_env_int("FLUSS_E2E_CLUSTER_STARTUP_TIMEOUT", 120),
            scenario_timeout_s=_env_int("FLUSS_E2E_SCENARIO_TIMEOUT", 300),
            health_check_interval_s=_env_int("FLUSS_E2E_HEALTH_CHECK_INTERVAL", 5),
            maven_command=os.getenv("FLUSS_E2E_MAVEN_COMMAND", "./mvnw"),
            java_command=os.getenv("FLUSS_E2E_JAVA_COMMAND", "java"),
            compose_file=cli_root / "docker/docker-compose.yml",
            runtime_dockerfile=cli_root / "docker/fluss-runtime.Dockerfile",
            build_target_dir=repo_root / "build-target",
            prometheus_url=os.getenv(
                "FLUSS_E2E_PROMETHEUS_URL", "http://localhost:9090"
            ),
        )

    @property
    def fluss_source_root(self) -> Path:
        """Root of the Fluss source tree (for Docker build context)."""
        # Monorepo layout: repo_root IS the Fluss source
        if (self.repo_root / "fluss-dist").is_dir():
            return self.repo_root
        # Dev workspace layout: Fluss source under docs/references/code/fluss/
        dev_path = self.repo_root / "docs/references/code/fluss"
        if dev_path.is_dir():
            return dev_path
        return self.repo_root

    @property
    def scenarios_dir(self) -> Path:
        cli_root = _resolve_cli_root(self.repo_root)
        return cli_root / "scenarios"

    @property
    def perf_compose_file(self) -> Path:
        cli_root = _resolve_cli_root(self.repo_root)
        return cli_root / "docker/docker-compose.perf.yml"

    @property
    def perf_client_module_dir(self) -> Path:
        # Monorepo layout: <repo>/tools/e2e/fluss-e2e-perf-client/
        monorepo_path = self.repo_root / "tools/e2e/fluss-e2e-perf-client"
        if monorepo_path.is_dir():
            return monorepo_path
        # Sibling layout: <repo>/fluss-e2e/fluss-e2e-perf-client/
        sibling_path = self.repo_root / "fluss-e2e/fluss-e2e-perf-client"
        if sibling_path.is_dir():
            return sibling_path
        # Dev workspace layout: <workspace>/fluss-e2e-perf-client/
        return self.repo_root / "fluss-e2e-perf-client"

    @property
    def java_client_target_dir(self) -> Path:
        # Monorepo layout: <repo>/tools/e2e/fluss-e2e-java-client/
        monorepo_path = self.repo_root / "tools/e2e/fluss-e2e-java-client/target"
        if monorepo_path.parent.is_dir():
            return monorepo_path
        # Sibling layout: <repo>/fluss-e2e/fluss-e2e-java-client/
        sibling_path = self.repo_root / "fluss-e2e/fluss-e2e-java-client/target"
        if sibling_path.parent.is_dir():
            return sibling_path
        # Dev workspace layout: <workspace>/fluss-e2e-java-client/
        return self.repo_root / "fluss-e2e-java-client/target"
