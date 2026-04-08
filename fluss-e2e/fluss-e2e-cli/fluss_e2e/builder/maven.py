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

"""Unified build orchestration: Fluss source, Java client, and Docker image."""

from __future__ import annotations

from pathlib import Path
import shutil
import tempfile
from typing import Any
import zipfile

from ..config import RuntimeConfig
from ..errors import BuildError, ConfigError
from ..subprocess_runner import CommandTimeout, failure_details, run_command, timeout_details


WRAPPER_BOOTSTRAP_ERRORS = (
    "Failed to validate Maven distribution SHA-256",
    "Failed to fetch https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/",
)
JAVA_CLIENT_MAIN_CLASS = "org.apache.fluss.e2e.ClientRunner"
REQUIRED_JAVA_CLIENT_JAR_ENTRIES = (
    "org/apache/fluss/e2e/ClientRunner.class",
    "org/apache/fluss/row/InternalRow.class",
)
REQUIRED_BUILD_TARGET_DIRS = ("bin", "conf", "lib")

DEFAULT_IMAGE_NAME = "fluss-local"
DEFAULT_IMAGE_TAG = "latest"

_DOCKERFILE_TEMPLATE = """\
FROM eclipse-temurin:17-jre-noble

RUN set -ex; \\
  apt-get update; \\
  apt-get -y install gpg libsnappy1v5 gettext-base libjemalloc-dev; \\
  rm -rf /var/lib/apt/lists/*

ENV FLUSS_HOME=/opt/fluss
ENV PATH=$FLUSS_HOME/bin:$PATH

RUN groupadd --system --gid=9999 fluss && \\
    useradd --system --home-dir $FLUSS_HOME --uid=9999 --gid=fluss fluss

WORKDIR $FLUSS_HOME

COPY --chown=fluss:fluss {build_target_rel}/ /opt/fluss/

RUN ["chown", "-R", "fluss:fluss", "."]
COPY docker/fluss/docker-entrypoint.sh /
RUN ["chmod", "+x", "/docker-entrypoint.sh"]
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
"""


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_fluss_source_dir(source_dir: Path) -> Path:
    """Validate that the given path is a Fluss source directory."""
    resolved = source_dir.resolve()
    if not resolved.is_dir():
        raise ConfigError(
            f"Fluss source directory does not exist: {resolved}",
            details={"source_dir": str(resolved)},
        )
    if not (resolved / "fluss-dist").is_dir():
        raise ConfigError(
            "Not a valid Fluss source directory: missing `fluss-dist/` module.",
            details={"source_dir": str(resolved)},
        )
    if not (resolved / "pom.xml").is_file():
        raise ConfigError(
            "Not a valid Fluss source directory: missing `pom.xml`.",
            details={"source_dir": str(resolved)},
        )
    return resolved


def resolve_build_target(source_dir: Path) -> Path:
    """Resolve the build-target directory from a Fluss source tree."""
    build_target = source_dir / "build-target"
    if not build_target.exists():
        raise BuildError(
            "build-target not found after building fluss-dist.",
            details={"source_dir": str(source_dir)},
        )
    resolved = build_target.resolve()
    missing = [d for d in REQUIRED_BUILD_TARGET_DIRS if not (resolved / d).is_dir()]
    if missing:
        raise BuildError(
            "build-target is incomplete: missing required directories.",
            details={
                "build_target": str(resolved),
                "missing_dirs": missing,
            },
        )
    return resolved


def _validate_docker_context(source_dir: Path) -> None:
    """Ensure the Fluss source has the required Dockerfile and entrypoint."""
    dockerfile = source_dir / "docker" / "fluss" / "Dockerfile"
    if not dockerfile.is_file():
        raise ConfigError(
            "Fluss source is missing `docker/fluss/Dockerfile`.",
            details={"source_dir": str(source_dir)},
        )
    entrypoint = source_dir / "docker" / "fluss" / "docker-entrypoint.sh"
    if not entrypoint.is_file():
        raise ConfigError(
            "Fluss source is missing `docker/fluss/docker-entrypoint.sh`.",
            details={"source_dir": str(source_dir)},
        )


# ---------------------------------------------------------------------------
# JAR validation / resolution
# ---------------------------------------------------------------------------

def _validate_java_client_jar(path: Path) -> None:
    try:
        with zipfile.ZipFile(path) as jar_file:
            names = set(jar_file.namelist())
            missing_entries = [
                entry for entry in REQUIRED_JAVA_CLIENT_JAR_ENTRIES if entry not in names
            ]
            if missing_entries:
                raise BuildError(
                    "Located Fluss E2E Java client JAR is not self-contained.",
                    details={"jar_path": str(path), "missing_entries": missing_entries},
                )

            manifest = jar_file.read("META-INF/MANIFEST.MF").decode(
                "utf-8", errors="replace"
            )
    except FileNotFoundError as exc:
        raise BuildError(
            "Unable to locate the shaded Fluss E2E Java client JAR.",
            details={"jar_path": str(path)},
        ) from exc
    except zipfile.BadZipFile as exc:
        raise BuildError(
            "Located Fluss E2E Java client JAR is not a valid archive.",
            details={"jar_path": str(path)},
        ) from exc
    except KeyError as exc:
        raise BuildError(
            "Located Fluss E2E Java client JAR is missing a manifest.",
            details={"jar_path": str(path)},
        ) from exc

    if f"Main-Class: {JAVA_CLIENT_MAIN_CLASS}" not in manifest:
        raise BuildError(
            "Located Fluss E2E Java client JAR is missing the expected main class.",
            details={"jar_path": str(path), "main_class": JAVA_CLIENT_MAIN_CLASS},
        )


def resolve_java_client_jar(config: RuntimeConfig) -> Path:
    candidates = sorted(
        config.java_client_target_dir.glob("fluss-e2e-java-client-*.jar"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    invalid_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        name = candidate.name
        if any(token in name for token in ("sources", "javadoc", "tests", "original")):
            continue
        try:
            _validate_java_client_jar(candidate)
        except BuildError as exc:
            invalid_candidates.append(exc.details)
            continue
        return candidate.resolve()
    raise BuildError(
        "Unable to locate the shaded Fluss E2E Java client JAR.",
        details={
            "target_dir": str(config.java_client_target_dir),
            "invalid_candidates": invalid_candidates,
        },
    )


# ---------------------------------------------------------------------------
# Maven command helpers
# ---------------------------------------------------------------------------

def _maven_command_for(cwd: Path, fallback_command: str) -> str:
    """Return the Maven command to use for a given working directory.

    Checks *cwd* first, then walks up to find a ``mvnw`` wrapper.
    Falls back to *fallback_command* (which may itself be ``./mvnw``).
    """
    # Check cwd itself
    wrapper = cwd / "mvnw"
    if wrapper.is_file():
        return "./mvnw"
    # Walk up to find mvnw in parent directories
    for parent in cwd.parents:
        candidate = parent / "mvnw"
        if candidate.is_file():
            return str(candidate.resolve())
    return fallback_command


def _candidate_commands(
    maven_cmd: str, base_args: list[str]
) -> list[list[str]]:
    """Build a list of Maven commands to try, with optional fallback."""
    commands = [[maven_cmd, *base_args]]
    is_wrapper = maven_cmd == "./mvnw" or maven_cmd.endswith("/mvnw")
    if is_wrapper:
        system_maven = shutil.which("mvn")
        if system_maven is not None:
            commands.append([system_maven, *base_args])
    return commands


def _should_fallback(result, attempt_index: int) -> bool:
    if attempt_index != 0:
        return False
    output = f"{result.stdout}\n{result.stderr}"
    return any(marker in output for marker in WRAPPER_BOOTSTRAP_ERRORS)


def _run_maven(
    *,
    cwd: Path,
    maven_cmd: str,
    base_args: list[str],
    timeout_s: int,
    stage_label: str,
) -> tuple[int, bool, list[dict[str, Any]], list[str]]:
    """Run Maven with fallback logic. Returns (total_duration_ms, fallback_used, attempts, final_command)."""
    total_duration_ms = 0
    attempts: list[dict[str, Any]] = []

    for index, attempt_command in enumerate(_candidate_commands(maven_cmd, base_args)):
        try:
            result = run_command(
                attempt_command,
                cwd=str(cwd),
                timeout_s=timeout_s,
            )
        except CommandTimeout as exc:
            total_duration_ms += exc.duration_ms
            attempts.append({
                "command": exc.command,
                "status": "timeout",
                "duration_ms": exc.duration_ms,
            })
            raise BuildError(
                f"{stage_label} timed out.",
                details=timeout_details(
                    exc.command,
                    exc.timeout_s,
                    stdout=exc.stdout,
                    stderr=exc.stderr,
                    duration_ms=exc.duration_ms,
                ),
            ) from exc

        total_duration_ms += result.duration_ms
        attempts.append({
            "command": result.command,
            "status": "success" if result.returncode == 0 else "failed",
            "duration_ms": result.duration_ms,
            "returncode": result.returncode,
        })

        if result.returncode == 0:
            return total_duration_ms, index > 0, attempts, result.command

        if not _should_fallback(result, index):
            raise BuildError(
                f"{stage_label} failed.",
                details=failure_details(result),
            )
    else:
        raise BuildError(
            f"{stage_label} failed.",
            details={"reason": "No usable Maven command was available."},
        )


# ---------------------------------------------------------------------------
# UnifiedBuilder
# ---------------------------------------------------------------------------

class UnifiedBuilder:
    """Builds Fluss source, Java client, and Docker image in one pipeline."""

    def __init__(
        self,
        config: RuntimeConfig,
        source_dir: Path,
        *,
        image_name: str = DEFAULT_IMAGE_NAME,
        image_tag: str = DEFAULT_IMAGE_TAG,
    ) -> None:
        self._config = config
        self._source_dir = validate_fluss_source_dir(source_dir)
        self._image_name = image_name
        self._image_tag = image_tag

    @property
    def full_image_tag(self) -> str:
        return f"{self._image_name}:{self._image_tag}"

    def build(self) -> dict[str, Any]:
        """Run the full build pipeline and return a unified report."""
        report: dict[str, Any] = {
            "status": "failed",
            "source_dir": str(self._source_dir),
            "image": self.full_image_tag,
            "stages": {},
            "duration_ms": 0,
        }
        total_duration_ms = 0

        try:
            # Stage 1: Install Fluss artifacts to local Maven repo
            stage1 = self._build_fluss_dist()
            report["stages"]["fluss_dist"] = stage1
            total_duration_ms += stage1["duration_ms"]

            # Stage 2: Build Java client
            stage2 = self._build_java_client()
            report["stages"]["java_client"] = stage2
            total_duration_ms += stage2["duration_ms"]

            # Stage 3: Build Docker image
            stage3 = self._build_docker_image()
            report["stages"]["docker_image"] = stage3
            total_duration_ms += stage3["duration_ms"]

            report["status"] = "success"
            report["duration_ms"] = total_duration_ms
        except (BuildError, ConfigError) as exc:
            report["duration_ms"] = total_duration_ms
            raise BuildError(
                exc.message,
                details=exc.details,
                section=report,
            ) from exc

        return report

    def _build_fluss_dist(self) -> dict[str, Any]:
        """Stage 1: mvn install -pl fluss-dist in the Fluss source tree."""
        maven_cmd = _maven_command_for(self._source_dir, self._config.maven_command)
        base_args = ["-pl", "fluss-dist", "-am", "-DskipTests", "-Drat.skip=true", "install"]

        duration_ms, fallback_used, attempts, final_command = _run_maven(
            cwd=self._source_dir,
            maven_cmd=maven_cmd,
            base_args=base_args,
            timeout_s=self._config.build_timeout_s,
            stage_label="Fluss dist build",
        )

        build_target = resolve_build_target(self._source_dir)

        return {
            "status": "success",
            "module": "fluss-dist",
            "source_dir": str(self._source_dir),
            "build_target": str(build_target),
            "duration_ms": duration_ms,
            "command": final_command,
            "command_attempts": attempts,
            "fallback_used": fallback_used,
        }

    def _build_java_client(self) -> dict[str, Any]:
        """Stage 2: mvn package the Java bridge client."""
        source_dir = self._source_dir

        # Determine the java client module directory
        candidates = [
            "tools/e2e/fluss-e2e-java-client",
            "fluss-e2e/fluss-e2e-java-client",
            "fluss-e2e-java-client",
        ]
        module_path = next(
            (c for c in candidates if (source_dir / c).is_dir()),
            candidates[-1],
        )
        module_dir = source_dir / module_path

        # Build directly from the module directory since it may not be listed
        # as a module in the root POM.  Stage 1 already installed fluss
        # artifacts to the local Maven repo, so the parent POM resolves.
        maven_cmd = _maven_command_for(module_dir, self._config.maven_command)
        base_args = ["-DskipTests", "package"]

        duration_ms, fallback_used, attempts, final_command = _run_maven(
            cwd=module_dir,
            maven_cmd=maven_cmd,
            base_args=base_args,
            timeout_s=self._config.build_timeout_s,
            stage_label="Java client build",
        )

        jar = resolve_java_client_jar(self._config)

        return {
            "status": "success",
            "module": module_path,
            "jar_path": str(jar),
            "duration_ms": duration_ms,
            "command": final_command,
            "command_attempts": attempts,
            "fallback_used": fallback_used,
        }

    def _build_docker_image(self) -> dict[str, Any]:
        """Stage 3: Build a Docker image from the Fluss source tree."""
        _validate_docker_context(self._source_dir)
        build_target = resolve_build_target(self._source_dir)

        try:
            build_target_rel = build_target.relative_to(self._source_dir)
        except ValueError:
            raise BuildError(
                "build-target resolves outside the source directory.",
                details={
                    "build_target": str(build_target),
                    "source_dir": str(self._source_dir),
                },
            )

        dockerfile_content = _DOCKERFILE_TEMPLATE.format(
            build_target_rel=build_target_rel.as_posix(),
        )
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".Dockerfile",
            prefix="fluss-e2e-",
            delete=False,
        ) as f:
            f.write(dockerfile_content)
            generated_dockerfile = Path(f.name)

        command = [
            "docker", "build",
            "-t", self.full_image_tag,
            "-f", str(generated_dockerfile),
            str(self._source_dir),
        ]

        try:
            try:
                result = run_command(
                    command,
                    cwd=str(self._source_dir),
                    timeout_s=self._config.build_timeout_s,
                )
            except CommandTimeout as exc:
                raise BuildError(
                    "Docker image build timed out.",
                    details=timeout_details(
                        exc.command,
                        exc.timeout_s,
                        stdout=exc.stdout,
                        stderr=exc.stderr,
                        duration_ms=exc.duration_ms,
                    ),
                ) from exc

            if result.returncode != 0:
                raise BuildError(
                    "Docker image build failed.",
                    details=failure_details(result),
                )

            return {
                "status": "success",
                "image": self.full_image_tag,
                "build_target": str(build_target),
                "duration_ms": result.duration_ms,
                "command": command,
            }
        finally:
            generated_dockerfile.unlink(missing_ok=True)
