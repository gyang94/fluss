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

"""Tests for UnifiedBuilder and build helpers."""

from __future__ import annotations

import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from fluss_e2e.builder.maven import (
    JAVA_CLIENT_MAIN_CLASS,
    WRAPPER_BOOTSTRAP_ERRORS,
    UnifiedBuilder,
    resolve_build_target,
    resolve_java_client_jar,
    validate_fluss_source_dir,
)
from fluss_e2e.config import RuntimeConfig
from fluss_e2e.errors import BuildError, ConfigError
from fluss_e2e.subprocess_runner import CommandResult, CommandTimeout


def _make_fluss_source(root: Path) -> Path:
    """Create a minimal Fluss source directory layout with Docker files."""
    (root / "fluss-dist").mkdir(parents=True, exist_ok=True)
    (root / "pom.xml").write_text("<project/>")
    (root / "mvnw").write_text("#!/bin/sh\n")
    docker_dir = root / "docker" / "fluss"
    docker_dir.mkdir(parents=True, exist_ok=True)
    (docker_dir / "Dockerfile").write_text("FROM eclipse-temurin:17-jre-noble\n")
    (docker_dir / "docker-entrypoint.sh").write_text("#!/bin/bash\n")
    return root


def _make_build_target(source_dir: Path) -> Path:
    """Create a valid build-target directory."""
    bt = source_dir / "build-target"
    for name in ("bin", "conf", "lib"):
        (bt / name).mkdir(parents=True, exist_ok=True)
    return bt


def _write_java_client_jar(
    path: Path, *, include_runtime_dependency: bool = True
) -> None:
    """Write a minimal valid shaded JAR."""
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "META-INF/MANIFEST.MF",
            f"Manifest-Version: 1.0\nMain-Class: {JAVA_CLIENT_MAIN_CLASS}\n",
        )
        zf.writestr("org/apache/fluss/e2e/ClientRunner.class", b"fake")
        if include_runtime_dependency:
            zf.writestr("org/apache/fluss/row/InternalRow.class", b"fake")
    path.write_bytes(buf.getvalue())


def _make_java_client_jar(config: RuntimeConfig) -> Path:
    """Create a minimal valid shaded JAR in the java client target dir."""
    target_dir = config.java_client_target_dir
    jar_path = target_dir / "fluss-e2e-java-client-0.7-SNAPSHOT.jar"
    _write_java_client_jar(jar_path)
    return jar_path


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


class ValidateFlussSourceDirTest(unittest.TestCase):
    def test_valid_source_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            result = validate_fluss_source_dir(source)
            self.assertEqual(result, source.resolve())

    def test_missing_directory(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            validate_fluss_source_dir(Path("/nonexistent/dir"))
        self.assertIn("does not exist", ctx.exception.message)

    def test_missing_fluss_dist(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "pom.xml").write_text("<project/>")
            with self.assertRaises(ConfigError) as ctx:
                validate_fluss_source_dir(root)
            self.assertIn("fluss-dist", ctx.exception.message)

    def test_missing_pom(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "fluss-dist").mkdir()
            with self.assertRaises(ConfigError) as ctx:
                validate_fluss_source_dir(root)
            self.assertIn("pom.xml", ctx.exception.message)


class ResolveBuildTargetTest(unittest.TestCase):
    def test_valid_build_target(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = Path(tempdir)
            bt = _make_build_target(source)
            result = resolve_build_target(source)
            self.assertEqual(result, bt.resolve())

    def test_missing_build_target(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            with self.assertRaises(BuildError) as ctx:
                resolve_build_target(Path(tempdir))
            self.assertIn("build-target not found", ctx.exception.message)

    def test_incomplete_build_target(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = Path(tempdir)
            bt = source / "build-target"
            (bt / "bin").mkdir(parents=True)
            with self.assertRaises(BuildError) as ctx:
                resolve_build_target(source)
            self.assertIn("incomplete", ctx.exception.message)
            self.assertIn("conf", ctx.exception.details["missing_dirs"])
            self.assertIn("lib", ctx.exception.details["missing_dirs"])


# ---------------------------------------------------------------------------
# JAR resolution
# ---------------------------------------------------------------------------


class ResolveJavaClientJarTest(unittest.TestCase):
    def test_ignores_non_runtime_jars(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            target_dir = repo_root / "tools/e2e/fluss-e2e-java-client/target"
            target_dir.mkdir(parents=True)
            (target_dir / "fluss-e2e-java-client-0.1.0-sources.jar").write_text("")
            runtime_jar = target_dir / "fluss-e2e-java-client-0.1.0.jar"
            _write_java_client_jar(runtime_jar)

            config = RuntimeConfig.discover(repo_root)
            self.assertEqual(resolve_java_client_jar(config), runtime_jar.resolve())

    def test_rejects_non_self_contained_jar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            target_dir = repo_root / "tools/e2e/fluss-e2e-java-client/target"
            invalid_jar = target_dir / "fluss-e2e-java-client-0.1.0.jar"
            _write_java_client_jar(invalid_jar, include_runtime_dependency=False)

            config = RuntimeConfig.discover(repo_root)
            with self.assertRaises(BuildError) as caught:
                resolve_java_client_jar(config)

            self.assertIn("invalid_candidates", caught.exception.details)


# ---------------------------------------------------------------------------
# UnifiedBuilder
# ---------------------------------------------------------------------------


class UnifiedBuilderInitTest(unittest.TestCase):
    def test_invalid_source_dir_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            with self.assertRaises(ConfigError):
                UnifiedBuilder(config, Path("/nonexistent"))

    def test_custom_image_tag(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(
                config, source, image_name="my-fluss", image_tag="v0.10"
            )
            self.assertEqual(builder.full_image_tag, "my-fluss:v0.10")


class UnifiedBuilderFlussDistTest(unittest.TestCase):
    def test_build_fluss_dist_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            _make_build_target(source)
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(config, source)

            with patch(
                "fluss_e2e.builder.maven.run_command",
                return_value=CommandResult(["./mvnw"], 0, "BUILD SUCCESS", "", 5000),
            ):
                stage = builder._build_fluss_dist()

            self.assertEqual(stage["status"], "success")
            self.assertEqual(stage["module"], "fluss-dist")
            self.assertIsNotNone(stage["build_target"])

    def test_build_fluss_dist_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(config, source)

            with patch(
                "fluss_e2e.builder.maven.run_command",
                return_value=CommandResult(["./mvnw"], 1, "", "compilation failed", 3000),
            ):
                with self.assertRaises(BuildError) as ctx:
                    builder._build_fluss_dist()
            self.assertIn("failed", ctx.exception.message)

    def test_build_fluss_dist_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(config, source)

            with patch(
                "fluss_e2e.builder.maven.run_command",
                side_effect=CommandTimeout(["./mvnw"], 600, duration_ms=600000),
            ):
                with self.assertRaises(BuildError) as ctx:
                    builder._build_fluss_dist()
            self.assertIn("timed out", ctx.exception.message)


class UnifiedBuilderJavaClientTest(unittest.TestCase):
    def test_build_java_client_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            config = RuntimeConfig.discover(Path(tempdir))
            _make_java_client_jar(config)
            builder = UnifiedBuilder(config, source)

            with patch(
                "fluss_e2e.builder.maven.run_command",
                return_value=CommandResult(["./mvnw"], 0, "BUILD SUCCESS", "", 4000),
            ):
                stage = builder._build_java_client()

            self.assertEqual(stage["status"], "success")
            self.assertIn("jar_path", stage)

    def test_build_java_client_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(config, source)

            with patch(
                "fluss_e2e.builder.maven.run_command",
                return_value=CommandResult(["./mvnw"], 1, "", "dependency error", 2000),
            ):
                with self.assertRaises(BuildError) as ctx:
                    builder._build_java_client()
            self.assertIn("failed", ctx.exception.message)


class UnifiedBuilderDockerImageTest(unittest.TestCase):
    def test_build_docker_image_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            _make_build_target(source)
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(config, source)

            with patch(
                "fluss_e2e.builder.maven.run_command",
                return_value=CommandResult(
                    ["docker", "build", "-t", "fluss-local:latest"], 0, "", "", 8000
                ),
            ):
                stage = builder._build_docker_image()

            self.assertEqual(stage["status"], "success")
            self.assertEqual(stage["image"], "fluss-local:latest")

    def test_build_docker_image_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            _make_build_target(source)
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(config, source)

            with patch(
                "fluss_e2e.builder.maven.run_command",
                return_value=CommandResult(
                    ["docker", "build"], 1, "", "error building image", 3000
                ),
            ):
                with self.assertRaises(BuildError) as ctx:
                    builder._build_docker_image()
            self.assertIn("Docker image build failed", ctx.exception.message)

    def test_build_docker_image_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            _make_build_target(source)
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(config, source)

            with patch(
                "fluss_e2e.builder.maven.run_command",
                side_effect=CommandTimeout(["docker", "build"], 600, duration_ms=600000),
            ):
                with self.assertRaises(BuildError) as ctx:
                    builder._build_docker_image()
            self.assertIn("timed out", ctx.exception.message)

    def test_missing_dockerfile_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = Path(tempdir)
            (source / "fluss-dist").mkdir()
            (source / "pom.xml").write_text("<project/>")
            _make_build_target(source)
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(config, source)

            with self.assertRaises(ConfigError) as ctx:
                builder._build_docker_image()
            self.assertIn("Dockerfile", ctx.exception.message)


class UnifiedBuilderFullPipelineTest(unittest.TestCase):
    def test_build_full_pipeline_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            _make_build_target(source)
            config = RuntimeConfig.discover(Path(tempdir))
            _make_java_client_jar(config)
            builder = UnifiedBuilder(config, source)

            def mock_run_command(command, **kwargs):
                return CommandResult(command, 0, "BUILD SUCCESS", "", 5000)

            with patch("fluss_e2e.builder.maven.run_command", side_effect=mock_run_command):
                report = builder.build()

            self.assertEqual(report["status"], "success")
            self.assertIn("fluss_dist", report["stages"])
            self.assertIn("java_client", report["stages"])
            self.assertIn("docker_image", report["stages"])
            self.assertEqual(report["stages"]["fluss_dist"]["status"], "success")
            self.assertEqual(report["stages"]["java_client"]["status"], "success")
            self.assertEqual(report["stages"]["docker_image"]["status"], "success")
            self.assertEqual(report["image"], "fluss-local:latest")
            self.assertGreater(report["duration_ms"], 0)

    def test_build_failure_at_dist_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            config = RuntimeConfig.discover(Path(tempdir))
            builder = UnifiedBuilder(config, source)

            with patch(
                "fluss_e2e.builder.maven.run_command",
                return_value=CommandResult(["./mvnw"], 1, "", "compile error", 3000),
            ):
                with self.assertRaises(BuildError) as ctx:
                    builder.build()
            self.assertIsNotNone(ctx.exception.section)
            self.assertEqual(ctx.exception.section["status"], "failed")

    def test_build_failure_at_docker_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            _make_build_target(source)
            config = RuntimeConfig.discover(Path(tempdir))
            _make_java_client_jar(config)
            builder = UnifiedBuilder(config, source)

            def mock_run_command(command, **kwargs):
                if "docker" in command:
                    return CommandResult(command, 1, "", "docker error", 2000)
                return CommandResult(command, 0, "BUILD SUCCESS", "", 5000)

            with patch("fluss_e2e.builder.maven.run_command", side_effect=mock_run_command):
                with self.assertRaises(BuildError) as ctx:
                    builder.build()
            section = ctx.exception.section
            self.assertEqual(section["status"], "failed")
            self.assertIn("fluss_dist", section["stages"])
            self.assertIn("java_client", section["stages"])

    def test_build_maven_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = _make_fluss_source(Path(tempdir))
            _make_build_target(source)
            config = RuntimeConfig.discover(Path(tempdir))
            _make_java_client_jar(config)
            builder = UnifiedBuilder(config, source)

            def mock_run_command(command, **kwargs):
                if command[0] == "./mvnw" and "install" in command:
                    return CommandResult(
                        command, 1, "",
                        WRAPPER_BOOTSTRAP_ERRORS[0], 10,
                    )
                return CommandResult(command, 0, "BUILD SUCCESS", "", 5000)

            with patch("fluss_e2e.builder.maven.shutil.which", return_value="/usr/local/bin/mvn"), \
                 patch("fluss_e2e.builder.maven.run_command", side_effect=mock_run_command):
                report = builder.build()

            self.assertEqual(report["status"], "success")
            self.assertTrue(report["stages"]["fluss_dist"]["fallback_used"])


if __name__ == "__main__":
    unittest.main()
