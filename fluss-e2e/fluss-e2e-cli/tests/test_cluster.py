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

"""Tests for cluster helpers."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from fluss_e2e.cluster.client import ClusterClient
from fluss_e2e.cluster.compose import ComposeClusterManager, ServicePort
from fluss_e2e.config import RuntimeConfig
from fluss_e2e.errors import ClusterError, ConfigError, ServiceCrashLoopError
from fluss_e2e.subprocess_runner import CommandResult, CommandTimeout


class ComposeClusterManagerTest(unittest.TestCase):
    def test_runtime_dockerfile_sources_bin_and_conf_from_dist_resources(self) -> None:
        config = RuntimeConfig.discover()
        dockerfile = config.runtime_dockerfile.read_text(encoding="utf-8")

        self.assertIn("fluss-dist/src/main/resources/bin/", dockerfile)
        self.assertIn("fluss-dist/src/main/resources/conf/", dockerfile)
        self.assertIn("fluss-dist/src/main/resources/server.yaml", dockerfile)
        self.assertNotIn("build-target/bin", dockerfile)
        self.assertNotIn("build-target/conf", dockerfile)
        self.assertNotIn("apt-get", dockerfile)

    @unittest.skipUnless(
        shutil.which("envsubst") and os.uname().sysname == "Linux",
        "Requires Linux sed -i semantics and envsubst in PATH",
    )
    def test_entrypoint_falls_back_when_envsubst_is_missing(self) -> None:
        config = RuntimeConfig.discover()
        # In monorepo: <repo>/docker/fluss/docker-entrypoint.sh
        # In dev workspace: <workspace>/docs/references/code/fluss/docker/fluss/docker-entrypoint.sh
        entrypoint = config.repo_root / "docker/fluss/docker-entrypoint.sh"
        if not entrypoint.is_file():
            entrypoint = config.repo_root / "docs/references/code/fluss/docker/fluss/docker-entrypoint.sh"

        with tempfile.TemporaryDirectory() as tempdir:
            fluss_home = Path(tempdir) / "fluss-home"
            conf_dir = fluss_home / "conf"
            conf_dir.mkdir(parents=True)
            conf_file = conf_dir / "server.yaml"
            conf_file.write_text(
                "bind.listeners: LEGACY://localhost:9123\n"
                "remote.data.dir: ${REMOTE_DIR}\n"
                "unset.value: $UNSET_VALUE\n",
                encoding="utf-8",
            )

            path_dir = Path(tempdir) / "path-bin"
            path_dir.mkdir()
            for command in ("basename", "mv", "perl", "rm", "sed"):
                target = shutil.which(command)
                self.assertIsNotNone(target)
                os.symlink(target, path_dir / command)

            result = subprocess.run(
                ["/bin/bash", str(entrypoint), "help"],
                capture_output=True,
                text=True,
                check=False,
                env={
                    "PATH": str(path_dir),
                    "FLUSS_HOME": str(fluss_home),
                    "REMOTE_DIR": "/tmp/remote-data",
                    "BIND_LISTENERS": "CLIENT://localhost:9123",
                    "FLUSS_PROPERTIES": "bind.listeners: ${BIND_LISTENERS}\n",
                },
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)
            rendered = conf_file.read_text(encoding="utf-8")
            self.assertNotIn("LEGACY://localhost:9123", rendered)
            self.assertIn("bind.listeners: CLIENT://localhost:9123", rendered)
            self.assertIn("remote.data.dir: /tmp/remote-data", rendered)
            self.assertIn("unset.value: ", rendered)
            self.assertNotIn("${REMOTE_DIR}", rendered)
            self.assertNotIn("$UNSET_VALUE", rendered)

    def test_up_does_not_force_compose_rebuild(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)

            with patch(
                "fluss_e2e.cluster.compose.run_command",
                return_value=CommandResult(["docker", "compose"], 0, "", "", 25),
            ) as run_command_mock, patch.object(
                ComposeClusterManager,
                "wait_until_ready",
                return_value=None,
            ):
                section = manager.up(client=object())

            command = run_command_mock.call_args.args[0]
            self.assertNotIn("--build", command)
            self.assertEqual(section["status"], "running")
            self.assertEqual(
                section["services"]["coordinator-server"]["status"],
                "healthy",
            )

    def test_service_status_parses_compose_ps_json(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)
            payload = (
                '[{"Service":"tablet-server-0","State":"running","Status":"Up 5 seconds",'
                '"Name":"docker-tablet-server-0-1","Health":"healthy","ExitCode":0}]'
            )

            with patch(
                "fluss_e2e.cluster.compose.run_command",
                return_value=CommandResult(["docker", "compose", "ps"], 0, payload, "", 12),
            ):
                status = manager.service_status("tablet-server-0")

            self.assertEqual(status["state"], "running")
            self.assertEqual(status["container_name"], "docker-tablet-server-0-1")
            self.assertEqual(status["health"], "healthy")
            self.assertEqual(status["exit_code"], 0)

    def test_custom_service_ports_and_env_overrides_are_used(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(
                config,
                service_ports=(
                    ServicePort("zookeeper", 2181),
                    ServicePort("coordinator-server", 9123),
                    ServicePort("tablet-server-3", 9127),
                ),
                controllable_services=frozenset({"tablet-server-3"}),
                env_overrides={"PERF_SCENARIO_FILE": "/tmp/scenario.yaml"},
            )
            payload = (
                '[{"Service":"tablet-server-3","State":"running","Status":"Up 3 seconds",'
                '"Name":"docker-tablet-server-3-1","ExitCode":0}]'
            )

            with patch(
                "fluss_e2e.cluster.compose.run_command",
                return_value=CommandResult(["docker", "compose", "ps"], 0, payload, "", 12),
            ):
                status = manager.service_status("tablet-server-3")

            section = manager._cluster_section(
                cluster_status="running",
                service_status="healthy",
                startup_duration_ms=25,
            )
            self.assertEqual(status["service"], "tablet-server-3")
            self.assertEqual(section["services"]["tablet-server-3"]["port"], 9127)
            self.assertEqual(
                manager._compose_env()["PERF_SCENARIO_FILE"],
                "/tmp/scenario.yaml",
            )

    def test_stop_service_uses_compose_stop_and_waits_for_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)
            responses = [
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"running","Status":"Up","Name":"ts0"}]',
                    "",
                    5,
                ),
                CommandResult(["docker", "compose", "stop"], 0, "", "", 20),
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"exited","Status":"Exited (0) 1 second ago","Name":"ts0","ExitCode":0}]',
                    "",
                    5,
                ),
            ]

            with patch(
                "fluss_e2e.cluster.compose.run_command",
                side_effect=responses,
            ) as run_command_mock:
                section = manager.stop_service("tablet-server-0")

            self.assertEqual(section["status"], "stopped")
            self.assertEqual(section["after"]["state"], "exited")
            self.assertGreaterEqual(section["duration_ms"], 0)
            self.assertIn("stop", run_command_mock.call_args_list[1].args[0])

    def test_restart_service_waits_for_cluster_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)
            responses = [
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"exited","Status":"Exited (0)","Name":"ts0","ExitCode":0}]',
                    "",
                    5,
                ),
                CommandResult(["docker", "compose", "restart"], 0, "", "", 20),
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"running","Status":"Up 2 seconds","Name":"ts0","Health":"healthy","ExitCode":0}]',
                    "",
                    5,
                ),
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"running","Status":"Up 4 seconds","Name":"ts0","Health":"healthy","ExitCode":0}]',
                    "",
                    5,
                ),
            ]

            with patch(
                "fluss_e2e.cluster.compose.run_command",
                side_effect=responses,
            ), patch.object(
                ComposeClusterManager,
                "wait_until_ready",
                return_value=None,
            ) as wait_until_ready_mock:
                section = manager.restart_service("tablet-server-0", client=object())

            self.assertEqual(section["status"], "running")
            self.assertGreaterEqual(section["duration_ms"], 0)
            wait_until_ready_mock.assert_called_once()

    def test_start_service_waits_for_cluster_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)
            responses = [
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"exited","Status":"Exited (0)","Name":"ts0","ExitCode":0}]',
                    "",
                    5,
                ),
                CommandResult(["docker", "compose", "start"], 0, "", "", 20),
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"running","Status":"Up 2 seconds","Name":"ts0","Health":"healthy","ExitCode":0}]',
                    "",
                    5,
                ),
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"running","Status":"Up 4 seconds","Name":"ts0","Health":"healthy","ExitCode":0}]',
                    "",
                    5,
                ),
            ]

            with patch(
                "fluss_e2e.cluster.compose.run_command",
                side_effect=responses,
            ), patch.object(
                ComposeClusterManager,
                "wait_until_ready",
                return_value=None,
            ) as wait_until_ready_mock:
                section = manager.start_service("tablet-server-0", client=object())

            self.assertEqual(section["status"], "running")
            self.assertEqual(section["action"], "start")
            self.assertGreaterEqual(section["duration_ms"], 0)
            wait_until_ready_mock.assert_called_once()

    def test_restart_service_surfaces_crash_loop_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)
            responses = [
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"running","Status":"Up","Name":"ts0"}]',
                    "",
                    5,
                ),
                CommandResult(["docker", "compose", "restart"], 0, "", "", 20),
                CommandResult(
                    ["docker", "compose", "ps"],
                    0,
                    '[{"Service":"tablet-server-0","State":"exited","Status":"Exited (1) 1 second ago","Name":"ts0","ExitCode":1}]',
                    "",
                    5,
                ),
            ]

            with patch(
                "fluss_e2e.cluster.compose.run_command",
                side_effect=responses,
            ):
                with self.assertRaises(ServiceCrashLoopError):
                    manager.restart_service("tablet-server-0", client=object())

    def test_service_control_rejects_unknown_target(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)

            with self.assertRaises(ConfigError):
                manager.stop_service("coordinator-server")

    def test_logs_persist_per_service_artifacts_and_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)
            artifact_dir = Path(tempdir) / "artifacts"

            def fake_run_command(command, *, cwd, env, timeout_s):
                service = command[-1]
                stdout = (
                    f"{service} started\n"
                    "WARN retrying request\n"
                    "ERROR failed to connect\n"
                )
                return CommandResult(command, 0, stdout, "", 25)

            with patch("fluss_e2e.cluster.compose.run_command", side_effect=fake_run_command):
                logs = manager.logs(
                    services=["coordinator-server", "tablet-server-0"],
                    artifact_dir=artifact_dir,
                )

            self.assertEqual(logs["status"], "captured")
            self.assertEqual(
                logs["services"]["coordinator-server"]["warn_count"],
                1,
            )
            self.assertEqual(
                logs["services"]["tablet-server-0"]["error_count"],
                1,
            )
            self.assertTrue((artifact_dir / "coordinator-server.log").exists())
            self.assertTrue((artifact_dir / "tablet-server-0.log").exists())

    def test_logs_return_partial_capture_when_best_effort(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)
            artifact_dir = Path(tempdir) / "artifacts"

            def fake_run_command(command, *, cwd, env, timeout_s):
                service = command[-1]
                if service == "tablet-server-0":
                    return CommandResult(command, 1, "", "missing container", 25)
                return CommandResult(command, 0, "INFO started\n", "", 25)

            with patch("fluss_e2e.cluster.compose.run_command", side_effect=fake_run_command):
                logs = manager.logs(
                    services=["coordinator-server", "tablet-server-0"],
                    artifact_dir=artifact_dir,
                    strict=False,
                )

            self.assertEqual(logs["status"], "partial")
            self.assertEqual(
                logs["services"]["coordinator-server"]["status"],
                "captured",
            )
            self.assertEqual(
                logs["services"]["tablet-server-0"]["status"],
                "failed",
            )
            self.assertEqual(
                logs["services"]["tablet-server-0"]["error"]["returncode"],
                1,
            )

    def test_logs_return_failed_when_best_effort_captures_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)
            artifact_dir = Path(tempdir) / "artifacts"

            with patch(
                "fluss_e2e.cluster.compose.run_command",
                return_value=CommandResult(
                    ["docker", "compose"],
                    1,
                    "",
                    "missing container",
                    25,
                ),
            ):
                logs = manager.logs(
                    services=["coordinator-server", "tablet-server-0"],
                    artifact_dir=artifact_dir,
                    strict=False,
                )

            self.assertEqual(logs["status"], "failed")
            self.assertEqual(
                logs["services"]["coordinator-server"]["status"],
                "failed",
            )
            self.assertEqual(
                logs["services"]["tablet-server-0"]["status"],
                "failed",
            )
            self.assertEqual(logs["artifact_dir"], str(artifact_dir))

    def test_logs_raise_cluster_error_in_strict_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            config = RuntimeConfig.discover(Path(tempdir))
            manager = ComposeClusterManager(config)

            with patch(
                "fluss_e2e.cluster.compose.run_command",
                return_value=CommandResult(
                    ["docker", "compose"],
                    1,
                    "",
                    "missing container",
                    25,
                ),
            ):
                with self.assertRaises(ClusterError):
                    manager.logs(services=["coordinator-server"])


class ClusterClientTest(unittest.TestCase):
    def test_create_table_passes_replication_factor_option(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            jar_path = repo_root / "client.jar"
            jar_path.write_text("")
            config = RuntimeConfig.discover(repo_root)
            client = ClusterClient(config, jar_path=jar_path)

            with patch.object(client, "_invoke", return_value={"ok": True}) as invoke_mock:
                client.create_table("orders", replication_factor=2)

            command = invoke_mock.call_args.args[0]
            self.assertIn("--replication-factor", command)
            self.assertIn("2", command)

    def test_write_rows_passes_start_id_option(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            jar_path = repo_root / "client.jar"
            jar_path.write_text("")
            config = RuntimeConfig.discover(repo_root)
            client = ClusterClient(config, jar_path=jar_path)

            with patch.object(client, "_invoke", return_value={"ok": True}) as invoke_mock:
                client.write_rows("orders", 10, start_id=50)

            command = invoke_mock.call_args.args[0]
            self.assertIn("--start-id", command)
            self.assertIn("50", command)

    def test_rebalance_invokes_admin_rebalance_command(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            jar_path = repo_root / "client.jar"
            jar_path.write_text("")
            config = RuntimeConfig.discover(repo_root)
            client = ClusterClient(config, jar_path=jar_path)

            with patch.object(client, "_invoke", return_value={"rebalance_id": "rb-1"}) as invoke_mock:
                payload = client.rebalance(["leader_distribution", "replica_distribution"])

            self.assertEqual(payload["rebalance_id"], "rb-1")
            command = invoke_mock.call_args.args[0]
            self.assertEqual(command[:2], ["admin", "rebalance"])
            self.assertIn("--goals", command)

    def test_list_rebalance_progress_invokes_admin_list_command(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            jar_path = repo_root / "client.jar"
            jar_path.write_text("")
            config = RuntimeConfig.discover(repo_root)
            client = ClusterClient(config, jar_path=jar_path)

            with patch.object(
                client,
                "_invoke",
                return_value={"present": True, "rebalance_id": "rb-1"},
            ) as invoke_mock:
                payload = client.list_rebalance_progress(rebalance_id="rb-1")

            self.assertTrue(payload["present"])
            command = invoke_mock.call_args.args[0]
            self.assertEqual(command[:2], ["admin", "list-rebalance-progress"])
            self.assertIn("--rebalance-id", command)

    def test_client_accepts_json_after_stdout_noise(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            jar_path = repo_root / "client.jar"
            jar_path.write_text("")
            config = RuntimeConfig.discover(repo_root)
            client = ClusterClient(config, jar_path=jar_path)

            with patch(
                "fluss_e2e.cluster.client.run_command",
                return_value=CommandResult(
                    ["java", "-jar", str(jar_path)],
                    0,
                    'WARNING: noisy stdout\n{"ok": true, "server_count": 3}\n',
                    "",
                    25,
                ),
            ):
                payload = client.ping()

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["server_count"], 3)

    def test_client_uses_json_error_payload_after_stdout_noise(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            jar_path = repo_root / "client.jar"
            jar_path.write_text("")
            config = RuntimeConfig.discover(repo_root)
            client = ClusterClient(config, jar_path=jar_path)

            with patch(
                "fluss_e2e.cluster.client.run_command",
                return_value=CommandResult(
                    ["java", "-jar", str(jar_path)],
                    1,
                    'WARNING: noisy stdout\n{"ok": false, "error": {"message": "bridge failed"}}\n',
                    "",
                    25,
                ),
            ):
                with self.assertRaises(ClusterError) as caught:
                    client.ping()

            self.assertIn("bridge failed", str(caught.exception))

    def test_client_timeout_maps_to_cluster_error(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            jar_path = repo_root / "client.jar"
            jar_path.write_text("")
            config = RuntimeConfig.discover(repo_root)
            client = ClusterClient(config, jar_path=jar_path)

            with patch(
                "fluss_e2e.cluster.client.run_command",
                side_effect=CommandTimeout(["java", "-jar", str(jar_path)], 30),
            ):
                with self.assertRaises(ClusterError) as caught:
                    client.ping()

            self.assertEqual(caught.exception.details["timeout_seconds"], 30)


class ComposeFileOverrideTest(unittest.TestCase):
    def test_compose_command_uses_override_file(self) -> None:
        config = RuntimeConfig.discover()
        override = Path("/tmp/test-compose.yml")
        manager = ComposeClusterManager(config, compose_file=override)
        command = manager._compose_command("up", "-d")
        self.assertIn(str(override), command)
        self.assertNotIn(str(config.compose_file), command)
