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

"""Functional E2E subcommand group."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from ..builder.maven import UnifiedBuilder
from ..cluster.client import ClusterClient
from ..cluster.compose import ComposeClusterManager
from ..config import RuntimeConfig
from ..errors import ConfigError
from ..scenarios.registry import list_scenarios


def register_functional_parser(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``functional`` subcommand group."""
    functional = subparsers.add_parser("functional")
    functional_sub = functional.add_subparsers(dest="functional_command")

    # functional build
    build = functional_sub.add_parser("build")
    build.add_argument("source_dir", type=Path)
    build.add_argument("--image-name", default="fluss-local")
    build.add_argument("--image-tag", default="latest")
    build.add_argument("--output")

    # functional cluster
    cluster = functional_sub.add_parser("cluster")
    cluster_sub = cluster.add_subparsers(dest="cluster_command")
    cluster_up = cluster_sub.add_parser("up")
    cluster_up.add_argument("scenario", nargs="?", default=None)
    cluster_up.add_argument("--bootstrap-servers", default="localhost:9123")
    cluster_up.add_argument("--fluss-image", default="fluss-local:latest")
    cluster_up.add_argument("--fluss-source", type=Path, default=None)
    cluster_up.add_argument(
        "--timeout", type=int, default=None,
        help="Startup timeout in seconds (default: FLUSS_E2E_CLUSTER_STARTUP_TIMEOUT or 120)",
    )
    cluster_up.add_argument("--output")
    cluster_down = cluster_sub.add_parser("down")
    cluster_down.add_argument("scenario", nargs="?", default=None)
    cluster_down.add_argument("--output")
    cluster_logs = cluster_sub.add_parser("logs")
    cluster_logs.add_argument("scenario", nargs="?", default=None)
    cluster_logs.add_argument("--output")
    cluster_status = cluster_sub.add_parser("status")
    cluster_status.add_argument("scenario", nargs="?", default=None)
    cluster_status.add_argument("--service", required=True)
    cluster_status.add_argument("--output")
    cluster_stop = cluster_sub.add_parser("stop")
    cluster_stop.add_argument("scenario", nargs="?", default=None)
    cluster_stop.add_argument("--service", required=True)
    cluster_stop.add_argument("--output")
    cluster_start = cluster_sub.add_parser("start")
    cluster_start.add_argument("scenario", nargs="?", default=None)
    cluster_start.add_argument("--service", required=True)
    cluster_start.add_argument("--output")
    cluster_restart = cluster_sub.add_parser("restart")
    cluster_restart.add_argument("scenario", nargs="?", default=None)
    cluster_restart.add_argument("--service", required=True)
    cluster_restart.add_argument("--bootstrap-servers", default="localhost:9123")
    cluster_restart.add_argument("--output")

    # functional list
    func_list = functional_sub.add_parser("list")
    func_list.add_argument("--output")

    # functional test
    from ..cli import _add_scenario_arguments

    test = functional_sub.add_parser("test")
    _add_scenario_arguments(test)
    test.add_argument(
        "--build",
        dest="build_enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    test.add_argument("--keep-cluster", action="store_true", default=False)


def handle_functional(
    args,
    config: RuntimeConfig,
    report: dict[str, Any],
) -> tuple[str, int]:
    """Dispatch functional subcommands. Returns ``(status, exit_code)``."""
    cmd = args.functional_command
    if cmd == "build":
        builder = UnifiedBuilder(
            config,
            args.source_dir,
            image_name=args.image_name,
            image_tag=args.image_tag,
        )
        report["build"] = builder.build()
        return "passed", 0
    elif cmd == "cluster":
        return _handle_cluster(args, config, report)
    elif cmd == "list":
        report["available_scenarios"] = list_scenarios()
        return "passed", 0
    elif cmd == "test":
        from ..cli import _prepare_scenario, _run_full_workflow

        definition, scenario, compose_path = _prepare_scenario(args)
        _run_full_workflow(
            args, config, report,
            scenario=scenario,
            definition=definition,
            compose_path=compose_path,
        )
        return "passed", 0
    else:
        raise ConfigError("Missing functional subcommand.")


def _handle_cluster(
    args,
    config: RuntimeConfig,
    report: dict[str, Any],
) -> tuple[str, int]:
    from ..cli import _artifact_dir_for_output

    compose_path = None
    scenario_name = getattr(args, "scenario", None)
    if scenario_name:
        from ..scenarios.loader import load_scenario as load_yaml_scenario

        _, compose_path = load_yaml_scenario(scenario_name)
    cluster = ComposeClusterManager(
        config,
        compose_file=compose_path,
        fluss_image=getattr(args, "fluss_image", None),
        fluss_source_dir=getattr(args, "fluss_source", None),
    )
    if args.cluster_command == "up":
        client = ClusterClient(config, bootstrap_servers=args.bootstrap_servers)
        timeout = getattr(args, "timeout", None)
        report["cluster"] = cluster.up(client, timeout_s=timeout)
    elif args.cluster_command == "down":
        report["cluster"] = cluster.down()
    elif args.cluster_command == "logs":
        log_dir = _artifact_dir_for_output(args.output, "cluster-logs")
        if log_dir is None and scenario_name:
            from ..scenarios.loader import scenarios_dir

            log_dir = scenarios_dir() / scenario_name / "logs"
        report["logs"] = cluster.logs(artifact_dir=log_dir)
    elif args.cluster_command == "status":
        report["cluster"] = {
            "status": "passed",
            "service": args.service,
            "service_state": cluster.service_status(args.service),
        }
    elif args.cluster_command == "stop":
        report["cluster"] = cluster.stop_service(args.service)
    elif args.cluster_command == "start":
        client = ClusterClient(config)
        report["cluster"] = cluster.start_service(args.service, client)
    elif args.cluster_command == "restart":
        client = ClusterClient(config, bootstrap_servers=args.bootstrap_servers)
        report["cluster"] = cluster.restart_service(args.service, client)
    else:
        raise ConfigError("Missing cluster subcommand.")
    return "passed", 0
