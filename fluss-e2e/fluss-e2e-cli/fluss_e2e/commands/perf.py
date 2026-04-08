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

"""Perf E2E subcommand group."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from ..config import RuntimeConfig
from ..errors import ConfigError


def register_perf_parser(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``perf`` subcommand group."""
    perf = subparsers.add_parser("perf")
    perf_sub = perf.add_subparsers(dest="perf_command")

    # perf build
    perf_build = perf_sub.add_parser("build")
    perf_build.add_argument("source_dir", type=Path)
    perf_build.add_argument("--image-name", default="fluss-local")
    perf_build.add_argument("--image-tag", default="latest")
    perf_build.add_argument("--perf-client-image", default=None)
    perf_build.add_argument("--output")

    # perf cluster
    perf_cluster = perf_sub.add_parser("cluster")
    perf_cluster_sub = perf_cluster.add_subparsers(dest="cluster_command")
    perf_cluster_up = perf_cluster_sub.add_parser("up")
    perf_cluster_up.add_argument("--fluss-image", default=None)
    perf_cluster_up.add_argument("--bootstrap-servers", default="localhost:9123")
    perf_cluster_up.add_argument(
        "--timeout", type=int, default=None,
        help="Startup timeout in seconds",
    )
    perf_cluster_up.add_argument("--output")
    perf_cluster_down = perf_cluster_sub.add_parser("down")
    perf_cluster_down.add_argument("--output")
    perf_cluster_logs = perf_cluster_sub.add_parser("logs")
    perf_cluster_logs.add_argument("--output")

    # perf list
    perf_list = perf_sub.add_parser("list")
    perf_list.add_argument("--output")

    # perf test
    perf_test = perf_sub.add_parser("test")
    perf_test.add_argument("--scenario", required=True)
    perf_test.add_argument("--fluss-image", default=None)
    perf_test.add_argument("--perf-client-image", default=None)
    perf_test.add_argument("--output", default=None)
    perf_test.add_argument(
        "--formats", default=None,
        help="Report formats (comma-separated)",
    )
    perf_test.add_argument(
        "--param",
        dest="perf_params",
        action="append",
        default=[],
        help="Override perf scenario values using key=value",
    )
    perf_test.add_argument("--keep-cluster", action="store_true", default=False)
    perf_test.add_argument("--grafana-port", type=int, default=3000)
    perf_test.add_argument(
        "--timeout", type=int, default=None,
        help="Overall timeout in seconds",
    )


def handle_perf(
    args,
    config: RuntimeConfig,
    report: dict[str, Any],
) -> tuple[str, int]:
    """Dispatch perf subcommands. Returns ``(status, exit_code)``."""
    cmd = args.perf_command
    if cmd == "build":
        return _handle_perf_build(args, config, report)
    elif cmd == "cluster":
        return _handle_perf_cluster(args, config, report)
    elif cmd == "list":
        from ..perf.engine import list_perf_scenarios

        report["perf_scenarios"] = list_perf_scenarios()
        return "passed", 0
    elif cmd == "test":
        return _handle_perf_test(args, config, report)
    else:
        raise ConfigError("Missing perf subcommand.")


def _handle_perf_build(
    args,
    config: RuntimeConfig,
    report: dict[str, Any],
) -> tuple[str, int]:
    from ..builder.maven import UnifiedBuilder

    builder = UnifiedBuilder(
        config,
        args.source_dir,
        image_name=args.image_name,
        image_tag=args.image_tag,
    )
    report["build"] = builder.build()
    return "passed", 0


def _handle_perf_cluster(
    args,
    config: RuntimeConfig,
    report: dict[str, Any],
) -> tuple[str, int]:
    from ..cli import _artifact_dir_for_output
    from ..cluster.client import ClusterClient
    from ..cluster.compose import ComposeClusterManager

    cluster = ComposeClusterManager(
        config,
        compose_file=config.perf_compose_file,
        fluss_image=getattr(args, "fluss_image", None),
    )
    if args.cluster_command == "up":
        client = ClusterClient(
            config,
            bootstrap_servers=getattr(args, "bootstrap_servers", "localhost:9123"),
        )
        timeout = getattr(args, "timeout", None)
        report["cluster"] = cluster.up(client, timeout_s=timeout)
    elif args.cluster_command == "down":
        report["cluster"] = cluster.down()
    elif args.cluster_command == "logs":
        log_dir = _artifact_dir_for_output(
            getattr(args, "output", None), "perf-cluster-logs",
        )
        report["logs"] = cluster.logs(artifact_dir=log_dir)
    else:
        raise ConfigError("Missing perf cluster subcommand.")
    return "passed", 0


def _handle_perf_test(
    args,
    config: RuntimeConfig,
    report: dict[str, Any],
) -> tuple[str, int]:
    from ..cli import (
        _parse_perf_param_overrides,
        _resolve_perf_formats,
        _resolve_perf_output_dir,
    )
    from ..perf.engine import PerfEngine
    from ..perf.loader import load_perf_scenario
    from ..perf.report import evaluate_thresholds, generate_reports

    scenario_name = args.scenario
    scenario = load_perf_scenario(
        scenario_name,
        overrides=_parse_perf_param_overrides(args.perf_params),
    )
    output_dir = _resolve_perf_output_dir(args, scenario)
    if args.output:
        setattr(args, "_report_output_path", str(output_dir / "cli-report.json"))
    formats = _resolve_perf_formats(args, scenario)

    engine = PerfEngine(
        config,
        fluss_image=args.fluss_image,
        keep_cluster=args.keep_cluster,
        grafana_port=args.grafana_port,
        timeout_s=args.timeout,
    )
    result = engine.run_scenario(
        scenario_name,
        scenario=scenario,
        output_dir=output_dir,
        perf_client_image=args.perf_client_image,
    )
    report["perf"] = result.to_dict()

    generated = generate_reports(
        result, scenario, output_dir=output_dir, formats=formats,
    )
    report["perf_reports"] = generated
    threshold_result = (
        evaluate_thresholds(result, scenario) if scenario.thresholds else None
    )
    if threshold_result is not None:
        report["perf_thresholds"] = threshold_result

    if result.status == "passed" and (
        threshold_result is None or threshold_result.get("status") != "failed"
    ):
        return "passed", 0
    else:
        return "failed", 1
