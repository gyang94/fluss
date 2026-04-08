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

"""CLI entry point."""

from __future__ import annotations

import argparse
import datetime
from pathlib import Path
import sys
import time
from typing import Any

from .builder.maven import UnifiedBuilder, resolve_java_client_jar
from .cluster.client import ClusterClient
from .cluster.compose import ComposeClusterManager
from .collector.metrics import MetricsCollector
from .commands.functional import handle_functional, register_functional_parser
from .commands.perf import handle_perf, register_perf_parser
from .config import RuntimeConfig
from .errors import ConfigError, E2EError, ScenarioFailure
from .reporter import attach_error, finalize_report, new_report, write_report
from .scenarios.registry import list_scenarios, resolve_scenario, validate_requested_options
from .scenarios.tracer import Tracer, TraceConfig


SCENARIO_OPTION_FLAGS = {
    "--rows": "rows",
    "--tables": "tables",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fluss-e2e")
    subparsers = parser.add_subparsers(dest="command")

    register_functional_parser(subparsers)
    register_perf_parser(subparsers)

    return parser


def _add_scenario_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--fluss-image", default=None)
    parser.add_argument("--fluss-source", type=Path, default=None)
    parser.add_argument("--image-name", default="fluss-local")
    parser.add_argument("--image-tag", default="latest")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--tables", type=int, default=5)
    parser.add_argument("--bootstrap-servers", default="localhost:9123")
    parser.add_argument("--timeout", type=int, default=None,
                        help="Cluster startup timeout in seconds")
    parser.add_argument(
        "--trace",
        dest="trace_enabled",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--output")


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)
    setattr(args, "_provided_scenario_options", _provided_scenario_options(raw_argv))
    if args.command is None:
        parser.print_help()
        return 4

    config = RuntimeConfig.discover()
    started = time.monotonic()
    command_name = _command_name(args)
    scenario_name = getattr(args, "scenario", None)

    # Set a default output dir if the user didn't specify one.
    # Pattern: docs/<scenario-or-command>-<YYYYMMDD-HHmmss>
    if not getattr(args, "output", None):
        test_name = scenario_name or command_name or "fluss-e2e"
        args.output = str(_default_output_dir(test_name))

    report = new_report(command_name, scenario_name=scenario_name)

    try:
        if args.command == "functional":
            status, exit_code = handle_functional(args, config, report)
        elif args.command == "perf":
            status, exit_code = handle_perf(args, config, report)
        else:
            raise ConfigError(f"Unsupported command `{args.command}`.")
    except E2EError as error:
        attach_error(report, error)
        if error.section is not None and error.exit_code == 1 and report["scenario"] is None:
            report["scenario"] = error.section
        status = "failed"
        exit_code = error.exit_code
    duration_ms = int((time.monotonic() - started) * 1000)
    finalize_report(report, status=status, exit_code=exit_code, duration_ms=duration_ms)
    write_report(report, _report_output_path(args))
    return exit_code


def _run_full_workflow(args, config: RuntimeConfig, report: dict[str, Any], *, scenario, definition, compose_path=None) -> None:
    if args.build_enabled:
        fluss_source = getattr(args, "fluss_source", None)
        if fluss_source is None:
            raise ConfigError(
                "`--fluss-source` is required when `--build` is enabled.",
                details={"hint": "Provide the path to a Fluss source checkout."},
            )
        builder = UnifiedBuilder(
            config,
            fluss_source,
            image_name=getattr(args, "image_name", "fluss-local"),
            image_tag=getattr(args, "image_tag", "latest"),
        )
        report["build"] = builder.build()
    else:
        try:
            resolve_java_client_jar(config)
        except E2EError as exc:
            raise ConfigError(exc.message, details=exc.details) from exc
    cluster = ComposeClusterManager(
        config,
        compose_file=compose_path,
        compose_profiles=definition.compose_profiles,
        fluss_image=getattr(args, "fluss_image", None),
        fluss_source_dir=getattr(args, "fluss_source", None),
    )
    client = ClusterClient(
        config,
        bootstrap_servers=args.bootstrap_servers,
        command_timeout_s=definition.timeout_override_s,
    )
    metrics_active = "metrics" in definition.compose_profiles or "metrics" in getattr(
        args, "_extra_profiles", ()
    )
    collector = MetricsCollector(config.prometheus_url) if metrics_active else None
    cluster_attempted = False
    try:
        cluster_attempted = True
        report["cluster"] = cluster.up(client)
        _run_scenario(
            args,
            config,
            report,
            scenario=scenario,
            definition=definition,
            client=client,
            cluster_manager=cluster,
        )
    finally:
        if collector is not None:
            try:
                if collector.is_available():
                    collector.scrape()
                    metrics_path = _artifact_dir_for_output(
                        args.output, "run"
                    )
                    if metrics_path is not None:
                        snapshot_path = metrics_path / "metrics-snapshot.json"
                    else:
                        import tempfile

                        snapshot_path = Path(
                            tempfile.mkdtemp(prefix="fluss-e2e-metrics-")
                        ) / "metrics-snapshot.json"
                    report["metrics"] = collector.export_summary(snapshot_path)
                else:
                    report["metrics"] = {
                        "prometheus_url": config.prometheus_url,
                        "status": "unavailable",
                    }
            except Exception:  # noqa: BLE001
                report["metrics"] = {
                    "prometheus_url": config.prometheus_url,
                    "status": "error",
                }
        if cluster_attempted:
            report["logs"] = _best_effort_collect_logs(
                cluster,
                output_path=args.output,
                command_name="run",
            )
            if not args.keep_cluster:
                try:
                    cluster.down()
                except E2EError:
                    pass


def _prepare_scenario(args):
    definition, compose_path = resolve_scenario(args.scenario)
    validate_requested_options(definition, args)
    if not definition.implemented or definition.handler_class is None:
        raise ConfigError(
            f"Scenario `{args.scenario}` is planned but not implemented in this milestone."
        )

    from .scenarios.base import YamlScenario
    from .scenarios.loader import load_scenario as load_yaml_scenario

    if definition.handler_class is YamlScenario:
        yaml_config, _compose_path = load_yaml_scenario(args.scenario)
        scenario = YamlScenario(yaml_config, compose_path=compose_path)
    else:
        scenario = definition.handler_class()
    scenario.validate_args(args)
    return definition, scenario, compose_path


def _run_scenario(
    args,
    config: RuntimeConfig,
    report: dict[str, Any],
    *,
    scenario=None,
    definition=None,
    client: ClusterClient | None = None,
    cluster_manager: ComposeClusterManager | None = None,
) -> None:
    if scenario is None or definition is None:
        definition, scenario, _compose_path = _prepare_scenario(args)
    if client is None:
        client = ClusterClient(
            config,
            bootstrap_servers=args.bootstrap_servers,
            command_timeout_s=definition.timeout_override_s,
        )

    tracer = _create_tracer(args)

    report["scenario"] = scenario.run(
        client=client,
        args=args,
        cluster_manager=cluster_manager,
        tracer=tracer,
    )

    if tracer.trace_dir is not None:
        report["trace_dir"] = str(tracer.trace_dir)


def _create_tracer(args) -> Tracer:
    trace_enabled = getattr(args, "trace_enabled", True)
    scenario_name = getattr(args, "scenario", None)
    if not trace_enabled or scenario_name is None:
        return Tracer(TraceConfig(enabled=False))

    from .scenarios.loader import scenarios_dir

    scenario_dir = scenarios_dir() / scenario_name
    return Tracer(TraceConfig(enabled=True, base_dir=scenario_dir))


def _command_name(args) -> str:
    if args.command == "functional":
        sub = getattr(args, "functional_command", None)
        if sub == "cluster":
            return f"functional cluster {args.cluster_command}"
        return f"functional {sub}" if sub else "functional"
    if args.command == "perf":
        sub = getattr(args, "perf_command", None)
        if sub == "cluster":
            return f"perf cluster {args.cluster_command}"
        return f"perf {sub}" if sub else "perf"
    return args.command


def _provided_scenario_options(argv: list[str]) -> set[str]:
    provided: set[str] = set()
    for token in argv:
        if token in SCENARIO_OPTION_FLAGS:
            provided.add(SCENARIO_OPTION_FLAGS[token])
            continue
        for flag, option_name in SCENARIO_OPTION_FLAGS.items():
            if token.startswith(f"{flag}="):
                provided.add(option_name)
                break
    return provided


def _artifact_dir_for_output(output_path: str | None, command_name: str) -> Path | None:
    if output_path is None:
        return None
    output = Path(output_path)
    return output.parent / f"{output.stem or command_name}-artifacts"


def _report_output_path(args) -> str | None:
    report_output = getattr(args, "_report_output_path", None)
    if report_output is not None:
        return report_output
    return getattr(args, "output", None)


def _parse_perf_param_overrides(values: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ConfigError(
                f"Invalid perf override `{item}`.",
                details={"override": item, "hint": "Use key=value."},
            )
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ConfigError(
                f"Invalid perf override `{item}`.",
                details={"override": item, "hint": "Override key must not be empty."},
            )
        overrides[key] = value
    return overrides


def _default_output_dir(test_name: str) -> Path:
    """Return docs/<test-name>-<YYYYMMDD-HHmmss> relative to CWD."""
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    return Path("docs") / f"{test_name}-{ts}"


def _resolve_perf_output_dir(args, scenario) -> Path:
    if args.output:
        return Path(args.output)
    if scenario.report is not None and scenario.report.output_dir:
        return Path(scenario.report.output_dir)
    return _default_output_dir(scenario.meta.name if scenario.meta else "perf")


def _resolve_perf_formats(args, scenario) -> tuple[str, ...]:
    if args.formats:
        return tuple(
            item.strip()
            for item in args.formats.split(",")
            if item.strip()
        )
    if scenario.report is not None and scenario.report.formats:
        return tuple(scenario.report.formats)
    return ("json", "html", "csv")


def _best_effort_collect_logs(
    cluster: ComposeClusterManager,
    *,
    output_path: str | None,
    command_name: str,
) -> dict[str, Any]:
    try:
        return cluster.logs(
            artifact_dir=_artifact_dir_for_output(output_path, command_name),
            strict=False,
        )
    except E2EError as exc:
        return {
            "status": "failed",
            "error": exc.to_error_block(),
        }


if __name__ == "__main__":
    raise SystemExit(main())
