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

"""Perf report generation — JSON, CSV, and HTML formats."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from string import Template
from typing import Any

from .engine import PerfRunResult
from .schema import PerfScenarioConfig


def generate_reports(
    result: PerfRunResult,
    scenario: PerfScenarioConfig,
    *,
    output_dir: Path,
    formats: tuple[str, ...] = ("json", "html", "csv"),
) -> dict[str, str]:
    """Generate perf reports in requested formats. Returns a map of format -> file path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    generated: dict[str, str] = {}

    if "json" in formats:
        path = output_dir / "summary.json"
        _write_json_report(result, scenario, path)
        generated["json"] = str(path)

    if "csv" in formats:
        path = output_dir / "timeseries.csv"
        _write_csv_report(result, path)
        generated["csv"] = str(path)

    if "html" in formats:
        path = output_dir / "report.html"
        _write_html_report(result, scenario, path)
        generated["html"] = str(path)

    return generated


def _write_json_report(
    result: PerfRunResult,
    scenario: PerfScenarioConfig,
    path: Path,
) -> None:
    """Write detailed JSON summary report."""
    report: dict[str, Any] = {
        "version": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "scenario": {
            "name": scenario.meta.name,
            "description": scenario.meta.description,
            "tags": list(scenario.meta.tags),
            "table": scenario.table.name,
        },
        "status": result.status,
        "elapsed_ms": result.elapsed_ms,
        "phases": result.phases,
    }
    if result.error:
        report["error"] = result.error
    if result.client_results:
        report["client_results"] = result.client_results
    if result.metrics:
        report["metrics"] = result.metrics

    # Add threshold evaluation if configured
    if scenario.thresholds:
        report["thresholds"] = evaluate_thresholds(result, scenario)

    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def _write_csv_report(result: PerfRunResult, path: Path) -> None:
    """Write Prometheus time-series results as CSV."""
    if not result.metrics:
        path.write_text("# No Prometheus time-series metrics\n", encoding="utf-8")
        return

    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=(
            "metric",
            "series",
            "timestamp",
            "iso_timestamp",
            "value",
            "labels",
        ),
    )
    writer.writeheader()
    for metric_name, metric_payload in sorted(result.metrics.items()):
        series_list = metric_payload.get("series", [])
        if not isinstance(series_list, list):
            continue
        for series_index, series in enumerate(series_list):
            labels = json.dumps(series.get("metric", {}), sort_keys=True)
            for point in series.get("points", []):
                timestamp = point.get("timestamp")
                value = point.get("value")
                if timestamp is None or value is None:
                    continue
                writer.writerow(
                    {
                        "metric": metric_name,
                        "series": series_index,
                        "timestamp": timestamp,
                        "iso_timestamp": datetime.fromtimestamp(
                            float(timestamp), tz=timezone.utc
                        )
                        .isoformat()
                        .replace("+00:00", "Z"),
                        "value": value,
                        "labels": labels,
                    }
                )
    path.write_text(buf.getvalue(), encoding="utf-8")


def _write_html_report(
    result: PerfRunResult,
    scenario: PerfScenarioConfig,
    path: Path,
) -> None:
    """Write HTML report with Chart.js visualization."""
    template_path = Path(__file__).parent / "templates" / "report.html"
    if template_path.exists():
        template_str = template_path.read_text(encoding="utf-8")
    else:
        template_str = _FALLBACK_HTML_TEMPLATE

    # Prepare template data
    phases_json = json.dumps(result.phases, indent=2)
    timeseries_json = json.dumps(result.metrics, indent=2)
    threshold_results = evaluate_thresholds(result, scenario) if scenario.thresholds else None
    threshold_json = json.dumps(threshold_results, indent=2) if threshold_results else "null"

    html = Template(template_str).safe_substitute(
        scenario_name=scenario.meta.name,
        scenario_description=scenario.meta.description or "",
        status=result.status,
        elapsed_ms=result.elapsed_ms,
        phases_json=phases_json,
        timeseries_json=timeseries_json,
        threshold_json=threshold_json,
        timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )
    path.write_text(html, encoding="utf-8")


def evaluate_thresholds(
    result: PerfRunResult,
    scenario: PerfScenarioConfig,
) -> dict[str, Any]:
    """Evaluate configured thresholds against results."""
    if not scenario.thresholds:
        return {"status": "skipped", "checks": []}

    checks: list[dict[str, Any]] = []
    has_failure = False
    has_missing = False

    for phase in result.phases:
        phase_name = phase.get("phase", "unknown")
        ops_per_sec_str = phase.get("opsPerSec", "0")
        p99_ms_str = phase.get("p99Ms", "0")

        try:
            ops_per_sec = float(ops_per_sec_str)
        except (ValueError, TypeError):
            ops_per_sec = 0.0
        try:
            p99_ms = float(p99_ms_str)
        except (ValueError, TypeError):
            p99_ms = 0.0

        # write_tps threshold only applies to write phases
        if phase_name == "write":
            if scenario.thresholds.write_tps and scenario.thresholds.write_tps.min is not None:
                passed = ops_per_sec >= scenario.thresholds.write_tps.min
                if not passed:
                    has_failure = True
                checks.append({
                    "phase": phase_name,
                    "metric": "write_tps",
                    "threshold_min": scenario.thresholds.write_tps.min,
                    "actual": ops_per_sec,
                    "passed": passed,
                    "status": "passed" if passed else "failed",
                })

        if scenario.thresholds.p99_ms and scenario.thresholds.p99_ms.max is not None:
            passed = p99_ms <= scenario.thresholds.p99_ms.max
            if not passed:
                has_failure = True
            checks.append({
                "phase": phase_name,
                "metric": "p99_ms",
                "threshold_max": scenario.thresholds.p99_ms.max,
                "actual": p99_ms,
                "passed": passed,
                "status": "passed" if passed else "failed",
            })

    if scenario.thresholds.heap_mb and scenario.thresholds.heap_mb.max is not None:
        heap_mb = _peak_metric_value_mb(result.metrics, "jvm_heap_bytes")
        if heap_mb is None:
            has_missing = True
            checks.append(
                {
                    "phase": "cluster",
                    "metric": "heap_mb",
                    "threshold_max": scenario.thresholds.heap_mb.max,
                    "actual": None,
                    "passed": None,
                    "status": "missing",
                }
            )
        else:
            passed = heap_mb <= scenario.thresholds.heap_mb.max
            if not passed:
                has_failure = True
            checks.append(
                {
                    "phase": "cluster",
                    "metric": "heap_mb",
                    "threshold_max": scenario.thresholds.heap_mb.max,
                    "actual": heap_mb,
                    "passed": passed,
                    "status": "passed" if passed else "failed",
                }
            )

    if scenario.thresholds.rss_peak_mb and scenario.thresholds.rss_peak_mb.max is not None:
        rss_mb = _peak_metric_value_mb(result.metrics, "process_rss_bytes")
        if rss_mb is None:
            has_missing = True
            checks.append(
                {
                    "phase": "cluster",
                    "metric": "rss_peak_mb",
                    "threshold_max": scenario.thresholds.rss_peak_mb.max,
                    "actual": None,
                    "passed": None,
                    "status": "missing",
                }
            )
        else:
            passed = rss_mb <= scenario.thresholds.rss_peak_mb.max
            if not passed:
                has_failure = True
            checks.append(
                {
                    "phase": "cluster",
                    "metric": "rss_peak_mb",
                    "threshold_max": scenario.thresholds.rss_peak_mb.max,
                    "actual": rss_mb,
                    "passed": passed,
                    "status": "passed" if passed else "failed",
                }
            )

    if has_failure:
        status = "failed"
    elif has_missing:
        status = "partial"
    else:
        status = "passed"

    return {
        "status": status,
        "checks": checks,
    }


def _peak_metric_value_mb(
    metrics: dict[str, Any],
    metric_name: str,
) -> float | None:
    peak = _peak_metric_value(metrics, metric_name)
    if peak is None:
        return None
    return peak / (1024.0 * 1024.0)


def _peak_metric_value(
    metrics: dict[str, Any],
    metric_name: str,
) -> float | None:
    metric_payload = metrics.get(metric_name)
    if not isinstance(metric_payload, dict):
        return None
    series_list = metric_payload.get("series")
    if not isinstance(series_list, list):
        return None

    peak: float | None = None
    for series in series_list:
        points = series.get("points", [])
        if not isinstance(points, list):
            continue
        for point in points:
            value = point.get("value")
            if value is None:
                continue
            if peak is None or float(value) > peak:
                peak = float(value)
    return peak


_FALLBACK_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Perf Report: $scenario_name</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body { font-family: system-ui, -apple-system, sans-serif; margin: 2rem; background: #f5f5f5; }
  .card { background: white; border-radius: 8px; padding: 1.5rem; margin-bottom: 1rem;
          box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  h1 { color: #1a1a2e; } h2 { color: #16213e; }
  .status-passed { color: #27ae60; } .status-failed { color: #e74c3c; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: 0.5rem 1rem; border-bottom: 1px solid #eee; }
  th { background: #f8f9fa; font-weight: 600; }
  .chart-container { max-width: 800px; margin: 1rem auto; }
  .meta { color: #666; font-size: 0.9rem; }
</style>
</head>
<body>
<h1>Perf Report: $scenario_name</h1>
<p class="meta">$scenario_description</p>
<p class="meta">Generated: $timestamp | Status:
  <span class="status-$status"><strong>$status</strong></span> |
  Elapsed: ${elapsed_ms}ms</p>

<div class="card">
<h2>Phase Results</h2>
<table>
<thead><tr>
  <th>Phase</th><th>Elapsed (ms)</th><th>Total Ops</th>
  <th>Ops/sec</th><th>P50 (ms)</th><th>P95 (ms)</th>
  <th>P99 (ms)</th><th>Max (ms)</th>
</tr></thead>
<tbody id="phase-table"></tbody>
</table>
</div>

<div class="card">
<h2>Throughput</h2>
<div class="chart-container"><canvas id="throughputChart"></canvas></div>
</div>

<div class="card">
<h2>Latency (ms)</h2>
<div class="chart-container"><canvas id="latencyChart"></canvas></div>
</div>

<script>
const phases = $phases_json;
const tbody = document.getElementById('phase-table');
phases.forEach(p => {
  const tr = document.createElement('tr');
  tr.innerHTML = '<td>'+p.phase+'</td><td>'+p.elapsedMs+'</td><td>'+p.totalOps+'</td>'
    +'<td>'+p.opsPerSec+'</td><td>'+(p.p50Ms||'-')+'</td><td>'+(p.p95Ms||'-')+'</td>'
    +'<td>'+(p.p99Ms||'-')+'</td><td>'+(p.maxMs||'-')+'</td>';
  tbody.appendChild(tr);
});
const labels = phases.map(p => p.phase);
new Chart(document.getElementById('throughputChart'), {
  type: 'bar', data: { labels,
    datasets: [{ label: 'Ops/sec', data: phases.map(p => parseFloat(p.opsPerSec)||0),
      backgroundColor: 'rgba(54,162,235,0.6)' }]
  }, options: { responsive: true }
});
new Chart(document.getElementById('latencyChart'), {
  type: 'bar', data: { labels,
    datasets: [
      { label: 'P50', data: phases.map(p => parseFloat(p.p50Ms)||0), backgroundColor: 'rgba(75,192,192,0.6)' },
      { label: 'P95', data: phases.map(p => parseFloat(p.p95Ms)||0), backgroundColor: 'rgba(255,206,86,0.6)' },
      { label: 'P99', data: phases.map(p => parseFloat(p.p99Ms)||0), backgroundColor: 'rgba(255,99,132,0.6)' },
    ]
  }, options: { responsive: true }
});
</script>
</body>
</html>
"""
