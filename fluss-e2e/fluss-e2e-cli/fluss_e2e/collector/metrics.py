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

"""Prometheus metrics collection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import urllib.error
import urllib.parse
import urllib.request


class MetricsCollector:
    """Scrapes metrics from Prometheus and exports raw snapshots."""

    def __init__(self, prometheus_url: str = "http://localhost:9090") -> None:
        self._url = prometheus_url.rstrip("/")
        self._snapshots: list[dict[str, Any]] = []

    def is_available(self) -> bool:
        """Check if Prometheus is reachable."""
        try:
            request = urllib.request.Request(
                f"{self._url}/-/ready",
                method="GET",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status == 200
        except (urllib.error.URLError, OSError):
            return False

    def scrape(self, query: str = '{__name__=~"fluss_.*"}') -> dict[str, Any]:
        """Scrape current metrics snapshot from Prometheus."""
        snapshot = self.query(query)
        self._snapshots.append(snapshot)
        return snapshot

    def query(self, query: str) -> dict[str, Any]:
        """Execute an instant Prometheus query."""
        return self._request_json(
            "/api/v1/query",
            {"query": query},
        )

    def query_range(
        self,
        query: str,
        *,
        start_s: int,
        end_s: int,
        step: str,
    ) -> dict[str, Any]:
        """Execute a Prometheus range query."""
        return self._request_json(
            "/api/v1/query_range",
            {
                "query": query,
                "start": str(start_s),
                "end": str(end_s),
                "step": step,
            },
        )

    def export_summary(self, output_path: Path) -> dict[str, Any]:
        """Write raw snapshots to file and return a summary dict."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(self._snapshots, indent=2, sort_keys=False) + "\n",
            encoding="utf-8",
        )

        summary = self._build_summary()
        return {
            "prometheus_url": self._url,
            "snapshot_path": str(output_path),
            "snapshot_count": len(self._snapshots),
            "summary": summary,
        }

    def _build_summary(self) -> dict[str, Any]:
        """Aggregate metric names and counts from collected snapshots."""
        metric_names: set[str] = set()
        total_series = 0

        for snapshot in self._snapshots:
            data = snapshot.get("data", {})
            results = data.get("result", [])
            if isinstance(results, list):
                total_series += len(results)
                for result in results:
                    metric = result.get("metric", {})
                    name = metric.get("__name__")
                    if name:
                        metric_names.add(name)

        return {
            "metric_names": sorted(metric_names),
            "metric_count": len(metric_names),
            "total_series": total_series,
        }

    def _request_json(
        self,
        path: str,
        params: dict[str, str],
    ) -> dict[str, Any]:
        encoded = urllib.parse.urlencode(params)
        url = "%s%s?%s" % (self._url, path, encoded)
        try:
            request = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(request, timeout=10) as response:
                body = response.read().decode("utf-8")
                payload = json.loads(body)
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            return {"status": "error", "error": str(exc), "data": {}}

        return {
            "status": payload.get("status", "unknown"),
            "data": payload.get("data", {}),
        }
