"""
reporter.py — CSV logging, stats computation, and summary output.
"""

import csv
import json
import threading
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import numpy as np

DEGRADED_S = 3.0


@dataclass
class RequestRow:
    timestamp: float          # Unix epoch
    scenario: str
    bucket: str
    expression: str
    chamber: str
    page: int
    status_code: int          # 0 = connection error
    latency_s: float
    timed_out: bool
    response_valid: bool
    error_msg: str = ""


_CSV_FIELDS = [
    "timestamp", "scenario", "bucket", "expression", "chamber", "page",
    "status_code", "latency_s", "timed_out", "response_valid", "error_msg",
]


class RequestLog:
    """Thread-safe in-memory log; flushes to CSV on close."""

    def __init__(self, csv_path: Path, scenario: str):
        self._rows: list[RequestRow] = []
        self._lock = threading.Lock()
        self._csv_path = csv_path
        self.scenario = scenario
        self._start_time = time.time()

    def append(self, row: RequestRow) -> None:
        with self._lock:
            self._rows.append(row)

    def rows(self) -> list[RequestRow]:
        with self._lock:
            return list(self._rows)

    def flush_csv(self) -> None:
        rows = self.rows()
        with open(self._csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
            writer.writeheader()
            for row in rows:
                d = asdict(row)
                # Round latency for readability
                d["latency_s"] = round(d["latency_s"], 4)
                writer.writerow(d)

    def close(self) -> None:
        self.flush_csv()


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def compute_stats(rows: list[RequestRow], scenario: str, timeout_s: float) -> dict:
    """Compute aggregate statistics from a list of RequestRow objects."""
    if not rows:
        return {"error": "no data"}

    n = len(rows)
    latencies = np.array([r.latency_s for r in rows], dtype=float)
    errors = sum(1 for r in rows if r.status_code != 200 or r.status_code == 0)
    timeouts = sum(1 for r in rows if r.timed_out)
    degraded = sum(1 for r in rows if r.latency_s > DEGRADED_S and not r.timed_out)
    invalid = sum(1 for r in rows if not r.response_valid and not r.timed_out)

    # Drift: compare first 10% vs last 10%
    split = max(1, n // 10)
    first_slice = latencies[:split]
    last_slice = latencies[-split:]
    p50_first = float(np.percentile(first_slice, 50))
    p50_last = float(np.percentile(last_slice, 50))
    drift_ratio = (p50_last - p50_first) / p50_first if p50_first > 0 else 0.0

    # Duration and throughput
    if n >= 2:
        duration_s = rows[-1].timestamp - rows[0].timestamp
        rpm = (n / duration_s) * 60 if duration_s > 0 else 0.0
    else:
        duration_s = 0.0
        rpm = 0.0

    # Per-bucket breakdown
    buckets: dict[str, list[float]] = {}
    for r in rows:
        buckets.setdefault(r.bucket, []).append(r.latency_s)
    bucket_stats = {
        bk: {
            "count": len(lats),
            "p50": round(float(np.percentile(lats, 50)), 3),
            "p95": round(float(np.percentile(lats, 95)), 3),
        }
        for bk, lats in buckets.items()
    }

    stats = {
        "scenario": scenario,
        "total_requests": n,
        "duration_s": round(duration_s, 1),
        "rpm": round(rpm, 1),
        "error_count": errors,
        "error_rate_pct": round(errors / n * 100, 2),
        "timeout_count": timeouts,
        "timeout_rate_pct": round(timeouts / n * 100, 2),
        "degraded_count": degraded,
        "degraded_rate_pct": round(degraded / n * 100, 2),
        "invalid_response_count": invalid,
        "p50_s": round(float(np.percentile(latencies, 50)), 3),
        "p75_s": round(float(np.percentile(latencies, 75)), 3),
        "p95_s": round(float(np.percentile(latencies, 95)), 3),
        "p99_s": round(float(np.percentile(latencies, 99)), 3),
        "mean_s": round(float(np.mean(latencies)), 3),
        "max_s": round(float(np.max(latencies)), 3),
        "min_s": round(float(np.min(latencies)), 3),
        "drift_ratio": round(drift_ratio, 3),
        "bucket_stats": bucket_stats,
        "thresholds": _evaluate_thresholds(
            error_rate=errors / n * 100,
            timeout_rate=timeouts / n * 100,
            degraded_rate=degraded / n * 100,
            p50=float(np.percentile(latencies, 50)),
            p95=float(np.percentile(latencies, 95)),
            p99=float(np.percentile(latencies, 99)),
            drift_ratio=drift_ratio,
        ),
    }
    return stats


def _evaluate_thresholds(
    error_rate: float,
    timeout_rate: float,
    degraded_rate: float,
    p50: float,
    p95: float,
    p99: float,
    drift_ratio: float,
) -> dict:
    """Return pass/warn/fail for each metric."""

    def grade(val: float, pass_: float, warn_: float, lower_is_better: bool = True) -> str:
        if lower_is_better:
            if val < pass_:
                return "PASS"
            elif val < warn_:
                return "WARN"
            else:
                return "FAIL"
        else:
            if val >= pass_:
                return "PASS"
            elif val >= warn_:
                return "WARN"
            else:
                return "FAIL"

    return {
        "error_rate":    {"value": round(error_rate, 2),   "unit": "%",  "result": grade(error_rate,   0.5,  2.0)},
        "timeout_rate":  {"value": round(timeout_rate, 2), "unit": "%",  "result": grade(timeout_rate, 0.5,  1.0)},
        "degraded_rate": {"value": round(degraded_rate, 2),"unit": "%",  "result": grade(degraded_rate, 5.0, 15.0)},
        "p50_latency":   {"value": round(p50, 3),          "unit": "s",  "result": grade(p50,  0.5,  1.5)},
        "p95_latency":   {"value": round(p95, 3),          "unit": "s",  "result": grade(p95,  3.0,  6.0)},
        "p99_latency":   {"value": round(p99, 3),          "unit": "s",  "result": grade(p99,  8.0, 15.0)},
        "drift_ratio":   {"value": round(drift_ratio, 3),  "unit": "ratio", "result": grade(drift_ratio, 0.20, 0.50)},
    }


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_summary_json(stats: dict, path: Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)


def write_summary_md(stats: dict, path: Path) -> None:
    lines = []
    lines.append(f"# Load Test Summary — {stats.get('scenario', 'unknown')}\n")
    lines.append(f"**Total requests:** {stats['total_requests']}  ")
    lines.append(f"**Duration:** {stats['duration_s']}s  ")
    lines.append(f"**Throughput:** {stats['rpm']} req/min\n")

    lines.append("## Latency Percentiles\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    for k in ("min_s", "mean_s", "p50_s", "p75_s", "p95_s", "p99_s", "max_s"):
        lines.append(f"| {k} | {stats[k]}s |")
    lines.append("")

    lines.append("## Error Rates\n")
    lines.append("| Metric | Count | Rate |")
    lines.append("|--------|-------|------|")
    lines.append(f"| Errors | {stats['error_count']} | {stats['error_rate_pct']}% |")
    lines.append(f"| Timeouts | {stats['timeout_count']} | {stats['timeout_rate_pct']}% |")
    lines.append(f"| Degraded (>{DEGRADED_S}s) | {stats['degraded_count']} | {stats['degraded_rate_pct']}% |")
    lines.append(f"| Invalid responses | {stats['invalid_response_count']} | — |")
    lines.append(f"| Drift ratio | — | {stats['drift_ratio']} |")
    lines.append("")

    lines.append("## Threshold Results\n")
    lines.append("| Metric | Value | Result |")
    lines.append("|--------|-------|--------|")
    for metric, info in stats["thresholds"].items():
        result = info["result"]
        emoji = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(result, result)
        lines.append(f"| {metric} | {info['value']}{info['unit']} | {emoji} {result} |")
    lines.append("")

    if stats.get("bucket_stats"):
        lines.append("## Per-Bucket Breakdown\n")
        lines.append("| Bucket | Count | p50 | p95 |")
        lines.append("|--------|-------|-----|-----|")
        for bk, bs in stats["bucket_stats"].items():
            lines.append(f"| {bk} | {bs['count']} | {bs['p50']}s | {bs['p95']}s |")
        lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def print_live_summary(stats: dict) -> None:
    """Print a concise summary to stdout."""
    t = stats.get("thresholds", {})
    print("\n" + "=" * 60)
    print(f"  Scenario : {stats.get('scenario', '?')}")
    print(f"  Requests : {stats['total_requests']}  ({stats['rpm']} req/min)")
    print(f"  Duration : {stats['duration_s']}s")
    print(f"  p50/p95/p99 : {stats['p50_s']}s / {stats['p95_s']}s / {stats['p99_s']}s")
    print(f"  Errors   : {stats['error_rate_pct']}%  Timeouts: {stats['timeout_rate_pct']}%"
          f"  Degraded: {stats['degraded_rate_pct']}%  Drift: {stats['drift_ratio']}")
    print("-" * 60)
    for metric, info in t.items():
        result = info["result"]
        tag = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL"}.get(result, result)
        print(f"  [{tag:4s}] {metric:15s} {info['value']}{info['unit']}")
    print("=" * 60 + "\n")
