"""
plotter.py — Optional matplotlib plots for load test results.

Soft import: if matplotlib is not installed, all functions are no-ops.
"""

from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    _HAS_MPL = True
except ImportError:
    _HAS_MPL = False

import time
from datetime import datetime
from collections import defaultdict

DEGRADED_S = 3.0
WARN_S = 6.0


def _requires_mpl(fn):
    def wrapper(*args, **kwargs):
        if not _HAS_MPL:
            return
        return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    return wrapper


@_requires_mpl
def plot_latency_over_time(rows, path: Path, bucket_size_s: int = 30) -> None:
    """Line chart: p50 and p95 latency in 30-second buckets."""
    if not rows:
        return

    # Group rows into time buckets
    t0 = rows[0].timestamp
    buckets: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        b = int((r.timestamp - t0) / bucket_size_s)
        buckets[b].append(r.latency_s)

    import numpy as np

    keys = sorted(buckets)
    times = [k * bucket_size_s for k in keys]
    p50s = [np.percentile(buckets[k], 50) for k in keys]
    p95s = [np.percentile(buckets[k], 95) for k in keys]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(times, p50s, label="p50", linewidth=1.5, color="steelblue")
    ax.plot(times, p95s, label="p95", linewidth=1.5, color="tomato")
    ax.axhline(DEGRADED_S, color="orange", linestyle="--", linewidth=1, label=f"Degraded ({DEGRADED_S}s)")
    ax.axhline(WARN_S, color="red", linestyle="--", linewidth=1, label=f"Warn ({WARN_S}s)")
    ax.set_xlabel("Elapsed (s)")
    ax.set_ylabel("Latency (s)")
    ax.set_title("Latency over time")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


@_requires_mpl
def plot_latency_histogram(rows, path: Path) -> None:
    """Log-scale histogram of latencies with threshold lines."""
    if not rows:
        return

    latencies = [r.latency_s for r in rows]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(latencies, bins=50, color="steelblue", alpha=0.75, edgecolor="white")
    ax.axvline(DEGRADED_S, color="orange", linestyle="--", linewidth=1.5, label=f"Degraded ({DEGRADED_S}s)")
    ax.axvline(WARN_S, color="red", linestyle="--", linewidth=1.5, label=f"Warn ({WARN_S}s)")
    ax.set_yscale("log")
    ax.set_xlabel("Latency (s)")
    ax.set_ylabel("Count (log scale)")
    ax.set_title("Latency histogram")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


@_requires_mpl
def plot_throughput(rows, path: Path, bucket_size_s: int = 30) -> None:
    """Bar chart: requests per minute in 30-second buckets."""
    if not rows:
        return

    from collections import Counter

    t0 = rows[0].timestamp
    bucket_counts: Counter = Counter()
    for r in rows:
        b = int((r.timestamp - t0) / bucket_size_s)
        bucket_counts[b] += 1

    keys = sorted(bucket_counts)
    times = [k * bucket_size_s for k in keys]
    # Convert count/bucket to req/min
    rpm = [bucket_counts[k] / bucket_size_s * 60 for k in keys]

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(times, rpm, width=bucket_size_s * 0.8, color="steelblue", alpha=0.75)
    ax.set_xlabel("Elapsed (s)")
    ax.set_ylabel("Requests / min")
    ax.set_title("Throughput over time")
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def generate_all_plots(rows, plots_dir: Path) -> None:
    """Generate all plots into plots_dir. No-op if matplotlib missing."""
    if not _HAS_MPL:
        return
    plots_dir.mkdir(parents=True, exist_ok=True)
    plot_latency_over_time(rows, plots_dir / "latency_over_time.png")
    plot_latency_histogram(rows, plots_dir / "latency_histogram.png")
    plot_throughput(rows, plots_dir / "throughput.png")
