#!/usr/bin/env python3
"""
Hansard Search — Load Test Report Generator
Generates a self-contained HTML report with Claude Opus prose + matplotlib plots.

Usage:
    python load_test/analyze.py [--runs DIR ...] [--out FILE] [--api-key sk-...]
"""

import argparse
import base64
import csv
import io
import json
import os
import re
import sys
from datetime import datetime
from glob import glob
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

try:
    import anthropic
except ImportError:
    print("ERROR: anthropic package not installed. Run: pip install anthropic", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCENARIO_ORDER = ["cache_friendly", "cache_unfriendly", "worst_case", "burst", "steady_state"]

SCENARIO_LABELS = {
    "cache_friendly": "Cache Friendly",
    "cache_unfriendly": "Cache Unfriendly",
    "worst_case": "Worst Case",
    "burst": "Burst",
    "steady_state": "Steady State",
}

BUCKET_COLOURS = {
    "easy":   "#4c9be8",
    "medium": "#f5a623",
    "heavy":  "#e85454",
    "worst":  "#9b59b6",
}

THRESHOLD_DEFS = """
Threshold definitions (grade = PASS/WARN/FAIL):
  error_rate    (%):   PASS < 0.5,  WARN < 2.0,  FAIL ≥ 2.0
  timeout_rate  (%):   PASS < 0.5,  WARN < 1.0,  FAIL ≥ 1.0
  degraded_rate (%):   PASS < 5.0,  WARN < 15.0, FAIL ≥ 15.0  (degraded = latency > 3.0 s)
  p50_latency   (s):   PASS < 0.5,  WARN < 1.5,  FAIL ≥ 1.5
  p95_latency   (s):   PASS < 3.0,  WARN < 6.0,  FAIL ≥ 6.0
  p99_latency   (s):   PASS < 8.0,  WARN < 15.0, FAIL ≥ 15.0
  drift_ratio (ratio): PASS < 0.20, WARN < 0.50, FAIL ≥ 0.50
    drift_ratio = (p50_last_third − p50_first_third) / p50_first_third
"""

LITTLES_LAW_CONTEXT = """
Little's Law concurrency analysis:
  L = λ × W
  where L = mean concurrent in-flight requests, λ = arrival rate (req/s), W = mean latency (s)

  Server safe zone: approximately 7 concurrent in-flight requests before queueing pressure appears.
  At 0.5 s mean think-time between virtual-user requests, with ~3–5 s mean latency:
    - Each virtual user generates ~0.17–0.25 req/s effective throughput
    - To saturate 7 concurrent slots: ~30–40 VUs needed
  Realistic user capacity estimate: ~150–200 active users (30 s think time between searches).
  The burst scenario drives ~7.3 concurrent requests (219 req / 29.4 s × 7.27 s mean) — right at the saturation boundary.
"""


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_runs(run_dirs: list[str]) -> list[dict]:
    """Load summary.json + requests.csv for each run dir. Skip dirs with no summary.json."""
    runs = []
    for d in sorted(run_dirs):
        p = Path(d)
        summary_path = p / "summary.json"
        csv_path = p / "requests.csv"
        if not summary_path.exists():
            continue
        with open(summary_path) as f:
            summary = json.load(f)
        scenario = summary.get("scenario", "")
        if scenario not in SCENARIO_ORDER:
            continue
        requests = []
        if csv_path.exists():
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    requests.append({
                        "timestamp": float(row["timestamp"]),
                        "scenario": row["scenario"],
                        "bucket": row["bucket"],
                        "latency_s": float(row["latency_s"]),
                        "timed_out": row["timed_out"].lower() == "true",
                        "response_valid": row["response_valid"].lower() == "true",
                        "status_code": int(row["status_code"]) if row["status_code"] else 0,
                    })
        runs.append({"summary": summary, "requests": requests, "dir": str(p)})
    # Deduplicate: keep the latest run per scenario (run dirs are sorted chronologically)
    seen: dict[str, dict] = {}
    for run in runs:
        scenario = run["summary"]["scenario"]
        seen[scenario] = run  # later runs overwrite earlier ones

    # Sort by SCENARIO_ORDER
    order_map = {s: i for i, s in enumerate(SCENARIO_ORDER)}
    deduped = list(seen.values())
    deduped.sort(key=lambda r: order_map.get(r["summary"]["scenario"], 99))
    return deduped


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------

def fig_to_base64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("ascii")
    plt.close(fig)
    return f"data:image/png;base64,{b64}"


def _scenario_colours(scenarios: list[str]) -> list[str]:
    palette = ["#4c9be8", "#f5a623", "#2ecc71", "#e85454", "#9b59b6"]
    return [palette[i % len(palette)] for i in range(len(scenarios))]


# ---------------------------------------------------------------------------
# Plot 1: Cross-scenario latency grouped bar (p50/p95/p99)
# ---------------------------------------------------------------------------

def plot_latency_bars(runs: list[dict]) -> str:
    scenarios = [r["summary"]["scenario"] for r in runs]
    labels = [SCENARIO_LABELS.get(s, s) for s in scenarios]
    p50 = [r["summary"]["p50_s"] for r in runs]
    p95 = [r["summary"]["p95_s"] for r in runs]
    p99 = [r["summary"]["p99_s"] for r in runs]

    x = np.arange(len(scenarios))
    width = 0.25

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(x - width, p50, width, label="p50", color="#4c9be8")
    ax.bar(x,         p95, width, label="p95", color="#f5a623")
    ax.bar(x + width, p99, width, label="p99", color="#e85454")

    # Threshold lines
    ax.axhline(1.5, color="#4c9be8", linestyle="--", linewidth=0.8, alpha=0.6, label="p50 WARN (1.5s)")
    ax.axhline(6.0, color="#f5a623", linestyle="--", linewidth=0.8, alpha=0.6, label="p95 WARN (6.0s)")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15, ha="right")
    ax.set_ylabel("Latency (s)")
    ax.set_title("Cross-Scenario Latency: p50 / p95 / p99")
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    return fig_to_base64(fig)


# ---------------------------------------------------------------------------
# Plot 2: Degraded & error rates horizontal bar
# ---------------------------------------------------------------------------

def plot_degraded_error_rates(runs: list[dict]) -> str:
    scenarios = [r["summary"]["scenario"] for r in runs]
    labels = [SCENARIO_LABELS.get(s, s) for s in scenarios]
    degraded = [r["summary"]["degraded_rate_pct"] for r in runs]
    errors = [r["summary"]["error_rate_pct"] for r in runs]

    y = np.arange(len(scenarios))
    height = 0.35

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.barh(y + height / 2, degraded, height, label="Degraded rate (%)", color="#f5a623")
    ax.barh(y - height / 2, errors,   height, label="Error rate (%)",    color="#e85454")

    ax.axvline(15.0, color="#f5a623", linestyle="--", linewidth=0.8, alpha=0.7, label="Degraded WARN (15%)")
    ax.axvline(2.0,  color="#e85454", linestyle="--", linewidth=0.8, alpha=0.7, label="Error WARN (2%)")

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlabel("Rate (%)")
    ax.set_title("Degraded & Error Rates by Scenario")
    ax.legend(fontsize=8)
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    return fig_to_base64(fig)


# ---------------------------------------------------------------------------
# Plot 3: Latency over time (scatter, one subplot per scenario)
# ---------------------------------------------------------------------------

def plot_latency_over_time(runs: list[dict]) -> str:
    n = len(runs)
    cols = min(3, n)
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 3.5 * rows), squeeze=False)

    for idx, run in enumerate(runs):
        ax = axes[idx // cols][idx % cols]
        reqs = run["requests"]
        if not reqs:
            ax.set_visible(False)
            continue

        t0 = min(r["timestamp"] for r in reqs)
        times = [r["timestamp"] - t0 for r in reqs]
        lats = [r["latency_s"] for r in reqs]
        buckets = [r["bucket"] for r in reqs]

        unique_buckets = list(dict.fromkeys(buckets))
        for bk in unique_buckets:
            mask = [b == bk for b in buckets]
            tx = [times[i] for i, m in enumerate(mask) if m]
            ly = [lats[i] for i, m in enumerate(mask) if m]
            ax.scatter(tx, ly, s=12, alpha=0.6, color=BUCKET_COLOURS.get(bk, "#888"),
                       label=bk, zorder=3)

        ax.axhline(3.0, color="orange", linestyle="--", linewidth=0.8, alpha=0.7)
        scenario = run["summary"]["scenario"]
        ax.set_title(SCENARIO_LABELS.get(scenario, scenario), fontsize=9)
        ax.set_xlabel("Time (s)", fontsize=8)
        ax.set_ylabel("Latency (s)", fontsize=8)
        ax.legend(fontsize=7, markerscale=1.5)
        ax.grid(alpha=0.2)

    # Hide any unused axes
    for idx in range(n, rows * cols):
        axes[idx // cols][idx % cols].set_visible(False)

    fig.suptitle("Latency Over Time (individual requests)", fontsize=11)
    fig.tight_layout()
    return fig_to_base64(fig)


# ---------------------------------------------------------------------------
# Plot 4: Latency distribution (overlapping KDE / histogram)
# ---------------------------------------------------------------------------

def plot_latency_distribution(runs: list[dict]) -> str:
    fig, ax = plt.subplots(figsize=(10, 5))
    colours = _scenario_colours([r["summary"]["scenario"] for r in runs])

    for run, colour in zip(runs, colours):
        reqs = run["requests"]
        if not reqs:
            continue
        lats = np.array([r["latency_s"] for r in reqs])
        scenario = run["summary"]["scenario"]
        label = SCENARIO_LABELS.get(scenario, scenario)

        # Histogram (density)
        ax.hist(lats, bins=30, density=True, alpha=0.25, color=colour)

        # KDE
        if len(lats) > 5:
            from scipy.stats import gaussian_kde  # optional
            kde = gaussian_kde(lats, bw_method=0.3)
            xs = np.linspace(lats.min(), lats.max(), 300)
            ax.plot(xs, kde(xs), color=colour, linewidth=2, label=label)
        else:
            ax.hist(lats, bins=10, density=True, alpha=0.6, color=colour, label=label)

    ax.axvline(3.0, color="orange", linestyle="--", linewidth=1, label="Degraded threshold (3s)")
    ax.set_xlabel("Latency (s)")
    ax.set_ylabel("Density")
    ax.set_title("Latency Distribution by Scenario")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    return fig_to_base64(fig)


# ---------------------------------------------------------------------------
# Plot 5: Bucket breakdown — p50 per bucket, grouped by scenario
# ---------------------------------------------------------------------------

def plot_bucket_breakdown(runs: list[dict]) -> str:
    # Only include runs with multiple buckets
    multi = [r for r in runs if len(r["summary"].get("bucket_stats", {})) > 1]
    if not multi:
        # Fall back to all runs
        multi = runs

    all_buckets = list(dict.fromkeys(
        bk for r in multi for bk in r["summary"].get("bucket_stats", {})
    ))

    x = np.arange(len(multi))
    width = 0.8 / max(len(all_buckets), 1)

    fig, ax = plt.subplots(figsize=(10, 5))
    for i, bk in enumerate(all_buckets):
        p50s = []
        for r in multi:
            bs = r["summary"].get("bucket_stats", {})
            p50s.append(bs[bk]["p50"] if bk in bs else 0.0)
        offset = (i - len(all_buckets) / 2 + 0.5) * width
        ax.bar(x + offset, p50s, width * 0.9,
               label=bk, color=BUCKET_COLOURS.get(bk, "#888"), alpha=0.85)

    ax.axhline(1.5, color="gray", linestyle="--", linewidth=0.8, alpha=0.6, label="p50 WARN (1.5s)")
    labels = [SCENARIO_LABELS.get(r["summary"]["scenario"], r["summary"]["scenario"]) for r in multi]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15, ha="right")
    ax.set_ylabel("p50 Latency (s)")
    ax.set_title("p50 Latency by Bucket (multi-bucket scenarios)")
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    return fig_to_base64(fig)


# ---------------------------------------------------------------------------
# Claude Opus analysis
# ---------------------------------------------------------------------------

def build_opus_prompt(runs: list[dict]) -> str:
    parts = ["# Hansard Search Load Test — Raw Results\n"]
    for run in runs:
        s = run["summary"]["scenario"]
        parts.append(f"## Scenario: {s}\n```json\n{json.dumps(run['summary'], indent=2)}\n```\n")

    parts.append(f"\n{THRESHOLD_DEFS}\n")
    parts.append(f"\n{LITTLES_LAW_CONTEXT}\n")

    parts.append("""
---

Please write a professional HTML-ready performance report based on the above load test results.

Structure your response with these sections (use ## for section headings):

## Executive Summary
3–5 sentences covering overall outcome, standout results, and key concern.

## Scenario Analysis
One focused paragraph for each scenario (cache_friendly, cache_unfriendly, worst_case, burst, steady_state).
For each, cover: what the scenario tests, how it performed against thresholds, and what that implies.

## Key Findings
A bulleted list of the 4–6 most important findings across all scenarios.

## Recommendations
A numbered list of actionable recommendations, ordered by priority.

---

Formatting rules:
- Output plain prose + markdown headings (##) and lists only
- Do NOT output any HTML tags
- Do NOT include a document title (it will be added separately)
- Use **bold** for emphasis on specific metrics or scenario names
- Keep language technical but accessible to a developer audience
""")
    return "\n".join(parts)


def call_opus(prompt: str, api_key: str) -> str:
    client = anthropic.Anthropic(api_key=api_key)
    print("Calling Claude Opus (streaming)...", flush=True)

    full_text = []
    with client.messages.stream(
        model="claude-opus-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for text in stream.text_stream:
            full_text.append(text)
            print(".", end="", flush=True)
    print(" done.", flush=True)
    return "".join(full_text)


# ---------------------------------------------------------------------------
# Markdown → HTML conversion
# ---------------------------------------------------------------------------

def markdown_to_html(md: str) -> str:
    """Convert markdown to HTML. Uses `markdown` library if available, else regex fallback."""
    try:
        import markdown as md_lib
        return md_lib.markdown(md, extensions=["nl2br"])
    except ImportError:
        pass
    # Manual conversion
    import html as html_lib
    lines = md.split("\n")
    html_lines = []
    in_ul = False
    in_ol = False

    def close_lists():
        nonlocal in_ul, in_ol
        if in_ul:
            html_lines.append("</ul>")
            in_ul = False
        if in_ol:
            html_lines.append("</ol>")
            in_ol = False

    def inline(text: str) -> str:
        # Bold
        text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
        text = re.sub(r"__(.+?)__",     r"<strong>\1</strong>", text)
        # Italic
        text = re.sub(r"\*(.+?)\*",     r"<em>\1</em>",         text)
        text = re.sub(r"_(.+?)_",       r"<em>\1</em>",         text)
        # Code
        text = re.sub(r"`(.+?)`",       r"<code>\1</code>",     text)
        return text

    i = 0
    while i < len(lines):
        line = lines[i]

        # Headings
        m = re.match(r"^(#{1,6})\s+(.*)", line)
        if m:
            close_lists()
            level = len(m.group(1))
            content = html_lib.escape(m.group(2))
            html_lines.append(f"<h{level}>{inline(content)}</h{level}>")
            i += 1
            continue

        # Unordered list
        m = re.match(r"^[-*]\s+(.*)", line)
        if m:
            if not in_ul:
                close_lists()
                html_lines.append("<ul>")
                in_ul = True
            content = html_lib.escape(m.group(1))
            html_lines.append(f"  <li>{inline(content)}</li>")
            i += 1
            continue

        # Ordered list
        m = re.match(r"^\d+\.\s+(.*)", line)
        if m:
            if not in_ol:
                close_lists()
                html_lines.append("<ol>")
                in_ol = True
            content = html_lib.escape(m.group(1))
            html_lines.append(f"  <li>{inline(content)}</li>")
            i += 1
            continue

        # Blank line
        if not line.strip():
            close_lists()
            html_lines.append("")
            i += 1
            continue

        # Paragraph
        close_lists()
        content = html_lib.escape(line)
        html_lines.append(f"<p>{inline(content)}</p>")
        i += 1

    close_lists()
    return "\n".join(html_lines)


# ---------------------------------------------------------------------------
# HTML assembly
# ---------------------------------------------------------------------------

RESULT_BADGE_CSS = {
    "PASS": "background:#2ecc71;color:#fff",
    "WARN": "background:#f39c12;color:#fff",
    "FAIL": "background:#e74c3c;color:#fff",
}


def _badge(result: str) -> str:
    style = RESULT_BADGE_CSS.get(result, "background:#999;color:#fff")
    return f'<span style="{style};padding:2px 6px;border-radius:3px;font-size:0.8em;font-weight:bold">{result}</span>'


def build_raw_table(runs: list[dict]) -> str:
    rows = []
    headers = ["Scenario", "Requests", "RPM", "p50 (s)", "p95 (s)", "p99 (s)",
               "Degraded %", "Error %", "Drift ratio"]
    rows.append("<table>")
    rows.append("<thead><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr></thead>")
    rows.append("<tbody>")
    for run in runs:
        s = run["summary"]
        t = s.get("thresholds", {})
        def cell(val, key=None):
            if key and key in t:
                return f"<td>{val} {_badge(t[key]['result'])}</td>"
            return f"<td>{val}</td>"

        rows.append(
            "<tr>"
            f"<td><strong>{s['scenario']}</strong></td>"
            f"<td>{s['total_requests']}</td>"
            f"<td>{s['rpm']}</td>"
            + cell(s['p50_s'], 'p50_latency')
            + cell(s['p95_s'], 'p95_latency')
            + cell(s['p99_s'], 'p99_latency')
            + cell(s['degraded_rate_pct'], 'degraded_rate')
            + cell(s['error_rate_pct'], 'error_rate')
            + cell(s['drift_ratio'], 'drift_ratio')
            + "</tr>"
        )
    rows.append("</tbody></table>")
    return "\n".join(rows)


def assemble_html(opus_html: str, plots: list[tuple[str, str]],
                  runs: list[dict], generated_at: str) -> str:
    plot_figures = ""
    for src, caption in plots:
        plot_figures += f"""
  <figure>
    <img src="{src}" alt="{caption}">
    <figcaption>{caption}</figcaption>
  </figure>
"""

    raw_table = build_raw_table(runs)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Hansard Search — Load Test Report</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: 15px;
      line-height: 1.65;
      color: #222;
      max-width: 960px;
      margin: 0 auto;
      padding: 2rem 1.5rem 4rem;
      background: #fafafa;
    }}
    h1 {{ font-size: 1.8rem; margin-bottom: 0.25rem; border-bottom: 2px solid #4c9be8; padding-bottom: 0.4rem; }}
    h2 {{ font-size: 1.3rem; margin-top: 2rem; color: #2c3e50; border-left: 4px solid #4c9be8; padding-left: 0.6rem; }}
    h3 {{ font-size: 1.1rem; margin-top: 1.5rem; color: #34495e; }}
    p  {{ margin: 0.6rem 0 1rem; }}
    ul, ol {{ margin: 0.5rem 0 1rem 1.5rem; }}
    li {{ margin-bottom: 0.3rem; }}
    code {{ background: #eee; padding: 1px 4px; border-radius: 3px; font-size: 0.9em; }}
    .meta {{ color: #666; font-size: 0.85rem; margin-top: 0.2rem; margin-bottom: 2rem; }}
    figure {{
      margin: 1.5rem 0;
      background: #fff;
      border: 1px solid #ddd;
      border-radius: 6px;
      padding: 1rem;
      text-align: center;
    }}
    figure img {{ max-width: 100%; height: auto; }}
    figcaption {{ font-size: 0.85rem; color: #555; margin-top: 0.5rem; font-style: italic; }}
    details {{ margin: 1.5rem 0; }}
    summary {{ cursor: pointer; font-weight: 600; color: #2c3e50; padding: 0.4rem 0; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 0.88rem; }}
    th, td {{ border: 1px solid #ccc; padding: 6px 10px; text-align: left; }}
    th {{ background: #f0f4f8; font-weight: 600; }}
    tr:nth-child(even) {{ background: #f9f9f9; }}
    .plots-section h2 {{ margin-top: 2.5rem; }}
  </style>
</head>
<body>
  <h1>Hansard Search — Load Test Report</h1>
  <p class="meta">Generated: {generated_at} | Model: claude-opus-4-6 | Scenarios: {len(runs)}</p>

  {opus_html}

  <div class="plots-section">
    <h2>Performance Plots</h2>
    {plot_figures}
  </div>

  <details>
    <summary>Raw scenario metrics</summary>
    {raw_table}
  </details>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate Hansard load test HTML report")
    parser.add_argument(
        "--runs", nargs="*", default=None,
        help="Run directories to include (default: all load_test/results/run_*/)",
    )
    parser.add_argument(
        "--out", default=None,
        help="Output HTML file path (default: load_test/results/report_YYYYMMDD_HHMMSS.html)",
    )
    parser.add_argument("--api-key", default=None, help="Anthropic API key")
    args = parser.parse_args()

    # Resolve API key
    api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set. Use --api-key or set the env var.", file=sys.stderr)
        sys.exit(1)

    # Resolve run directories
    if args.runs:
        run_dirs = args.runs
    else:
        # Auto-discover relative to this script's location or cwd
        script_dir = Path(__file__).parent
        run_dirs = sorted(glob(str(script_dir / "results" / "run_*")))
        if not run_dirs:
            # Try relative to cwd
            run_dirs = sorted(glob("load_test/results/run_*"))

    if not run_dirs:
        print("ERROR: No run directories found.", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {len(run_dirs)} candidate run dir(s)...")
    runs = load_runs(run_dirs)
    if not runs:
        print("ERROR: No valid scenario runs found (missing summary.json or unknown scenario).", file=sys.stderr)
        sys.exit(1)
    print(f"Loaded {len(runs)} scenario(s): {[r['summary']['scenario'] for r in runs]}")

    # Generate plots
    print("Generating plots...")
    plots_data = [
        (plot_latency_bars(runs),          "Cross-Scenario Latency: p50 / p95 / p99"),
        (plot_degraded_error_rates(runs),  "Degraded & Error Rates by Scenario"),
        (plot_latency_over_time(runs),     "Latency Over Time (individual requests)"),
        (plot_latency_distribution(runs),  "Latency Distribution by Scenario"),
        (plot_bucket_breakdown(runs),      "p50 Latency by Bucket (multi-bucket scenarios)"),
    ]
    print(f"Generated {len(plots_data)} plot(s).")

    # Call Claude Opus
    prompt = build_opus_prompt(runs)
    opus_text = call_opus(prompt, api_key)

    # Convert markdown to HTML
    opus_html = markdown_to_html(opus_text)

    # Assemble HTML
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html_content = assemble_html(opus_html, plots_data, runs, generated_at)

    # Determine output path
    if args.out:
        out_path = Path(args.out)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        script_dir = Path(__file__).parent
        out_path = script_dir / "results" / f"report_{ts}.html"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_content, encoding="utf-8")
    print(f"Report saved to: {out_path}")


if __name__ == "__main__":
    main()
