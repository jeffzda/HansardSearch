#!/usr/bin/env python3
"""
load_test.py — Async load-testing harness for Hansard Search.

Usage:
  python load_test/load_test.py --url http://localhost:5000 --scenario cache_friendly
  python load_test/load_test.py --scenario steady_state --users 30 --duration 1500
  python load_test/load_test.py --scenario burst --users 50 --duration 30
  python load_test/load_test.py --scenario worst_case --users 10 --duration 180

Scenarios:
  cache_friendly    20 users, 5 min, 3-7s delay, 5 fixed easy queries
  cache_unfriendly  20 users, 5 min, 3-7s delay, random unique queries
  worst_case        10 users, 3 min, 1-3s delay, 100% worst bucket
  burst             50 users, 30s,   0s delay,   easy+medium
  steady_state      30 users, 25 min, 4-8s delay, default weights (primary soak)
"""

import argparse
import asyncio
import random
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp is required.  Run: pip install aiohttp", file=sys.stderr)
    sys.exit(1)

# Ensure load_test/ is importable regardless of working directory
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

from queries import QueryGenerator, make_day_context_request
from reporter import RequestLog, RequestRow, compute_stats, write_summary_json, write_summary_md, print_live_summary
from plotter import generate_all_plots

# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

SCENARIOS = {
    "cache_friendly": {
        "users": 20,
        "duration": 300,
        "delay_min": 3.0,
        "delay_max": 7.0,
        "weights": (0.80, 0.20, 0.00, 0.00),
        "fixed_queries": [
            {"expression": "water", "chamber": "both", "page": 1, "page_size": 20,
             "filters": {}, "case_sensitive": False, "sort_col": "date", "sort_dir": "desc"},
            {"expression": "climate", "chamber": "both", "page": 1, "page_size": 20,
             "filters": {}, "case_sensitive": False, "sort_col": "date", "sort_dir": "desc"},
            {"expression": "budget", "chamber": "senate", "page": 1, "page_size": 20,
             "filters": {}, "case_sensitive": False, "sort_col": "date", "sort_dir": "desc"},
            {"expression": "health", "chamber": "house", "page": 1, "page_size": 20,
             "filters": {}, "case_sensitive": False, "sort_col": "date", "sort_dir": "desc"},
            {"expression": "defence", "chamber": "both", "page": 1, "page_size": 20,
             "filters": {}, "case_sensitive": False, "sort_col": "date", "sort_dir": "desc"},
        ],
    },
    "cache_unfriendly": {
        "users": 20,
        "duration": 300,
        "delay_min": 3.0,
        "delay_max": 7.0,
        "weights": (0.25, 0.35, 0.30, 0.10),
        "fixed_queries": None,
    },
    "worst_case": {
        "users": 10,
        "duration": 180,
        "delay_min": 1.0,
        "delay_max": 3.0,
        "weights": (0.00, 0.00, 0.00, 1.00),
        "fixed_queries": None,
    },
    "burst": {
        "users": 50,
        "duration": 30,
        "delay_min": 0.0,
        "delay_max": 0.0,
        "weights": (0.60, 0.40, 0.00, 0.00),
        "fixed_queries": None,
    },
    "steady_state": {
        "users": 30,
        "duration": 1500,
        "delay_min": 4.0,
        "delay_max": 8.0,
        "weights": (0.40, 0.30, 0.20, 0.10),
        "fixed_queries": None,
    },
}

# ---------------------------------------------------------------------------
# Virtual user coroutine
# ---------------------------------------------------------------------------

async def virtual_user(
    user_id: int,
    base_url: str,
    scenario_cfg: dict,
    gen: QueryGenerator,
    log: RequestLog,
    deadline: float,
    timeout_s: float,
    session: aiohttp.ClientSession,
    stop_event: asyncio.Event,
) -> None:
    rng = random.Random(user_id * 999983)
    fixed = scenario_cfg.get("fixed_queries")
    delay_min = scenario_cfg["delay_min"]
    delay_max = scenario_cfg["delay_max"]

    while time.monotonic() < deadline and not stop_event.is_set():
        # Pick payload
        if fixed:
            payload = dict(rng.choice(fixed))
            payload["bucket"] = "easy"
        else:
            payload = gen.next_request()

        bucket = payload.pop("bucket", "easy")
        expression = payload.get("expression", "")
        chamber = payload.get("chamber", "both")
        page = payload.get("page", 1)

        url = f"{base_url}/api/search"
        t_start = time.monotonic()
        ts = time.time()
        status_code = 0
        latency_s = 0.0
        timed_out = False
        response_valid = False
        error_msg = ""

        try:
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout_s),
            ) as resp:
                latency_s = time.monotonic() - t_start
                status_code = resp.status
                try:
                    data = await resp.json(content_type=None)
                    response_valid = isinstance(data, dict) and "results" in data
                except Exception:
                    response_valid = False
        except asyncio.TimeoutError:
            latency_s = time.monotonic() - t_start
            timed_out = True
            status_code = 0
            error_msg = "timeout"
        except aiohttp.ClientConnectionError as e:
            latency_s = time.monotonic() - t_start
            status_code = 0
            error_msg = f"connection_error: {type(e).__name__}"
        except Exception as e:
            latency_s = time.monotonic() - t_start
            status_code = 0
            error_msg = f"unexpected: {type(e).__name__}: {e}"

        log.append(RequestRow(
            timestamp=ts,
            scenario=log.scenario,
            bucket=bucket,
            expression=expression,
            chamber=chamber,
            page=page,
            status_code=status_code,
            latency_s=latency_s,
            timed_out=timed_out,
            response_valid=response_valid,
            error_msg=error_msg,
        ))

        # Jitter sleep between requests
        if delay_min == delay_max == 0:
            await asyncio.sleep(0)
        else:
            sleep_s = rng.uniform(delay_min, delay_max)
            try:
                await asyncio.wait_for(
                    asyncio.shield(stop_event.wait()),
                    timeout=sleep_s,
                )
            except asyncio.TimeoutError:
                pass


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

async def run_scenario(
    base_url: str,
    scenario_name: str,
    scenario_cfg: dict,
    out_dir: Path,
    timeout_s: float,
    no_plots: bool,
    seed: int,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    plots_dir = out_dir / "plots"

    csv_path = out_dir / "requests.csv"
    log = RequestLog(csv_path, scenario_name)

    gen = QueryGenerator(weights=scenario_cfg["weights"], seed=seed)
    users_n = scenario_cfg["users"]
    duration_s = scenario_cfg["duration"]
    deadline = time.monotonic() + duration_s

    stop_event = asyncio.Event()

    print(f"\nScenario : {scenario_name}")
    print(f"Users    : {users_n}")
    print(f"Duration : {duration_s}s")
    print(f"URL      : {base_url}")
    print(f"Output   : {out_dir}\n")

    connector = aiohttp.TCPConnector(limit=users_n + 10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.create_task(
                virtual_user(
                    user_id=i,
                    base_url=base_url,
                    scenario_cfg=scenario_cfg,
                    gen=gen,
                    log=log,
                    deadline=deadline,
                    timeout_s=timeout_s,
                    session=session,
                    stop_event=stop_event,
                )
            )
            for i in range(users_n)
        ]

        # Progress ticker
        ticker_task = asyncio.create_task(
            _progress_ticker(log, duration_s, stop_event)
        )

        def _handle_interrupt():
            print("\nInterrupt received — finishing in-flight requests...")
            stop_event.set()

        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, _handle_interrupt)
        except NotImplementedError:
            pass  # Windows

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            stop_event.set()
            ticker_task.cancel()
            try:
                await ticker_task
            except asyncio.CancelledError:
                pass

    # Flush & compute
    log.close()
    rows = log.rows()
    stats = compute_stats(rows, scenario_name, timeout_s)
    write_summary_json(stats, out_dir / "summary.json")
    write_summary_md(stats, out_dir / "summary.md")
    print_live_summary(stats)

    if not no_plots and rows:
        generate_all_plots(rows, plots_dir)
        print(f"Plots saved to {plots_dir}")

    print(f"Results  : {out_dir}\n")
    return stats


async def _progress_ticker(log: RequestLog, total_s: float, stop_event: asyncio.Event) -> None:
    start = time.monotonic()
    while not stop_event.is_set():
        await asyncio.sleep(15)
        elapsed = time.monotonic() - start
        rows = log.rows()
        n = len(rows)
        pct = min(100, int(elapsed / total_s * 100))
        print(f"  [{pct:3d}%] {int(elapsed)}s elapsed — {n} requests completed")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_weights(s: str) -> tuple[float, float, float, float]:
    parts = s.split(":")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("weights must be 4 colon-separated numbers, e.g. 40:30:20:10")
    return tuple(float(p) for p in parts)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hansard Search load tester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--url", default="http://localhost:5000", help="Base URL of the webapp")
    parser.add_argument(
        "--scenario",
        choices=list(SCENARIOS),
        default="steady_state",
        help="Test scenario to run (default: steady_state)",
    )
    parser.add_argument("--users", type=int, help="Override scenario user count")
    parser.add_argument("--duration", type=int, help="Override scenario duration (seconds)")
    parser.add_argument("--delay-mean", type=float, help="Override mean inter-request delay (s)")
    parser.add_argument("--timeout", type=float, default=10.0, help="Per-request timeout (default: 10s)")
    parser.add_argument(
        "--weights",
        type=parse_weights,
        default=None,
        metavar="easy:medium:heavy:worst",
        help="Override bucket weights, e.g. 40:30:20:10",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory (default: load_test/results/run_YYYYMMDD_HHMMSS)",
    )
    parser.add_argument("--no-plots", action="store_true", help="Skip matplotlib plot generation")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")

    args = parser.parse_args()

    cfg = dict(SCENARIOS[args.scenario])  # copy

    if args.users is not None:
        cfg["users"] = args.users
    if args.duration is not None:
        cfg["duration"] = args.duration
    if args.delay_mean is not None:
        half = args.delay_mean * 0.5
        cfg["delay_min"] = max(0.0, args.delay_mean - half)
        cfg["delay_max"] = args.delay_mean + half
    if args.weights is not None:
        cfg["weights"] = args.weights

    if args.out_dir is None:
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_root = _HERE / "results"
        out_dir = results_root / f"run_{run_ts}"
    else:
        out_dir = args.out_dir

    asyncio.run(
        run_scenario(
            base_url=args.url.rstrip("/"),
            scenario_name=args.scenario,
            scenario_cfg=cfg,
            out_dir=out_dir,
            timeout_s=args.timeout,
            no_plots=args.no_plots,
            seed=args.seed,
        )
    )


if __name__ == "__main__":
    main()
