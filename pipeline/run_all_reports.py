#!/usr/bin/env python3
"""
run_all_reports.py — Generate all org-type comparative reports in parallel.

Each org type's report is generated in its own process (matplotlib is not
thread-safe), using up to MAX_WORKERS parallel processes.

Usage:
    python run_all_reports.py                       # all types
    python run_all_reports.py --type trade_unions   # one type
    python run_all_reports.py --workers 8           # limit parallelism
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
CASE = ROOT / "case_studies"

MAX_WORKERS = min(13, os.cpu_count() or 4)   # one process per type max


def _worker(type_key: str, output_path: str) -> tuple[str, float, str | None]:
    """Run in a child process — isolated matplotlib state per report."""
    import sys
    sys.path.insert(0, str(HERE))

    t0 = time.time()
    try:
        from ngo_comparative_analysis import generate_report
        generate_report(type_key, Path(output_path))
        return type_key, time.time() - t0, None
    except Exception as e:
        import traceback
        return type_key, time.time() - t0, traceback.format_exc()


def main() -> None:
    ap = argparse.ArgumentParser(description="Parallel org-type report generator.")
    ap.add_argument("--type", nargs="+", metavar="TYPE",
                    help="Specific type keys to generate (default: all)")
    ap.add_argument("--workers", type=int, default=MAX_WORKERS,
                    help=f"Max parallel processes (default: {MAX_WORKERS})")
    args = ap.parse_args()

    sys.path.insert(0, str(HERE))
    from org_types_config import ORG_TYPES

    type_keys = args.type if args.type else list(ORG_TYPES.keys())
    reports_dir = CASE / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    tasks = [(k, str(reports_dir / f"{k}_report.html")) for k in type_keys
             if k in ORG_TYPES]

    if not tasks:
        print("No valid type keys found.")
        return

    workers = min(args.workers, len(tasks))
    print(f"Generating {len(tasks)} reports with {workers} parallel workers …\n")
    t_start = time.time()

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, k, p): k for k, p in tasks}
        for future in as_completed(futures):
            type_key, elapsed, err = future.result()
            if err:
                print(f"  ✗ {type_key} FAILED ({elapsed:.1f}s):\n{err}")
            else:
                print(f"  ✓ {type_key} ({elapsed:.1f}s)")

    print(f"\nAll done in {time.time() - t_start:.1f}s")
    print(f"Reports in: {reports_dir}")


if __name__ == "__main__":
    main()
