#!/usr/bin/env python3
"""
07c_corpus_committee.py — Assemble committee Hansard daily parquets into a single corpus file.

Usage:
    python 07c_corpus_committee.py \\
        --daily-dir ../data/output/committee/daily \\
        --out-dir   ../data/output/committee/corpus \\
        --prefix    committee_hansard_corpus
"""
import argparse
from pathlib import Path
import pandas as pd

from parallel_utils import eager_threaded_map

def _load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily-dir", required=True)
    ap.add_argument("--out-dir",   required=True)
    ap.add_argument("--prefix",    default="committee_hansard_corpus")
    args = ap.parse_args()

    daily_dir = Path(args.daily_dir)
    out_dir   = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(daily_dir.glob("*.parquet"))
    print(f"Reading {len(files)} parquet files…")
    dfs = eager_threaded_map(_load_parquet, files, desc="Loading committee parquets")
    corpus = pd.concat([df for df in dfs if df is not None], ignore_index=True).sort_values(["date", "order"])

    dates     = pd.to_datetime(corpus["date"], errors="coerce").dropna()
    dates     = dates[dates.dt.year >= 1990]   # filter out malformed dates
    year_min  = dates.min().year
    year_max  = dates.max().year
    n_days    = corpus["date"].nunique()

    stem = f"{args.prefix}_{year_min}_to_{year_max}"
    corpus.to_parquet(out_dir / f"{stem}.parquet", index=False)
    corpus.to_csv(   out_dir / f"{stem}.csv",     index=False)

    print(f"\nCorpus assembled:")
    print(f"  Rows:        {len(corpus):,}")
    print(f"  Files:       {n_days:,}")
    print(f"  Date range:  {year_min}–{year_max}")
    print(f"  Columns:     {', '.join(corpus.columns[:5])} … ({len(corpus.columns)} total)")
    print(f"  Output:      {out_dir / stem}.parquet / .csv")

if __name__ == "__main__":
    main()
