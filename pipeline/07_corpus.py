"""
07_corpus.py — Assemble all daily parquet files into a single corpus.

Uses threaded parallel reads to load daily files quickly (I/O-bound),
then concatenates and writes.  Column layout is inferred from the actual
data so the script works correctly for both Senate (state / senate_flag)
and House (electorate / fedchamb_flag) daily directories without requiring
a --chamber flag.

Usage:
    # Senate
    python 07_corpus.py --daily-dir ../data/output/senate/daily
                        --out-dir   ../data/output/senate/corpus

    # House  (--prefix overrides the output filename stem)
    python 07_corpus.py --daily-dir ../data/output/house/daily
                        --out-dir   ../data/output/house/corpus
                        --prefix    house_hansard_corpus
"""

import argparse
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from parallel_utils import eager_threaded_map


# Canonical column order for each chamber.  The assembler selects the set
# that matches the columns actually present in the daily files.
_SENATE_COLUMNS = [
    "date", "name", "order", "speech_no", "page_no", "time_stamp",
    "name_id", "state", "electorate", "party", "in_gov", "first_speech", "body",
    "question", "answer", "q_in_writing", "div_flag", "gender",
    "unique_id", "interject", "senate_flag", "partyfacts_id",
]
_HOUSE_COLUMNS = [
    "date", "name", "order", "speech_no", "page_no", "time_stamp",
    "name_id", "state", "electorate", "party", "in_gov", "first_speech", "body",
    "question", "answer", "q_in_writing", "div_flag", "gender",
    "unique_id", "interject", "fedchamb_flag", "partyfacts_id",
    "has_embedded_interject",
]


def _detect_columns(sample_cols: list[str]) -> list[str]:
    """Return the canonical column list that best matches the sample columns."""
    house_score  = sum(c in sample_cols for c in _HOUSE_COLUMNS)
    senate_score = sum(c in sample_cols for c in _SENATE_COLUMNS)
    return _HOUSE_COLUMNS if house_score >= senate_score else _SENATE_COLUMNS


def _load_parquet(path_str: str) -> pd.DataFrame:
    """Load one parquet file. Designed to run inside a thread pool."""
    p = Path(path_str)
    df = pd.read_parquet(p)
    if "date" not in df.columns:
        df.insert(0, "date", pd.to_datetime(p.stem).date())
    return df


def assemble_corpus(daily_dir: Path, out_dir: Path,
                    prefix: str = "senate_hansard_corpus") -> None:
    parquet_files = sorted(daily_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"No parquet files found in {daily_dir}")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Assembling corpus from {len(parquet_files)} daily files "
          f"(parallel reads)…")

    # ── Parallel I/O ─────────────────────────────────────────────────────────
    items = [str(p) for p in parquet_files]
    dfs = eager_threaded_map(_load_parquet, items)

    print("Concatenating…")
    corpus = pd.concat(dfs, ignore_index=True)

    # ── Normalise date type ───────────────────────────────────────────────────
    corpus["date"] = pd.to_datetime(corpus["date"]).dt.date

    # ── Column selection ──────────────────────────────────────────────────────
    # Detect chamber from actual columns, then enforce canonical order.
    # Columns absent from the data are added as NULL; extra columns are dropped.
    canon_cols = _detect_columns(list(corpus.columns))
    for col in canon_cols:
        if col not in corpus.columns:
            corpus[col] = None
    corpus = corpus[canon_cols]

    # ── Write ─────────────────────────────────────────────────────────────────
    min_date = corpus["date"].min()
    max_date = corpus["date"].max()
    suffix   = (f"{min_date.year}_to_{max_date.year}"
                if hasattr(min_date, "year") else "full")

    out_name = f"{prefix}_{suffix}"
    corpus.to_parquet(out_dir / f"{out_name}.parquet", index=False)
    corpus.to_csv(out_dir / f"{out_name}.csv", index=False)

    n_days = corpus["date"].nunique()
    n_rows = len(corpus)
    print(f"\nCorpus assembled:")
    print(f"  Rows:        {n_rows:,}")
    print(f"  Sitting days:{n_days:,}")
    print(f"  Date range:  {min_date} to {max_date}")
    print(f"  Columns:     {', '.join(canon_cols[:5])} … ({len(canon_cols)} total)")
    print(f"  Output:      {out_dir / out_name}.parquet / .csv")


def main():
    _here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Assemble Hansard corpus from daily parquet files."
    )
    parser.add_argument("--daily-dir", default=str(_here / "../data/output/senate/daily"))
    parser.add_argument("--out-dir",   default=str(_here / "../data/output/senate/corpus"))
    parser.add_argument("--prefix",    default="senate_hansard_corpus")
    args = parser.parse_args()
    assemble_corpus(Path(args.daily_dir), Path(args.out_dir), prefix=args.prefix)


if __name__ == "__main__":
    main()
