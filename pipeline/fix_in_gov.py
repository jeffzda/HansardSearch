#!/usr/bin/env python3
"""
fix_in_gov.py — Recompute the in_gov column for both corpus parquet files.

The APH Hansard XML carried a per-utterance <in.gov> flag that was reliable
only up to ~2011.  From 2012 onward every tag is self-closing (<in.gov />,
parsed as null → 0), so the entire column is wrong for the last 13 years.

Fix: derive in_gov from the known government timeline (which party held
power on each date) and the speaker's party column.

Government periods (House of Representatives swearing-in dates):
  1996-03-11 – 2007-11-23  Coalition (LP, LNP, NP, NATS, CLP)
  2007-11-24 – 2013-09-17  Labor     (ALP)
  2013-09-18 – 2022-05-22  Coalition (LP, LNP, NP, NATS, CLP)
  2022-05-23 – present     Labor     (ALP)

"In government" = speaker's party is one of the governing parties on that date.
Senate usage follows the same convention (government = lower-house majority).
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

CORPUS_FILES = [
    ROOT / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.parquet",
    ROOT / "data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet",
]

# ── Government timeline ───────────────────────────────────────────────────────
# Each entry: (start_date_inclusive, end_date_inclusive, governing_parties_set)
COALITION = {"LP", "LNP", "NP", "NATS", "CLP"}
LABOR     = {"ALP"}

GOV_PERIODS = [
    ("1996-03-11", "2007-11-23", COALITION),
    ("2007-11-24", "2013-09-17", LABOR),
    ("2013-09-18", "2022-05-22", COALITION),
    ("2022-05-23", "2099-12-31", LABOR),
]

def build_in_gov(df: pd.DataFrame) -> pd.Series:
    """Return a 0/1 Series computed from date + party."""
    dates  = pd.to_datetime(df["date"], errors="coerce")
    party  = df["party"].fillna("")
    result = pd.Series(0, index=df.index, dtype="int64")

    for start, end, gov_parties in GOV_PERIODS:
        start_dt = pd.Timestamp(start)
        end_dt   = pd.Timestamp(end)
        mask = (dates >= start_dt) & (dates <= end_dt) & party.isin(gov_parties)
        result[mask] = 1

    return result


def fix_corpus(path: Path) -> None:
    print(f"Loading {path.name}…", flush=True)
    df = pd.read_parquet(path)

    old_sum = int(df["in_gov"].sum())
    df["in_gov"] = build_in_gov(df)
    new_sum = int(df["in_gov"].sum())

    print(f"  in_gov sum: {old_sum:,} → {new_sum:,}  (diff {new_sum - old_sum:+,})")

    # Sanity check: show gov % by year
    df["_year"] = pd.to_datetime(df["date"], errors="coerce").dt.year
    check = df.groupby("_year")["in_gov"].mean().round(2)
    print("  Gov fraction by year (sample):")
    print(check.tail(15).to_string())
    df.drop(columns=["_year"], inplace=True)

    df.to_parquet(path, index=False)
    print(f"  Saved → {path}\n")


if __name__ == "__main__":
    for p in CORPUS_FILES:
        if p.exists():
            fix_corpus(p)
        else:
            print(f"  SKIP (not found): {p}")
