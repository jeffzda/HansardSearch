#!/usr/bin/env python3
"""
06b_fill_first_speech.py — Backfill first_speech flag from lookup entry dates.

The APH XML stopped populating <first.speech>1</first.speech> after ~2009.
This script infers first speeches from state_lookup / electorate_lookup:

  For each name_id and each qualifying parliamentary term:
    1. First term:   flag the earliest substantive speech on/after entry date.
    2. Re-entry term: if the member was absent for >180 days before returning,
       flag the earliest substantive speech on/after the re-entry date too.
       (Re-election maiden speeches are an established parliamentary convention.)

Existing first_speech=1 flags from the XML (pre-2010) are preserved unless
--override is passed.

Outputs: updated daily parquet files + updated corpus file.

Usage:
    python 06b_fill_first_speech.py --chamber senate
    python 06b_fill_first_speech.py --chamber house
    python 06b_fill_first_speech.py --chamber senate --override
    python 06b_fill_first_speech.py --chamber senate --corpus-only
"""
import argparse
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from parallel_utils import eager_map

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

PATHS = {
    "senate": {
        "daily":       ROOT / "data/output/senate/daily",
        "corpus":      ROOT / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.parquet",
        "entry_lookup": ROOT / "data/lookup/state_lookup.csv",
        "entry_col":   "senator_from",
        "end_col":     "senator_to",
    },
    "house": {
        "daily":       ROOT / "data/output/house/daily",
        "corpus":      ROOT / "data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet",
        "entry_lookup": ROOT / "data/lookup/electorate_lookup.csv",
        "entry_col":   "member_from",
        "end_col":     "member_to",
    },
}

# Speech types that count as substantive (not interjections or procedural turns)
SUBSTANTIVE = {"speech", "answer", "question", "written_question"}


def classify_turn(row) -> str:
    if int(row.get("q_in_writing", 0) or 0):
        return "written_question"
    if int(row.get("question", 0) or 0):
        return "question"
    if int(row.get("answer", 0) or 0):
        return "answer"
    if int(row.get("interject", 0) or 0):
        return "interjection"
    return "speech"


REENTRY_GAP_DAYS = 180  # minimum gap to count as a re-entry


def build_entry_date_lookup(lookup_path: Path,
                             entry_col: str,
                             end_col: str) -> list[tuple[str, pd.Timestamp, bool]]:
    """
    Returns a list of (name_id, entry_date, is_reentry) tuples covering:
      • Every member's first (earliest) term.
      • Any re-entry term where the gap since the previous term exceeds
        REENTRY_GAP_DAYS days.
    """
    df = pd.read_csv(lookup_path)
    df[entry_col] = pd.to_datetime(df[entry_col], errors="coerce")
    df[end_col]   = pd.to_datetime(df[end_col],   errors="coerce")
    df = df.dropna(subset=[entry_col])

    entries: list[tuple[str, pd.Timestamp, bool]] = []

    for name_id, grp in df.groupby("name_id"):
        grp = grp.sort_values(entry_col).reset_index(drop=True)

        for i, row in grp.iterrows():
            entry_date = row[entry_col]
            if i == 0:
                entries.append((str(name_id), entry_date, False))
            else:
                prev_end = grp.loc[i - 1, end_col]
                if pd.notna(prev_end):
                    gap = (entry_date - prev_end).days
                    if gap > REENTRY_GAP_DAYS:
                        entries.append((str(name_id), entry_date, True))

    return entries


def compute_first_speech_rows(corpus: pd.DataFrame,
                               entries: list[tuple[str, pd.Timestamp, bool]],
                               override: bool) -> set[tuple]:
    """
    Returns a set of (date_str, order) tuples that should have first_speech=1.

    entries is a list of (name_id, entry_date, is_reentry).

    For first terms:  find the earliest substantive turn on/after entry_date.
    For re-entries:   same logic but never fall back to pre-entry turns —
                      the member must have a speech on/after the re-entry date.

    If override=False, entries whose name_id already has first_speech=1 in the
    corpus are skipped for the first-term case (XML flags preserved).
    Re-entry entries are always evaluated regardless, since the XML never
    flagged re-entry speeches after ~2009.
    """
    corpus = corpus.copy()
    corpus["date_ts"] = pd.to_datetime(corpus["date"], errors="coerce")
    corpus["order_int"] = pd.to_numeric(corpus["order"], errors="coerce")

    for col in ("question", "answer", "q_in_writing", "interject"):
        corpus[col] = pd.to_numeric(corpus[col], errors="coerce").fillna(0).astype(int)
    corpus["speech_type"] = corpus.apply(classify_turn, axis=1)

    # name_ids already flagged in XML — skip their FIRST-TERM entry if not overriding
    if not override:
        already_flagged = set(
            corpus[corpus["first_speech"] == 1]["name_id"].dropna().unique()
        )
    else:
        already_flagged = set()

    # Only substantive turns
    sub = corpus[
        corpus["speech_type"].isin(SUBSTANTIVE) &
        corpus["name_id"].notna() &
        (corpus["name_id"] != "")
    ].copy()

    result: set[tuple] = set()

    for name_id, entry_date, is_reentry in entries:
        # Skip first-term entries for name_ids with existing XML flags
        if not is_reentry and not override and name_id in already_flagged:
            continue

        turns = sub[sub["name_id"] == name_id].copy()
        if turns.empty:
            continue

        eligible = turns[turns["date_ts"] >= entry_date]

        if eligible.empty:
            if is_reentry:
                # No speeches found on/after re-entry date — skip
                continue
            # First-term member entered before corpus start: use first corpus turn
            eligible = turns

        first = eligible.sort_values(["date_ts", "order_int"]).iloc[0]
        result.add((str(first["date"]), first["order_int"]))

    return result


# ── Worker globals (module-level for pickle compatibility) ────────────────────

_KEYS_BY_DATE: dict[str, set] | None = None
_OVERRIDE: bool | None = None


def _init_worker(keys_and_override: tuple) -> None:
    """Set shared state once per worker process."""
    global _KEYS_BY_DATE, _OVERRIDE
    _KEYS_BY_DATE, _OVERRIDE = keys_and_override


def _update_one(path_str: str) -> tuple[str, int, int]:
    """
    Update first_speech flags in one daily parquet file.
    Returns (filepath_str, files_changed, rows_flagged).
    """
    f = Path(path_str)
    stem = f.stem
    date_str = stem[:10]

    if date_str not in _KEYS_BY_DATE:
        return (path_str, 0, 0)

    df = pd.read_parquet(f)
    df["order_int"] = pd.to_numeric(df["order"], errors="coerce")

    orders = _KEYS_BY_DATE[date_str]
    mask   = df["order_int"].isin(orders)

    if not mask.any():
        return (path_str, 0, 0)

    if _OVERRIDE:
        df.loc[mask, "first_speech"] = 1
    else:
        df.loc[mask & (df["first_speech"] != 1), "first_speech"] = 1

    df = df.drop(columns=["order_int"])
    df.to_parquet(f, index=False)
    return (path_str, 1, int(mask.sum()))


def update_daily_files(daily_dir: Path,
                        first_speech_keys: set[tuple],
                        override: bool) -> tuple[int, int]:
    """
    Update daily parquet files: set first_speech=1 for matching (date, order) pairs.
    Returns (files_updated, rows_flagged).
    """
    files = sorted(daily_dir.glob("*.parquet"))

    # Group keys by date for fast lookup
    by_date: dict[str, set] = {}
    for date_str, order in first_speech_keys:
        by_date.setdefault(date_str, set()).add(order)

    items   = [str(f) for f in files]
    results = eager_map(
        _update_one,
        items,
        initializer=_init_worker,
        initargs=((by_date, override),),
        desc="Updating daily files",
        unit="file",
    )

    files_updated = sum(r[1] for r in results)
    rows_flagged  = sum(r[2] for r in results)
    return files_updated, rows_flagged


def update_corpus(corpus_path: Path,
                   first_speech_keys: set[tuple],
                   override: bool) -> int:
    """Update corpus parquet in-place. Returns rows flagged."""
    df = pd.read_parquet(corpus_path)
    df["order_int"] = pd.to_numeric(df["order"], errors="coerce")
    # Normalise date to string (corpus may store datetime.date objects)
    df["date_str"] = df["date"].astype(str)

    by_date: dict[str, set] = {}
    for date_str, order in first_speech_keys:
        by_date.setdefault(date_str, set()).add(order)

    rows_flagged = 0
    for date_str, orders in by_date.items():
        mask = (df["date_str"] == date_str) & (df["order_int"].isin(orders))
        if not mask.any():
            continue
        if override:
            df.loc[mask, "first_speech"] = 1
        else:
            df.loc[mask & (df["first_speech"] != 1), "first_speech"] = 1
        rows_flagged += int(mask.sum())

    df = df.drop(columns=["order_int", "date_str"])
    df.to_parquet(corpus_path, index=False)
    return rows_flagged


def main():
    ap = argparse.ArgumentParser(description="Backfill first_speech from entry-date lookups.")
    ap.add_argument("--chamber",     choices=["senate", "house"], required=True)
    ap.add_argument("--override",    action="store_true",
                    help="Override existing first_speech=1 flags from XML.")
    ap.add_argument("--corpus-only", action="store_true",
                    help="Update corpus file only, skip daily files.")
    args = ap.parse_args()

    cfg = PATHS[args.chamber]

    # ── Build entry date lookup ────────────────────────────────────────────────
    print(f"Building entry-date lookup from {cfg['entry_lookup'].name}…")
    entries = build_entry_date_lookup(cfg["entry_lookup"], cfg["entry_col"], cfg["end_col"])
    n_first   = sum(1 for _, _, r in entries if not r)
    n_reentry = sum(1 for _, _, r in entries if r)
    print(f"  {n_first:,} first-term entries, {n_reentry:,} re-entry terms")

    # ── Load corpus and compute first-speech rows ──────────────────────────────
    print(f"Loading corpus to identify first-speech rows…")
    corpus = pd.read_parquet(cfg["corpus"])
    print(f"  {len(corpus):,} rows loaded")

    first_speech_keys = compute_first_speech_rows(corpus, entries, args.override)
    n_reentry_keys = 0  # counted below after breakdown
    print(f"  {len(first_speech_keys):,} first-speech rows identified")

    # Breakdown: how many are new vs already known
    existing = int((corpus["first_speech"] == 1).sum())
    print(f"  Existing first_speech=1 in corpus: {existing:,}")

    # ── Update corpus ──────────────────────────────────────────────────────────
    print(f"Updating corpus: {cfg['corpus'].name}…")
    rows_flagged = update_corpus(cfg["corpus"], first_speech_keys, args.override)
    print(f"  {rows_flagged:,} rows updated in corpus")

    # ── Update daily files ─────────────────────────────────────────────────────
    if not args.corpus_only:
        print(f"Updating daily parquet files in {cfg['daily']}…")
        files_updated, rows_flagged_daily = update_daily_files(
            cfg["daily"], first_speech_keys, args.override
        )
        print(f"  {files_updated:,} daily files updated, {rows_flagged_daily:,} rows flagged")

    # ── Summary ────────────────────────────────────────────────────────────────
    updated_corpus = pd.read_parquet(cfg["corpus"])
    total_now = int((updated_corpus["first_speech"] == 1).sum())
    updated_corpus["date_ts"] = pd.to_datetime(updated_corpus["date"], errors="coerce")
    updated_corpus["year"] = updated_corpus["date_ts"].dt.year
    by_year = updated_corpus[updated_corpus["first_speech"] == 1].groupby("year").size()

    print(f"\nDone.")
    print(f"  Total first_speech=1 in corpus: {total_now:,}  (was {existing:,})")
    print(f"\nFirst speeches by year:")
    print(by_year.to_string())


if __name__ == "__main__":
    main()
