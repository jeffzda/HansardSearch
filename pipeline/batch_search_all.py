#!/usr/bin/env python3
"""
batch_search_all.py — Parallel batch search across all org types.

Strategy:
  - Load both corpora ONCE in the main process.
  - On Linux, ProcessPoolExecutor uses fork, so child processes inherit the
    in-memory DataFrames via copy-on-write without re-loading from disk.
  - Each worker searches one (org, chamber) pair and returns the result.
  - Up to MAX_WORKERS parallel searches run simultaneously.

Usage:
    python batch_search_all.py                  # all types
    python batch_search_all.py --type trade_unions business_industry
    python batch_search_all.py --skip-existing  # skip if matches.csv already present
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd

HERE  = Path(__file__).resolve().parent
ROOT  = HERE.parent
CASE  = ROOT / "case_studies"
SENATE_PATH = ROOT / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.parquet"
HOUSE_PATH  = ROOT / "data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet"

MAX_WORKERS = min(32, os.cpu_count() or 8)

# ── Fork-safe globals (set before pool creation, inherited by workers) ────────
_SENATE_DF: pd.DataFrame | None = None
_HOUSE_DF:  pd.DataFrame | None = None

_NL_RE   = re.compile(r"\s*\n\s*")
_HEADING_SKIP_RE = re.compile(
    r"took the chair|adjourned|resumed|interrupted|suspended|"
    r"the chair was taken|presiding|in the chair|"
    r"the (?:house|senate) divided|question so resolved|"
    r"question negatived|bill read|ordered that",
    re.IGNORECASE,
)


def _flatten(text: str) -> str:
    return _NL_RE.sub(" ", text).strip()


def _safe(series: pd.Series, key: str, default: str = "") -> str:
    v = series.get(key, None)
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    return s if s not in ("nan", "None", "NaT") else default


def _find_debate_heading(day_df: pd.DataFrame, pos: int) -> str:
    for i in range(pos - 1, -1, -1):
        name = _safe(day_df.iloc[i], "name").lower()
        if name in ("stage direction", "business start"):
            body = _safe(day_df.iloc[i], "body")
            if body and not _HEADING_SKIP_RE.search(body):
                return body
    return ""


def _count_hits(text: str, aliases: list[str]) -> int:
    return sum(text.count(a) for a in aliases)


def _worker(chamber: str, folder: str, aliases: list[str]) -> tuple[str, str, pd.DataFrame]:
    """
    Worker function — runs inside a forked child process.
    Reads the corpus from the fork-inherited global, returns (folder, chamber, df).
    """
    df = _SENATE_DF if chamber == "senate" else _HOUSE_DF

    df = df.copy()
    df["body"] = df["body"].fillna("").astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    mask = pd.Series(False, index=df.index)
    for alias in aliases:
        mask |= df["body"].str.contains(re.escape(alias), regex=True, na=False, case=True)

    matched = df[mask].copy()
    if matched.empty:
        return folder, chamber, pd.DataFrame()

    match_dates = set(matched["date"].unique())
    day_lookup: dict[str, pd.DataFrame] = {
        str(date): grp.sort_values("order").reset_index(drop=True)
        for date, grp in df[df["date"].isin(match_dates)].groupby("date")
    }

    enriched: list[dict] = []
    for _, row in matched.iterrows():
        date_val  = str(row["date"])
        order_val = row.get("order")
        day_df    = day_lookup.get(date_val, pd.DataFrame())
        body_text = str(row.get("body") or "")

        pos: int | None = None
        if not day_df.empty and order_val is not None:
            hits = (day_df["order"] == order_val).values.nonzero()[0]
            if len(hits):
                pos = int(hits[0])

        debate_heading = _find_debate_heading(day_df, pos) if pos is not None else ""
        hit_terms = sorted({a for a in aliases if a in body_text})
        geo = _safe(row, "state") or _safe(row, "electorate")

        enriched.append({
            "match_id":            f"{chamber}-{date_val}-{_safe(row,'order')}",
            "chamber":             chamber,
            "date":                _safe(row, "date"),
            "time_stamp":          _safe(row, "time_stamp"),
            "page_no":             _safe(row, "page_no"),
            "speech_no":           _safe(row, "speech_no"),
            "row_order":           _safe(row, "order"),
            "speaker_name":        _safe(row, "name"),
            "name_id":             _safe(row, "name_id"),
            "unique_id":           _safe(row, "unique_id"),
            "party":               _safe(row, "party"),
            "partyfacts_id":       _safe(row, "partyfacts_id"),
            "state_or_electorate": geo,
            "gender":              _safe(row, "gender"),
            "in_gov":              _safe(row, "in_gov"),
            "first_speech":        _safe(row, "first_speech"),
            "is_question":         _safe(row, "question"),
            "is_answer":           _safe(row, "answer"),
            "q_in_writing":        _safe(row, "q_in_writing"),
            "is_interjection":     _safe(row, "interject"),
            "div_flag":            _safe(row, "div_flag"),
            "body":                _flatten(body_text),
            "debate_heading":      _flatten(debate_heading),
            "matched_terms":       " | ".join(hit_terms),
            "mention_count":       _count_hits(body_text, aliases),
        })

    return folder, chamber, pd.DataFrame(enriched)


def run_all(type_keys: list[str], skip_existing: bool) -> None:
    from org_types_config import ORG_TYPES

    global _SENATE_DF, _HOUSE_DF

    # ── Load corpora once ─────────────────────────────────────────────────────
    print(f"Loading corpora …")
    _SENATE_DF = pd.read_parquet(SENATE_PATH)
    print(f"  Senate: {len(_SENATE_DF):,} rows")
    _HOUSE_DF = pd.read_parquet(HOUSE_PATH)
    print(f"  House:  {len(_HOUSE_DF):,} rows")
    print(f"  Workers: {MAX_WORKERS}\n")

    # ── Build work queue ──────────────────────────────────────────────────────
    # Each item: (chamber, type_key, folder, aliases)
    tasks: list[tuple[str, str, str, list[str]]] = []
    for type_key in type_keys:
        if type_key not in ORG_TYPES:
            print(f"WARNING: unknown type {type_key!r}, skipping")
            continue
        org_type = ORG_TYPES[type_key]
        for org in org_type.orgs:
            out_dir = CASE / "LLM_free" / type_key / org.folder
            if skip_existing and (out_dir / "matches.csv").exists():
                continue
            for chamber in ("senate", "house"):
                tasks.append((chamber, type_key, org.folder, org.aliases))

    if not tasks:
        print("Nothing to do (all already extracted or no matching types).")
        return

    print(f"Submitting {len(tasks)} (org × chamber) search tasks to pool …\n")

    # Accumulate results per (type_key, folder)
    results: dict[tuple[str, str], list[pd.DataFrame]] = {}

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_worker, chamber, folder, aliases): (type_key, folder, chamber)
                   for chamber, type_key, folder, aliases in tasks}

        completed = 0
        for future in as_completed(futures):
            type_key, folder, chamber = futures[future]
            try:
                _, _, df = future.result()
                if not df.empty:
                    results.setdefault((type_key, folder), []).append(df)
                    print(f"  ✓ {type_key}/{folder}/{chamber}: {len(df):,} turns, "
                          f"{int(df['mention_count'].sum()):,} mentions")
                else:
                    print(f"  – {type_key}/{folder}/{chamber}: no matches")
            except Exception as e:
                print(f"  ✗ {type_key}/{folder}/{chamber}: {e}", file=sys.stderr)
            completed += 1
            if completed % 20 == 0:
                print(f"  [{completed}/{len(tasks)} tasks complete]")

    # ── Save combined results ─────────────────────────────────────────────────
    print(f"\nSaving results …")

    batch_summary: list[dict] = []
    for (type_key, folder), frames in results.items():
        combined = (
            pd.concat(frames, ignore_index=True)
            .sort_values(["date", "chamber", "row_order"])
            .reset_index(drop=True)
        )
        out_dir = CASE / "LLM_free" / type_key / folder
        out_dir.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out_dir / "matches.csv", index=False)

        batch_summary.append({
            "folder":          folder,
            "total_turns":     len(combined),
            "total_mentions":  int(combined["mention_count"].sum()),
            "senate_turns":    len(combined[combined["chamber"] == "senate"]),
            "house_turns":     len(combined[combined["chamber"] == "house"]),
            "date_min":        combined["date"].min(),
            "date_max":        combined["date"].max(),
            "unique_speakers": combined["unique_id"].nunique(),
        })
        print(f"  Saved {folder}/matches.csv  ({len(combined):,} rows)")

    summary_df = pd.DataFrame(batch_summary)
    summary_df.to_csv(CASE / "batch_search_summary.csv", index=False)
    print(f"\nDone. {len(results)} orgs extracted → {CASE / 'batch_search_summary.csv'}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Parallel batch NGO search across all org types.")
    ap.add_argument("--type", nargs="+", metavar="TYPE",
                    help="Org type keys to process (default: all except environmental_ngos)")
    ap.add_argument("--skip-existing", action="store_true",
                    help="Skip orgs that already have a matches.csv")
    args = ap.parse_args()

    from org_types_config import ORG_TYPES
    if args.type:
        type_keys = args.type
    else:
        # Skip env NGOs — already extracted by batch_search_ngos.py
        type_keys = [k for k in ORG_TYPES if k != "environmental_ngos"]

    run_all(type_keys, args.skip_existing)


if __name__ == "__main__":
    main()
