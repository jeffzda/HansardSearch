#!/usr/bin/env python3
"""
batch_search_ngos.py — Run multiple NGO searches across both corpora in one pass.

Loads each corpus once, then runs all searches in memory.
Saves matches.csv + summary.txt to case_studies/<NAME>/ for each NGO.
Designed to feed into ngo_comparative_analysis.py.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
HERE   = Path(__file__).resolve().parent
ROOT   = HERE.parent
CASE   = ROOT / "case_studies"
SENATE = ROOT / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.parquet"
HOUSE  = ROOT / "data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet"

# ── NGO definitions ──────────────────────────────────────────────────────────
# Each entry: (folder_name, display_name, [list of alias strings])
# Aliases are OR-combined; all are case-sensitive substring matches.
#
# Alias curation notes:
#  ACF  — 'ACF' alone included; overwhelmingly refers to Aust. Conservation
#          Foundation in parliamentary speech. Flag outliers in analysis.
#  EDO  — 'Environmental Defenders Office' (historical) | 'EDO' + qualifiers
#  AMCS — 'AMCS' distinctive in Australian env context
#  BirdLife — includes legacy name 'Birds Australia' (pre-2011 rebrand)
#  Landcare — very high volume expected; also a govt program, intentionally
#             broad to capture full parliamentary footprint
#  Climate Council — includes 'Climate Commission' (Gillard govt body 2011-13,
#                    which Abbott defunded; volunteers re-formed as Climate Council)
#  Lock the Gate — 'Lock the Gate Alliance' and variants
#  Sea Shepherd — clean; very distinctive
#  Greenpeace — no qualifier needed; distinctive
#  Wilderness Society — no 'The' prefix to catch all forms
#  AWC — full name only; 'AWC' too ambiguous alone

NGOS: list[tuple[str, str, list[str]]] = [
    (
        "ACF",
        "Australian Conservation Foundation",
        ["Australian Conservation Foundation", "ACF"],
    ),
    (
        "Wilderness_Society",
        "The Wilderness Society",
        ["Wilderness Society"],
    ),
    (
        "Greenpeace",
        "Greenpeace Australia",
        ["Greenpeace"],
    ),
    (
        "BirdLife",
        "BirdLife Australia",
        ["BirdLife Australia", "BirdLife", "Birds Australia",
         "Royal Australasian Ornithologists Union"],
    ),
    (
        "AMCS",
        "Australian Marine Conservation Society",
        ["Australian Marine Conservation Society", "AMCS"],
    ),
    (
        "EDO",
        "Environmental Defenders Office / EDO",
        ["Environmental Defenders Office", "EDO Australia",
         "EDO NSW", "EDO Qld", "EDO Victoria", "Environmental Defender"],
    ),
    (
        "Friends_of_the_Earth",
        "Friends of the Earth Australia",
        ["Friends of the Earth"],
    ),
    (
        "Lock_the_Gate",
        "Lock the Gate Alliance",
        ["Lock the Gate", "LtG Alliance"],
    ),
    (
        "Sea_Shepherd",
        "Sea Shepherd",
        ["Sea Shepherd"],
    ),
    (
        "Climate_Council",
        "Climate Council / Climate Commission",
        ["Climate Council", "Climate Commission"],
    ),
    (
        "Landcare",
        "Landcare Australia",
        ["Landcare Australia", "Landcare"],
    ),
    (
        "AWC",
        "Australian Wildlife Conservancy",
        ["Australian Wildlife Conservancy"],
    ),
    (
        "Bob_Brown_Foundation",
        "Bob Brown Foundation",
        ["Bob Brown Foundation"],
    ),
    (
        "Humane_Society_International",
        "Humane Society International Australia",
        ["Humane Society International", "HSI Australia"],
    ),
]

# ── Helpers (subset of search_corpus.py logic) ───────────────────────────────

_NL_RE   = re.compile(r"\s*\n\s*")
_CTX_SEP = " ¶ "


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


_HEADING_SKIP_RE = re.compile(
    r"took the chair|adjourned|resumed|interrupted|suspended|"
    r"the chair was taken|presiding|in the chair|"
    r"the (?:house|senate) divided|question so resolved|"
    r"question negatived|bill read|ordered that",
    re.IGNORECASE,
)


def _find_debate_heading(day_df: pd.DataFrame, pos: int) -> str:
    for i in range(pos - 1, -1, -1):
        name = _safe(day_df.iloc[i], "name").lower()
        if name in ("stage direction", "business start"):
            body = _safe(day_df.iloc[i], "body")
            if body and not _HEADING_SKIP_RE.search(body):
                return body
    return ""


def _count_hits(text: str, aliases: list[str]) -> int:
    """Count total alias occurrences in text (for mentions-per-turn metric)."""
    return sum(text.count(a) for a in aliases)


def search_one(
    df: pd.DataFrame,
    chamber: str,
    aliases: list[str],
    context_n: int = 0,
) -> pd.DataFrame:
    """Search a pre-loaded corpus DataFrame for any alias match."""
    df = df.copy()
    df["body"] = df["body"].fillna("").astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Boolean OR across all aliases (case-sensitive)
    mask = pd.Series(False, index=df.index)
    for alias in aliases:
        mask |= df["body"].str.contains(re.escape(alias), regex=True, na=False, case=True)

    matched = df[mask].copy()
    if matched.empty:
        return pd.DataFrame()

    # Build per-date sorted lookup (only dates with matches)
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

        ctx_before: list[str] = []
        ctx_after:  list[str] = []
        if pos is not None and context_n > 0:
            for i in range(max(0, pos - context_n), pos):
                r2 = day_df.iloc[i]
                ctx_before.append(f"{_safe(r2,'name')}: {_flatten(_safe(r2,'body'))}")
            for i in range(pos + 1, min(len(day_df), pos + context_n + 1)):
                r2 = day_df.iloc[i]
                ctx_after.append(f"{_safe(r2,'name')}: {_flatten(_safe(r2,'body'))}")

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
            "context_before":      _CTX_SEP.join(ctx_before),
            "context_after":       _CTX_SEP.join(ctx_after),
        })

    return pd.DataFrame(enriched)


# ── Output helpers ────────────────────────────────────────────────────────────

def write_summary(df: pd.DataFrame, path: Path, display_name: str, aliases: list[str]) -> None:
    senate = df[df["chamber"] == "senate"]
    house  = df[df["chamber"] == "house"]
    dates  = pd.to_datetime(df["date"], errors="coerce").dropna()
    df2    = df.copy()
    df2["year"] = pd.to_datetime(df2["date"], errors="coerce").dt.year
    by_year = df2.groupby("year").size().sort_index()
    max_c   = int(by_year.max()) if not by_year.empty else 1
    scale   = min(1.0, 40 / max_c)

    top_speakers = df.groupby("speaker_name").size().sort_values(ascending=False).head(15)
    top_parties  = (
        df[df["party"].notna() & (df["party"] != "")]
        .groupby("party").size().sort_values(ascending=False).head(10)
    )

    lines = [
        f"Search summary — {display_name}",
        f"Aliases    :  {', '.join(repr(a) for a in aliases)}",
        f"Generated  :  {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "─" * 64,
        f"Total speech turns (matches) :  {len(df):>7,}",
        f"  Senate                      :  {len(senate):>7,}",
        f"  House                       :  {len(house):>7,}",
        f"Total alias mentions          :  {int(df['mention_count'].sum()):>7,}",
        f"Avg mentions per speech turn  :  {df['mention_count'].mean():.2f}",
        "",
    ]
    if not dates.empty:
        lines += [
            f"Date range     :  {dates.min().date()} → {dates.max().date()}",
            f"Unique sittings:  {df['date'].nunique():>5,}",
            "",
        ]
    lines += ["Top 15 speakers", "─" * 54, ""]
    for name, count in top_speakers.items():
        lines.append(f"  {str(name):<48}  {count:>5,}")
    lines += ["", "Top 10 parties", "─" * 34, ""]
    for party, count in top_parties.items():
        lines.append(f"  {str(party):<24}  {count:>5,}")
    lines += ["", "Mentions by year", "─" * 54, ""]
    for year, count in by_year.items():
        bar = "█" * max(1, int(count * scale))
        lines.append(f"  {int(year)}  {count:>5,}  {bar}")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    CASE.mkdir(parents=True, exist_ok=True)

    # ── Load corpora once ─────────────────────────────────────────────────────
    print("Loading corpora …")
    senate_df = pd.read_parquet(SENATE)
    print(f"  Senate: {len(senate_df):,} rows")
    house_df  = pd.read_parquet(HOUSE)
    print(f"  House:  {len(house_df):,} rows\n")

    results_summary: list[dict] = []

    for folder, display, aliases in NGOS:
        out_dir = CASE / folder
        out_dir.mkdir(parents=True, exist_ok=True)

        print(f"Searching: {display}")
        print(f"  Aliases: {aliases}")

        frames: list[pd.DataFrame] = []
        for df, chamber in [(senate_df, "senate"), (house_df, "house")]:
            r = search_one(df, chamber, aliases, context_n=0)
            if not r.empty:
                frames.append(r)
                print(f"  {chamber.title()}: {len(r):,} speech turns, "
                      f"{int(r['mention_count'].sum()):,} mentions, "
                      f"avg {r['mention_count'].mean():.2f}/turn")

        if not frames:
            print(f"  → No matches found\n")
            continue

        combined = (
            pd.concat(frames, ignore_index=True)
            .sort_values(["date", "chamber", "row_order"])
            .reset_index(drop=True)
        )

        combined.to_csv(out_dir / "matches.csv", index=False, encoding="utf-8")
        write_summary(combined, out_dir / "summary.txt", display, aliases)

        results_summary.append({
            "org":          display,
            "folder":       folder,
            "total_turns":  len(combined),
            "total_mentions": int(combined["mention_count"].sum()),
            "avg_per_turn": round(combined["mention_count"].mean(), 3),
            "senate_turns": len(combined[combined["chamber"] == "senate"]),
            "house_turns":  len(combined[combined["chamber"] == "house"]),
            "date_min":     combined["date"].min(),
            "date_max":     combined["date"].max(),
            "unique_speakers": combined["unique_id"].nunique(),
        })
        print(f"  → Saved to {out_dir}\n")

    # Save overall summary table
    summary_df = pd.DataFrame(results_summary)
    summary_df.to_csv(CASE / "ngo_batch_summary.csv", index=False)
    print("Batch summary saved to case_studies/ngo_batch_summary.csv")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
