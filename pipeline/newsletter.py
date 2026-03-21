#!/usr/bin/env python3
"""
newsletter.py — Hansard Weekly Newsletter Pipeline

Identifies the most recent sitting week, extracts trending bigrams/trigrams via
TF-IDF novelty scoring, searches the full corpus for every historical mention of
each phrase, and produces a self-contained HTML newsletter with charts and Claude
narrative histories.  Safe to run at any cadence (weekly cron, manual, etc.).

Usage
-----
    python pipeline/newsletter.py                      # auto-detect last sitting week
    python pipeline/newsletter.py --week 2026-W10     # explicit ISO week
    python pipeline/newsletter.py --dry-run            # placeholder narratives, no API cost
    python pipeline/newsletter.py --phrases 1          # phrases per chamber (default 1)
    python pipeline/newsletter.py --min-count 3        # min week mentions (default 3)
    python pipeline/newsletter.py --lookback 20        # weeks to search back (default 12)
    python pipeline/newsletter.py --model claude-opus-4-6
    python pipeline/newsletter.py --no-cache           # disable prompt caching
    python pipeline/newsletter.py --no-citations       # suppress Hansard citations block
    python pipeline/newsletter.py --max-citations 3    # citations per phrase (default 2, max 3)
    python pipeline/newsletter.py --out-dir /tmp/test  # override output directory

Output
------
    newsletters/
      manifest.json               phrase deduplication index (all sitting weeks)
      YYYY-WNN_issue-N/
        newsletter.html           self-contained HTML (inline CSS, base64 charts)
        phrases_senate.csv        scored phrase table for Senate
        phrases_house.csv         scored phrase table for House
        run_log.jsonl             event log (timings, API usage, cost)

How it works
------------
1.  Sitting week — reads data/lookup/session_info_all.csv to find the most
    recent ISO week in which parliament sat (Senate or House).  Falls back up to
    --lookback weeks.  Use --week to override.

2.  Phrase extraction — loads only the current week's rows from the FTS database
    (~1-2k rows), counts bigrams and trigrams, and ranks by raw frequency.
    Procedural parliamentary language is filtered out.

3.  Manifest deduplication — phrases used in a prior issue for the same sitting
    week are excluded.  Re-running produces issue-2, issue-3, etc. with fresh
    phrases, until the week is exhausted.

4.  Editorial filter — Claude Haiku reviews the top ~20 TF-IDF candidates and
    selects the most politically interesting phrase (one reflecting a real debate,
    controversy, or policy moment rather than a procedural cliché).

5.  Historical search — queries the FTS5 index (hansard_fts.db) with a quoted
    phrase match.  Only matching rows are fetched from the speeches table — no
    full corpus load required.

6.  Body selection for Claude — if all matched rows fit within the 150k-token
    budget they are all sent.  If not, a proportional + speaker-stratified sample
    is drawn: rows per year allocated proportionally to year frequency (capped at
    20/year), with spike years prioritised by speaker frequency then phrase
    density.  The first-ever mention is always included.

7.  Claude narrative — one API call per phrase using claude-opus-4-6 with
    prompt caching (cache_control: ephemeral on system block).  Claude receives
    full corpus statistics (year trend, party breakdown, gov/opp split, top
    speakers, spike annotations) plus the selected full speech bodies.  Outputs
    a narrative history (length determined by the material) plus a formal citations block.

8.  Output — self-contained HTML newsletter with inline CSS, base64-encoded
    matplotlib charts (year trend + amber spike highlights, party breakdown,
    gov/opp), top-5 speakers table, first-mention callout, and optional Hansard
    citations.

Memory management
-----------------
Only the sitting week rows (~1-2k per chamber) are ever loaded into memory.
Historical phrase matching uses the FTS5 index — only matched rows are fetched.
No full corpus load at any stage.  Peak RAM is negligible.
Safe for the 7.8GB/no-swap production server.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import re
import sqlite3
import sys
import time
import warnings
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

# Import boolean expression parser from search_corpus (same logic as webapp)
sys.path.insert(0, str(Path(__file__).parent))
from search_corpus import parse_expression, _ast_to_fts5  # noqa: E402

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# ── Paths ──────────────────────────────────────────────────────────────────────

HERE    = Path(__file__).resolve().parent
ROOT    = HERE.parent
SESSION_CSV   = ROOT / "data/lookup/session_info_all.csv"
NEWSLETTERS   = ROOT / "newsletters"
MANIFEST_PATH = NEWSLETTERS / "manifest.json"
FTS_DB_PATH   = ROOT / "data/output/fts/hansard_fts.db"


# ── Constants (verbatim from ngo_comparative_analysis.py lines 74-118) ────────

CORPUS_TURNS_BY_YEAR: dict[int, int] = {
    1998: 23286 + 21446,
    1999: 29496 + 21009,
    2000: 24600 + 23168,
    2001: 17528 + 18934,
    2002: 17822 + 22691,
    2003: 18033 + 23171,
    2004: 13863 + 17610,
    2005: 16859 + 21995,
    2006: 16178 + 24108,
    2007: 12763 + 17154,
    2008: 15675 + 21406,
    2009: 19375 + 21131,
    2010: 14027 + 16503,
    2011: 25227 + 26029,
    2012: 22538 + 25761,
    2013: 13486 + 18639,
    2014: 22456 + 29461,
    2015: 22571 + 29438,
    2016: 17153 + 19735,
    2017: 22699 + 25146,
    2018: 23682 + 24660,
    2019: 14787 + 17810,
    2020: 17449 + 21368,
    2021: 19031 + 24553,
    2022: 15021 + 17511,
    2023: 32053 + 29437,
    2024: 24121 + 27672,
    2025: 18085 + 16360,
    2026: 18085 + 16360,   # TODO: update when 2026 full-year counts are available
}

PARLIAMENT_YEARS: dict[int, tuple[int, int]] = {
    36: (1996, 1998),
    37: (1998, 2001),
    38: (2001, 2004),
    39: (2004, 2007),
    40: (2007, 2010),
    41: (2010, 2013),
    42: (2013, 2016),
    43: (2016, 2019),
    44: (2019, 2022),
    45: (2022, 2025),
    46: (2025, 2028),
}

# Verbatim from analyse_case_study.py lines 36-42, 58
PARTY_COLOURS: dict[str, str] = {
    "Coalition": "#003087",
    "Labor":     "#E3231F",
    "Greens":    "#009B55",
    "Nationals": "#006633",
    "Other":     "#888888",
}
PARTY_ORDER: list[str] = ["Coalition", "Labor", "Greens", "Nationals", "Other"]

PRICING = {
    "claude-opus-4-6":           dict(input=5.00, output=25.00, cache_write=6.25,  cache_read=0.50),
    "claude-sonnet-4-6":         dict(input=3.00, output=15.00, cache_write=3.75,  cache_read=0.30),
    "claude-haiku-4-5-20251001": dict(input=1.00, output=5.00,  cache_write=1.25,  cache_read=0.10),
}

HAIKU_MODEL = "claude-haiku-4-5-20251001"

# Opus 4.6 and Sonnet 4.6 both have a 1M token context window.
# Budget leaves ~100k headroom for system prompt, user-prompt stats block, and response.
EXCERPT_TOKEN_BUDGET = 900_000


# ── Procedural stopwords ──────────────────────────────────────────────────────



# ── Logging ───────────────────────────────────────────────────────────────────

def _log(log_path: Path, event: dict) -> None:
    event.setdefault("ts", datetime.utcnow().isoformat())
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


# ── Phase 1: Find last sitting week ──────────────────────────────────────────

def _normalise_chamber(value: str) -> str:
    """Normalise chamber string to 'SENATE' or 'REPS'."""
    v = str(value).strip().upper()
    if v in ("SENATE",):
        return "SENATE"
    if v in ("REPS", "HOUSE OF REPS", "HOUSE OF REPRESENTATIVES"):
        return "REPS"
    return v


def find_last_sitting_week(lookback_weeks: int = 12) -> tuple[date, date, str]:
    """Scan backward from last calendar week to find the most recent sitting week.

    Queries the FTS database directly (authoritative, always current) and falls
    back to session_info_all.csv for any dates not yet indexed.
    """
    # Primary: FTS DB has every parsed sitting day for both chambers
    sitting_dates: set[date] = set()
    if FTS_DB_PATH.exists():
        conn = sqlite3.connect(str(FTS_DB_PATH), check_same_thread=False)
        rows = conn.execute("SELECT DISTINCT date FROM speeches").fetchall()
        conn.close()
        for (d,) in rows:
            try:
                sitting_dates.add(date.fromisoformat(d))
            except (ValueError, TypeError):
                pass

    # Fallback: also include any dates in the session CSV not yet in the DB
    if SESSION_CSV.exists():
        df = pd.read_csv(SESSION_CSV, dtype=str)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for d in df["date"].dropna().dt.date:
            sitting_dates.add(d)

    today = date.today()
    # Start from last completed calendar week (Monday)
    last_monday = today - timedelta(days=today.weekday() + 7)

    for week_offset in range(lookback_weeks):
        week_start = last_monday - timedelta(weeks=week_offset)
        week_end   = week_start + timedelta(days=6)
        if any(week_start <= d <= week_end for d in sitting_dates):
            iso_year, iso_week, _ = week_start.isocalendar()
            week_label = f"{iso_year}-W{iso_week:02d}"
            return week_start, week_end, week_label

    raise RuntimeError(
        f"No sitting week found in the last {lookback_weeks} weeks. "
        f"Use --week YYYY-WNN to specify a week, or increase --lookback."
    )


def parse_week_label(week_label: str) -> tuple[date, date, str]:
    """Parse 'YYYY-WNN' into (week_start, week_end, week_label)."""
    m = re.fullmatch(r"(\d{4})-W(\d{1,2})", week_label)
    if not m:
        raise ValueError(f"Invalid week format: {week_label!r}. Use YYYY-WNN (e.g. 2026-W10).")
    year, week = int(m.group(1)), int(m.group(2))
    # ISO week date: year, week, Monday=1
    week_start = date.fromisocalendar(year, week, 1)
    week_end   = week_start + timedelta(days=6)
    return week_start, week_end, f"{year}-W{week:02d}"


def get_sitting_days(week_start: date, week_end: date) -> dict[str, list[str]]:
    """Return sitting days per chamber for the given week.

    Queries the FTS database directly so results are always current.
    """
    result: dict[str, list[str]] = {"SENATE": [], "REPS": []}
    if FTS_DB_PATH.exists():
        conn = sqlite3.connect(str(FTS_DB_PATH), check_same_thread=False)
        rows = conn.execute(
            "SELECT DISTINCT date, chamber FROM speeches WHERE date >= ? AND date <= ?",
            (week_start.isoformat(), week_end.isoformat()),
        ).fetchall()
        conn.close()
        for d, chamber in rows:
            key = "SENATE" if chamber == "senate" else "REPS"
            if d not in result[key]:
                result[key].append(d)
        result["SENATE"] = sorted(result["SENATE"])
        result["REPS"]   = sorted(result["REPS"])
    return result


# ── Phase 2: Manifest ─────────────────────────────────────────────────────────

def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return {}


def get_used_phrases(manifest: dict, week_label: str) -> dict[str, set[str]]:
    entry = manifest.get(week_label, {})
    return {
        "senate": set(entry.get("used_senate", [])),
        "house":  set(entry.get("used_house",  [])),
        "joint":  set(entry.get("used_joint",  [])),
    }


def next_issue_label(manifest: dict, week_label: str) -> str:
    n = len(manifest.get(week_label, {}).get("issues", [])) + 1
    return f"{week_label}_issue-{n}"


def update_manifest(
    manifest: dict,
    week_label: str,
    issue_label: str,
    new_phrases: dict[str, list[str]],
) -> None:
    entry = manifest.setdefault(week_label, {"issues": [], "used_senate": [], "used_house": [], "used_joint": []})
    if issue_label not in entry["issues"]:
        entry["issues"].append(issue_label)
    for chamber_key, phrases in new_phrases.items():
        field_key = f"used_{chamber_key}"
        existing  = set(entry.get(field_key, []))
        existing.update(phrases)
        entry[field_key] = sorted(existing)

    # Atomic write
    tmp = MANIFEST_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp.replace(MANIFEST_PATH)


# ── Phase 3: Corpus loading (SQLite FTS DB) ───────────────────────────────────

def load_week_from_fts(conn: sqlite3.Connection, chamber_key: str, sitting_days: list[str]) -> pd.DataFrame:
    """Load sitting-week rows for a chamber from the FTS database.

    Uses the speeches_chamber_date index for efficient date-filtered lookup.
    chamber_key: 'senate', 'house', or None (both chambers).
    """
    if not sitting_days:
        return pd.DataFrame()
    placeholders = ",".join("?" * len(sitting_days))
    if chamber_key is not None:
        df = pd.read_sql_query(
            f"SELECT date, name, name_id, party, in_gov, chamber, body "
            f"FROM speeches WHERE chamber = ? AND date IN ({placeholders})",
            conn, params=[chamber_key] + sitting_days,
        )
    else:
        df = pd.read_sql_query(
            f"SELECT date, name, name_id, party, in_gov, chamber, body "
            f"FROM speeches WHERE date IN ({placeholders})",
            conn, params=sitting_days,
        )
    df["body"] = df["body"].fillna("").astype(str)
    return df


# ── Phase 4: Topic extraction from stage directions ───────────────────────────

# Stage direction first-line patterns to discard — procedural noise
_SD_SKIP = re.compile(
    r"""^(
        first\s+reading | second\s+reading | third\s+reading |
        consideration\s+resumed | consideration\s+in\s+detail |
        returned\s+from | message\s+received | reference\s+to |
        report\s+from | government\s+response |
        membership | personal\s+explanation | postponement |
        rearrangement | temporary\s+arrangements | limitation\s+of\s+debate |
        tabling | presentation | business | sitting\s+suspended |
        in\s+committee | adjournment | appropriation |
        answers\s+to\s+questions | leave\s+of\s+absence |
        order\s+for\s+the\s+production | acknowledgement |
        ^report$ | ^notice$ | ^notices$ |
        selection\s+of\s+bills | selection\s+committee |
        scrutiny | delegated\s+legislation | reporting\s+date |
        appointment | assent | withdrawal | reference |
        consideration | approval\s+of\s+work | meeting |
        the\s+(senate|house|committee)\s+divided |
        .*\selectorate\s*:   # electorate community reports
    )""",
    re.IGNORECASE | re.VERBOSE,
)

# Personal tribute pattern: "Lastname, Title Firstname" or similar
_SD_PERSON = re.compile(r"^[A-Z][a-zA-Z]+,\s+(Mr|Ms|Mrs|Dr|Prof|Hon)\b")


def extract_stage_direction_topics(
    conn: sqlite3.Connection,
    all_days: list[str],
) -> list[tuple[str, int]]:
    """Extract substantive debate topics from stage directions in the sitting week.

    Queries stage direction rows from the FTS DB, takes the first line of each
    body as the heading, filters procedural and personal-tribute noise, and
    returns a ranked list of (topic, count) sorted by frequency descending —
    the same format consumed by select_phrase_editorially().
    """
    if not all_days:
        return []

    placeholders = ",".join("?" * len(all_days))
    rows = conn.execute(
        f"SELECT body FROM speeches WHERE name = 'stage direction' AND date IN ({placeholders})",
        all_days,
    ).fetchall()

    counter: Counter = Counter()
    for (body,) in rows:
        if not body:
            continue
        heading = body.strip().splitlines()[0].strip()
        if not heading or len(heading) < 4:
            continue
        if _SD_SKIP.match(heading):
            continue
        if _SD_PERSON.match(heading):
            continue
        counter[heading] += 1

    return counter.most_common()


# ── Phase 5: Historical search (FTS5 indexed) ────────────────────────────────

def _normalise_phrase(phrase: str) -> str:
    """Auto-quote unquoted multi-word terms around boolean operators.

    Converts e.g. "mental health & addiction" → "'mental health' & 'addiction'"
    so that parse_expression can handle it correctly.
    """
    if not any(op in phrase for op in ('&', '|', '~')):
        return phrase
    parts = re.split(r'(\s*[&|~]\s*)', phrase)
    out = []
    for part in parts:
        stripped = part.strip()
        if stripped in ('&', '|', '~'):
            out.append(f' {stripped} ')
        elif stripped and not stripped.startswith(("'", '"')):
            out.append(f"'{stripped}'" if ' ' in stripped else stripped)
        else:
            out.append(part)
    return ''.join(out).strip()


def search_phrase_in_fts(
    conn: sqlite3.Connection,
    phrase: str,
    chamber_key: Optional[str],
) -> pd.DataFrame:
    """Return all speeches containing phrase using the FTS5 index.

    chamber_key: 'senate', 'house', or None (both chambers for joint phrases).
    FTS5 phrase search is case-insensitive by default.
    """
    # Build FTS5 query using the same boolean expression parser as the webapp.
    normalised = _normalise_phrase(phrase)
    try:
        ast = parse_expression(normalised)
        fts5_query = _ast_to_fts5(ast)
    except Exception:
        # Fallback: treat as a literal phrase
        fts5_query = f'"{phrase.replace(chr(34), chr(34)*2)}"'

    rowids = [
        r[0] for r in conn.execute(
            "SELECT rowid FROM speeches_fts WHERE body MATCH ?", (fts5_query,)
        ).fetchall()
    ]
    if not rowids:
        return pd.DataFrame()

    placeholders = ",".join("?" * len(rowids))
    if chamber_key is not None:
        df = pd.read_sql_query(
            f"SELECT date, name, name_id, party, in_gov, body, "
            f"'{chamber_key.capitalize()}' AS chamber "
            f"FROM speeches WHERE rowid IN ({placeholders}) AND chamber = ?",
            conn, params=rowids + [chamber_key],
        )
    else:
        # Joint: both chambers — capitalise the stored lowercase value for display
        df = pd.read_sql_query(
            f"SELECT date, name, name_id, party, in_gov, body, chamber "
            f"FROM speeches WHERE rowid IN ({placeholders})",
            conn, params=rowids,
        )
        df["chamber"] = df["chamber"].str.capitalize()

    if df.empty:
        return df

    df["body"]       = df["body"].fillna("").astype(str)
    df["year"]       = pd.to_datetime(df["date"], errors="coerce").dt.year
    df["party_group"] = df["party"].apply(build_party_group)
    df["in_gov"]     = pd.to_numeric(df["in_gov"], errors="coerce").fillna(0).astype(int)
    return df


def extract_phrase_centered_snippet(body: str, phrase: str, window: int = 300) -> str:
    """Return a snippet centred on the occurrence with the most surrounding occurrences.

    Used ONLY for HTML display (first-mention callout). NOT sent to Claude.
    """
    phrase_lower = phrase.lower()
    body_lower   = body.lower()

    # Find all start positions
    positions: list[int] = []
    start = 0
    while True:
        pos = body_lower.find(phrase_lower, start)
        if pos == -1:
            break
        positions.append(pos)
        start = pos + 1

    if not positions:
        return body[:600] + ("..." if len(body) > 600 else "")

    # For each occurrence, count how many other occurrences fall within ±window
    best_pos   = positions[0]
    best_score = -1
    for pos in positions:
        lo = max(0, pos - window)
        hi = pos + len(phrase) + window
        score = sum(1 for p in positions if lo <= p <= hi)
        if score > best_score:
            best_score = score
            best_pos   = pos

    lo = max(0, best_pos - window)
    hi = min(len(body), best_pos + len(phrase) + window)
    snippet = body[lo:hi]
    prefix  = "..." if lo > 0 else ""
    suffix  = "..." if hi < len(body) else ""
    return prefix + snippet + suffix


# ── Phase 6: Claude body selection ───────────────────────────────────────────

def detect_spikes(matches_df: pd.DataFrame) -> list[dict]:
    """Return years where mention count > 2× mean annual count."""
    if matches_df.empty:
        return []
    year_counts = matches_df["year"].value_counts().sort_index()
    if len(year_counts) == 0:
        return []
    mean_count = year_counts.mean()
    threshold  = mean_count * 2

    spikes: list[dict] = []
    for year, count in year_counts.items():
        if count > threshold and mean_count > 0:
            year_df = matches_df[matches_df["year"] == year]
            top_speakers = (
                year_df[year_df["name_id"].astype(str) != "10000"]
                .groupby(["name", "party"])
                .size()
                .nlargest(3)
                .reset_index(name="count")
            )
            top_list = [
                {"name": row["name"], "party": row["party"], "count": int(row["count"])}
                for _, row in top_speakers.iterrows()
            ]
            spikes.append({
                "year":         int(year),
                "count":        int(count),
                "mean":         round(float(mean_count), 1),
                "ratio":        round(float(count) / float(mean_count), 1),
                "top_speakers": top_list,
            })
    return sorted(spikes, key=lambda s: s["year"])


def _phrase_density(body: str, phrase: str) -> float:
    """Phrase occurrences per 1000 tokens (approximate).

    For boolean expressions (containing & | ~), counts occurrences of the
    first quoted/bare term as a proxy for density.
    """
    tokens = max(len(body.split()), 1)
    # Use the first simple term for density scoring
    simple = re.split(r'[&|~]', phrase)[0].strip().strip("'\"")
    count  = body.lower().count(simple.lower())
    return count * 1000 / tokens


def _df_matches_phrase(df: pd.DataFrame, phrase: str) -> pd.DataFrame:
    """Filter a DataFrame to rows whose body matches the phrase expression.

    Handles boolean expressions (&, |, ~) and simple literals.
    Returns a filtered copy.
    """
    from search_corpus import parse_expression, _build_masks, _eval_tree  # noqa

    if df.empty:
        return df

    bodies = df["body"].fillna("").astype(str)

    try:
        normalised = _normalise_phrase(phrase)
        ast = parse_expression(normalised)
        mask = _eval_tree(ast, bodies)
        return df[mask].copy()
    except Exception:
        # Fallback: simple case-insensitive substring match on first term
        simple = re.split(r'[&|~]', phrase)[0].strip().strip("'\"")
        pat = re.compile(re.escape(simple), re.IGNORECASE)
        return df[bodies.str.contains(pat, na=False)].copy()


MAX_BODIES_FOR_CLAUDE = 500   # default cap regardless of token budget


def select_bodies_for_claude(
    matches_df: pd.DataFrame,
    phrase: str,
    token_budget: int = EXCERPT_TOKEN_BUDGET,
    max_turns: int = MAX_BODIES_FOR_CLAUDE,
) -> list[dict]:
    """Select speech body texts to send to Claude within the token budget.

    Case A — all fit and under MAX_BODIES_FOR_CLAUDE: return everything sorted by date.
    Case B — exceeds budget or cap: proportional + speaker-stratified selection.
    """
    if matches_df.empty:
        return []

    # Exclude stage directions — they are procedural markers, not citable speakers.
    # _NON_SPEAKERS is defined at module level; resolved at call time, not definition time.
    matches_df = matches_df[~matches_df["name"].str.lower().isin(_NON_SPEAKERS)]
    if matches_df.empty:
        return []

    def row_to_dict(row, is_spike: bool = False) -> dict:
        ch       = str(getattr(row, "chamber", ""))
        dt       = str(getattr(row, "date", ""))
        nid      = str(getattr(row, "name_id", ""))
        body_raw = str(getattr(row, "body", ""))
        body_out = _excerpt_around_matches(body_raw, phrase)
        return {
            "date":          dt,
            "speaker":       str(getattr(row, "name", "")),
            "party":         str(getattr(row, "party", "")),
            "chamber":       ch,
            "in_gov":        int(getattr(row, "in_gov", 0) or 0),
            "body":          body_out,
            "is_spike_year": is_spike,
            "turn_hash":     _turn_hash(ch, dt, nid, body_raw),
        }

    spike_years = {s["year"] for s in detect_spikes(matches_df)}

    # Estimate total tokens
    total_tokens = sum(len(str(r.body)) // 4 for r in matches_df.itertuples())

    if total_tokens <= token_budget and len(matches_df) <= max_turns:
        # Case A — send everything
        rows = sorted(
            [row_to_dict(row, int(getattr(row, "year", 0) or 0) in spike_years)
             for row in matches_df.itertuples()],
            key=lambda d: d["date"],
        )
        return rows

    # Case B — proportional + speaker-stratified selection
    avg_tokens  = total_tokens / max(len(matches_df), 1)
    total_fits  = min(int(token_budget / max(avg_tokens, 1)), max_turns)

    year_counts = matches_df["year"].value_counts().sort_index()
    total_count = len(matches_df)

    # Allocate slots per year proportionally, cap at 20
    year_slots: dict[int, int] = {}
    for year, count in year_counts.items():
        slots = round(total_fits * count / total_count)
        year_slots[int(year)] = slots

    # Ensure first-ever mention is always included
    first_idx = matches_df["date"].idxmin()

    selected_rows: list[dict] = []
    first_row_included = False

    def _norm_speaker(n: str) -> str:
        return n.rstrip(":").strip()

    for year, slots in sorted(year_slots.items()):
        if slots == 0:
            continue
        year_df  = matches_df[matches_df["year"] == year].copy()
        is_spike = year in spike_years

        year_df["_density"] = year_df["body"].apply(lambda b: _phrase_density(b, phrase))

        # Round-robin across speakers ranked by their best phrase density.
        # This prevents any single prolific speaker from sweeping all slots in a year.
        speaker_groups: dict[str, list] = {}
        for spkr, grp in year_df.groupby("name"):
            speaker_groups[_norm_speaker(str(spkr))] = (
                grp.sort_values("_density", ascending=False).itertuples()
            )
        # Order speakers by their top turn's density so quality comes first
        speakers_ranked = sorted(
            speaker_groups.keys(),
            key=lambda s: year_df[year_df["name"].str.rstrip(":").str.strip() == s]["_density"].max(),
            reverse=True,
        )

        picked: list = []
        exhausted: set[str] = set()
        while len(picked) < slots and len(exhausted) < len(speakers_ranked):
            for spkr in speakers_ranked:
                if len(picked) >= slots:
                    break
                if spkr in exhausted:
                    continue
                try:
                    picked.append(next(speaker_groups[spkr]))
                except StopIteration:
                    exhausted.add(spkr)

        for row in picked:
            if row.Index == first_idx:
                first_row_included = True
            selected_rows.append(row_to_dict(row, is_spike))

    # Guarantee first mention
    if not first_row_included:
        first_row_tuple = next(matches_df.loc[[first_idx]].itertuples())
        first_dict = row_to_dict(first_row_tuple, int(getattr(first_row_tuple, "year", 0) or 0) in spike_years)
        selected_rows.insert(0, first_dict)

    # Global per-speaker cap as a backstop: no speaker may exceed 15% of total turns.
    # Applies after round-robin so dominant speakers don't accumulate across many years.
    speaker_limit = max(3, round(len(selected_rows) * 0.15))
    from collections import defaultdict as _dd
    speaker_count: dict[str, int] = _dd(int)
    capped: list[dict] = []
    for row in sorted(selected_rows, key=lambda d: d["date"]):
        norm = _norm_speaker(row["speaker"])
        if speaker_count[norm] < speaker_limit:
            capped.append(row)
            speaker_count[norm] += 1
    selected_rows = capped

    selected_rows.sort(key=lambda d: d["date"])
    return selected_rows


# ── Phase 7: Statistics + charts ──────────────────────────────────────────────

def build_party_group(party: str) -> str:
    if pd.isna(party):
        return "Other"
    party = str(party).strip()
    if party in ("LP", "LNP", "CLP"):
        return "Coalition"
    if party == "ALP":
        return "Labor"
    if party == "AG":
        return "Greens"
    if party in ("NP", "NATS"):
        return "Nationals"
    return "Other"


def fig_to_b64(fig: plt.Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    buf.seek(0)
    data = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return data


def chart_year_trend(matches_df: pd.DataFrame, phrase: str, chamber_label: str) -> str:
    """Stacked bar chart matching the Hansard Search app style exactly.

    House (bottom, eucalyptus green) + Senate (top, red ochre), dark bg,
    all years 1998–present on x axis, no rate line, no title, no spike highlights.
    """
    if matches_df.empty:
        return ""

    # ── Exact colours from the app ──────────────────────────────────────────
    _BG          = "#282828"
    _TICK        = "#928374"                          # --tn-comment
    _GRID        = (146/255, 131/255, 116/255, 0.18)  # rgba(146,131,116,0.18)
    _SENATE_FILL = (158/255,  61/255,  31/255, 0.75)  # rgba(158,61,31,0.75)
    _SENATE_EDGE = (158/255,  61/255,  31/255, 1.0)
    _HOUSE_FILL  = ( 78/255, 107/255,  74/255, 0.75)  # rgba(78,107,74,0.75)
    _HOUSE_EDGE  = ( 78/255, 107/255,  74/255, 1.0)

    # ── Build per-year, per-chamber counts across all years 1998–present ────
    min_year = 1998
    max_year = int(matches_df["year"].max()) if not matches_df.empty else date.today().year
    all_years = list(range(min_year, max_year + 1))

    ch = matches_df["chamber"].str.lower()
    senate_counts = matches_df[ch == "senate"]["year"].value_counts()
    house_counts  = matches_df[ch == "house"]["year"].value_counts()
    sen = [int(senate_counts.get(y, 0)) for y in all_years]
    hou = [int(house_counts.get(y,  0)) for y in all_years]

    # ── Plot ────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(14, 2.8), facecolor=_BG)
    ax.set_facecolor(_BG)

    import numpy as np
    x   = np.arange(len(all_years))
    hou = np.array(hou)
    sen = np.array(sen)
    ax.bar(x, hou, color=_HOUSE_FILL,  edgecolor=_HOUSE_EDGE,  linewidth=0.6, width=0.8, label="House")
    ax.bar(x, sen, color=_SENATE_FILL, edgecolor=_SENATE_EDGE, linewidth=0.6, width=0.8, label="Senate", bottom=hou)

    # ── Axes styling to match app ────────────────────────────────────────────
    ax.set_xticks(list(x))
    ax.set_xticklabels(
        [str(y) if y % 4 == 0 else "" for y in all_years],
        fontsize=9, color=_TICK, rotation=0,
    )
    ax.tick_params(axis="x", colors=_TICK, length=0)
    ax.tick_params(axis="y", colors=_TICK, labelsize=9)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True, nbins=4))
    ax.grid(axis="y", color=_GRID, linewidth=0.6, zorder=0)
    ax.set_axisbelow(True)
    for spine in ax.spines.values():
        spine.set_visible(False)

    # ── Legend (top-left, matching app's HTML overlay style) ─────────────────
    from matplotlib.patches import Patch
    ax.legend(
        handles=[
            Patch(facecolor=_SENATE_FILL, edgecolor=_SENATE_EDGE, linewidth=0.8, label="Senate"),
            Patch(facecolor=_HOUSE_FILL,  edgecolor=_HOUSE_EDGE,  linewidth=0.8, label="House"),
        ],
        loc="upper left", fontsize=8.5, framealpha=0,
        labelcolor=_TICK, handlelength=1.2, handleheight=0.9,
    )

    fig.tight_layout(pad=0.4)
    return fig_to_b64(fig)


def chart_party_breakdown(matches_df: pd.DataFrame, phrase: str) -> str:
    """Horizontal bar chart of mention count by party group."""
    if matches_df.empty or "party_group" not in matches_df.columns:
        return ""

    counts = {pg: 0 for pg in PARTY_ORDER}
    for pg, n in matches_df["party_group"].value_counts().items():
        if pg in counts:
            counts[pg] = int(n)

    labels = [pg for pg in PARTY_ORDER if counts[pg] > 0]
    values = [counts[pg] for pg in labels]
    colours= [PARTY_COLOURS.get(pg, "#888888") for pg in labels]

    _BG = "#1d2021"; _TEXT = "#ebdbb2"; _GRID = "#3c3836"
    fig, ax = plt.subplots(figsize=(5, max(2.0, len(labels) * 0.5)), facecolor=_BG)
    ax.set_facecolor(_BG)
    bars = ax.barh(labels, values, color=colours, alpha=0.88)
    ax.set_xlabel("Mentions", fontsize=9, color=_TEXT)
    ax.set_title("By party", fontsize=9, pad=6, color=_TEXT)
    ax.invert_yaxis()
    ax.bar_label(bars, padding=3, fontsize=8, color=_TEXT)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="x", color=_GRID, linewidth=0.6)
    ax.tick_params(colors=_TEXT)
    for spine in ax.spines.values(): spine.set_edgecolor(_GRID)
    fig.tight_layout()
    return fig_to_b64(fig)


def chart_gov_opp(matches_df: pd.DataFrame, phrase: str) -> str:
    """Paired horizontal bar: gov% vs opposition%."""
    if matches_df.empty or "in_gov" not in matches_df.columns:
        return ""

    total   = len(matches_df)
    in_gov  = int(matches_df["in_gov"].sum())
    in_opp  = total - in_gov
    gov_pct = round(in_gov / max(total, 1) * 100, 1)
    opp_pct = round(in_opp / max(total, 1) * 100, 1)

    _BG = "#1d2021"; _TEXT = "#ebdbb2"; _GRID = "#3c3836"
    fig, ax = plt.subplots(figsize=(5, 1.8), facecolor=_BG)
    ax.set_facecolor(_BG)
    ax.barh(["Government", "Opposition"], [gov_pct, opp_pct],
            color=["#fb4934", "#83a598"], alpha=0.88)
    ax.set_xlabel("%", fontsize=9, color=_TEXT)
    ax.set_title("Gov vs Opposition", fontsize=9, pad=6, color=_TEXT)
    ax.set_xlim(0, 100)
    for i, v in enumerate([gov_pct, opp_pct]):
        ax.text(v + 1, i, f"{v}%", va="center", fontsize=8, color=_TEXT)
    ax.grid(axis="x", color=_GRID, linewidth=0.6)
    ax.tick_params(colors=_TEXT)
    for spine in ax.spines.values(): spine.set_edgecolor(_GRID)
    fig.tight_layout()
    return fig_to_b64(fig)


_NON_SPEAKERS = frozenset({"stage direction", "stage directions"})

def build_top_speakers_table(matches_df: pd.DataFrame, top_n: int = 5) -> str:
    """HTML table: Speaker | Party | Total Mentions | Peak Year."""
    if matches_df.empty:
        return ""

    df = matches_df[matches_df["name_id"].astype(str) != "10000"]
    df = df[~df["name"].str.lower().isin(_NON_SPEAKERS)]
    if df.empty:
        return ""

    speaker_stats = (
        df.groupby(["name", "party"])
        .agg(
            total=("year", "count"),
            peak_year=("year", lambda s: int(s.value_counts().idxmax())),
        )
        .reset_index()
        .nlargest(top_n, "total")
    )

    rows_html = ""
    for _, row in speaker_stats.iterrows():
        party_group = build_party_group(row["party"])
        colour      = PARTY_COLOURS.get(party_group, "#888888")
        rows_html += (
            f'<tr>'
            f'<td>{row["name"]}</td>'
            f'<td><span style="color:{colour};font-weight:600">{row["party"]}</span></td>'
            f'<td style="text-align:center">{int(row["total"])}</td>'
            f'<td style="text-align:center">{row["peak_year"]}</td>'
            f'</tr>'
        )

    return (
        '<table class="speakers-table">'
        '<thead><tr><th>Speaker</th><th>Party</th><th>Mentions</th><th>Peak year</th></tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        '</table>'
    )


def select_week_turns_for_context(
    week_turns_df: pd.DataFrame,
    phrase: str = "",
) -> list[dict]:
    """Return all week turns sorted by date for narrative/citation context."""
    if week_turns_df is None or week_turns_df.empty:
        return []
    df = week_turns_df.copy()
    if "chamber" not in df.columns:
        df["chamber"] = "parliament"
    df = df.sort_values("date")
    result = []
    for r in df.to_dict("records"):
        ch       = r.get("chamber", "parliament")
        dt       = str(r.get("date", ""))
        nid      = str(r.get("name_id", ""))
        body_raw = str(r.get("body", ""))
        body_out = _excerpt_around_matches(body_raw, phrase) if phrase else body_raw
        result.append({
            "name":      r.get("name", "Unknown"),
            "speaker":   r.get("name", "Unknown"),
            "party":     r.get("party", ""),
            "date":      dt,
            "chamber":   ch,
            "body":      body_out,
            "turn_hash": _turn_hash(ch, dt, nid, body_raw),
        })
    return result


def get_first_mention(matches_df: pd.DataFrame, phrase: str) -> dict:
    """Return earliest match: date, speaker, party, snippet."""
    if matches_df.empty:
        return {}
    row = matches_df.loc[matches_df["date"].idxmin()]
    return {
        "date":    str(row.get("date", "")),
        "speaker": str(row.get("name", "")),
        "party":   str(row.get("party", "")),
        "snippet": extract_phrase_centered_snippet(str(row.get("body", "")), phrase, window=200),
    }


def build_year_stats_table(matches_df: pd.DataFrame) -> str:
    """Plain-text table of year | count | rate for Claude's stats block."""
    if matches_df.empty:
        return ""
    year_counts = matches_df["year"].value_counts().sort_index()
    lines = ["Year | Mentions | Rate/10k turns"]
    for year, count in year_counts.items():
        total_turns = CORPUS_TURNS_BY_YEAR.get(int(year), 1)
        rate = count / total_turns * 10_000
        lines.append(f"{int(year)} | {int(count)} | {rate:.2f}")
    return "\n".join(lines)


def build_party_stats_text(matches_df: pd.DataFrame) -> str:
    """Plain-text party breakdown for Claude."""
    if matches_df.empty or "party_group" not in matches_df.columns:
        return ""
    total = len(matches_df)
    parts = []
    for pg in PARTY_ORDER:
        n = int((matches_df["party_group"] == pg).sum())
        if n:
            parts.append(f"{pg}: {n} ({n/total*100:.1f}%)")
    return ", ".join(parts)


def build_top_speakers_text(matches_df: pd.DataFrame, top_n: int = 5) -> str:
    """Plain-text top speaker list for Claude."""
    if matches_df.empty:
        return ""
    df = matches_df[matches_df["name_id"].astype(str) != "10000"]
    df = df[~df["name"].str.lower().isin(_NON_SPEAKERS)]
    if df.empty:
        return ""
    speaker_stats = (
        df.groupby(["name", "party"])
        .agg(total=("year", "count"), peak_year=("year", lambda s: int(s.value_counts().idxmax())))
        .reset_index()
        .nlargest(top_n, "total")
    )
    lines = []
    for _, row in speaker_stats.iterrows():
        lines.append(f"  {row['name']} ({row['party']}): {int(row['total'])} mentions, peak {row['peak_year']}")
    return "\n".join(lines)


# ── Phase 8: Claude API ───────────────────────────────────────────────────────

def build_system_prompt(citations: bool = True, has_week_turns: bool = True) -> str:
    citation_instructions = ""
    if citations:
        citation_instructions = """

INLINE CITATIONS (mandatory):
The speech turns are numbered [1], [2], [3]… in the user message.

CITATION FORMAT — one style only: a bare number in square brackets immediately before
the closing punctuation of the sentence, e.g. "[3]." or "[7, 12]." or "[2, 9, 15]."
Maximum 3 numbers per citation. No other citation style permitted — no footnotes,
no endnotes, no parenthetical author-date, no superscripts in the raw output.
The post-processing step will convert [N] markers into superscript HTML links.

CITATION DENSITY:
- Every sentence that names a specific politician, quotes a speaker, or references
  a specific debate, bill, year, or event drawn from the supplied data MUST end with
  an inline [N] citation.
- Where a claim is supported by multiple turns, cite up to 3 of the most salient —
  prefer turns that most directly and specifically support the claim over those that
  merely mention the phrase in passing. Do not cite indiscriminately; choose the
  strongest evidence. Where only one turn directly supports a claim, cite just that one.

TREATMENT OF PARLIAMENTARY STATEMENTS:
- Do NOT derive a factual statement solely from what a politician said in Hansard.
  A politician asserting something is true does not make it a fact.
  Write "Senator X argued that…", "the minister claimed…", "X contended that…"
  when the underlying claim is contested, uncertain, or is the politician's own framing.
- HOWEVER: if an MP's statement aligns with something you independently know to be
  factually accurate from your background knowledge, you may state it as fact — the
  Hansard citation then corroborates rather than establishes the claim.
- Uncontested procedural facts (a bill passed, a vote was held, a committee reported)
  may always be stated directly.

EXAMPLE of correct style:
  <p>Australia's liquid fuel reserves have historically sat well below the IEA's
  90-day benchmark — a vulnerability that Senator Canavan argued made the country
  dangerously exposed to supply shocks [14]. Labor members countered that the
  government's own inaction over the preceding decade was the root cause [8, 22],
  while crossbenchers pressed both sides on the absence of a domestic refining
  strategy [31, 45, 67].</p>

Do NOT cite turns for pre-1998 background knowledge — only cite the numbered turns.
Do NOT fabricate turn numbers. Output ONLY the narrative HTML with [N] markers —
no JSON, no ---CITATIONS--- separator, no reference list (that is built separately).

NEVER add [N] citation markers to pipeline-derived statistics: total mention counts,
year-by-year counts, rates per 10,000 turns, dataset highs/lows, or any figure
drawn from the STATISTICS block rather than from a specific speech turn.
These are computed by the analysis pipeline and cannot be attributed to individual turns."""

    if has_week_turns:
        narrative_arc_instruction = (
            "NARRATIVE ARC:\n"
            "1. THIS WEEK (opening paragraph — mandatory) — Open by grounding the reader in the current sitting week. "
            "Using the \"THIS WEEK'S PROCEEDINGS\" speech turns provided, describe specifically what debate, bill, or "
            "issue drove the use of this phrase in parliament this week. Name the politicians, the chamber, the context. "
            "This is the news hook.\n"
            "2 onwards. Tell the story of this phrase through the parliamentary record: how its use has grown, peaked, "
            "or faded; which parties and politicians have driven the debate; what the key controversies were; how the "
            "framing has shifted over time. Weave in broader political or historical context only where it genuinely "
            "explains something in the data — not as a mandatory origin story. Close by connecting the current week "
            "back to that longer arc. Let the richness of the material determine how much you write."
        )
    else:
        narrative_arc_instruction = (
            "NARRATIVE ARC:\n"
            "This phrase did not appear in the current sitting week's proceedings, so there is no news hook. "
            "Write a pure historical analysis: tell the story of this phrase through the parliamentary record — "
            "how its use has grown, peaked, or faded; which parties and politicians have driven the debate; "
            "what the key controversies were; how the framing has shifted over time. Open with the most compelling "
            "or dramatic moment in the record rather than a chronological origin. Weave in broader political or "
            "historical context only where it genuinely illuminates the data. Let the richness of the material "
            "determine how much you write."
        )

    return f"""You are an expert parliamentary historian and political analyst of Australia.

Your task is to write a compelling narrative history of how a particular phrase or term has appeared in the Australian Federal Parliament across both the Senate and House of Representatives — the story of how an idea entered parliament, who championed or fought it, what controversies it sparked, and how its political significance has shifted over time.

FORMAT: Output only valid HTML using these tags: <h3>, <p>, <blockquote>, <p class="attribution">. No markdown. No bullet points. No tables. No headings other than <h3>. Write in flowing paragraphs.

TONE: Engaging narrative for an educated general public — like a high-quality political magazine, not a parliamentary report or academic paper. Name the politicians. Describe the moments of conflict. Show how the debate evolved. Maintain analytical distance: report what politicians said and argued, not what is true. Parliamentary debate is a record of claims, not a source of facts.

CORPUS SCOPE: The data covers 1998–present. Do not treat 1998 as the origin of anything — it is simply where the data starts. Do not speculate about when a phrase "entered" political discourse unless you have genuine historical knowledge that makes it relevant and interesting.

GROUNDING: Claims about the data (specific quotes, speaker counts, spike years, party breakdowns) must be traceable to the statistics or speech turns provided. Where broader historical or political context genuinely illuminates the story, you may draw on your background knowledge — but only where it adds insight, not as a structural obligation.

{narrative_arc_instruction}

Then add a <blockquote> of the most revealing or striking quote from the provided speech turns, followed by <p class="attribution"> with: [Speaker Name, Party, Date, Chamber].
{citation_instructions}"""


def build_phrase_user_prompt(
    phrase: str,
    chamber: str,
    week_label: str,
    week_count: int,
    novelty_score: float,
    first_mention: dict,
    matches_df: pd.DataFrame,
    spike_annotations: list[dict],
    bodies_for_claude: list[dict],
    citations: bool = True,
    max_citations: int = 2,
    week_bodies: Optional[list] = None,
) -> str:
    total = len(matches_df)
    first_year = int(matches_df["year"].min()) if not matches_df.empty else "?"
    last_year  = int(matches_df["year"].max()) if not matches_df.empty else "?"

    gov_total   = int(matches_df["in_gov"].sum()) if "in_gov" in matches_df.columns else 0
    opp_total   = total - gov_total
    gov_pct     = round(gov_total / max(total, 1) * 100, 1)
    opp_pct     = round(opp_total / max(total, 1) * 100, 1)

    spike_text = ""
    if spike_annotations:
        lines = []
        for s in spike_annotations:
            top_str = "; ".join(f"{sp['name']} {sp['party']} ({sp['count']})" for sp in s["top_speakers"])
            lines.append(
                f"  {s['year']}: {s['count']} mentions ({s['ratio']}× avg). Top speakers: {top_str}"
            )
        spike_text = "NOTABLE SPIKES:\n" + "\n".join(lines)

    turns_text = ""
    for i, body_dict in enumerate(bodies_for_claude, start=1):
        spike_tag = " [SPIKE YEAR]" if body_dict.get("is_spike_year") else ""
        gov_tag   = " [GOV]" if body_dict.get("in_gov") else " [OPP]"
        turns_text += (
            f"\n[{i}] {body_dict['date']} | {body_dict['speaker']} | "
            f"{body_dict['party']} | {body_dict['chamber']}{spike_tag}{gov_tag}\n"
            f"{body_dict['body']}\n"
        )

    today = date.today()
    partial_year_note = ""
    if not matches_df.empty and last_year == today.year and today.month < 11:
        pct = round(today.month / 12 * 100)
        partial_year_note = (
            f"\nNOTE: {today.year} figures are partial — the corpus only extends through "
            f"{today.strftime('%B')} ({pct}% of the year complete). A lower {today.year} "
            f"count does not necessarily indicate a declining trend.\n"
        )

    # Build current-week context section — numbering continues from historical turns
    week_context_text = ""
    week_offset = len(bodies_for_claude)
    if week_bodies:
        week_lines = []
        for i, wb in enumerate(week_bodies, week_offset + 1):
            ch   = wb.get("chamber", "parliament")
            spkr = wb.get("name", "Unknown")
            pty  = wb.get("party", "")
            dt   = wb.get("date", "")
            body = str(wb.get("body", "")).strip()
            week_lines.append(f"  [{i}] [{dt}] {spkr} ({pty}) [{ch}]\n  {body}")
        week_context_text = (
            f"\n=== THIS WEEK'S PROCEEDINGS ({week_label}) — {week_count} uses of \"{phrase}\" ===\n"
            f"These are the speech turns from the most recent sitting week that contain the phrase.\n"
            f"USE THESE to write the opening paragraph: explain the current parliamentary context —\n"
            f"what debate, bill, or issue drove the use of this phrase this week.\n"
            f"Cite these turns with their numbers [{week_offset+1}]–[{week_offset+len(week_bodies)}].\n\n"
            + "\n\n".join(week_lines)
            + "\n"
        )

    return f"""PHRASE: "{phrase}"
CHAMBER: {chamber}
SITTING WEEK: {week_label} (appeared {week_count} times this week)
{week_context_text}
=== STATISTICS (computed from ALL {total} historical matches, 1998–present) ==={partial_year_note}

Total mentions: {total} speeches across {first_year}–{last_year}

YEAR-BY-YEAR TREND:
{build_year_stats_table(matches_df)}

PARTY BREAKDOWN:
{build_party_stats_text(matches_df)}

GOV / OPPOSITION SPLIT:
Government: {gov_pct}% ({gov_total} speeches), Opposition: {opp_pct}% ({opp_total} speeches)

TOP 5 SPEAKERS (all time):
{build_top_speakers_text(matches_df)}


{spike_text}

=== SPEECH TURNS ({len(bodies_for_claude)} full texts sent for analysis) ===
{turns_text}"""


_BRACKET_RE = re.compile(r'\[(\d+(?:\s*,\s*\d+)*)\]')


def apply_inline_citations(
    narrative_html: str,
    all_turns: list[dict],
) -> tuple[str, str]:
    """Convert [N] bracket citations in narrative HTML to superscript anchor links.

    all_turns is the unified list: historical turns first, then current-week turns,
    all 1-based indexed in the same sequence used by the narrative and citation checker.

    Returns (processed_html, references_html).
    """
    seq_map: dict[int, int] = {}  # turn_num -> display seq
    counter = [0]

    def _seq(turn_num: int) -> int:
        if turn_num not in seq_map:
            counter[0] += 1
            seq_map[turn_num] = counter[0]
        return seq_map[turn_num]

    def replace_citation(m: re.Match) -> str:
        raw_ids = [x.strip() for x in m.group(1).split(",")]
        sups = []
        for raw_id in raw_ids:
            try:
                turn_num = int(raw_id)
            except ValueError:
                continue
            if turn_num < 1 or turn_num > len(all_turns):
                continue
            seq = _seq(turn_num)
            sups.append(
                f'<sup class="cite" id="citeref-{turn_num}">'
                f'<a href="#ref-{turn_num}">{seq}</a>'
                f'</sup>'
            )
        return "&#8202;".join(sups) if sups else m.group(0)

    processed = _BRACKET_RE.sub(replace_citation, narrative_html)

    if not seq_map:
        return processed, ""

    chamber_label = {
        "senate": "Senate",
        "house":  "House of Representatives",
    }

    ref_entries: list[tuple[int, str]] = []
    for turn_num, seq in seq_map.items():
        idx = turn_num - 1
        if not (0 <= idx < len(all_turns)):
            continue
        t = all_turns[idx]
        # Support both historical dict keys (speaker/chamber) and week dict keys (name/chamber)
        speaker = t.get("speaker") or t.get("name", "?")
        party   = t.get("party", "?")
        chamber = t.get("chamber", "")
        date_str = t.get("date", "")
        ch = chamber_label.get(str(chamber).lower(), chamber)
        try:
            date_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%-d %B %Y")
        except (ValueError, AttributeError):
            date_fmt = date_str
        url  = _turn_url(t.get("turn_hash", ""))
        link = (
            f' <a href="{url}" target="_blank" rel="noopener">[Hansard&#8599;]</a>'
            if url else ""
        )
        ref_entries.append((seq,
            f'<li id="ref-{turn_num}">'
            f'<span class="ref-num">{seq}.</span> '
            f'<span class="citation-speaker">{speaker}</span> ({party}), '
            f'<em>Parliamentary Debates ({ch})</em>, {date_fmt}.{link}'
            f'</li>'
        ))

    items = "".join(html for _, html in sorted(ref_entries))
    refs_html = (
        f'<div class="references">'
        f'<h4>References</h4>'
        f'<ol class="ref-list">{items}</ol>'
        f'</div>'
    )
    return processed, refs_html


_PARLINFO_DATASETS = {
    "senate": "hansardS%2ChansardS80",
    "house":  "hansardr%2Chansardr80",
}


def _aph_url(chamber: str, date_str: str) -> str:
    """ParlInfo day-level URL — used only by legacy format_citations_html."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return ""
    datasets = _PARLINFO_DATASETS.get(chamber.lower(), "hansardS%2ChansardS80")
    return (
        "https://parlinfo.aph.gov.au/parlInfo/search/display/display.w3p"
        f";adv=yes;orderBy=_fragment_number,doc_date-rev;page=0"
        f";query=Dataset%3A{datasets}%20Date%3A{d.day}%2F{d.month}%2F{d.year}"
        f";rec=0;resCount=Default"
    )


def _turn_hash(chamber: str, date: str, name_id, body: str) -> str:
    """Stable 12-char hex identifier for a specific speech turn.

    Both chamber and name_id are normalised so that the pipeline and webapp
    always produce the same hash regardless of capitalisation or None/NaN values:
      - chamber: lowercased ('Senate' → 'senate')
      - name_id: None / NaN / 'nan' / 'None' → ''
    """
    import math as _math
    ch = str(chamber or "").lower()
    if name_id is None or (isinstance(name_id, float) and _math.isnan(name_id)):
        nid = ""
    else:
        nid = str(name_id)
        if nid in ("nan", "None", "NaN"):
            nid = ""
    raw = f"{ch}|{date}|{nid}|{body[:200]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def _excerpt_around_matches(
    body: str,
    phrase: str,
    window: int = 400,
    head_tail: int = 400,
) -> str:
    """Return an excerpt of `body` preserving context around matched phrase terms.

    Keeps the first/last `head_tail` chars and a ±`window` char window around
    every match. Overlapping windows are merged. Non-adjacent sections are joined
    with ' […] '. Returns the full body unchanged if windows cover it entirely.
    The turn hash must always be computed from the original body, not the excerpt.
    """
    raw_terms = re.split(r'[&|~]', phrase)
    terms = [t.strip().strip("'\"").strip() for t in raw_terms if t.strip().strip("'\"").strip()]
    if not terms:
        return body

    positions: list[tuple[int, int]] = []
    for term in terms:
        if not term:
            continue
        # Word-boundary match: 'EV' matches standalone 'EV' but not 'every', 'never', etc.
        pattern = r'\b' + re.escape(term) + r'\b'
        for m in re.finditer(pattern, body, flags=re.IGNORECASE):
            positions.append((m.start(), m.end()))

    if not positions:
        return body

    spans: list[tuple[int, int]] = [
        (0, min(head_tail, len(body))),
        *[(max(0, s - window), min(len(body), e + window)) for s, e in positions],
        (max(0, len(body) - head_tail), len(body)),
    ]
    spans.sort()

    merged: list[list[int]] = []
    for s, e in spans:
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])

    # All spans merged into one contiguous range — nothing to cut
    if len(merged) == 1:
        return body

    return " […] ".join(body[s:e] for s, e in merged)


def _turn_url(turn_hash: str) -> str:
    """Deep link to a specific speech turn on hansardsearch.com.au."""
    if not turn_hash:
        return ""
    return f"https://hansardsearch.com.au/t/{turn_hash}"


def inject_citation_popups(html: str) -> str:
    """Post-process newsletter HTML to add hover popups on inline citations.

    Injects a self-contained CSS+HTML+JS block before </body>.  On hover over a
    [N] citation superscript, fetches the corresponding turn page from
    hansardsearch.com.au/t/<hash> and displays speaker + body text in a floating
    popup.  Gracefully degrades if the fetch fails.
    """
    _POPUP_BLOCK = r"""
<style>
#cite-popup{position:fixed;z-index:9999;background:#282828;border:1px solid #504945;
border-radius:6px;box-shadow:0 4px 20px rgba(0,0,0,.55);padding:14px 16px;
max-width:500px;max-height:380px;overflow-y:auto;font-size:13px;line-height:1.55;
color:#ebdbb2;pointer-events:auto;display:none;scrollbar-color:#504945 #1d2021;scrollbar-width:thin}
#cite-popup::-webkit-scrollbar{width:6px}
#cite-popup::-webkit-scrollbar-track{background:#1d2021}
#cite-popup::-webkit-scrollbar-thumb{background:#504945;border-radius:3px}
#cite-popup::-webkit-scrollbar-thumb:hover{background:#665c54}
#cite-popup .pop-speaker{font-weight:700;color:#fabd2f;margin-bottom:3px}
#cite-popup .pop-meta{font-size:11px;color:#928374;margin-bottom:10px}
#cite-popup .pop-body{color:#d5c4a1;white-space:pre-wrap;word-break:break-word}
</style>
<div id="cite-popup"><div class="pop-speaker"></div><div class="pop-meta"></div><div class="pop-body"></div></div>
<script>
(function(){
  var popup=document.getElementById('cite-popup');
  var spkEl=popup.querySelector('.pop-speaker');
  var metEl=popup.querySelector('.pop-meta');
  var bodEl=popup.querySelector('.pop-body');
  var hideTimer=null;
  var _cache={};

  function posFromEl(sup){
    var r=sup.getBoundingClientRect();
    var x=r.left,y=r.bottom+6;
    var pw=500,ph=380;
    if(x+pw>window.innerWidth)x=Math.max(4,window.innerWidth-pw-10);
    if(y+ph>window.innerHeight)y=Math.max(4,r.top-ph-6);
    popup.style.left=x+'px';
    popup.style.top=y+'px';
  }

  async function show(sup){
    var a=sup.querySelector('a');
    if(!a)return;
    var refEl=document.getElementById(a.getAttribute('href').slice(1));
    if(!refEl)return;
    var link=refEl.querySelector('a[href*="/t/"]');
    var url=link?link.href:null;
    var speaker=(refEl.querySelector('.citation-speaker')||{}).textContent||'';
    var meta=refEl.textContent.replace(speaker,'').replace(/\[Hansard[^\]]*]/g,'').replace(/\s+/g,' ').trim().replace(/,\s*$/,'');
    spkEl.textContent=speaker;
    metEl.textContent=meta;
    bodEl.textContent=url?'Loading\u2026':'';
    posFromEl(sup);
    popup.style.display='block';
    if(!url)return;
    if(_cache[url]!==undefined){bodEl.textContent=_cache[url];return;}
    try{
      var r=await fetch(url);
      if(!r.ok)throw r.status;
      var t=await r.text();
      var d=new DOMParser().parseFromString(t,'text/html');
      var b=(d.querySelector('.body')||{}).textContent||'';
      _cache[url]=b.trim();
      bodEl.textContent=_cache[url];
    }catch(err){
      bodEl.textContent='(Could not load turn text)';
    }
  }

  document.querySelectorAll('sup.cite').forEach(function(sup){
    sup.addEventListener('mouseenter',function(){clearTimeout(hideTimer);show(sup);});
    sup.addEventListener('mouseleave',function(){hideTimer=setTimeout(function(){popup.style.display='none';},300);});
  });
  popup.addEventListener('mouseenter',function(){clearTimeout(hideTimer);});
  popup.addEventListener('mouseleave',function(){hideTimer=setTimeout(function(){popup.style.display='none';},300);});
})();
</script>
"""
    return html.replace("</body>", _POPUP_BLOCK + "\n</body>", 1)


def format_citations_html(citations: list[dict]) -> str:
    """Format resolved citations as an HTML block with APH Hansard hyperlinks."""
    if not citations:
        return ""
    chamber_label = {
        "senate": "Senate", "SENATE": "Senate",
        "house":  "House of Representatives", "HOUSE": "House of Representatives",
        "reps":   "House of Representatives", "REPS":  "House of Representatives",
    }
    items = ""
    for cit in citations:
        chamber_str = chamber_label.get(cit["chamber"], cit["chamber"])
        try:
            date_fmt = datetime.strptime(cit["date"], "%Y-%m-%d").strftime("%-d %B %Y")
        except (ValueError, AttributeError):
            date_fmt = cit["date"]
        url = _aph_url(cit["chamber"], cit["date"])
        link = (
            f' <a href="{url}" target="_blank" rel="noopener">[Hansard&#8599;]</a>'
            if url else ""
        )
        items += (
            f'<li>'
            f'<span class="citation-speaker">{cit["speaker"]} ({cit["party"]})</span>, '
            f'<em>Parliamentary Debates ({chamber_str})</em>, {date_fmt}.{link}'
            f'<blockquote class="citation-quote">"{cit["quote"]}"</blockquote>'
            f'</li>'
        )
    return f'<div class="citations"><h4>Sources</h4><ol>{items}</ol></div>'


def generate_phrase_narrative(
    client,
    system_prompt: str,
    user_prompt: str,
    model: str,
    no_cache: bool,
    citations: bool,
    max_retries: int = 3,
) -> tuple[str, str, dict]:
    """Call Claude and return (narrative_html, citations_html, usage_dict).

    Retry: RateLimitError → 30s × attempt; APIStatusError → retry × 3.
    Failure → placeholder HTML + empty citations + zero usage.
    """
    import anthropic as ant

    system_block: dict = {"type": "text", "text": system_prompt}
    if not no_cache:
        system_block["cache_control"] = {"type": "ephemeral"}

    for attempt in range(max_retries):
        try:
            # Streaming is required for large max_tokens values (SDK enforces this
            # for requests that may take longer than 10 minutes).
            chunks: list[str] = []
            usage: dict = {}
            with client.messages.stream(
                model=model,
                max_tokens=128_000,
                temperature=0,
                system=[system_block],
                messages=[{"role": "user", "content": user_prompt}],
            ) as stream:
                for text in stream.text_stream:
                    chunks.append(text)
                final = stream.get_final_message()
                usage = {
                    "input_tokens":                final.usage.input_tokens,
                    "output_tokens":               final.usage.output_tokens,
                    "cache_creation_input_tokens": getattr(final.usage, "cache_creation_input_tokens", 0),
                    "cache_read_input_tokens":     getattr(final.usage, "cache_read_input_tokens", 0),
                }

            raw = "".join(chunks)
            return raw.strip(), raw, usage

        except ant.RateLimitError:
            wait = 30 * (attempt + 1)
            print(f"  Rate limit — waiting {wait}s…", flush=True)
            time.sleep(wait)
        except ant.APIStatusError as e:
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                placeholder = f'<p><em>[Narrative unavailable — API error: {e}]</em></p>'
                return placeholder, "", {}

    placeholder = '<p><em>[Narrative unavailable — max retries exceeded]</em></p>'
    return placeholder, "", {}


def correct_citations(
    client,
    narrative_html: str,
    bodies_for_claude: list[dict],
    model: str,
    week_bodies: Optional[list] = None,
) -> tuple[str, str, dict]:
    """Second-pass citation correction.

    Uses a unified turn list: historical turns [1..N] followed by week_bodies [N+1..N+M].
    Verifies [N] markers, corrects misattributions, adds missing citations for named speakers,
    and strips citation markers from pipeline-derived statistics.

    Returns (corrected_narrative_html, summary, usage_dict).
    """
    import anthropic as ant

    if not narrative_html or not bodies_for_claude:
        return narrative_html, "", {}

    # Build unified turn list [1], [2], … — historical first, then current-week
    turn_lines = []
    for i, body in enumerate(bodies_for_claude, 1):
        full_body = str(body.get("body", "")).strip().replace("\n", " ")
        turn_lines.append(
            f"[{i}] {body.get('speaker', '?')} ({body.get('party', '?')}),"
            f" {body.get('date', '?')} [{body.get('chamber', '?')}]: {full_body}"
        )
    offset = len(bodies_for_claude)
    if week_bodies:
        for i, wb in enumerate(week_bodies, offset + 1):
            full_body = str(wb.get("body", "")).strip().replace("\n", " ")
            turn_lines.append(
                f"[{i}] {wb.get('name', '?')} ({wb.get('party', '?')}),"
                f" {wb.get('date', '?')} [{wb.get('chamber', '?')}]: {full_body}"
            )
    turns_text = "\n".join(turn_lines)

    system = (
        "You are a citation-accuracy editor for a parliamentary newsletter.\n\n"
        "You will be given:\n"
        "  1. A narrative HTML document with [N] citation markers.\n"
        "  2. A unified numbered list of speech turns — historical turns first, "
        "then current-week turns continuing the same sequence.\n\n"
        "YOUR TASKS — in order of priority:\n\n"
        "1. SPEAKER NAME CORRECTIONS (light text edits — highest priority):\n"
        "   If the inline text names a specific speaker but the cited turn belongs to a "
        "different speaker, correct the name in the inline text to match the actual speaker "
        "in that turn. This is the ONLY permitted text edit — change nothing else.\n\n"
        "2. CITATION MARKER CORRECTIONS:\n"
        "   - If a [N] marker cites the wrong turn, replace it with the correct number.\n"
        "   - If no supplied turn supports the claim, remove the marker.\n\n"
        "3. ADD MISSING CITATIONS:\n"
        "   If a sentence makes a specific attributable claim about a named speaker or "
        "a specific speech — quoting them, paraphrasing their argument, or describing "
        "what they said — and has no [N] marker, check whether a matching turn exists "
        "and add the citation if found.\n"
        "   Example of a substantive uncited claim that should receive a citation:\n"
        "   'Senator Hodgins-May invoked it in a sharp attack on the gambling industry's "
        "political influence, arguing that the harm caused by gambling demanded urgent "
        "legislative action.' — this names a specific senator making a specific argument "
        "and must be cited if a matching turn is in the provided list.\n\n"
        "4. STATISTICS — never cited:\n"
        "   Remove any [N] marker attached to a pipeline-derived statistic: "
        "total mention counts, year-by-year counts, rates per 10,000 turns, dataset "
        "highs/lows, or any figure from the statistics block. These cannot be attributed "
        "to individual speech turns.\n\n"
        "5. DO NOT remove citations for claims that are accurate summaries of multiple turns "
        "— if the claim is broadly supported by the cited turn(s) even as a synthesis, keep "
        "the citation. Only remove when the cited turn is genuinely irrelevant or wrong.\n\n"
        "Respond in exactly this format:\n"
        "<summary>\n"
        "Concise bullet list of every change, or 'No changes needed.'\n"
        "Format: [N] → [M] — reason | [N] removed — reason | "
        "citation added [N] — reason | Speaker name corrected: 'X' → 'Y' at [N] — reason\n"
        "</summary>\n"
        "<html>\n"
        "Complete corrected HTML with all changes applied.\n"
        "</html>"
    )
    user = (
        f"=== SPEECH TURNS ===\n{turns_text}\n\n"
        f"=== NARRATIVE ===\n{narrative_html}"
    )

    try:
        chunks: list[str] = []
        with client.messages.stream(
            model=model,
            max_tokens=64_000,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}],
        ) as stream:
            for text in stream.text_stream:
                chunks.append(text)
            final = stream.get_final_message()
        raw = "".join(chunks)
        import re as _re
        summary_m = _re.search(r'<summary>(.*?)</summary>', raw, _re.DOTALL)
        html_m    = _re.search(r'<html>(.*?)</html>',    raw, _re.DOTALL)
        summary   = summary_m.group(1).strip() if summary_m else ""
        corrected = html_m.group(1).strip()    if html_m    else narrative_html
        usage = {
            "input_tokens":  final.usage.input_tokens,
            "output_tokens": final.usage.output_tokens,
            "cache_creation_input_tokens": getattr(final.usage, "cache_creation_input_tokens", 0),
            "cache_read_input_tokens":     getattr(final.usage, "cache_read_input_tokens", 0),
        }
        return corrected, summary, usage
    except Exception as e:
        print(f"  [citation-pass] error: {e} — skipping correction", flush=True)
        return narrative_html, "", {}


def compute_cost(usage: dict, model: str) -> float:
    p = PRICING.get(model, PRICING["claude-sonnet-4-6"])
    return (
        usage.get("input_tokens",                0) / 1e6 * p["input"] +
        usage.get("cache_creation_input_tokens", 0) / 1e6 * p["cache_write"] +
        usage.get("cache_read_input_tokens",     0) / 1e6 * p["cache_read"] +
        usage.get("output_tokens",               0) / 1e6 * p["output"]
    )


def select_phrase_editorially(
    client,
    ranked_phrases: list[tuple[str, int]],
    chamber_label: str,
    week_label: str,
) -> str:
    """Ask Claude Haiku to pick the most politically interesting phrase from the candidates.

    ranked_phrases: list of (phrase, week_count) sorted by frequency descending.
    Returns the selected phrase string; falls back to ranked_phrases[0][0] on error.
    """
    import anthropic as ant

    candidates_text = "\n".join(
        f"  {i+1}. \"{p}\" — {w} mentions this week"
        for i, (p, w) in enumerate(ranked_phrases)
    )

    system = (
        "You are an editorial assistant for a newsletter about Australian Federal Parliament. "
        "Your job is to select the single topic from a list that would make the most compelling "
        "long-read newsletter feature — one with genuine depth across multiple years of debate.\n\n"
        "STRONG PREFERENCE: Pick a substantive policy topic or political issue with broad, "
        "enduring significance — the kind of topic that has been debated across many years and "
        "many speakers. Examples of ideal picks: 'Fuel', 'Artificial Intelligence', 'Housing', "
        "'Climate Change', 'Middle East', 'Nuclear Energy', 'Immigration'.\n\n"
        "AVOID — these make poor features because they have almost no historical depth:\n"
        "  • Specific bill names by their formal title: 'Migration Amendment (2026 Measures No. 1) Bill 2026'\n"
        "  • Procedural motions: 'second reading', 'in committee', 'the amendment'\n"
        "  • Generic political actors: 'the coalition', 'the government', 'prime minister'\n"
        "  • Generic filler: 'very important', 'going forward', 'at this time'\n\n"
        "A bill name is only acceptable if the legislation is so significant that its name has "
        "become a political shorthand debated across years (e.g. 'AUKUS', 'stage three tax cuts'). "
        "A routine bill with a bureaucratic name like 'Parliamentary Frameworks Legislation "
        "Amendment (Reviews) Bill 2026' should always lose to a substantive topic.\n\n"
        "If multiple strong topics appear, prefer the one with the highest mention count — "
        "frequency signals what parliament was actually focused on this week."
    )
    user = (
        f"Topics from parliamentary debates during sitting week {week_label} — ranked by frequency:\n\n"
        f"{candidates_text}\n\n"
        f"Choose the single topic that would make the best long-read feature: substantive, "
        f"historically rich, and genuinely in focus this week.\n\n"
        f"Reply with ONLY the chosen topic, exactly as written above, with no explanation."
    )

    try:
        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=64,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        chosen = response.content[0].text.strip().strip('"')
        # Match against candidates (case-insensitive, strip quotes)
        for phrase, *_ in ranked_phrases:
            if phrase.lower() == chosen.lower():
                return phrase
        # Fuzzy fallback: check if the response contains the candidate phrase
        for phrase, *_ in ranked_phrases:
            if phrase.lower() in chosen.lower():
                return phrase
        print(f"  Haiku returned unrecognised phrase {chosen!r} — using top-ranked", flush=True)
    except Exception as e:
        print(f"  Haiku editorial filter failed ({e}) — using top-ranked", flush=True)

    return ranked_phrases[0][0]


# ── Phase 9: Data container ───────────────────────────────────────────────────

@dataclass
class PhraseResult:
    phrase:           str
    chamber:          str           # "Senate" | "House" | "Both"
    novelty_score:    float
    week_count:       int
    total_historical: int
    n_bodies_sent:    int
    spike_years:      list[int]
    matches_df:       pd.DataFrame
    narrative_html:   str = ""
    citations_html:   str = ""
    chart_year_b64:   str = ""
    chart_party_b64:  str = ""
    chart_gov_b64:    str = ""
    speakers_html:    str = ""
    first_mention:    dict = field(default_factory=dict)


# ── Phase 9: HTML assembly ─────────────────────────────────────────────────────

_INLINE_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: Georgia, 'Times New Roman', serif; background: #282828;
       color: #ebdbb2; font-size: 16px; line-height: 1.7; }
.container { max-width: 960px; margin: 0 auto; padding: 20px 24px; }
header { background: #1d2021; color: #ebdbb2; padding: 28px 32px 20px; border-bottom: 3px solid #fabd2f; }
header h1 { font-size: 24px; font-weight: 700; letter-spacing: 0.5px; color: #fabd2f; }
header .meta { font-size: 13px; color: #928374; margin-top: 5px; }
.header-chart { background: #1d2021; padding: 0; margin-bottom: 0; }
.header-chart img { width: 100%; display: block; }
.stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px;
              margin: 20px 0; }
.stat-card { background: #1d2021; border-radius: 6px; padding: 14px 16px;
             border: 1px solid #3c3836; text-align: center; }
.stat-card .num { font-size: 24px; font-weight: 700; color: #fabd2f; }
.stat-card .label { font-size: 11px; color: #928374; margin-top: 2px;
                    text-transform: uppercase; letter-spacing: 0.5px; }
.sitting-days { background: #1d2021; border-radius: 6px; padding: 14px 20px;
                margin-bottom: 20px; font-size: 14px; color: #d5c4a1;
                border: 1px solid #3c3836; }
.sitting-days strong { color: #83a598; }
.phrase-block { background: #1d2021; border-radius: 8px; padding: 28px 32px;
                margin-bottom: 28px; border: 1px solid #3c3836; }
.phrase-header { display: flex; align-items: baseline; gap: 14px; margin-bottom: 10px; }
.phrase-header h3 { font-size: 20px; color: #fabd2f; flex: 1; }
.phrase-meta { font-size: 13px; color: #928374; margin-bottom: 18px; }
.phrase-meta span { margin-right: 16px; }
.spike-callout { background: #32302f; border-left: 4px solid #fe8019;
                 padding: 10px 14px; margin: 12px 0; font-size: 13px;
                 border-radius: 0 4px 4px 0; color: #d5c4a1; }
.narrative p { margin-bottom: 14px; text-align: justify; color: #ebdbb2; }
.narrative h3 { font-size: 17px; margin-bottom: 12px; color: #fabd2f; }
.narrative blockquote { border-left: 4px solid #fabd2f; padding: 10px 16px;
                        margin: 16px 0; font-style: italic;
                        color: #d5c4a1; background: #32302f; border-radius: 0 4px 4px 0; }
.narrative .attribution { font-size: 13px; color: #928374; margin-top: -8px;
                          margin-bottom: 16px; padding-left: 20px; }
.chart-pair { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin: 20px 0; }
.chart-pair img { width: 100%; border-radius: 4px; border: 1px solid #3c3836; }
.speakers-table { width: 100%; border-collapse: collapse; font-size: 13px; margin: 16px 0; }
.speakers-table th { background: #32302f; padding: 7px 10px; text-align: left; color: #928374;
                     font-size: 11px; text-transform: uppercase; letter-spacing: 0.4px; }
.speakers-table td { padding: 7px 10px; border-bottom: 1px solid #3c3836; color: #d5c4a1; }
.citations { margin-top: 20px; padding-top: 16px; border-top: 1px solid #3c3836; }
.citations h4 { font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px;
                color: #928374; margin-bottom: 10px; }
.citations ol { padding-left: 20px; font-size: 13px; }
.citations li { margin-bottom: 10px; }
.citation-speaker { font-weight: 600; color: #83a598; }
.citation-quote { border-left: 3px solid #504945; padding: 4px 10px; margin: 4px 0;
                  font-style: italic; color: #928374; }
footer { margin-top: 48px; padding: 24px 0; border-top: 2px solid #3c3836;
         font-size: 12px; color: #928374; text-align: center; line-height: 1.6; }
sup.cite { font-size: .68em; font-weight: 600; vertical-align: super; line-height: 0; }
sup.cite a { color: #fabd2f; text-decoration: none; }
sup.cite a:hover { text-decoration: underline; }
.references { margin-top: 24px; padding-top: 16px; border-top: 1px solid #3c3836; }
.references h4 { font-size: 14px; font-weight: 700; margin-bottom: 10px; color: #d5c4a1; }
.ref-list { padding-left: 20px; font-size: 13px; color: #928374; line-height: 1.6; }
.ref-list li { margin-bottom: 6px; }
.ref-list a { color: #83a598; }
.ref-list .ref-num { font-weight: 600; margin-right: 4px; color: #d5c4a1; }
@media (max-width: 600px) {
  .chart-pair { grid-template-columns: 1fr; }
  .stats-grid { grid-template-columns: repeat(2, 1fr); }
}
"""


def _phrase_block_html(pr: PhraseResult, citations_on: bool) -> str:
    """Render one phrase section."""
    spike_callout = ""
    if pr.spike_years:
        spike_text = ", ".join(
            f"{y}" for y in sorted(pr.spike_years)
        )
        spike_callout = (
            f'<div class="spike-callout">'
            f'Notable spike year{"s" if len(pr.spike_years) > 1 else ""}: <strong>{spike_text}</strong>'
            f'</div>'
        )

    chart_party_tag = f'<img src="data:image/png;base64,{pr.chart_party_b64}" alt="Party breakdown">' if pr.chart_party_b64 else ""
    chart_gov_tag   = f'<img src="data:image/png;base64,{pr.chart_gov_b64}" alt="Gov/Opp">'    if pr.chart_gov_b64   else ""

    citations_block = pr.citations_html if citations_on else ""

    return (
        f'<article class="phrase-block">'
        f'<div class="phrase-header">'
        f'<div style="flex:1">'
        f'<div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">'
        f'<h2 style="margin:0;font-size:1.4em;color:#ebdbb2">Search term: '
        f'<em style="color:#fabd2f">"{pr.phrase}"</em></h2>'
        f'</div>'
        f'<div style="margin-top:4px;font-size:14px;color:#928374">'
        f'{pr.week_count} mentions this week · {pr.total_historical:,} total since 1998'
        f'</div>'
        f'</div>'
        f'</div>'
        f'<div class="phrase-meta">'
        f'<span>Chamber: <strong style="color:#83a598">{pr.chamber}</strong></span>'
        f'<span>Analysed: {pr.n_bodies_sent} speech turns</span>'
        f'</div>'
        f'{spike_callout}'
        f'<div class="narrative">{pr.narrative_html}</div>'
        f'<div class="chart-pair">{chart_party_tag}{chart_gov_tag}</div>'
        f'{pr.speakers_html}'
        f'{citations_block}'
        f'</article>'
    )


def build_newsletter_html(
    result: PhraseResult,
    week_label: str,
    issue_label: str,
    sitting_days: dict[str, list[str]],
    citations_on: bool,
) -> str:
    generated_at   = datetime.utcnow().strftime("%d %B %Y, %H:%M UTC")
    senate_days_str = ", ".join(sitting_days.get("SENATE", [])) or "Did not sit"
    house_days_str  = ", ".join(sitting_days.get("REPS", []))   or "Did not sit"
    issue_n        = issue_label.split("issue-")[-1] if "issue-" in issue_label else "1"
    phrase_display = result.phrase.title() if result.phrase else ""

    stats_html = (
        f'<div class="stats-grid">'
        f'<div class="stat-card"><div class="num">{len(sitting_days.get("SENATE", []))}</div><div class="label">Senate sitting days</div></div>'
        f'<div class="stat-card"><div class="num">{len(sitting_days.get("REPS", []))}</div><div class="label">House sitting days</div></div>'
        f'<div class="stat-card"><div class="num">{result.total_historical:,}</div><div class="label">Historical mentions</div></div>'
        f'<div class="stat-card"><div class="num">#{issue_n}</div><div class="label">Issue for this week</div></div>'
        f'</div>'
    )

    phrase_display = result.phrase.title() if result.phrase else ""
    header_chart_html = (
        f'<div class="header-chart">'
        f'<img src="data:image/png;base64,{result.chart_year_b64}" alt="Year trend for {result.phrase}">'
        f'</div>'
    ) if result.chart_year_b64 else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hansard Weekly Digest — {week_label} Issue {issue_n}</title>
<style>{_INLINE_CSS}</style>
</head>
<body>
<header>
  <h1>Hansard Weekly Digest</h1>
  <div style="font-size:18px;color:#ebdbb2;margin-top:6px;">Topic: <em style="color:#fabd2f">{phrase_display}</em></div>
  <div class="meta">Sitting Week {week_label} &nbsp;·&nbsp; Issue {issue_n} &nbsp;·&nbsp; Generated {generated_at}</div>
</header>
{header_chart_html}
<div class="container">
  {stats_html}
  <div class="sitting-days">
    <strong>Senate sat:</strong> {senate_days_str} &nbsp;&nbsp;
    <strong>House sat:</strong> {house_days_str}
  </div>
  {_phrase_block_html(result, citations_on)}
  <footer>
    <p><strong>Methodology:</strong> Phrases ranked by frequency across both chambers in the sitting week, editorially selected by Claude Haiku.
    Historical context drawn from Australian Federal Parliament Hansard 1998–present (parliament dates to 1901).
    Narratives generated by Claude ({datetime.utcnow().year}) and grounded in the speech-turn data shown.</p>
    <p style="margin-top:8px">Generated {generated_at}</p>
  </footer>
</div>
</body>
</html>"""


# ── Phase 10: main() ──────────────────────────────────────────────────────────

def _process_chamber_phrase(
    phrase: str,
    week_count: int,
    conn: sqlite3.Connection,
    chamber_key: Optional[str],
    chamber_label: str,
    log_path: Path,
    client,
    system_prompt: str,
    args,
    week_turns: Optional[pd.DataFrame] = None,
    out_dir: Optional[Path] = None,
) -> Optional[PhraseResult]:
    """Full pipeline for a single phrase (FTS5 search → spikes → select → narrative).

    Historical search always spans both chambers regardless of chamber_key, so the
    narrative has the full parliamentary record.  chamber_label is used for display only.
    """
    print(f"  Phrase: '{phrase}' (this_week={week_count})", flush=True)
    matches_df = search_phrase_in_fts(conn, phrase, None)  # always both chambers
    if matches_df.empty:
        print(f"    → no historical matches, skipping")
        return None

    print(f"    → {len(matches_df):,} historical matches")
    spikes      = detect_spikes(matches_df)
    spike_years = [s["year"] for s in spikes]
    bodies      = select_bodies_for_claude(matches_df, phrase, max_turns=args.max_turns)
    first_men   = get_first_mention(matches_df, phrase)

    # Pre-select week turns once — used consistently by narrative, citation pass, and renderer
    week_bodies = select_week_turns_for_context(week_turns, phrase=phrase) if week_turns is not None else []
    if week_bodies:
        print(f"    → {len(week_bodies)} week turns selected ({week_count} matching this week)", flush=True)

    # Charts
    chart_y = chart_year_trend(matches_df,   phrase, chamber_label)
    chart_p = chart_party_breakdown(matches_df, phrase)
    chart_g = chart_gov_opp(matches_df,        phrase)

    # Narrative
    # Rebuild system prompt now that we know whether there are current-week turns
    phrase_system_prompt = build_system_prompt(
        citations=args.citations,
        has_week_turns=bool(week_bodies),
    )
    if args.dry_run:
        narrative_html = f'<p><em>[Dry run — no API call. Phrase: "{phrase}", {len(bodies)} turns analysed.]</em></p>'
        raw_response   = ""
        usage          = {}
    else:
        user_prompt = build_phrase_user_prompt(
            phrase=phrase,
            chamber=chamber_label,
            week_label=args.week_label,
            week_count=week_count,
            novelty_score=0.0,
            first_mention=first_men,
            matches_df=matches_df,
            spike_annotations=spikes,
            bodies_for_claude=bodies,
            citations=args.citations,
            max_citations=args.max_citations,
            week_bodies=week_bodies,
        )
        narrative_html, raw_response, usage = generate_phrase_narrative(
            client, phrase_system_prompt, user_prompt,
            model=args.model,
            no_cache=args.no_cache,
            citations=args.citations,
        )
        cost = compute_cost(usage, args.model)
        _log(log_path, {
            "event":      "narrative",
            "phrase":     phrase,
            "chamber":    chamber_label,
            "n_bodies":   len(bodies),
            "n_matches":  len(matches_df),
            "cost_usd":   round(cost, 4),
            "usage":      usage,
        })
        print(f"    → narrative generated ({usage.get('output_tokens',0)} output tokens, ${cost:.3f})", flush=True)

        # Second-pass citation correction — only when using Sonnet and citations are on.
        # Haiku/Opus don't get the correction pass (cost vs benefit).
        if args.citations and narrative_html and "sonnet" in args.model.lower():
            if out_dir:
                (out_dir / "narrative_pass1.html").write_text(narrative_html, encoding="utf-8")
            print("    → citation-pass (Sonnet)…", flush=True)
            narrative_html, citation_summary, usage2 = correct_citations(
                client, narrative_html, bodies, args.model, week_bodies=week_bodies,
            )
            if out_dir:
                (out_dir / "narrative_pass2.html").write_text(narrative_html, encoding="utf-8")
                if citation_summary:
                    (out_dir / "citation_pass_summary.txt").write_text(citation_summary, encoding="utf-8")
            cost2 = compute_cost(usage2, args.model)
            _log(log_path, {
                "event":    "citation_pass",
                "phrase":   phrase,
                "chamber":  chamber_label,
                "cost_usd": round(cost2, 4),
                "usage":    usage2,
                "summary":  citation_summary,
            })
            if citation_summary:
                print(f"    → citation-pass summary: {citation_summary[:120]}…", flush=True)
            print(f"    → citation-pass done (${cost2:.3f})", flush=True)

    # Post-process inline [N] citations → superscript links + reference list
    # unified turn list: historical bodies first, then week_bodies (same ordering used by prompts)
    citations_html = ""
    if args.citations and narrative_html:
        all_turns = list(bodies) + [
            {**wb, "speaker": wb.get("name", wb.get("speaker", "?"))}
            for wb in (week_bodies or [])
        ]
        narrative_html, citations_html = apply_inline_citations(narrative_html, all_turns)

    return PhraseResult(
        phrase=phrase,
        chamber=chamber_label,
        novelty_score=0.0,
        week_count=week_count,
        total_historical=len(matches_df),
        n_bodies_sent=len(bodies),
        spike_years=spike_years,
        matches_df=matches_df,
        narrative_html=narrative_html,
        citations_html=citations_html,
        chart_year_b64=chart_y,
        chart_party_b64=chart_p,
        chart_gov_b64=chart_g,
        speakers_html=build_top_speakers_table(matches_df),
        first_mention=first_men,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Hansard Weekly Newsletter Pipeline")
    ap.add_argument("--week",          default=None,
                    help="ISO week to analyse, e.g. 2026-W10. Default: auto-detect last sitting week.")
    ap.add_argument("--lookback",      type=int, default=12,
                    help="Weeks to look back when auto-detecting sitting week (default 12).")
    ap.add_argument("--model",         default="claude-sonnet-4-6")
    ap.add_argument("--dry-run",       action="store_true",
                    help="Skip API calls; use placeholder narratives.")
    ap.add_argument("--no-cache",      action="store_true",
                    help="Disable prompt caching.")
    ap.add_argument("--no-citations",  action="store_true",
                    help="Suppress Hansard citations block (citations on by default).")
    ap.add_argument("--max-citations", type=int, default=2,
                    help="Max citations per phrase section (default 2, range 1-3).")
    ap.add_argument("--out-dir",       type=Path, default=None,
                    help="Override output directory (default: newsletters/<issue_label>/).")
    ap.add_argument("--allow-repeats", action="store_true",
                    help="Allow phrases already used in prior issues of this week (useful when the pool is exhausted).")
    ap.add_argument("--phrase",        default=None,
                    help="Manually specify the search phrase; skips stage direction extraction and Haiku selection.")
    ap.add_argument("--max-turns",     type=int, default=MAX_BODIES_FOR_CLAUDE,
                    help=f"Maximum historical speech turns passed to Claude (default: {MAX_BODIES_FOR_CLAUDE}).")
    args = ap.parse_args()

    args.citations    = not args.no_citations
    args.max_citations = max(1, min(3, args.max_citations))

    # ── 1. Resolve week ──────────────────────────────────────────────────────
    if args.week:
        week_start, week_end, week_label = parse_week_label(args.week)
        print(f"Using specified week: {week_label}")
    else:
        week_start, week_end, week_label = find_last_sitting_week(args.lookback)
        print(f"Auto-detected sitting week: {week_label}")
    args.week_label = week_label

    # ── 2. Sitting days ──────────────────────────────────────────────────────
    sitting_days = get_sitting_days(week_start, week_end)
    senate_days  = sitting_days["SENATE"]
    house_days   = sitting_days["REPS"]
    print(f"Senate sitting days ({len(senate_days)}): {senate_days}")
    print(f"House  sitting days ({len(house_days)}):  {house_days}")

    # ── 3. Manifest ──────────────────────────────────────────────────────────
    NEWSLETTERS.mkdir(parents=True, exist_ok=True)
    manifest    = load_manifest()
    used        = get_used_phrases(manifest, week_label)
    issue_label = next_issue_label(manifest, week_label)
    print(f"Issue: {issue_label}")

    # ── 4. Output directory ───────────────────────────────────────────────────
    out_dir = args.out_dir or (NEWSLETTERS / issue_label)
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "run_log.jsonl"
    _log(log_path, {"event": "start", "week": week_label, "issue": issue_label,
                    "senate_days": senate_days, "house_days": house_days,
                    "dry_run": args.dry_run})

    # ── 5. FTS database ───────────────────────────────────────────────────────
    print(f"Opening FTS database: {FTS_DB_PATH}", flush=True)
    conn = sqlite3.connect(str(FTS_DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA query_only = ON")

    # ── 6. Claude client ──────────────────────────────────────────────────────
    client        = None
    system_prompt = ""
    if not args.dry_run:
        import anthropic as ant
        client        = ant.Anthropic()
        system_prompt = build_system_prompt(citations=args.citations)

    result: Optional[PhraseResult] = None

    # ── COMBINED PASS (Senate + House) ────────────────────────────────────────
    all_days = sorted(set(senate_days + house_days))
    if all_days:
        print(f"\n── Combined pass ({len(all_days)} sitting days across both chambers) ──")
        print(f"  Loading week corpus…", flush=True)
        week_df = load_week_from_fts(conn, None, all_days)
        print(f"  {len(week_df):,} rows this week")

        if args.phrase:
            # Manual phrase — skip topic extraction and Haiku selection entirely
            chosen = args.phrase.strip()
            week_turns = _df_matches_phrase(week_df, chosen)
            week_count = len(week_turns)
            del week_df
            print(f"  Manual phrase: '{chosen}' ({week_count} matching turns this week)", flush=True)
        else:
            print(f"  Extracting topics from stage directions…", flush=True)
            all_topics = extract_stage_direction_topics(conn, all_days)
            used_all = set() if args.allow_repeats else (used["senate"] | used["house"] | used["joint"])
            top_topics = [(t, c) for t, c in all_topics if t not in used_all][:30]

            pd.DataFrame([{"topic": t, "count": c} for t, c in top_topics]
                         ).to_csv(out_dir / "topics_stage_directions.csv", index=False)
            print(f"  Top topics: {[t for t, _ in top_topics[:5]]}")

            if not top_topics:
                del week_df
                print("  No candidate topics found.")
                chosen = None
            else:
                if args.dry_run or client is None:
                    chosen, week_count = top_topics[0][0], top_topics[0][1]
                else:
                    chosen = select_phrase_editorially(client, top_topics, "Both Chambers", week_label)
                    week_count = next((c for t, c in top_topics if t == chosen), top_topics[0][1])
                    print(f"  Haiku selected: '{chosen}'", flush=True)

                week_turns = _df_matches_phrase(week_df, chosen)
                del week_df

        if chosen:
            result = _process_chamber_phrase(
                chosen, week_count, conn, None, "Both Chambers",
                log_path, client, system_prompt, args,
                week_turns=week_turns,
                out_dir=out_dir,
            )
    else:
        print("Parliament did not sit this week.")

    # ── BUILD HTML ────────────────────────────────────────────────────────────
    print(f"\n── Building newsletter HTML…")
    if result is None:
        result = PhraseResult(
            phrase="(no phrase)", chamber="—", novelty_score=0.0, week_count=0,
            total_historical=0, n_bodies_sent=0, spike_years=[], matches_df=pd.DataFrame(),
            narrative_html="<p>No candidate phrases were found for this sitting week.</p>",
        )
    html = build_newsletter_html(
        result=result,
        week_label=week_label,
        issue_label=issue_label,
        sitting_days=sitting_days,
        citations_on=args.citations,
    )
    html = inject_citation_popups(html)
    html_path = out_dir / "newsletter.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"  Written: {html_path}")
    if result.phrase and result.phrase != "(no phrase)":
        (out_dir / "phrase.txt").write_text(result.phrase, encoding="utf-8")

    # ── UPDATE MANIFEST ───────────────────────────────────────────────────────
    phrase_used = result.phrase if result.phrase != "(no phrase)" else None
    if phrase_used:
        new_phrases = {"senate": [phrase_used], "house": [phrase_used], "joint": [phrase_used]}
        update_manifest(manifest, week_label, issue_label, new_phrases)
        print(f"  Manifest updated ('{phrase_used}' recorded).")

    # ── COST SUMMARY ─────────────────────────────────────────────────────────
    total_cost = 0.0
    if not args.dry_run:
        with open(log_path, encoding="utf-8") as f:
            for line in f:
                try:
                    ev = json.loads(line)
                    total_cost += ev.get("cost_usd", 0.0)
                except json.JSONDecodeError:
                    pass

    _log(log_path, {"event": "complete", "issue": issue_label, "total_cost_usd": round(total_cost, 4)})

    conn.close()

    print(f"\n{'─'*60}")
    print(f"  Issue:          {issue_label}")
    print(f"  Sitting week:   {week_label}")
    print(f"  Phrase:         {result.phrase}")
    print(f"  Output:         {out_dir}")
    if not args.dry_run:
        print(f"  API cost:       ${total_cost:.3f}")
    print(f"{'─'*60}")


if __name__ == "__main__":
    main()