#!/usr/bin/env python3
"""
10_build_fts_index.py — Build (or rebuild) the Hansard SQLite database.

Writes all speech metadata + body text into a single `speeches` table and
builds an FTS5 content table (`speeches_fts`) on top of it.  The SQLite rowid
is used directly as the cross-reference key (no offset arithmetic needed).

Usage
-----
    python pipeline/10_build_fts_index.py

Output: data/output/fts/hansard_fts.db  (~4–6 GB)

Schema
------
    speeches       — one row per speech (metadata + body)
    speeches_fts   — FTS5 content table backed by speeches.body
"""

import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE        = Path(__file__).parent.parent
SENATE_PATH = BASE / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2026.parquet"
HOUSE_PATH  = BASE / "data/output/house/corpus/house_hansard_corpus_1998_to_2026.parquet"
FTS_DIR     = BASE / "data/output/fts"
FTS_DB_PATH = FTS_DIR / "hansard_fts.db"

CHUNK_SIZE  = 50_000   # rows per INSERT batch


# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_SPEECHES = """
CREATE TABLE speeches (
    chamber               TEXT NOT NULL,
    date                  TEXT NOT NULL,
    name                  TEXT,
    name_id               TEXT,
    "order"               INTEGER,
    speech_no             INTEGER,
    page_no               TEXT,
    time_stamp            TEXT,
    party                 TEXT,
    state                 TEXT,
    electorate            TEXT,
    in_gov                INTEGER,
    first_speech          INTEGER,
    question              INTEGER,
    answer                INTEGER,
    q_in_writing          INTEGER,
    div_flag              INTEGER,
    gender                TEXT,
    interject             INTEGER,
    has_embedded_interject INTEGER,
    unique_id             TEXT,
    partyfacts_id         REAL,
    senate_flag           INTEGER,
    fedchamb_flag         INTEGER,
    body                  TEXT NOT NULL DEFAULT ''
)
"""

_CREATE_INDEXES = [
    "CREATE INDEX speeches_chamber_date ON speeches(chamber, date)",
    "CREATE INDEX speeches_party        ON speeches(party)",
    "CREATE INDEX speeches_name_id      ON speeches(name_id)",
]

_CREATE_FTS = """
CREATE VIRTUAL TABLE speeches_fts USING fts5(
    body,
    content='speeches',
    content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 2'
)
"""

# Columns to extract from each Parquet file; missing columns become NULL.
_SENATE_COLS = [
    "date", "name", "name_id", "order", "speech_no", "page_no", "time_stamp",
    "party", "state", "in_gov", "first_speech", "question", "answer",
    "q_in_writing", "div_flag", "gender", "interject", "unique_id",
    "partyfacts_id", "senate_flag", "body",
]
_HOUSE_COLS = [
    "date", "name", "name_id", "order", "speech_no", "page_no", "time_stamp",
    "party", "electorate", "in_gov", "first_speech", "question", "answer",
    "q_in_writing", "div_flag", "gender", "interject", "has_embedded_interject",
    "unique_id", "partyfacts_id", "fedchamb_flag", "body",
]

# INSERT column order matches CREATE TABLE (excluding rowid and chamber which are
# handled separately).
_INSERT_SQL = """
INSERT INTO speeches (
    chamber, date, name, name_id, "order", speech_no, page_no, time_stamp,
    party, state, electorate, in_gov, first_speech, question, answer,
    q_in_writing, div_flag, gender, interject, has_embedded_interject,
    unique_id, partyfacts_id, senate_flag, fedchamb_flag, body
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS speeches_fts")
    conn.execute("DROP TABLE IF EXISTS speeches")
    conn.execute(_CREATE_SPEECHES)
    for idx in _CREATE_INDEXES:
        conn.execute(idx)
    conn.execute(_CREATE_FTS)
    conn.commit()


def _load_parquet(path: Path, wanted_cols: list, chamber: str) -> pd.DataFrame:
    if not path.exists():
        print(f"  WARNING: corpus not found: {path}", file=sys.stderr)
        return pd.DataFrame()
    print(f"  Reading {chamber} corpus …", flush=True)
    existing = pd.read_parquet(path, columns=None).columns.tolist()
    cols = [c for c in wanted_cols if c in existing]
    df = pd.read_parquet(path, columns=cols)
    df["body"] = df["body"].fillna("") if "body" in df.columns else ""
    print(f"    {len(df):,} rows")
    return df


def _row_iter(df: pd.DataFrame, chamber: str):
    """Yield tuples matching the INSERT_SQL parameter order."""
    for row in df.itertuples(index=False):
        def g(col):
            v = getattr(row, col, None)
            if v is None:
                return None
            # Convert numpy scalar → Python scalar; leave str/None as-is
            try:
                if hasattr(v, 'item'):
                    return v.item()
            except Exception:
                pass
            return v

        yield (
            chamber,
            g("date"),
            g("name"),
            g("name_id"),
            g("order"),
            g("speech_no"),
            g("page_no"),
            g("time_stamp"),
            g("party"),
            g("state"),
            g("electorate"),
            g("in_gov"),
            g("first_speech"),
            g("question"),
            g("answer"),
            g("q_in_writing"),
            g("div_flag"),
            g("gender"),
            g("interject"),
            g("has_embedded_interject"),
            g("unique_id"),
            g("partyfacts_id"),
            g("senate_flag"),
            g("fedchamb_flag"),
            g("body"),
        )


def _insert_corpus(conn: sqlite3.Connection, df: pd.DataFrame, chamber: str) -> int:
    if df.empty:
        return 0
    total = len(df)
    inserted = 0
    rows = list(_row_iter(df, chamber))
    for start in range(0, total, CHUNK_SIZE):
        chunk = rows[start: start + CHUNK_SIZE]
        conn.executemany(_INSERT_SQL, chunk)
        inserted += len(chunk)
        print(f"    {chamber}: {inserted:,} / {total:,}\r", end="", flush=True)
    print()
    conn.commit()
    return inserted


def full_build() -> None:
    FTS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Building Hansard SQLite DB at {FTS_DB_PATH} …")
    t0 = time.perf_counter()

    conn = sqlite3.connect(str(FTS_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-131072")   # 128 MB page cache

    try:
        _create_schema(conn)

        senate_df = _load_parquet(SENATE_PATH, _SENATE_COLS, "senate")
        n_senate  = _insert_corpus(conn, senate_df, "senate")
        del senate_df

        house_df  = _load_parquet(HOUSE_PATH, _HOUSE_COLS, "house")
        n_house   = _insert_corpus(conn, house_df, "house")
        del house_df

        print(f"  Total rows inserted: {n_senate + n_house:,} ({n_senate:,} senate, {n_house:,} house)")

        print("  Rebuilding FTS5 index …", flush=True)
        conn.execute("INSERT INTO speeches_fts(speeches_fts) VALUES('rebuild')")
        conn.commit()

        print("  Optimising FTS5 index …", flush=True)
        conn.execute("INSERT INTO speeches_fts(speeches_fts) VALUES('optimize')")
        conn.commit()

    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    print(f"Done in {elapsed:.1f}s — {FTS_DB_PATH}")


if __name__ == "__main__":
    full_build()
