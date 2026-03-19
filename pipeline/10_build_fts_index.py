#!/usr/bin/env python3
"""
10_build_fts_index.py — Build (or rebuild) an SQLite FTS5 search index from
the Senate and House corpus Parquet files.

Usage
-----
Full rebuild (default):
    python 10_build_fts_index.py

The output database is written to:
    data/output/fts/hansard_fts.db

Schema
------
Each row stores the chamber name, the 0-based parquet row index (matching
the _parquet_idx column added by app._load_corpus), and the speech body.
Using parquet_idx as the key (not unique_id, which is a speaker identifier
and appears ~2300 times per person) ensures exact per-speech lookup.

    CREATE VIRTUAL TABLE speeches_fts USING fts5(
        chamber     UNINDEXED,   -- 'senate' or 'house'
        parquet_idx UNINDEXED,   -- integer, matches df._parquet_idx
        body,
        tokenize='unicode61 remove_diacritics 2'
    )
"""

import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
BASE        = Path(__file__).parent.parent
SENATE_PATH = BASE / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2026.parquet"
HOUSE_PATH  = BASE / "data/output/house/corpus/house_hansard_corpus_1998_to_2026.parquet"
FTS_DIR     = BASE / "data/output/fts"
FTS_DB_PATH = FTS_DIR / "hansard_fts.db"

CHUNK_SIZE  = 50_000   # rows per INSERT batch


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS speeches_fts")
    conn.execute("""
        CREATE VIRTUAL TABLE speeches_fts USING fts5(
            chamber     UNINDEXED,
            parquet_idx UNINDEXED,
            body,
            tokenize='unicode61 remove_diacritics 2'
        )
    """)
    conn.commit()


def _load_corpus(path: Path, chamber: str) -> pd.DataFrame:
    if not path.exists():
        print(f"  WARNING: corpus not found: {path}", file=sys.stderr)
        return pd.DataFrame(columns=["body"])
    print(f"  Reading {chamber} corpus …", flush=True)
    df = pd.read_parquet(path, columns=["body"])
    df["body"] = df["body"].fillna("")
    print(f"    {len(df):,} rows")
    return df


def _insert_corpus(conn: sqlite3.Connection, df: pd.DataFrame, chamber: str) -> int:
    """Bulk-insert all rows for one chamber; parquet_idx = row number (0-based)."""
    inserted = 0
    total = len(df)
    for start in range(0, total, CHUNK_SIZE):
        chunk = df.iloc[start : start + CHUNK_SIZE]
        conn.executemany(
            "INSERT INTO speeches_fts(chamber, parquet_idx, body) VALUES (?, ?, ?)",
            (
                (chamber, start + i, body)
                for i, body in enumerate(chunk["body"])
            ),
        )
        inserted += len(chunk)
        print(f"    {chamber}: {inserted:,} / {total:,}\r", end="", flush=True)
    print()
    return inserted


def full_build() -> None:
    FTS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Building FTS5 index at {FTS_DB_PATH} …")
    t0 = time.perf_counter()

    conn = sqlite3.connect(str(FTS_DB_PATH))
    try:
        _create_schema(conn)

        senate = _load_corpus(SENATE_PATH, "senate")
        _insert_corpus(conn, senate, "senate")

        house = _load_corpus(HOUSE_PATH, "house")
        _insert_corpus(conn, house, "house")

        total = len(senate) + len(house)
        print(f"  Total rows indexed: {total:,}")
        print("  Running FTS optimize …", flush=True)
        conn.execute("INSERT INTO speeches_fts(speeches_fts) VALUES('optimize')")
        conn.commit()

    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    print(f"Done in {elapsed:.1f}s — {FTS_DB_PATH}")


if __name__ == "__main__":
    full_build()
