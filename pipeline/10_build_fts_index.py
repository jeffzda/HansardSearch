#!/usr/bin/env python3
"""
10_build_fts_index.py — Build (or rebuild) an SQLite FTS5 search index from
the Senate and House corpus Parquet files.

Usage
-----
Full rebuild (default):
    python 10_build_fts_index.py

Incremental — delete and re-insert specific rows by unique_id:
    python 10_build_fts_index.py --incremental --unique-ids S1234 H5678 ...

The output database is written to:
    data/output/fts/hansard_fts.db
"""

import argparse
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
            unique_id UNINDEXED,
            body,
            tokenize='unicode61 remove_diacritics 2'
        )
    """)
    conn.commit()


def _load_corpus(path: Path, chamber: str) -> pd.DataFrame:
    if not path.exists():
        print(f"  WARNING: corpus not found: {path}", file=sys.stderr)
        return pd.DataFrame(columns=["unique_id", "body"])
    print(f"  Reading {chamber} corpus …", flush=True)
    df = pd.read_parquet(path, columns=["unique_id", "body"])
    df = df.dropna(subset=["unique_id"])
    df["body"] = df["body"].fillna("")
    print(f"    {len(df):,} rows")
    return df


def _insert_df(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Bulk-insert in chunks; returns total rows inserted."""
    inserted = 0
    for start in range(0, len(df), CHUNK_SIZE):
        chunk = df.iloc[start : start + CHUNK_SIZE][["unique_id", "body"]]
        conn.executemany(
            "INSERT INTO speeches_fts(unique_id, body) VALUES (?, ?)",
            chunk.itertuples(index=False, name=None),
        )
        inserted += len(chunk)
        print(f"    inserted {inserted:,} / {len(df):,}\r", end="", flush=True)
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
        house  = _load_corpus(HOUSE_PATH,  "house")
        combined = pd.concat([senate, house], ignore_index=True)
        print(f"  Total rows to index: {len(combined):,}")

        _insert_df(conn, combined)

        print("  Running FTS optimize …", flush=True)
        conn.execute("INSERT INTO speeches_fts(speeches_fts) VALUES('optimize')")
        conn.commit()

    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    print(f"Done in {elapsed:.1f}s — {FTS_DB_PATH}")


def incremental_build(unique_ids: list[str]) -> None:
    if not FTS_DB_PATH.exists():
        print("No existing FTS DB found — running full build instead.")
        full_build()
        return

    print(f"Incremental update: {len(unique_ids)} unique_ids …")
    t0 = time.perf_counter()

    senate = _load_corpus(SENATE_PATH, "senate")
    house  = _load_corpus(HOUSE_PATH,  "house")
    combined = pd.concat([senate, house], ignore_index=True)
    uid_set  = set(unique_ids)
    rows     = combined[combined["unique_id"].isin(uid_set)]

    conn = sqlite3.connect(str(FTS_DB_PATH))
    try:
        # Delete existing rows for these ids
        for uid in unique_ids:
            conn.execute(
                "DELETE FROM speeches_fts WHERE unique_id = ?", (uid,)
            )
        # Re-insert
        _insert_df(conn, rows)
        conn.execute("INSERT INTO speeches_fts(speeches_fts) VALUES('optimize')")
        conn.commit()
    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    print(f"Done in {elapsed:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Hansard FTS5 index")
    parser.add_argument(
        "--incremental", action="store_true",
        help="Delete + re-insert only the specified rows (default: full rebuild)"
    )
    parser.add_argument(
        "--unique-ids", nargs="+", metavar="ID",
        help="unique_id values to update (required with --incremental)"
    )
    args = parser.parse_args()

    if args.incremental:
        if not args.unique_ids:
            parser.error("--incremental requires --unique-ids")
        incremental_build(args.unique_ids)
    else:
        full_build()


if __name__ == "__main__":
    main()
