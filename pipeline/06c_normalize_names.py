#!/usr/bin/env python3
"""
06c_normalize_names.py — Normalise the `name` field in daily parquets and corpus.

Fixes three issues introduced by XML variation across years:
  1. Trailing colons:   "Senator ABETZ:"  → "Senator ABETZ"
  2. Mixed-case forms:  "Senator Abetz"   → "Senator ABETZ"   (same name_id)
  3. Full first names:  "Mr TONY SMITH"   → "Mr SMITH"

Canonical form is derived from the lookup tables:
  • Senate  : senator_lookup.form4  e.g. "Senator ABETZ"
  • House   : {form2_honorific} + {SURNAME} e.g. "Mr ABBOTT", "Dr ALLEN"
    Mc/Mac prefix surnames are preserved:  "Mr McCLELLAND", "Ms MacTIERNAN"

Name_ids not in any lookup (garbled IDs, procedural roles) receive only the
colon-strip treatment, plus first-name stripping for 3-word names where the
second token is not a procedural keyword.

Usage:
    python 06c_normalize_names.py --chamber senate
    python 06c_normalize_names.py --chamber house
    python 06c_normalize_names.py --chamber senate --corpus-only
    python 06c_normalize_names.py --chamber senate --dry-run
"""
import argparse
import re
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from parallel_utils import eager_map

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

PATHS = {
    "senate": {
        "daily":   ROOT / "data/output/senate/daily",
        "corpus":  ROOT / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.parquet",
        "lookup":  ROOT / "data/lookup/senator_lookup.csv",
    },
    "house": {
        "daily":   ROOT / "data/output/house/daily",
        "corpus":  ROOT / "data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet",
        "lookup":  ROOT / "data/lookup/member_lookup.csv",
    },
}

# Procedural second-word tokens: names containing these are NOT stripped
_PROCEDURAL = {
    "DEPUTY", "ACTING", "SPEAKER", "PRESIDENT", "CHAIR", "CHAIRMAN",
    "CHAIRPERSON", "TEMPORARY", "HONOURABLE", "HON",
}

# Standard parliamentary honorifics
_HONORIFICS = {"Mr", "Mrs", "Ms", "Miss", "Dr", "Senator", "Prof", "Rev"}


def canonical_surname_case(surname: str) -> str:
    """Return surname in Hansard canonical uppercase form.

    Preserves Mc/Mac prefix for compound surnames:
        McClelland → McCLELLAND
        MacTiernan → MacTIERNAN
        Macklin    → MACKLIN   (lowercase after Mac → not a compound)
        Abbott     → ABBOTT
        O'Brien    → O'BRIEN
    """
    if re.match(r"Mc[A-Z]", surname):
        return "Mc" + surname[2:].upper()
    if re.match(r"Mac[A-Z]", surname):
        return "Mac" + surname[3:].upper()
    return surname.upper()


def build_canonical_map_senate(lookup_path: Path) -> dict[str, str]:
    """Return {name_id: canonical_name} for senators using form4."""
    df = pd.read_csv(lookup_path)
    result = {}
    for _, row in df.iterrows():
        nid = row.get("name_id")
        f4 = row.get("form4")
        if pd.notna(nid) and pd.notna(f4) and str(f4).strip():
            result[str(nid)] = str(f4).strip()
    return result


def build_canonical_map_house(lookup_path: Path) -> dict[str, str]:
    """Return {name_id: canonical_name} for House members.

    Constructs '{honorific} {CANONICAL_SURNAME}' from form2 + surname.
    """
    df = pd.read_csv(lookup_path)
    result = {}
    for _, row in df.iterrows():
        nid = row.get("name_id")
        surname = row.get("surname")
        form2 = row.get("form2")

        if pd.isna(nid) or pd.isna(surname) or pd.isna(form2):
            continue
        nid = str(nid)
        surname = str(surname).strip()
        form2 = str(form2).strip()

        # Extract honorific (first word of form2)
        parts = form2.split()
        if not parts:
            continue
        honorific = parts[0]  # e.g. "Mr", "Ms", "Dr"

        canonical = f"{honorific} {canonical_surname_case(surname)}"
        result[nid] = canonical
    return result


def normalize_name(name: str,
                   name_id,
                   canon_map: dict[str, str]) -> str:
    """Return the normalised name for a single row."""
    if not isinstance(name, str) or not name.strip():
        return name

    # Step 1: strip trailing colon and whitespace
    cleaned = name.rstrip(":").strip()

    # Step 2: lookup-based canonical replacement
    nid = str(name_id) if pd.notna(name_id) else None
    if nid and nid in canon_map:
        return canon_map[nid]

    # Step 3: fallback — strip first name from 3-word patterns
    # Only apply to names like "Mr TONY SMITH" where a first name is sandwiched
    tokens = cleaned.split()
    if len(tokens) == 3 and tokens[0] in _HONORIFICS:
        # Don't strip if middle token looks procedural
        if tokens[1].upper() not in _PROCEDURAL:
            honorific = tokens[0]
            surname = tokens[2].rstrip(")")
            return f"{honorific} {surname}"
    elif len(tokens) >= 4 and tokens[0] in _HONORIFICS:
        # 4-word: "Mr TED O'BRIEN", "Mr MARTIN FERGUSON" → keep honorific + last token
        if tokens[1].upper() not in _PROCEDURAL and not cleaned.startswith("The "):
            honorific = tokens[0]
            surname = tokens[-1].rstrip(")")
            # Only strip if last token looks like a plain surname (no brackets etc.)
            if "(" not in surname and ")" not in surname:
                return f"{honorific} {surname}"

    return cleaned


def apply_to_df(df: pd.DataFrame, canon_map: dict[str, str]) -> tuple[pd.DataFrame, int]:
    """Apply name normalization to a DataFrame. Returns (df, n_changed)."""
    original = df["name"].copy()
    df["name"] = [
        normalize_name(n, nid, canon_map)
        for n, nid in zip(df["name"], df.get("name_id", [None] * len(df)))
    ]
    changed = (df["name"] != original).sum()
    return df, int(changed)


# ── Worker globals (module-level for pickle compatibility) ────────────────────

_CANON_MAP: dict[str, str] | None = None
_DRY_RUN: bool | None = None


def _init_worker(canon_map_tuple: tuple) -> None:
    """Set shared state once per worker process."""
    global _CANON_MAP, _DRY_RUN
    _CANON_MAP, _DRY_RUN = canon_map_tuple


def _normalize_one(path_str: str) -> tuple[str, int]:
    """
    Apply name normalisation to one daily parquet file.
    Returns (path_str, n_changed).
    """
    f  = Path(path_str)
    df = pd.read_parquet(f)
    df, changed = apply_to_df(df, _CANON_MAP)
    if changed > 0 and not _DRY_RUN:
        df.to_parquet(f, index=False)
    return (path_str, changed)


def update_daily_files(daily_dir: Path, canon_map: dict[str, str],
                       dry_run: bool) -> tuple[int, int]:
    """Update all daily parquet files in-place."""
    files = sorted(daily_dir.glob("*.parquet"))
    items = [str(f) for f in files]

    results = eager_map(
        _normalize_one,
        items,
        initializer=_init_worker,
        initargs=((canon_map, dry_run),),
        desc="Normalising daily files",
        unit="file",
    )

    files_updated = sum(1 for _, n in results if n > 0)
    rows_changed  = sum(n for _, n in results)
    return files_updated, rows_changed


def update_corpus(corpus_path: Path, canon_map: dict[str, str],
                  dry_run: bool) -> int:
    """Update corpus parquet in-place."""
    df = pd.read_parquet(corpus_path)
    df, changed = apply_to_df(df, canon_map)
    if changed > 0 and not dry_run:
        df.to_parquet(corpus_path, index=False)
    return changed


def main():
    ap = argparse.ArgumentParser(description="Normalise name field in Hansard parquets.")
    ap.add_argument("--chamber",     choices=["senate", "house"], required=True)
    ap.add_argument("--corpus-only", action="store_true",
                    help="Update corpus file only, skip daily files.")
    ap.add_argument("--dry-run",     action="store_true",
                    help="Report what would change without writing files.")
    args = ap.parse_args()

    cfg = PATHS[args.chamber]

    # Build canonical name map
    print(f"Building canonical name map from {cfg['lookup'].name}…")
    if args.chamber == "senate":
        canon_map = build_canonical_map_senate(cfg["lookup"])
    else:
        canon_map = build_canonical_map_house(cfg["lookup"])
    print(f"  {len(canon_map):,} name_ids in canonical map")

    # Show a sample
    sample = list(canon_map.items())[:6]
    for nid, cname in sample:
        print(f"    {nid!r:12s} → {cname!r}")

    # Update corpus
    print(f"\nUpdating corpus{' (DRY RUN)' if args.dry_run else ''}…")
    n_corpus = update_corpus(cfg["corpus"], canon_map, args.dry_run)
    print(f"  {n_corpus:,} rows changed in corpus")

    # Update daily files
    if not args.corpus_only:
        print(f"\nUpdating daily files{' (DRY RUN)' if args.dry_run else ''}…")
        files_updated, rows_changed = update_daily_files(
            cfg["daily"], canon_map, args.dry_run)
        print(f"  {files_updated:,} files updated, {rows_changed:,} rows changed")

    if args.dry_run:
        print("\n(Dry run — no files written.)")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
