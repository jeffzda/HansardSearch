"""
06_add_partyfacts.py — Add PartyFacts cross-national party IDs to daily files.

Usage:
    python 06_add_partyfacts.py --daily-dir ../data/output/senate/daily
                                --lookup-dir ../data/lookup
                                [--no-skip]
                                [--sequential]
"""

import argparse
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from parallel_utils import eager_map


# ── Worker globals (module-level for pickle compatibility) ────────────────────

_PFMAP: pd.DataFrame | None = None


def _init_worker(pfmap_tuple: tuple) -> None:
    """Load the partyfacts map once per worker process."""
    global _PFMAP
    (pfmap_data,) = pfmap_tuple
    _PFMAP = pfmap_data


def _process_one(pq_path_str: str) -> str:
    """Merge partyfacts IDs into one daily parquet file."""
    pq_path = Path(pq_path_str)
    df = pd.read_parquet(pq_path)

    # Remove existing partyfacts_id column if present
    if "partyfacts_id" in df.columns:
        df = df.drop(columns=["partyfacts_id"])

    # Join on party abbreviation
    df = df.merge(
        _PFMAP[["party_abbrev", "partyfacts_id"]].rename(
            columns={"party_abbrev": "party"}
        ),
        on="party",
        how="left",
    )

    df.to_parquet(pq_path, index=False)
    df.to_csv(str(pq_path).replace(".parquet", ".csv"), index=False)
    return pq_path.stem


# ── Main batch function ───────────────────────────────────────────────────────

def add_partyfacts(daily_dir: Path, lookup_dir: Path,
                   skip_existing: bool = True,
                   sequential: bool = False) -> None:
    pf_map = pd.read_csv(lookup_dir / "partyfacts_map.csv", dtype=str)
    pf_map["partyfacts_id"] = pd.to_numeric(pf_map["partyfacts_id"], errors="coerce")

    parquet_files = sorted(daily_dir.glob("*.parquet"))
    print(f"Adding PartyFacts IDs to {len(parquet_files)} files")

    if not parquet_files:
        print("Done (nothing to do).")
        return

    if sequential:
        for pq_path in tqdm(parquet_files, desc="PartyFacts"):
            df = pd.read_parquet(pq_path)
            if "partyfacts_id" in df.columns:
                df = df.drop(columns=["partyfacts_id"])
            df = df.merge(
                pf_map[["party_abbrev", "partyfacts_id"]].rename(
                    columns={"party_abbrev": "party"}
                ),
                on="party",
                how="left",
            )
            df.to_parquet(pq_path, index=False)
            df.to_csv(str(pq_path).replace(".parquet", ".csv"), index=False)
    else:
        pfmap_tuple = (pf_map,)
        items = [str(p) for p in parquet_files]
        eager_map(
            _process_one,
            items,
            initializer=_init_worker,
            initargs=(pfmap_tuple,),
            desc="Adding partyfacts",
            unit="file",
        )

    print("Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Add PartyFacts IDs to daily Senate Hansard files."
    )
    parser.add_argument("--daily-dir", default="../data/output/senate/daily")
    parser.add_argument("--lookup-dir", default="../data/lookup")
    parser.add_argument("--no-skip", action="store_true")
    parser.add_argument("--sequential", action="store_true",
                        help="Disable parallelism (useful for debugging)")
    args = parser.parse_args()

    add_partyfacts(Path(args.daily_dir), Path(args.lookup_dir),
                   skip_existing=not args.no_skip,
                   sequential=args.sequential)


if __name__ == "__main__":
    main()
