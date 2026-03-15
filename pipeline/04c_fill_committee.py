"""
04c_fill_committee.py — Enrich committee Hansard parquet files with member metadata.

Reads per-file parquet files from daily_raw/ and writes enriched versions to daily/.

For rows where witness_flag=0 (committee members — senators/MPs):
  - Joins name_id (PHID) to senator_lookup.csv and member_lookup.csv
    to fill: unique_id, gender
  - Joins unique_id + date to party_lookup.csv / party_lookup_house.csv
    to fill: party, partyfacts_id
  - Computes in_gov from hardcoded government timeline (same logic as fix_in_gov.py)

For witness rows (witness_flag=1): all metadata columns remain NULL.

CHAIR resolution: some CHAIR rows lose their name_id in paragraphs without an
<a> anchor. After the per-row join, we forward-fill name_id / unique_id / gender /
party within each (date, file_stem, name) group so that all CHAIR rows in a panel
share the same attribution where the PHID was identified at least once.

Usage:
    python 04c_fill_committee.py \\
        --daily-dir  ../data/output/committee/daily_raw \\
        --lookup-dir ../data/lookup \\
        --out-dir    ../data/output/committee/daily
"""

import argparse
from datetime import date
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from parallel_utils import eager_map

# ── Government timeline (same as fix_in_gov.py) ───────────────────────────────
# (start_date, party_set_in_gov)
GOV_PERIODS = [
    (date(1996, 3, 2),  {"LP", "NP", "NATS", "CLP", "LNP", "NatsWA", "NAT"}),
    (date(2007, 11, 24), {"ALP"}),
    (date(2013, 9, 7),  {"LP", "NP", "NATS", "CLP", "LNP", "NatsWA", "NAT"}),
    (date(2022, 5, 21), {"ALP"}),
]


def _in_gov(party: str | None, sitting_date: date) -> int | None:
    if not party:
        return None
    gov_parties = None
    for period_start, parties in reversed(GOV_PERIODS):
        if sitting_date >= period_start:
            gov_parties = parties
            break
    if gov_parties is None:
        return None
    return 1 if party in gov_parties else 0


# ── Lookup builders ───────────────────────────────────────────────────────────

def _build_phid_to_member(lookup_dir: Path) -> dict[str, dict]:
    """Build PHID → {unique_id, gender} from senator_lookup + member_lookup."""
    mapping: dict[str, dict] = {}

    for fname in ("senator_lookup.csv", "member_lookup.csv"):
        path = lookup_dir / fname
        if not path.exists():
            continue
        df = pd.read_csv(path, dtype=str)
        # senator_lookup uses 'name_id'; member_lookup uses 'name_id' as well
        if "name_id" not in df.columns:
            continue
        for _, row in df.iterrows():
            phid = str(row.get("name_id", "")).strip().upper()
            if not phid or phid in ("", "NAN"):
                continue
            mapping[phid] = {
                "unique_id": str(row.get("uniqueID", row.get("unique_id", ""))).strip() or None,
                "gender":    str(row.get("gender", "")).strip().lower() or None,
            }
    return mapping


def _build_party_lookup(lookup_dir: Path) -> tuple[list[dict], dict[str, float]]:
    """
    Build party records and partyfacts mapping.

    Returns:
      records     — list of {unique_id, party, party_from, party_to}
      pf_map      — {party_abbrev: partyfacts_id}
    """
    records = []
    seen_uids: set[str] = set()

    for fname in ("party_lookup.csv", "party_lookup_house.csv"):
        path = lookup_dir / fname
        if not path.exists():
            continue
        df = pd.read_csv(path, dtype=str)
        for _, row in df.iterrows():
            uid   = str(row.get("unique_id", "")).strip()
            party = str(row.get("party_abbrev", "")).strip()
            from_str = str(row.get("party_from", "")).strip()
            to_str   = str(row.get("party_to",   "")).strip()
            if not uid or not party:
                continue
            # Avoid duplicate unique_id entries across both files
            key = (uid, party, from_str, to_str)
            if key in seen_uids:
                continue
            seen_uids.add(key)
            try:
                party_from = date.fromisoformat(from_str) if from_str.lower() not in ("nat", "nan", "") else date(1900, 1, 1)
            except ValueError:
                party_from = date(1900, 1, 1)
            try:
                party_to = date.fromisoformat(to_str) if to_str.lower() not in ("nat", "nan", "") else date(2099, 12, 31)
            except ValueError:
                party_to = date(2099, 12, 31)
            records.append({
                "unique_id":  uid,
                "party":      party,
                "party_from": party_from,
                "party_to":   party_to,
            })

    # PartyFacts map: party_abbrev → partyfacts_id
    pf_map: dict[str, float] = {}
    pf_path = lookup_dir / "partyfacts_map.csv"
    if pf_path.exists():
        pf_df = pd.read_csv(pf_path, dtype=str)
        for _, row in pf_df.iterrows():
            abbrev = str(row.get("party_abbrev", "")).strip()
            pf_id  = str(row.get("partyfacts_id", "")).strip()
            if abbrev and pf_id.lower() not in ("nan", ""):
                try:
                    pf_map[abbrev] = float(pf_id)
                except ValueError:
                    pass

    return records, pf_map


def _party_on_date(
    unique_id: str,
    sitting_date: date,
    party_records: list[dict],
    pf_map: dict[str, float],
) -> tuple[str | None, float | None]:
    """Return (party, partyfacts_id) for a member on a given date."""
    candidates = [
        r for r in party_records
        if r["unique_id"] == unique_id
        and r["party_from"] <= sitting_date <= r["party_to"]
    ]
    if not candidates:
        all_member = [r for r in party_records if r["unique_id"] == unique_id]
        if all_member:
            all_member.sort(key=lambda x: x["party_to"], reverse=True)
            party = all_member[0]["party"]
            return party, pf_map.get(party)
        return None, None
    candidates.sort(key=lambda x: (x["party_to"] - x["party_from"]))
    party = candidates[0]["party"]
    return party, pf_map.get(party)


# ── Per-file enrichment ────────────────────────────────────────────────────────

def _enrich_file(
    path: Path,
    phid_map: dict[str, dict],
    party_records: list[dict],
    pf_map: dict[str, float],
) -> pd.DataFrame:
    df = pd.read_parquet(path)

    # Normalise name_id to uppercase for consistent lookups
    df["name_id"] = df["name_id"].where(df["name_id"].notna(), None)
    df["name_id"] = df["name_id"].apply(
        lambda x: str(x).strip().upper() if x is not None else None
    )

    # Forward-fill name_id within (file, name) groups so CHAIR rows that
    # initially lack an anchor PHID inherit it from the first identified row.
    df["_fill_group"] = df["name"].str.upper().str.strip()
    df["name_id"] = (
        df.groupby("_fill_group")["name_id"]
        .transform(lambda s: s.ffill().bfill())
    )
    df.drop(columns=["_fill_group"], inplace=True)

    # Parse date for use in date-sensitive lookups
    try:
        sitting_date = date.fromisoformat(str(df["date"].iloc[0]))
    except (ValueError, IndexError):
        sitting_date = None

    # Enrich member rows
    unique_ids = []
    genders    = []
    parties    = []
    pf_ids     = []
    in_govs    = []

    for _, row in df.iterrows():
        if row.get("witness_flag", 1) == 1 or pd.isna(row.get("name_id")):
            unique_ids.append(None)
            genders.append(None)
            parties.append(None)
            pf_ids.append(None)
            in_govs.append(None)
            continue

        phid = str(row["name_id"]).strip().upper()
        if phid in ("", "NAN", "NONE", "NA"):
            unique_ids.append(None)
            genders.append(None)
            parties.append(None)
            pf_ids.append(None)
            in_govs.append(None)
            continue
        member = phid_map.get(phid, {})
        uid = member.get("unique_id")
        gender = member.get("gender")

        party = None
        pf_id = None
        if uid and sitting_date:
            party, pf_id = _party_on_date(uid, sitting_date, party_records, pf_map)

        in_gov_val = _in_gov(party, sitting_date) if sitting_date else None

        unique_ids.append(uid)
        genders.append(gender)
        parties.append(party)
        pf_ids.append(pf_id)
        in_govs.append(in_gov_val)

    df["unique_id"]    = unique_ids
    df["gender"]       = genders
    df["party"]        = parties
    df["partyfacts_id"] = pf_ids
    df["in_gov"]       = in_govs

    # Coerce in_gov
    df["in_gov"] = pd.to_numeric(df["in_gov"], errors="coerce")

    return df


# ── Worker functions (module-level for pickle compatibility) ──────────────────

_LOOKUPS: tuple | None = None  # (phid_map, party_records, pf_map, out_dir_str, no_skip)


def _init_worker(lookups_tuple: tuple) -> None:
    """Load shared lookup data once per worker process."""
    global _LOOKUPS
    _LOOKUPS = lookups_tuple


def _enrich_one(path_str: str) -> tuple:
    """Enrich one committee parquet file. Runs inside a worker process."""
    phid_map, party_records, pf_map, out_dir_str, no_skip = _LOOKUPS
    path    = Path(path_str)
    out_dir = Path(out_dir_str)
    out_path = out_dir / path.name

    if not no_skip and out_path.exists():
        return ("skipped", 0, 0)

    try:
        df = _enrich_file(path, phid_map, party_records, pf_map)
    except Exception as e:
        return ("error", str(e), path.name)

    df.to_parquet(out_path, index=False)
    n_rows   = len(df)
    n_filled = int((df["unique_id"].notna() & (df["witness_flag"] == 0)).sum())
    return ("enriched", n_rows, n_filled)


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    ap = argparse.ArgumentParser(
        description="Enrich committee parquet files with member metadata."
    )
    ap.add_argument("--daily-dir",  required=True, help="Input: daily_raw/ directory")
    ap.add_argument("--lookup-dir", required=True, help="Lookup table directory")
    ap.add_argument("--out-dir",    required=True, help="Output: daily/ directory")
    ap.add_argument("--no-skip",    action="store_true", help="Overwrite existing files")
    ap.add_argument("--sequential", action="store_true", help="Disable parallelism (useful for debugging)")
    args = ap.parse_args()

    daily_dir  = Path(args.daily_dir)
    lookup_dir = Path(args.lookup_dir)
    out_dir    = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading lookup tables...")
    phid_map              = _build_phid_to_member(lookup_dir)
    party_records, pf_map = _build_party_lookup(lookup_dir)
    print(f"  PHID map: {len(phid_map):,} entries")
    print(f"  Party records: {len(party_records):,} entries")
    print(f"  PartyFacts map: {len(pf_map):,} entries")

    parquet_files = sorted(daily_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"No parquet files found in {daily_dir}")
        return

    stats         = {"enriched": 0, "skipped": 0, "errors": 0}
    member_filled = 0
    total_rows    = 0

    if args.sequential:
        for path in tqdm(parquet_files):
            out_path = out_dir / path.name
            if not args.no_skip and out_path.exists():
                stats["skipped"] += 1
                continue
            try:
                df = _enrich_file(path, phid_map, party_records, pf_map)
            except Exception as e:
                print(f"  [ERROR] {path.name}: {e}")
                stats["errors"] += 1
                continue
            df.to_parquet(out_path, index=False)
            stats["enriched"] += 1
            total_rows    += len(df)
            member_filled += int((df["unique_id"].notna() & (df["witness_flag"] == 0)).sum())
    else:
        lookups_tuple = (phid_map, party_records, pf_map, str(out_dir), args.no_skip)
        items   = [str(p) for p in parquet_files]
        results = eager_map(
            _enrich_one,
            items,
            initializer=_init_worker,
            initargs=(lookups_tuple,),
            desc="Enriching committee files",
            unit="file",
        )
        for res in results:
            status = res[0]
            if status == "skipped":
                stats["skipped"] += 1
            elif status == "error":
                stats["errors"] += 1
                print(f"  [ERROR] {res[2]}: {res[1]}")
            else:
                stats["enriched"] += 1
                total_rows    += res[1]
                member_filled += res[2]

    print(
        f"\nDone. Enriched: {stats['enriched']}, Skipped: {stats['skipped']}, "
        f"Errors: {stats['errors']}"
    )
    if total_rows:
        print(f"Total rows processed: {total_rows:,}")
        print(f"Member rows with unique_id filled: {member_filled:,}")


if __name__ == "__main__":
    main()
