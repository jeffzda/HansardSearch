"""
04_fill_details.py — Post-parse member detail enrichment.

Seven sequential passes:
  Pass 1: Fill by name_id (PHID) — direct join on senator_lookup
  Pass 2: Fill missing by name form matching (form1–form5)
  Pass 3: Date-aware party and state correction from party_lookup/state_lookup
  Pass 4: President/Deputy President name variant resolution
  Pass 5: Normalise state abbreviations to full names (WA→Western Australia etc.)
  Pass 6: Normalise party abbreviation variants to canonical forms
  Pass 7: Forward-fill page_no within sitting day

Usage:
    python 04_fill_details.py --daily-dir ../data/output/senate/daily
                              --lookup-dir ../data/lookup
                              --out-dir ../data/output/senate/daily
                              [--no-skip]
                              [--sequential]
"""

import argparse
import re
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from parallel_utils import eager_map


# ── Load lookup tables ────────────────────────────────────────────────────────

def load_lookups(lookup_dir: Path) -> dict:
    lookup_dir = Path(lookup_dir)
    result = {
        "senator":   pd.read_csv(lookup_dir / "senator_lookup.csv", dtype=str),
        "party":     pd.read_csv(lookup_dir / "party_lookup.csv", dtype=str),
        "state":     pd.read_csv(lookup_dir / "state_lookup.csv", dtype=str),
        "president": pd.read_csv(lookup_dir / "president_lookup.csv", dtype=str),
    }
    for key, fname in [("member", "member_lookup.csv"), ("electorate", "electorate_lookup.csv")]:
        p = lookup_dir / fname
        if p.exists():
            result[key] = pd.read_csv(p, dtype=str)
    return result


# ── Pass 1: Fill by name_id ───────────────────────────────────────────────────

def fill_by_name_id(df: pd.DataFrame, senator_lookup: pd.DataFrame) -> pd.DataFrame:
    """
    For rows where name_id is known, fill gender, unique_id, and state
    from the senator_lookup. Party is handled by the date-aware pass later.
    """
    lookup = senator_lookup[["name_id", "unique_id", "gender", "state_abbrev"]].dropna(
        subset=["name_id"]
    ).drop_duplicates("name_id")

    # Rows that need filling
    mask = df["name_id"].notna() & df["unique_id"].isna()
    if not mask.any():
        return df

    before_shape = df.shape
    df = df.merge(
        lookup.rename(columns={
            "unique_id": "_uid",
            "gender": "_gender",
            "state_abbrev": "_state_abbrev",
        }),
        on="name_id",
        how="left",
    )
    assert df.shape[0] == before_shape[0], "Pass 1: row count changed"

    # Fill only where currently missing
    for src, dst in [("_uid", "unique_id"), ("_gender", "gender")]:
        df.loc[df[dst].isna() & df[src].notna(), dst] = df[src]

    # State: fill from lookup only where state is missing
    # (XML state should take priority if present)
    df.loc[df["state"].isna() & df["_state_abbrev"].notna(), "state"] = df["_state_abbrev"]

    df = df.drop(columns=["_uid", "_gender", "_state_abbrev"], errors="ignore")
    return df


# ── Pass 2: Fill by name form matching ───────────────────────────────────────

def _build_term_date_index(state_lookup: pd.DataFrame) -> dict[str, list[tuple]]:
    """
    Build a name_id → list of (senator_from, senator_to) tuples from
    state_lookup.  Both dates are Python date objects or None if unknown.
    A senator with multiple terms will have multiple tuples in the list.
    """
    index: dict[str, list[tuple]] = {}
    sl = state_lookup.copy()
    sl["senator_from"] = pd.to_datetime(sl["senator_from"], errors="coerce").dt.date
    sl["senator_to"]   = pd.to_datetime(sl["senator_to"],   errors="coerce").dt.date
    for _, row in sl.iterrows():
        nid = str(row.get("name_id") or "").strip()
        if not nid or nid in ("nan", "None"):
            continue
        from_d = row["senator_from"] if not pd.isna(row["senator_from"]) else None
        to_d   = row["senator_to"]   if not pd.isna(row["senator_to"])   else None
        index.setdefault(nid, []).append((from_d, to_d))
    return index


def _term_covers_date(term_list: list[tuple], sitting_date) -> bool:
    """
    Return True if *any* term in term_list covers sitting_date.
    A term with both dates NULL is treated as always active (unknown extent).
    A term with only one date NULL is treated as open-ended on that side.
    """
    for from_d, to_d in term_list:
        if from_d is None and to_d is None:
            # No date information — accept to avoid over-rejecting
            return True
        from_ok = (from_d is None) or (sitting_date >= from_d)
        to_ok   = (to_d   is None) or (sitting_date <= to_d)
        if from_ok and to_ok:
            return True
    return False


def fill_by_name_forms(df: pd.DataFrame, senator_lookup: pd.DataFrame,
                       state_lookup: pd.DataFrame | None = None,
                       date_str: str | None = None) -> pd.DataFrame:
    """
    For rows where unique_id is still missing, attempt to match the 'name'
    column against form1–form5 variants in senator_lookup.

    If state_lookup and date_str are provided, a date-range guard is applied:
    after finding a candidate match, the senator's term dates (from
    state_lookup) are checked against the sitting date.  If the sitting date
    falls outside every known term for that senator, the match is rejected
    (fields remain NULL) to prevent false assignments between senators who
    share a surname across different eras.

    If state_lookup is None, or the matched name_id has no entry in
    state_lookup, the match is accepted as before (no over-rejection on
    missing data).
    """
    missing_mask = df["unique_id"].isna() & df["name"].notna()
    if not missing_mask.any():
        return df

    # Build a flat form→record map
    form_map: dict[str, dict] = {}
    for _, row in senator_lookup.iterrows():
        for form_col in ("form1", "form2", "form3", "form4", "form5"):
            form_val = str(row.get(form_col, "") or "").strip()
            if form_val and form_val not in ("nan", "None"):
                if form_val not in form_map:
                    form_map[form_val] = {
                        "unique_id": row.get("unique_id"),
                        "name_id": row.get("name_id"),
                        "gender": row.get("gender"),
                        "state_abbrev": row.get("state_abbrev"),
                    }

    # Build term-date index for the guard (keyed by name_id)
    term_index: dict[str, list[tuple]] | None = None
    sitting_date = None
    if state_lookup is not None and date_str is not None:
        try:
            sitting_date = pd.to_datetime(date_str).date()
            term_index = _build_term_date_index(state_lookup)
        except Exception:
            term_index = None
            sitting_date = None

    def match_name(name_val: str | None) -> dict | None:
        if not name_val or pd.isna(name_val):
            return None
        name_val = str(name_val).strip()
        # Direct match
        if name_val in form_map:
            return form_map[name_val]
        # Case-insensitive match
        name_lower = name_val.lower()
        for form, rec in form_map.items():
            if form.lower() == name_lower:
                return rec
        return None

    before_shape = df.shape
    for idx in df.index[missing_mask]:
        match = match_name(df.at[idx, "name"])
        if match:
            # ── Date-range guard ──────────────────────────────────────────────
            # If we have term data, verify this match is plausible for the
            # sitting date before accepting it.
            if term_index is not None and sitting_date is not None:
                candidate_nid = str(match.get("name_id") or "").strip()
                if candidate_nid and candidate_nid in term_index:
                    # Term data exists — check the sitting date is covered
                    if not _term_covers_date(term_index[candidate_nid], sitting_date):
                        # Sitting date outside all known terms — reject match
                        continue
                # If candidate_nid has no entry in term_index, accept the match
                # (we don't want to reject based on missing data).

            if pd.isna(df.at[idx, "unique_id"]) and match.get("unique_id"):
                df.at[idx, "unique_id"] = match["unique_id"]
            if pd.isna(df.at[idx, "name_id"]) and match.get("name_id"):
                df.at[idx, "name_id"] = match["name_id"]
            if pd.isna(df.at[idx, "gender"]) and match.get("gender"):
                df.at[idx, "gender"] = match["gender"]

    assert df.shape == before_shape, "Pass 2: row count changed"
    return df


# ── Pass 3: Date-aware party and state correction ─────────────────────────────

def fill_date_aware(df: pd.DataFrame, date_str: str,
                    party_lookup: pd.DataFrame,
                    state_lookup: pd.DataFrame) -> pd.DataFrame:
    """
    For each name_id, confirm that party and state match the correct values
    for this sitting date. Override where there is a discrepancy.

    This handles senators who changed parties or whose state assignment
    needs confirmation.
    """
    sitting_date = pd.to_datetime(date_str).date()

    # Build date-filtered party map: name_id → party_abbrev
    pl = party_lookup.copy()
    pl["party_from"] = pd.to_datetime(pl["party_from"], errors="coerce").dt.date
    pl["party_to"] = pd.to_datetime(pl["party_to"], errors="coerce").dt.date

    # Active party on sitting date
    active_party = pl[
        (pl["party_from"].isna() | (pl["party_from"] <= sitting_date)) &
        (pl["party_to"].isna()   | (pl["party_to"]   >= sitting_date))
    ][["name_id", "party_abbrev"]].drop_duplicates("name_id")

    # Build date-filtered state map: name_id → state_abbrev
    sl = state_lookup.copy()
    sl["senator_from"] = pd.to_datetime(sl["senator_from"], errors="coerce").dt.date
    sl["senator_to"] = pd.to_datetime(sl["senator_to"], errors="coerce").dt.date

    active_state = sl[
        (sl["senator_from"].isna() | (sl["senator_from"] <= sitting_date)) &
        (sl["senator_to"].isna()   | (sl["senator_to"]   >= sitting_date))
    ][["name_id", "state_abbrev"]].drop_duplicates("name_id")

    before_shape = df.shape

    # Override party from lookup where the lookup has a date-range entry
    # (party_from or party_to is set, indicating a specific period).
    # For open-ended entries (no date range), only fill where party is NULL.
    pl_dated = pl[pl["party_from"].notna() | pl["party_to"].notna()]
    active_dated = pl_dated[
        (pl_dated["party_from"].isna() | (pl_dated["party_from"] <= sitting_date)) &
        (pl_dated["party_to"].isna()   | (pl_dated["party_to"]   >= sitting_date))
    ][["name_id", "party_abbrev"]].drop_duplicates("name_id")

    df = df.merge(
        active_party.rename(columns={"party_abbrev": "_party_date"}),
        on="name_id", how="left"
    )
    df = df.merge(
        active_dated.rename(columns={"party_abbrev": "_party_dated"}),
        on="name_id", how="left"
    )
    # Override: date-range entries always win (corrects XML inconsistencies for party-changers)
    df.loc[df["_party_dated"].notna(), "party"] = df["_party_dated"]
    # Fill: open-ended entries fill only NULLs
    df.loc[df["party"].isna() & df["_party_date"].notna(), "party"] = df["_party_date"]
    df = df.drop(columns=["_party_date", "_party_dated"], errors="ignore")

    # Fill state where still missing (XML electorate field may have been empty)
    df = df.merge(
        active_state.rename(columns={"state_abbrev": "_state_date"}),
        on="name_id", how="left"
    )
    df.loc[df["state"].isna() & df["_state_date"].notna(), "state"] = df["_state_date"]
    df = df.drop(columns=["_state_date"], errors="ignore")

    assert df.shape[0] == before_shape[0], "Pass 3: row count changed"
    return df


# ── Pass 4: President/Deputy President fill ───────────────────────────────────

def fill_president(df: pd.DataFrame, date_str: str,
                   president_lookup: pd.DataFrame) -> pd.DataFrame:
    """
    For rows where name matches a presiding-officer pattern, fill name_id,
    unique_id, party, state, and gender from president_lookup.
    """
    sitting_date = pd.to_datetime(date_str).date()

    # Filter to date-active president records
    pl = president_lookup.copy()
    pl["from_date"] = pd.to_datetime(pl["from_date"], errors="coerce").dt.date
    pl["to_date"] = pd.to_datetime(pl["to_date"], errors="coerce").dt.date

    # Build pattern → details map for this date
    pattern_map: dict[str, dict] = {}
    for _, row in pl.iterrows():
        from_d = row["from_date"]
        to_d = row["to_date"]
        in_range = (
            (pd.isna(from_d) or from_d <= sitting_date) and
            (pd.isna(to_d)   or to_d   >= sitting_date)
        )
        pattern = str(row.get("xml_name_pattern", "") or "").strip()
        if pattern and in_range and pattern not in pattern_map:
            pattern_map[pattern] = {
                "name_id":    row.get("name_id"),
                "unique_id":  row.get("unique_id"),
                "party":      row.get("party"),
                "state":      row.get("state"),
            }
        # Always include the generic patterns (no date range)
        if pd.isna(from_d) and pd.isna(to_d) and pattern not in pattern_map:
            pattern_map[pattern] = {
                "name_id": row.get("name_id"),
                "unique_id": row.get("unique_id"),
                "party": None,
                "state": None,
            }

    # Presiding officer detection: name_id = "10000" or name matches pattern
    president_mask = df["name_id"] == "10000"
    for pattern, details in pattern_map.items():
        if pattern:
            president_mask |= df["name"].str.contains(
                re.escape(pattern), regex=True, na=False, case=False
            )

    before_shape = df.shape
    for idx in df.index[president_mask]:
        name_val = str(df.at[idx, "name"] or "")
        # Find best matching pattern
        match = None
        for pattern, details in pattern_map.items():
            if pattern and pattern.lower() in name_val.lower():
                match = details
                break
        if match is None and "10000" in str(df.at[idx, "name_id"] or ""):
            match = pattern_map.get("The PRESIDENT")

        if match:
            # Always set name_id to 10000 for presiding officers
            df.at[idx, "name_id"] = "10000"
            if pd.isna(df.at[idx, "unique_id"]) and match.get("unique_id"):
                df.at[idx, "unique_id"] = match["unique_id"]
            if pd.isna(df.at[idx, "party"]) and match.get("party"):
                df.at[idx, "party"] = match["party"]

    assert df.shape == before_shape, "Pass 4: row count changed"
    return df


# ── Pass 4b: Populate electorate for joint-sitting House members ─────────────

def fill_joint_sitting_electorate(df: pd.DataFrame, date_str: str,
                                   member_lookup: pd.DataFrame,
                                   electorate_lookup: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    When House members attend a joint sitting their speech appears in Senate
    XML with their electorate in the <electorate> tag (mapped to 'state').
    This pass detects those rows (name_id present in member_lookup) and
    populates a new 'electorate' column from the date-aware electorate_lookup.
    Pass 5 (normalise_state) will subsequently null the 'state' value for
    these rows because electorate names are not valid state names.
    """
    if "electorate" not in df.columns:
        df["electorate"] = pd.NA

    house_ids = set(member_lookup["name_id"].dropna().astype(str))
    house_mask = df["name_id"].astype(str).isin(house_ids)
    if not house_mask.any():
        return df

    # Base map from member_lookup (canonical electorate, no date range)
    elec_map = (
        member_lookup[["name_id", "electorate"]]
        .dropna(subset=["name_id", "electorate"])
        .drop_duplicates("name_id")
        .set_index("name_id")["electorate"]
        .to_dict()
    )
    # Override with date-aware entries where available
    if electorate_lookup is not None:
        sitting_date = pd.to_datetime(date_str).date()
        el = electorate_lookup.copy()
        el["member_from"] = pd.to_datetime(el["member_from"], errors="coerce").dt.date
        el["member_to"]   = pd.to_datetime(el["member_to"],   errors="coerce").dt.date
        valid_el = el[
            (el["member_from"].isna() | (el["member_from"] <= sitting_date)) &
            (el["member_to"].isna()   | (el["member_to"]   >= sitting_date))
        ].drop_duplicates("name_id")
        elec_map.update(valid_el.set_index("name_id")["electorate"].to_dict())

    _e = df.loc[house_mask, "name_id"].map(elec_map)
    has_entry = house_mask & _e.notna()
    df.loc[has_entry, "electorate"] = _e[has_entry]
    return df


# ── Pass 5: Normalise state values ───────────────────────────────────────────

# v2.1 XML files (1998–~2011) store the state as an abbreviation in
# <electorate>; v2.2 files use the full name in <span class="HPS-Electorate">.
# Normalise everything to the full canonical name.
_STATE_NORM: dict[str, str] = {
    "NSW":  "New South Wales",
    "VIC":  "Victoria",
    "QLD":  "Queensland",
    "SA":   "South Australia",
    "WA":   "Western Australia",
    "TAS":  "Tasmania",
    "NT":   "Northern Territory",
    "ACT":  "Australian Capital Territory",
    # Capitalisation variants
    "Qld":  "Queensland",
    "Tas":  "Tasmania",
    "Vic":  "Victoria",
    "Northern territory": "Northern Territory",
    # All-caps variant (early XML)
    "NEW SOUTH WALES":      "New South Wales",
    "VICTORIA":             "Victoria",
    "QUEENSLAND":           "Queensland",
    "SOUTH AUSTRALIA":      "South Australia",
    "WESTERN AUSTRALIA":    "Western Australia",
    "TASMANIA":             "Tasmania",
    "NORTHERN TERRITORY":   "Northern Territory",
    # Typo
    "Queesland": "Queensland",
}

_VALID_STATES: frozenset[str] = frozenset({
    "New South Wales", "Victoria", "Queensland", "Western Australia",
    "South Australia", "Tasmania", "Northern Territory", "Australian Capital Territory",
})


def normalise_state(df: pd.DataFrame) -> pd.DataFrame:
    """Pass 5: Normalise abbreviated and mis-cased state values to full names."""
    # v2.2 XML can encode state as 'ACT—Minister for...' — strip the title suffix
    df["state"] = df["state"].str.split("—").str[0].str.strip()
    df["state"] = df["state"].replace(_STATE_NORM)
    # Null out anything that isn't one of the eight recognised states/territories
    # (catches PO, UNKNOWN, House electorates from joint-sitting records, etc.)
    df.loc[df["state"].notna() & ~df["state"].isin(_VALID_STATES), "state"] = pd.NA
    return df


# ── Pass 6: Normalise party abbreviations ────────────────────────────────────

# Maps non-canonical party strings → canonical abbreviations used in
# partyfacts_map.csv (which are the join keys for partyfacts_id).
# NULL/NaN values are intentionally excluded — use pd.DataFrame.replace()
# which leaves NaN untouched.
_PARTY_NORM: dict[str, str] = {
    # Liberal Party variants
    "LIB":      "LP",   # early XML abbreviation for Liberal Party
    # Nationals variants
    "NPA":      "NP",   # old "National Party of Australia" abbreviation
    "NATS":     "NATS", # already canonical — included for explicitness (no-op)
    "Nats":     "NATS", # mixed-case variant seen in House XML
    "NatsWA":   "NatsWA",  # already canonical — no-op
    # Greens variants
    "GRN":      "AG",   # alternative Greens abbreviation → canonical AG
    "G(WA)":    "GWA",  # Western Australian Greens punctuation variant
    # One Nation variants
    "ON":       "PHON", # early "One Nation" abbreviation → canonical PHON
    # Independent variants
    "Ind.":     "IND",  # abbreviated with period
    "Ind":      "IND",  # abbreviated without period
    # Sentinel / unknown values (string, not NaN — safe to remap to NaN)
    "N/A":      pd.NA,  # explicit not-applicable string
    "UNKNOWN":  pd.NA,  # explicit unknown string
    # Apparent data-entry errors (House, 2 rows)
    "LPI0":     "LP",   # likely OCR/parse error for LP
    # Acting presiding-officer role label that leaked into party column
    "NPActing": "NP",   # NP member serving as acting chair
}


def normalise_party(df: pd.DataFrame) -> pd.DataFrame:
    """Pass 6: Normalise party abbreviation variants to canonical forms."""
    df["party"] = df["party"].replace(_PARTY_NORM)
    return df


# ── Pass 7: Forward-fill page numbers ────────────────────────────────────────

def forward_fill_page_no(df: pd.DataFrame) -> pd.DataFrame:
    """Pass 7: Forward-fill page_no within sitting day.

    Interjections and brief procedural turns do not receive explicit page
    citations in v2.x XML (pre-2012).  They physically occur on the same
    page as the surrounding speech, so forward-filling from the previous
    row is safe and derivable from the XML alone.
    """
    df["page_no"] = df["page_no"].ffill()
    return df


# ── Main fill function ────────────────────────────────────────────────────────

def fill_details(df: pd.DataFrame, date_str: str, lookups: dict) -> pd.DataFrame:
    """Apply all five enrichment passes to one sitting-day DataFrame."""
    original_shape = df.shape

    # Sanitise garbled party values (e.g. '(' from v2.1 XML) before any fill pass.
    # Any value that doesn't start with a letter is treated as missing.
    if "party" in df.columns:
        bad = df["party"].notna() & ~df["party"].astype(str).str.match(r"^[A-Za-z]")
        df.loc[bad, "party"] = None

    df = fill_by_name_id(df, lookups["senator"])
    assert df.shape[0] == original_shape[0], "After Pass 1: row count changed"

    df = fill_by_name_forms(df, lookups["senator"],
                            state_lookup=lookups["state"],
                            date_str=date_str)
    assert df.shape[0] == original_shape[0], "After Pass 2: row count changed"

    df = fill_date_aware(df, date_str, lookups["party"], lookups["state"])
    assert df.shape[0] == original_shape[0], "After Pass 3: row count changed"

    df = fill_president(df, date_str, lookups["president"])
    assert df.shape[0] == original_shape[0], "After Pass 4: row count changed"

    if lookups.get("member") is not None:
        df = fill_joint_sitting_electorate(
            df, date_str, lookups["member"], lookups.get("electorate")
        )
        assert df.shape[0] == original_shape[0], "After Pass 4b: row count changed"
    elif "electorate" not in df.columns:
        df["electorate"] = pd.NA

    df = normalise_state(df)
    assert df.shape[0] == original_shape[0], "After Pass 5: row count changed"

    df = normalise_party(df)
    assert df.shape[0] == original_shape[0], "After Pass 6: row count changed"

    df = forward_fill_page_no(df)
    assert df.shape[0] == original_shape[0], "After Pass 7: row count changed"

    return df


# ── Worker functions (module-level for pickle compatibility) ──────────────────

_LOOKUPS: dict | None = None
_g_out_dir: str | None = None


def _init_worker(lookups_tuple: tuple) -> None:
    """Load shared lookup data once per worker process."""
    global _LOOKUPS, _g_out_dir
    senator_df, party_df, state_df, president_df, member_df, electorate_df, out_dir_str = lookups_tuple
    _LOOKUPS = {
        "senator":    senator_df,
        "party":      party_df,
        "state":      state_df,
        "president":  president_df,
        "member":     member_df,
        "electorate": electorate_df,
    }
    _g_out_dir = out_dir_str


def _fill_one(pq_path_str: str) -> str:
    """Enrich one daily parquet file. Runs inside a worker process."""
    pq_path = Path(pq_path_str)
    out_dir = Path(_g_out_dir)
    out_pq  = out_dir / pq_path.name
    out_csv = out_dir / pq_path.name.replace(".parquet", ".csv")

    date_str = pq_path.stem
    df = pd.read_parquet(pq_path)
    original_len = len(df)

    df = fill_details(df, date_str, _LOOKUPS)
    assert len(df) == original_len, f"Row count changed for {date_str}"

    df.to_parquet(out_pq,  index=False)
    df.to_csv(out_csv,     index=False)
    return pq_path.stem


# ── Batch processing ──────────────────────────────────────────────────────────

def fill_all(daily_dir: Path, lookup_dir: Path, out_dir: Path,
             skip_existing: bool = True,
             sequential: bool = False) -> None:
    parquet_files = sorted(daily_dir.glob("*.parquet"))
    out_dir.mkdir(parents=True, exist_ok=True)

    if skip_existing:
        parquet_files = [
            p for p in parquet_files
            if not (out_dir / p.name).exists()
            or not (out_dir / p.name.replace(".parquet", ".csv")).exists()
        ]

    print(f"Filling details for {len(parquet_files)} daily files → {out_dir}")

    if not parquet_files:
        print("Done (nothing to do).")
        return

    if sequential:
        lookups = load_lookups(lookup_dir)
        for pq_path in tqdm(parquet_files, desc="Fill details"):
            out_csv = out_dir / pq_path.name.replace(".parquet", ".csv")
            out_pq  = out_dir / pq_path.name
            date_str = pq_path.stem
            df = pd.read_parquet(pq_path)
            original_len = len(df)
            df = fill_details(df, date_str, lookups)
            assert len(df) == original_len, f"Row count changed for {date_str}"
            df.to_csv(out_csv, index=False)
            df.to_parquet(out_pq, index=False)
    else:
        lookups = load_lookups(lookup_dir)
        lookups_tuple = (
            lookups["senator"],
            lookups["party"],
            lookups["state"],
            lookups["president"],
            lookups.get("member"),
            lookups.get("electorate"),
            str(out_dir),
        )
        items = [str(p) for p in parquet_files]
        eager_map(
            _fill_one,
            items,
            initializer=_init_worker,
            initargs=(lookups_tuple,),
            desc="Filling details",
            unit="file",
        )

    print("Done.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fill member details in parsed Hansard daily files."
    )
    parser.add_argument("--daily-dir", default="../data/output/senate/daily",
                        help="Directory with daily parquet files from 03_parse.py")
    parser.add_argument("--lookup-dir", default="../data/lookup",
                        help="Directory with lookup CSV files from 02_member_lookup.py")
    parser.add_argument("--out-dir", default="../data/output/senate/daily",
                        help="Output directory (may be same as --daily-dir)")
    parser.add_argument("--no-skip", action="store_true",
                        help="Re-process files that already have output")
    parser.add_argument("--sequential", action="store_true",
                        help="Disable parallelism (useful for debugging)")
    args = parser.parse_args()

    fill_all(
        Path(args.daily_dir),
        Path(args.lookup_dir),
        Path(args.out_dir),
        skip_existing=not args.no_skip,
        sequential=args.sequential,
    )


if __name__ == "__main__":
    main()
