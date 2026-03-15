"""
04b_fill_details_house.py — Post-parse member detail enrichment for House of Reps.

Seven passes:
  Pass 1: Fill gender and unique_id by name_id (PHID) — join on member_lookup
  Pass 2: Fill missing by name form matching (form1–form5)
  Pass 3: Presiding officer normalisation (Speaker / Deputy Speaker / Chair)
  Pass 4: Normalise party abbreviation variants to canonical forms
  Pass 5: Forward-fill page_no within sitting day
  Pass 6: Derive in_gov from party + government-period lookup (v2.2 XML omits this)
  Pass 7: Flag rows whose body contains embedded interjection markers (v2.2 XML only)

Usage:
    python 04b_fill_details_house.py --daily-dir ../data/output/house/daily_raw
                                     --lookup-dir ../data/lookup
                                     --out-dir    ../data/output/house/daily
                                     [--no-skip]
                                     [--sequential]
"""

import argparse
import re
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from parallel_utils import eager_map


# ── Lookup loading ────────────────────────────────────────────────────────────

def load_lookups(lookup_dir: Path) -> dict:
    lookup_dir = Path(lookup_dir)
    member_file = lookup_dir / "member_lookup.csv"
    if not member_file.exists():
        member_file = lookup_dir / "senator_lookup.csv"
    party_file      = lookup_dir / "party_lookup_house.csv"
    electorate_file = lookup_dir / "electorate_lookup.csv"
    result = {"member": pd.read_csv(member_file, dtype=str)}
    if party_file.exists():
        result["party"] = pd.read_csv(party_file, dtype=str)
    if electorate_file.exists():
        result["electorate"] = pd.read_csv(electorate_file, dtype=str)
    for key, fname in [("senator", "senator_lookup.csv"), ("state", "state_lookup.csv")]:
        p = lookup_dir / fname
        if p.exists():
            result[key] = pd.read_csv(p, dtype=str)
    return result


# ── Pass 1: Fill by name_id ───────────────────────────────────────────────────

def fill_by_name_id(df: pd.DataFrame, member_lookup: pd.DataFrame) -> pd.DataFrame:
    lookup = (
        member_lookup[["name_id", "unique_id", "gender"]]
        .dropna(subset=["name_id"])
        .drop_duplicates("name_id")
    )
    mask = df["name_id"].notna() & df["unique_id"].isna()
    if not mask.any():
        return df
    before = df.shape
    df = df.merge(
        lookup.rename(columns={"unique_id": "_uid", "gender": "_gender"}),
        on="name_id", how="left",
    )
    assert df.shape[0] == before[0]
    df.loc[df["unique_id"].isna() & df["_uid"].notna(), "unique_id"] = df["_uid"]
    df.loc[df["gender"].isna()    & df["_gender"].notna(), "gender"]  = df["_gender"]
    return df.drop(columns=["_uid", "_gender"], errors="ignore")


# ── Pass 2: Fill by name forms ────────────────────────────────────────────────

def _build_form_map(member_lookup: pd.DataFrame) -> dict[str, dict]:
    """Pre-build a name → member-record dict from all form1–form5 columns."""
    form_map: dict[str, dict] = {}
    for _, row in member_lookup.iterrows():
        for col in ("form1", "form2", "form3", "form4", "form5"):
            val = str(row.get(col, "") or "").strip()
            if val and val not in ("nan", "None") and val not in form_map:
                form_map[val] = {
                    "unique_id": row.get("unique_id"),
                    "name_id":   row.get("name_id"),
                    "gender":    row.get("gender"),
                }
    return form_map


def _build_term_date_index(electorate_lookup: pd.DataFrame) -> dict[str, list[tuple]]:
    """
    Build a name_id → list of (member_from, member_to) tuples from
    electorate_lookup.  Both dates are Python date objects or None if unknown.
    A member with multiple terms will have multiple tuples in the list.
    """
    index: dict[str, list[tuple]] = {}
    el = electorate_lookup.copy()
    el["member_from"] = pd.to_datetime(el["member_from"], errors="coerce").dt.date
    el["member_to"]   = pd.to_datetime(el["member_to"],   errors="coerce").dt.date
    for _, row in el.iterrows():
        nid = str(row.get("name_id") or "").strip()
        if not nid or nid in ("nan", "None"):
            continue
        from_d = row["member_from"] if not pd.isna(row["member_from"]) else None
        to_d   = row["member_to"]   if not pd.isna(row["member_to"])   else None
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


def fill_by_name_forms(df: pd.DataFrame,
                       form_map: dict[str, dict],
                       term_index: dict[str, list[tuple]] | None = None,
                       sitting_date=None) -> pd.DataFrame:
    """
    For rows where unique_id is still missing, attempt to match the 'name'
    column against form1–form5 variants in member_lookup.

    If term_index and sitting_date are provided, a date-range guard is applied:
    after finding a candidate match, the member's term dates (from
    electorate_lookup) are checked against the sitting date.  If the sitting
    date falls outside every known term for that member, the match is rejected
    (fields remain NULL) to prevent false assignments between members who share
    a surname across different eras.

    If term_index is None, or the matched name_id has no entry in term_index,
    the match is accepted as before (no over-rejection on missing data).
    """
    missing_mask = df["unique_id"].isna() & df["name"].notna()
    if not missing_mask.any():
        return df

    def match(name_val):
        if not name_val or pd.isna(name_val):
            return None
        s = str(name_val).strip()
        if s in form_map:
            return form_map[s]
        sl = s.lower()
        for f, rec in form_map.items():
            if f.lower() == sl:
                return rec
        return None

    before = df.shape
    for idx in df.index[missing_mask]:
        m = match(df.at[idx, "name"])
        if m:
            # ── Date-range guard ──────────────────────────────────────────────
            # If we have term data, verify this match is plausible for the
            # sitting date before accepting it.
            if term_index is not None and sitting_date is not None:
                candidate_nid = str(m.get("name_id") or "").strip()
                if candidate_nid and candidate_nid in term_index:
                    # Term data exists — check the sitting date is covered
                    if not _term_covers_date(term_index[candidate_nid], sitting_date):
                        # Sitting date outside all known terms — reject match
                        continue
                # If candidate_nid has no entry in term_index, accept the match
                # (we don't want to reject based on missing data).

            if pd.isna(df.at[idx, "unique_id"]) and m.get("unique_id"):
                df.at[idx, "unique_id"] = m["unique_id"]
            if pd.isna(df.at[idx, "name_id"]) and m.get("name_id"):
                df.at[idx, "name_id"] = m["name_id"]
            if pd.isna(df.at[idx, "gender"]) and m.get("gender"):
                df.at[idx, "gender"] = m["gender"]
    assert df.shape == before
    return df


# ── Pass 3: Presiding officers ────────────────────────────────────────────────

_PRESIDING_PATTERNS = [
    "The SPEAKER", "The DEPUTY SPEAKER", "The CHAIR",
    "The DEPUTY CHAIR", "The ACTING CHAIR",
]
_PRESIDING_ID = "10000"


def fill_presiding_officers(df: pd.DataFrame) -> pd.DataFrame:
    mask = pd.Series(False, index=df.index)
    for pat in _PRESIDING_PATTERNS:
        mask |= df["name"].str.contains(re.escape(pat), case=False, na=False, regex=True)
    mask |= df["name_id"] == _PRESIDING_ID
    df.loc[mask, "name_id"] = _PRESIDING_ID
    return df


# ── Pass 3b: Populate state for joint-sitting Senators ───────────────────────

_STATE_NORM_ABBREV: dict[str, str] = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "QLD": "Queensland",
    "SA":  "South Australia",
    "WA":  "Western Australia",
    "TAS": "Tasmania",
    "NT":  "Northern Territory",
    "ACT": "Australian Capital Territory",
}


_VALID_STATES_FULL: frozenset[str] = frozenset({
    "New South Wales", "Victoria", "Queensland", "Western Australia",
    "South Australia", "Tasmania", "Northern Territory", "Australian Capital Territory",
})


def fill_joint_sitting_state(df: pd.DataFrame, date_str: str,
                              senator_lookup: pd.DataFrame,
                              state_lookup: pd.DataFrame | None = None,
                              member_lookup: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    When Senators attend a joint sitting their speech appears in House XML
    with their state in the <electorate> tag.  This pass detects those rows
    and populates a new 'state' column, nulling 'electorate' for those rows.

    Detection uses two conditions to avoid false positives from people who
    served in both chambers at different times:
      1. The raw electorate value is a valid state/territory name.
      2. The name_id is in senator_lookup but NOT in member_lookup.
    Condition 1 filters out genuine House members (e.g. NT members whose
    electorate is literally "Northern Territory") if they're also ex-senators.
    """
    if "state" not in df.columns:
        df["state"] = pd.NA

    senator_ids = set(senator_lookup["name_id"].dropna().astype(str))
    member_ids  = set(member_lookup["name_id"].dropna().astype(str)) if member_lookup is not None else set()
    # Senators-only ids: in senator_lookup but not member_lookup
    senator_only_ids = senator_ids - member_ids

    senator_mask = (
        df["name_id"].astype(str).isin(senator_only_ids) &
        df["electorate"].isin(_VALID_STATES_FULL)
    )
    if not senator_mask.any():
        return df

    # Base map from senator_lookup (canonical state, no date range)
    state_map = (
        senator_lookup[["name_id", "state_abbrev"]]
        .dropna(subset=["name_id", "state_abbrev"])
        .drop_duplicates("name_id")
        .set_index("name_id")["state_abbrev"]
        .to_dict()
    )
    # Override with date-aware entries where available
    if state_lookup is not None:
        sitting_date = pd.to_datetime(date_str).date()
        sl = state_lookup.copy()
        sl["senator_from"] = pd.to_datetime(sl["senator_from"], errors="coerce").dt.date
        sl["senator_to"]   = pd.to_datetime(sl["senator_to"],   errors="coerce").dt.date
        valid_sl = sl[
            (sl["senator_from"].isna() | (sl["senator_from"] <= sitting_date)) &
            (sl["senator_to"].isna()   | (sl["senator_to"]   >= sitting_date))
        ].drop_duplicates("name_id")
        state_map.update(valid_sl.set_index("name_id")["state_abbrev"].to_dict())

    _s = df.loc[senator_mask, "name_id"].map(state_map).map(
        lambda v: _STATE_NORM_ABBREV.get(v, v) if pd.notna(v) else v
    )
    has_entry = senator_mask & _s.notna()
    df.loc[has_entry, "state"] = _s[has_entry]
    df.loc[senator_mask, "electorate"] = pd.NA
    return df


# ── Pass 5: Fill party and electorate from lookup by name_id ─────────────────

def fill_party_electorate(df: pd.DataFrame,
                          member_lookup: pd.DataFrame,
                          party_lookup: pd.DataFrame,
                          electorate_lookup: pd.DataFrame | None = None) -> pd.DataFrame:
    """Fill NULL party/electorate from lookups, and fix inline interjection rows.

    Inline interjection rows (name ends with ':') inherit the current speaker's
    party/electorate from the parser — always wrong.  We overwrite those too.
    """
    sitting_date = pd.to_datetime(df["date"].dropna().iloc[0]).date()

    # ── Date-aware electorate map: name_id → electorate ─────────────────────
    if electorate_lookup is not None:
        el = electorate_lookup.copy()
        el["member_from"] = pd.to_datetime(el["member_from"], errors="coerce").dt.date
        el["member_to"]   = pd.to_datetime(el["member_to"],   errors="coerce").dt.date
        valid_el = el[
            (el["member_from"].isna() | (el["member_from"] <= sitting_date)) &
            (el["member_to"].isna()   | (el["member_to"]   >= sitting_date))
        ].drop_duplicates("name_id")
        elec_map = valid_el.set_index("name_id")["electorate"].to_dict()
    else:
        # Fallback: use member_lookup (single electorate per member, no date guard)
        elec_map = (
            member_lookup[["name_id", "electorate"]]
            .dropna(subset=["name_id", "electorate"])
            .drop_duplicates("name_id")
            .set_index("name_id")["electorate"]
            .to_dict()
        )

    # ── Date-aware party map: name_id → party_abbrev ────────────────────────
    pl = party_lookup.copy()
    pl["party_from"] = pd.to_datetime(pl["party_from"], errors="coerce").dt.date
    pl["party_to"]   = pd.to_datetime(pl["party_to"],   errors="coerce").dt.date
    valid = pl[
        (pl["party_from"].isna() | (pl["party_from"] <= sitting_date)) &
        (pl["party_to"].isna()   | (pl["party_to"]   >= sitting_date))
    ].drop_duplicates("name_id")
    party_map = valid.set_index("name_id")["party_abbrev"].to_dict()

    # ── Apply ────────────────────────────────────────────────────────────────
    has_nid    = df["name_id"].notna()
    null_party = df["party"].isna()
    inline_int = df["name"].str.endswith(":", na=False)

    # Party: fill NULLs only (XML party values are generally authoritative)
    df.loc[has_nid & null_party, "party"] = (
        df.loc[has_nid & null_party, "name_id"].map(party_map)
    )
    # Overwrite inline interjection party (inherited wrong value from parser)
    _p = df["name_id"].map(party_map)
    overwrite_p = has_nid & inline_int & _p.notna()
    df.loc[overwrite_p, "party"] = _p[overwrite_p]

    # Electorate: always overwrite from the date-aware lookup when an entry
    # exists for this name_id.  This ensures members who changed electorates
    # (or whose XML contains spelling/apostrophe variants) get a canonical
    # value consistent with the lookup, not whatever the XML happened to say.
    _e = df["name_id"].map(elec_map)
    has_elec_entry = has_nid & _e.notna()
    df.loc[has_elec_entry, "electorate"] = _e[has_elec_entry]

    return df


# ── Pass 4: Party normalisation ───────────────────────────────────────────────

_PARTY_NORM: dict[str, str] = {
    "LIB":      "LP",
    "NPA":      "NP",
    "NATS":     "NATS",
    "Nats":     "NATS",
    "NatsWA":   "NatsWA",
    "GRN":      "AG",
    "G(WA)":    "GWA",
    "ON":       "PHON",
    "Ind.":     "IND",
    "Ind":      "IND",
    "N/A":      pd.NA,
    "UNKNOWN":  pd.NA,
    "LPI0":     "LP",
    "NPActing": "NP",
}


def normalise_party(df: pd.DataFrame) -> pd.DataFrame:
    df["party"] = df["party"].replace(_PARTY_NORM)
    return df


# ── Pass 5: Forward-fill page numbers ────────────────────────────────────────

def forward_fill_page_no(df: pd.DataFrame) -> pd.DataFrame:
    """Pass 5: Forward-fill page_no within sitting day.

    Interjections and brief procedural turns do not receive explicit page
    citations in v2.x XML (pre-2012).  They physically occur on the same
    page as the surrounding speech, so forward-filling from the previous
    row is safe and derivable from the XML alone.
    """
    df["page_no"] = df["page_no"].ffill()
    return df


# ── Pass 6: Derive in_gov from party + government-period lookup ───────────────
#
# APH stopped populating <in.gov> in v2.2 XML (post-2012).  We reconstruct it
# from the party column and known Australian government change-of-party dates.
# Applied to ALL rows so the column is consistent across eras.
#
# Sources:
#   https://www.aph.gov.au/About_Parliament/Parliamentary_Departments/
#   Parliamentary_Library/pubs/rp/rp2425/GovernmentChangeAustralia

_COALITION_PARTIES = frozenset({
    "LP", "LIB", "LNP", "NP", "NPA", "NAT", "NATS", "NATS WA", "NatsWA", "CLP",
})
_ALP_PARTIES = frozenset({"ALP"})

# Each entry: (period_start inclusive, period_end exclusive, parties_in_gov)
# Dates are the day the new PM was sworn in.
_GOV_PERIODS: list[tuple[str, str, frozenset]] = [
    ("1996-03-11", "2007-12-03", _COALITION_PARTIES),  # Howard (LP/NP)
    ("2007-12-03", "2013-09-18", _ALP_PARTIES),         # Rudd → Gillard → Rudd
    ("2013-09-18", "2022-05-23", _COALITION_PARTIES),   # Abbott → Turnbull → Morrison
    ("2022-05-23", "2099-12-31", _ALP_PARTIES),         # Albanese
]
_GOV_PERIODS_PARSED = [
    (pd.Timestamp(s).date(), pd.Timestamp(e).date(), parties)
    for s, e, parties in _GOV_PERIODS
]


def _party_in_gov(party: str, sitting_date) -> int:
    if not party or pd.isna(party):
        return 0
    p = str(party).strip()
    for start, end, parties in _GOV_PERIODS_PARSED:
        if start <= sitting_date < end:
            return 1 if p in parties else 0
    return 0


def derive_in_gov(df: pd.DataFrame) -> pd.DataFrame:
    """Pass 6: Overwrite in_gov using party + government-period lookup.

    The v2.2 XML (post-2012) has empty <in.gov> elements, so the column is
    all-zero for modern files.  We derive the correct value for every row.
    Pre-2012 rows are also recalculated for consistency — the XML values were
    reliable but this guarantees uniformity across schema eras.
    """
    try:
        sitting_date = pd.to_datetime(df["date"].dropna().iloc[0]).date()
    except Exception:
        return df
    df["in_gov"] = df["party"].apply(lambda p: _party_in_gov(p, sitting_date))
    return df


# ── Pass 7: Flag embedded interjection markers ────────────────────────────────
#
# In v2.2 XML (post-2012) interjections are often encoded inline in the
# speaking member's body text rather than as separate <interjection> elements.
# The marker pattern is always:
#   "[Name/Group] interjecting—"
# e.g. "Mr HAYES interjecting—", "Opposition members interjecting—"
#
# We flag these rows rather than splitting them (splitting would fragment speech
# turns).  Downstream consumers can call strip_embedded_interjections() to
# remove the markers from body text before text analysis or display.

_EMBEDDED_INTERJECT_RE = re.compile(
    r"(?:"
    r"(?:Mr|Mrs|Ms|Dr|Hon\.?)\s+[A-Z][A-Za-z'\-]+"  # titled individual
    r"|(?:An?\s+)?(?:honourable\s+)?(?:[Oo]pposition|[Gg]overnment|[Hh]onourable)\s+members?"
    r")\s+interjecting[—\-]",
    re.IGNORECASE,
)

# Also matches bare "X interjecting—" where X is all-caps surname
_BARE_CAPS_INTERJECT_RE = re.compile(
    r"\b[A-Z]{2,}(?:\s+[A-Z]{2,})?\s+interjecting[—\-]"
)


def flag_embedded_interjections(df: pd.DataFrame) -> pd.DataFrame:
    """Pass 7: Add has_embedded_interject column (bool).

    True for any row whose body text contains an inline interjection marker.
    Applies to all eras but is only meaningful for post-2012 (v2.2) files
    where interjections were not split into separate rows by the parser.
    """
    mask = (
        df["body"].str.contains(_EMBEDDED_INTERJECT_RE, na=False) |
        df["body"].str.contains(_BARE_CAPS_INTERJECT_RE, na=False)
    )
    df["has_embedded_interject"] = mask
    return df


def strip_embedded_interjections(body: str) -> str:
    """Remove inline interjection markers from a body string.

    Strips patterns like "Mr HAYES interjecting—" and
    "Opposition members interjecting—" from the text, collapsing
    resulting extra whitespace.  Does not modify the stored corpus —
    call this on-demand when clean text is needed (e.g. for LLM input
    or search snippet display).
    """
    text = _EMBEDDED_INTERJECT_RE.sub(" ", body)
    text = _BARE_CAPS_INTERJECT_RE.sub(" ", text)
    # Collapse multiple spaces and strip
    return re.sub(r"  +", " ", text).strip()


# ── Combined per-file enrichment ──────────────────────────────────────────────

def fill_details_house(df: pd.DataFrame,
                       member_lookup: pd.DataFrame,
                       form_map: dict[str, dict],
                       party_lookup: pd.DataFrame | None = None,
                       electorate_lookup: pd.DataFrame | None = None,
                       term_index: dict[str, list[tuple]] | None = None,
                       senator_lookup: pd.DataFrame | None = None,
                       state_lookup: pd.DataFrame | None = None) -> pd.DataFrame:
    n = len(df)
    df = fill_by_name_id(df, member_lookup)

    # Derive sitting date from the dataframe for the Pass 2 date-range guard
    sitting_date = None
    date_str = None
    try:
        sitting_date = pd.to_datetime(df["date"].dropna().iloc[0]).date()
        date_str = str(sitting_date)
    except Exception:
        pass

    df = fill_by_name_forms(df, form_map,
                            term_index=term_index,
                            sitting_date=sitting_date)
    df = fill_presiding_officers(df)

    if senator_lookup is not None and date_str is not None:
        df = fill_joint_sitting_state(df, date_str, senator_lookup, state_lookup,
                                      member_lookup=member_lookup)
    elif "state" not in df.columns:
        df["state"] = pd.NA

    if party_lookup is not None:
        df = fill_party_electorate(df, member_lookup, party_lookup, electorate_lookup)
    df = normalise_party(df)
    assert len(df) == n, "After Pass 4: row count changed"

    df = forward_fill_page_no(df)
    assert len(df) == n, "After Pass 5: row count changed"

    df = derive_in_gov(df)
    assert len(df) == n, "After Pass 6: row count changed"

    df = flag_embedded_interjections(df)
    assert len(df) == n, "After Pass 7: row count changed"
    return df


# ── Worker functions (module-level for pickle compatibility) ──────────────────

_g_member_lookup: pd.DataFrame | None = None
_g_form_map: dict | None = None
_g_party_lookup: pd.DataFrame | None = None
_g_electorate_lookup: pd.DataFrame | None = None
_g_senator_lookup: pd.DataFrame | None = None
_g_state_lookup: pd.DataFrame | None = None
_g_term_index: dict | None = None
_g_out_dir: str | None = None


def _worker_init(lookup_dir: str, out_dir: str) -> None:
    """Load shared lookup data once per worker process."""
    global _g_member_lookup, _g_form_map, _g_party_lookup, _g_electorate_lookup
    global _g_senator_lookup, _g_state_lookup, _g_term_index, _g_out_dir
    lookups              = load_lookups(Path(lookup_dir))
    _g_member_lookup     = lookups["member"]
    _g_form_map          = _build_form_map(_g_member_lookup)
    _g_party_lookup      = lookups.get("party")
    _g_electorate_lookup = lookups.get("electorate")
    _g_senator_lookup    = lookups.get("senator")
    _g_state_lookup      = lookups.get("state")
    # Pre-build the Pass 2 date-range guard index from electorate_lookup
    if _g_electorate_lookup is not None:
        _g_term_index = _build_term_date_index(_g_electorate_lookup)
    else:
        _g_term_index = None
    _g_out_dir           = out_dir


def _fill_worker(pq_path_str: str) -> str:
    """Enrich one daily parquet file. Runs inside a worker process."""
    pq_path = Path(pq_path_str)
    out_dir = Path(_g_out_dir)
    out_pq  = out_dir / pq_path.name
    out_csv = out_dir / pq_path.name.replace(".parquet", ".csv")

    df = pd.read_parquet(pq_path)
    df = fill_details_house(
        df, _g_member_lookup, _g_form_map, _g_party_lookup, _g_electorate_lookup,
        term_index=_g_term_index,
        senator_lookup=_g_senator_lookup,
        state_lookup=_g_state_lookup,
    )
    df.to_parquet(out_pq,  index=False)
    df.to_csv(out_csv,     index=False)
    return pq_path.stem


# ── Main runner ───────────────────────────────────────────────────────────────

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
        lookups           = load_lookups(lookup_dir)
        form_map          = _build_form_map(lookups["member"])
        party_lookup      = lookups.get("party")
        electorate_lookup = lookups.get("electorate")
        senator_lookup    = lookups.get("senator")
        state_lookup      = lookups.get("state")
        term_index        = _build_term_date_index(electorate_lookup) if electorate_lookup is not None else None
        for pq_path in tqdm(parquet_files, desc="Fill details"):
            out_pq  = out_dir / pq_path.name
            out_csv = out_dir / pq_path.name.replace(".parquet", ".csv")
            df = pd.read_parquet(pq_path)
            df = fill_details_house(
                df, lookups["member"], form_map, party_lookup, electorate_lookup,
                term_index=term_index,
                senator_lookup=senator_lookup,
                state_lookup=state_lookup,
            )
            df.to_parquet(out_pq,  index=False)
            df.to_csv(out_csv,     index=False)
    else:
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(name)s] %(message)s",
            datefmt="%H:%M:%S",
        )
        items = [str(p) for p in parquet_files]
        eager_map(
            _fill_worker,
            items,
            initializer=_worker_init,
            initargs=(str(lookup_dir), str(out_dir)),
        )

    print("Done.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    _here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Fill member details in parsed House Hansard daily files."
    )
    parser.add_argument("--daily-dir",  default=str(_here / "../data/output/house/daily_raw"))
    parser.add_argument("--lookup-dir", default=str(_here / "../data/lookup"))
    parser.add_argument("--out-dir",    default=str(_here / "../data/output/house/daily"))
    parser.add_argument("--no-skip",    action="store_true",
                        help="Re-process files that already have output")
    parser.add_argument("--sequential", action="store_true",
                        help="Disable parallelism (useful for debugging)")
    args = parser.parse_args()
    fill_all(
        Path(args.daily_dir), Path(args.lookup_dir), Path(args.out_dir),
        skip_existing=not args.no_skip,
        sequential=args.sequential,
    )


if __name__ == "__main__":
    main()
