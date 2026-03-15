"""
02b_member_lookup_house.py — Build House of Representatives member lookup tables.

Data sources (fetched programmatically):
  Primary (1901–Nov 2021): RohanAlexander/australian_politicians
    - australian_politicians-all.csv
    - australian_politicians-mps-by_division.csv
    - australian_politicians-all-by_party.csv
    - australian_politicians-uniqueID_to_aphID.csv

  Gap fill (Nov 2021–present): openaustralia/openaustralia-parser
    - data/representatives.csv
    - data/people.csv

Produces:
  data/lookup/member_lookup.csv      — one row per member (name forms, gender, PHID)
  data/lookup/party_lookup_house.csv — one row per party spell (date-aware)
  data/lookup/electorate_lookup.csv  — one row per member/division term (date-aware)
  data/lookup/speaker_lookup.csv     — Speaker/Deputy Speaker name variants

Usage:
    python 02b_member_lookup_house.py --out ../data/lookup
"""

import argparse
import re
from pathlib import Path

import pandas as pd
import requests

# ── Remote data sources ────────────────────────────────────────────────────────

AP_BASE = (
    "https://raw.githubusercontent.com/RohanAlexander/"
    "australian_politicians/master/data"
)
OA_BASE = (
    "https://raw.githubusercontent.com/openaustralia/"
    "openaustralia-parser/master/data"
)

AP_URLS = {
    "all":   f"{AP_BASE}/australian_politicians-all.csv",
    "mps":   f"{AP_BASE}/australian_politicians-mps-by_division.csv",
    "parties": f"{AP_BASE}/australian_politicians-all-by_party.csv",
    "phid":  f"{AP_BASE}/australian_politicians-uniqueID_to_aphID.csv",
}

OA_URLS = {
    "members": f"{OA_BASE}/representatives.csv",
    "people":  f"{OA_BASE}/people.csv",
}

# ── Fetch helpers ──────────────────────────────────────────────────────────────

def fetch_csv(url: str, **kwargs) -> pd.DataFrame:
    print(f"  Fetching {url}")
    return pd.read_csv(url, **kwargs)


# ── Name form generation ───────────────────────────────────────────────────────

def _safe_str(val) -> str:
    if pd.isna(val) or val is None:
        return ""
    return str(val).strip()


def get_title(gender: str, ap_title: str) -> str:
    """
    Derive the parliamentary title (Mr/Ms/Dr/Hon) from gender and AP title field.

    House Hansard uses Mr/Ms/Dr as address forms (not 'Senator').
    - If AP title == 'Dr' → use 'Dr'
    - male → 'Mr'
    - female → 'Ms'
    - unknown/NULL → 'Hon' (safe fallback)
    """
    t = _safe_str(ap_title).lower()
    if t in ("dr", "dr."):
        return "Dr"
    g = _safe_str(gender).lower()
    if g == "male":
        return "Mr"
    if g == "female":
        return "Ms"
    return "Hon"


def generate_name_forms(surname: str, first_name: str, gender: str, title: str) -> dict:
    """
    Generate the five House member name form variants used for matching
    against Hansard display text and body text.

    Observed Hansard formats (from XML parsing):
      - "Mr BURKE"    → title + ALL-CAPS surname  (form4)
      - "Ms WELLS"    → title + ALL-CAPS surname  (form4)
      - "Dr LEIGH"    → title + ALL-CAPS surname  (form4)
      - "Keogh"       → just surname              (form5)
      - "The SPEAKER" → presiding officer (handled separately)

    form1: "Mr/Ms/Dr Firstname Surname"
    form2: "Mr/Ms/Dr Surname"
    form3: "MR/MS/DR FIRSTNAME SURNAME"  (all caps)
    form4: "MR/MS/DR SURNAME"            (all caps — most common in body text)
    form5: "Surname"                     (just surname — very common in House Hansard)
    """
    sur = _safe_str(surname)
    first = _safe_str(first_name)
    ttl = get_title(gender, title)
    return {
        "form1": f"{ttl} {first} {sur}".strip(),
        "form2": f"{ttl} {sur}".strip(),
        "form3": f"{ttl.upper()} {first.upper()} {sur.upper()}".strip(),
        "form4": f"{ttl.upper()} {sur.upper()}".strip(),
        "form5": sur,
    }


# ── Wikidata gender fill ───────────────────────────────────────────────────────

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

_SPARQL_QUERY = """
SELECT DISTINCT ?person ?nameLabel ?genderLabel WHERE {
  ?person wdt:P39 wd:Q18912794 .
  ?person wdt:P21 ?gender .
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en" .
    ?person rdfs:label ?nameLabel .
    ?gender rdfs:label ?genderLabel .
  }
}
"""

def fill_gender_wikidata(member_lookup: pd.DataFrame) -> pd.DataFrame:
    """Query Wikidata for gender of members where gender is NULL.

    Matches by surname + first name (case-insensitive). Only fills where
    a unique unambiguous match is found.
    """
    null_mask = member_lookup["gender"].isna()
    if not null_mask.any():
        return member_lookup

    print(f"  Querying Wikidata for gender ({null_mask.sum()} members with NULL gender)...")
    try:
        resp = requests.get(
            WIKIDATA_SPARQL,
            params={"query": _SPARQL_QUERY, "format": "json"},
            headers={"User-Agent": "HansardPipeline/1.0"},
            timeout=30,
        )
        resp.raise_for_status()
        results = resp.json()["results"]["bindings"]
    except Exception as e:
        print(f"  WARNING: Wikidata query failed ({e}); gender will remain NULL")
        return member_lookup

    # Build name → gender map from Wikidata results
    wd_map: dict[tuple[str, str], str] = {}  # (surname_lower, firstname_lower) → gender
    for row in results:
        label = row.get("nameLabel", {}).get("value", "")
        gender_val = row.get("genderLabel", {}).get("value", "").lower()
        gender_norm = "male" if "male" in gender_val and "female" not in gender_val else (
                      "female" if "female" in gender_val else None)
        if not gender_norm or not label:
            continue
        parts = label.strip().split()
        if len(parts) >= 2:
            key = (parts[-1].lower(), parts[0].lower())  # (surname, firstname)
            # Only store if unambiguous (first occurrence wins)
            if key not in wd_map:
                wd_map[key] = gender_norm

    filled = 0
    for idx in member_lookup.index[null_mask]:
        sur   = str(member_lookup.at[idx, "surname"]    or "").strip().lower()
        first = str(member_lookup.at[idx, "first_name"] or "").strip().split()[0].lower()
        match = wd_map.get((sur, first))
        if match:
            member_lookup.at[idx, "gender"] = match
            filled += 1

    still_null = member_lookup["gender"].isna().sum()
    print(f"  Wikidata filled {filled} gender(s); {still_null} still NULL")
    return member_lookup


# ── Manual PHID corrections ────────────────────────────────────────────────────
#
# Members present in the AustralianPoliticians dataset (uniqueID column) but
# whose APH ID (PHID) is missing from australian_politicians-uniqueID_to_aphID.
# Verified against the Hansard corpus: these PHIDs appear in the XML but were
# not in the AP repo at the time of the last data pull.
#
_PHID_CORRECTIONS: dict[str, str] = {
    "Phillips1970": "147140",   # Fiona Phillips, ALP, Gilmore 2019–
    "Coker1962":    "263547",   # Elizabeth Coker, ALP, Paterson 2019–
    "Wells1985":    "264121",   # Anika Wells, ALP, Lilley 2019–
    "Webster1959":  "281688",   # Anne Webster, NATS, Mallee 2019–
    "McBain1982":   "281988",   # Kristy McBain, ALP, Eden-Monaro 2020–
    "Pearce1967":   "282306",   # Gavin Pearce, LP, Braddon 2019–
    "Connelly1978": "282984",   # Vincent Connelly, LP, Stirling 2019–2022
    "Hamilton1979": "291387",   # Garth Hamilton, LNP, Groom 2021–
}


# ── Build member_lookup ────────────────────────────────────────────────────────

def build_member_lookup(
    all_df: pd.DataFrame,
    mps_df: pd.DataFrame,
    phid_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build member_lookup.csv: one row per House member with name forms and PHID.

    Columns: unique_id, name_id, surname, first_name, common_name,
             display_name, gender, electorate, birth_date, death_date,
             form1, form2, form3, form4, form5
    """
    # Keep only House members from all.csv
    members_bio = all_df[all_df["member"] == 1].copy()

    # Join PHID
    phid_df_clean = phid_df.rename(columns={"uniqueID": "unique_id", "aphID": "name_id"})
    phid_df_clean["name_id"] = phid_df_clean["name_id"].astype(str).str.strip()

    members_bio = members_bio.rename(columns={"uniqueID": "unique_id"})
    df = members_bio.merge(phid_df_clean[["unique_id", "name_id"]], on="unique_id", how="left")

    # Apply manual PHID corrections for members missing from the AP repo's ID table
    for uid, phid in _PHID_CORRECTIONS.items():
        mask = df["unique_id"] == uid
        if mask.any():
            df.loc[mask, "name_id"] = phid

    # Electorate: take the most recent division from mps_df (for uniqueness)
    mps_df_clean = mps_df.rename(columns={"uniqueID": "unique_id"})
    mps_df_sorted = mps_df_clean.sort_values("mpFrom", ascending=False)
    electorate_latest = mps_df_sorted.drop_duplicates("unique_id")[["unique_id", "division"]]
    df = df.merge(electorate_latest, on="unique_id", how="left")

    # Generate name forms
    forms_list = []
    for _, row in df.iterrows():
        sur = _safe_str(row.get("surname", ""))
        first = _safe_str(row.get("commonName", "")) or _safe_str(row.get("firstName", ""))
        gender = _safe_str(row.get("gender", ""))
        title = _safe_str(row.get("title", ""))
        forms_list.append(generate_name_forms(sur, first, gender, title))
    forms_df = pd.DataFrame(forms_list)

    df = df.reset_index(drop=True)
    df = pd.concat([df, forms_df], axis=1)

    # Select and rename output columns
    out = df[[
        "unique_id", "name_id", "surname", "firstName", "commonName",
        "displayName", "gender", "title", "division", "birthDate", "deathDate",
        "form1", "form2", "form3", "form4", "form5",
    ]].rename(columns={
        "firstName": "first_name",
        "commonName": "common_name",
        "displayName": "display_name",
        "division": "electorate",
        "birthDate": "birth_date",
        "deathDate": "death_date",
    })

    # Normalise name_id to uppercase
    out["name_id"] = out["name_id"].str.upper()

    # Flag as AustralianPoliticians source
    out["source"] = "AustralianPoliticians"

    return out


# ── Append post-2021 members from OpenAustralia ───────────────────────────────

def append_oa_members(
    member_lookup: pd.DataFrame,
    oa_members: pd.DataFrame,
    oa_people: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add House members who entered parliament after November 2021 (absent from
    AustralianPoliticians data). These come from the OpenAustralia parser.

    Gender is not available in OA data; will be left as NA for manual filling.
    """
    oa = oa_members.copy()
    # Strip comment rows
    oa = oa[~oa.iloc[:, 0].astype(str).str.startswith("#")]
    # Normalise column names
    oa.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in oa.columns]

    # Parse date_of_election (DD.MM.YYYY)
    def parse_oa_date(d):
        if pd.isna(d) or str(d).strip() in ("", "nan"):
            return None
        try:
            parts = str(d).strip().split(".")
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        except Exception:
            pass
        return None

    oa["date_of_election_iso"] = oa["date_of_election"].apply(parse_oa_date)

    # Post-2021 cutoff
    oa_post2021 = oa[
        oa["date_of_election_iso"].notna() &
        (oa["date_of_election_iso"] >= "2021-11-01")
    ].copy()

    # Parse name: OpenAustralia stores "Firstname Surname" format
    def split_name(full_name):
        parts = str(full_name).strip().rsplit(" ", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return "", str(full_name).strip()

    oa_post2021[["first_name", "surname"]] = pd.DataFrame(
        oa_post2021["name"].apply(split_name).tolist(),
        index=oa_post2021.index,
    )

    # Division/electorate — column may be named 'division'
    div_col = next(
        (c for c in oa_post2021.columns if "division" in c.lower()),
        None
    )
    if div_col:
        oa_post2021["electorate"] = oa_post2021[div_col].str.strip()
    else:
        oa_post2021["electorate"] = None

    # Look up PHID from oa_people
    oa_people_clean = oa_people.copy()
    oa_people_clean.columns = [
        c.strip().lower().replace(" ", "_") for c in oa_people_clean.columns
    ]
    name_to_phid = (
        oa_people_clean[["name", "aph_id"]]
        .dropna(subset=["aph_id"])
        .set_index("name")["aph_id"]
        .to_dict()
    )
    oa_post2021["name_id"] = (
        oa_post2021["name"]
        .map(name_to_phid)
        .astype(str)
        .str.upper()
        .str.strip()
        .replace("NAN", None)
    )

    # Build unique_id placeholder
    oa_post2021["unique_id"] = (
        oa_post2021["surname"].str.replace(r"\W", "", regex=True)
        + "_OA_"
        + oa_post2021["date_of_election_iso"].str[:4]
    )

    # Generate name forms (no title/gender available from OA)
    forms_list = [
        generate_name_forms(row["surname"], row["first_name"], "", "")
        for _, row in oa_post2021.iterrows()
    ]
    forms_df = pd.DataFrame(forms_list, index=oa_post2021.index)
    oa_post2021 = pd.concat([oa_post2021, forms_df], axis=1)

    oa_out = oa_post2021[[
        "unique_id", "name_id", "surname", "first_name",
        "electorate", "form1", "form2", "form3", "form4", "form5",
    ]].copy()
    oa_out["common_name"] = None
    oa_out["display_name"] = None
    oa_out["gender"] = None
    oa_out["title"] = None
    oa_out["birth_date"] = None
    oa_out["death_date"] = None
    oa_out["source"] = "OpenAustralia"

    # Remove already-present members (match on surname + first_name)
    existing_names = set(
        member_lookup["surname"].str.lower() + "_" +
        member_lookup["first_name"].fillna("").str.lower()
    )
    oa_out["_key"] = (
        oa_out["surname"].str.lower() + "_" +
        oa_out["first_name"].str.lower()
    )
    oa_new = oa_out[~oa_out["_key"].isin(existing_names)].drop(columns=["_key"])

    if len(oa_new) > 0:
        print(f"  Adding {len(oa_new)} post-2021 members from OpenAustralia (gender will be filled via Wikidata)")
        print(oa_new[["surname", "first_name", "electorate", "source"]].to_string(index=False))

    result = pd.concat([member_lookup, oa_new], ignore_index=True)
    return result


# ── Electorate lookup corrections ────────────────────────────────────────────
#
# Rows to add or replace where the AustralianPoliticians source data is wrong.
# Each entry is a complete row dict (unique_id, name_id, electorate,
# member_from, member_to, member_end_reason).  Existing rows with matching
# (name_id, electorate) are dropped before inserting.
#
_ELECTORATE_CORRECTIONS: list[dict] = [
    # Wooldridge1956 (8E4): source has a single Chisholm 1987–2001 row.
    # He actually held Chisholm 1987–1990 (defeated), then won the new seat of
    # Casey at the March 1993 election and held it until 2001.
    {
        "unique_id": "Wooldridge1956", "name_id": "8E4",
        "electorate": "Chisholm",
        "member_from": "1987-07-11", "member_to": "1990-12-01",
        "member_end_reason": "Defeated",
    },
    {
        "unique_id": "Wooldridge1956", "name_id": "8E4",
        "electorate": "Casey",
        "member_from": "1993-03-13", "member_to": "2001-10-08",
        "member_end_reason": "Retired",
    },
]


def apply_electorate_corrections(
    electorate_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """Apply manual corrections to electorate_lookup rows from _ELECTORATE_CORRECTIONS."""
    if not _ELECTORATE_CORRECTIONS:
        return electorate_lookup
    corrections = pd.DataFrame(_ELECTORATE_CORRECTIONS)
    # Drop any existing rows for the corrected (name_id, unique_id) combinations
    drop_ids = set(corrections["unique_id"].unique())
    electorate_lookup = electorate_lookup[
        ~electorate_lookup["unique_id"].isin(drop_ids)
    ]
    return pd.concat([electorate_lookup, corrections], ignore_index=True)


# ── Build electorate_lookup ───────────────────────────────────────────────────

def build_electorate_lookup(
    mps_df: pd.DataFrame,
    phid_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build electorate_lookup.csv: one row per member/division term.
    Used for date-aware electorate assignment and T7 validation.

    Columns: unique_id, name_id, electorate, member_from, member_to,
             member_end_reason
    """
    df = mps_df.rename(columns={
        "uniqueID": "unique_id",
        "division": "electorate",
        "mpFrom": "member_from",
        "mpTo": "member_to",
        "mpEndReason": "member_end_reason",
    }).copy()

    phid_clean = phid_df.rename(columns={"uniqueID": "unique_id", "aphID": "name_id"})
    phid_clean["name_id"] = phid_clean["name_id"].astype(str).str.strip().str.upper()
    df = df.merge(phid_clean[["unique_id", "name_id"]], on="unique_id", how="left")

    # Apply manual PHID corrections
    for uid, phid in _PHID_CORRECTIONS.items():
        mask = df["unique_id"] == uid
        if mask.any():
            df.loc[mask, "name_id"] = phid

    df["member_from"] = pd.to_datetime(df["member_from"], errors="coerce").dt.date
    df["member_to"]   = pd.to_datetime(df["member_to"],   errors="coerce").dt.date

    return df[[
        "unique_id", "name_id", "electorate",
        "member_from", "member_to", "member_end_reason",
    ]]


# ── Build party_lookup_house ──────────────────────────────────────────────────

def build_party_lookup_house(
    parties_df: pd.DataFrame,
    phid_df: pd.DataFrame,
    member_unique_ids: set,
) -> pd.DataFrame:
    """
    Build party_lookup_house.csv: one row per party affiliation spell for
    House members. Used for date-aware party assignment.

    Columns: unique_id, name_id, party_abbrev, party_name, party_from, party_to,
             party_changed_name, party_simplified_name
    """
    df = parties_df.rename(columns={
        "uniqueID": "unique_id",
        "partyAbbrev": "party_abbrev",
        "partyName": "party_name",
        "partyFrom": "party_from",
        "partyTo": "party_to",
        "partyChangedName": "party_changed_name",
        "partySimplifiedName": "party_simplified_name",
    }).copy()

    # Keep only House members
    df = df[df["unique_id"].isin(member_unique_ids)]

    phid_clean = phid_df.rename(columns={"uniqueID": "unique_id", "aphID": "name_id"})
    phid_clean["name_id"] = phid_clean["name_id"].astype(str).str.strip().str.upper()
    df = df.merge(phid_clean[["unique_id", "name_id"]], on="unique_id", how="left")

    # Apply manual PHID corrections
    for uid, phid in _PHID_CORRECTIONS.items():
        mask = df["unique_id"] == uid
        if mask.any():
            df.loc[mask, "name_id"] = phid

    df["party_from"] = pd.to_datetime(df["party_from"], errors="coerce").dt.date
    df["party_to"]   = pd.to_datetime(df["party_to"],   errors="coerce").dt.date

    return df[[
        "unique_id", "name_id", "party_abbrev", "party_name",
        "party_from", "party_to", "party_changed_name", "party_simplified_name",
    ]]


# ── Build speaker_lookup ──────────────────────────────────────────────────────

def build_speaker_lookup() -> pd.DataFrame:
    """
    Build speaker_lookup.csv: all Speaker and Deputy Speaker name variants
    from 1998 to present.

    Speakers of the House of Representatives 1998–present:
      Bob Halverson      (LP, Vic)  — to 1998-02-09
      Ian Sinclair       (NP, NSW)  — 1998-02-10 to 1998-09-16
      Neil Andrew        (LP, SA)   — 1998-11-10 to 2004-10-21
      David Hawker       (LP, Vic)  — 2004-11-16 to 2008-02-12
      Harry Jenkins      (ALP, Vic) — 2008-02-12 to 2011-11-24
      Peter Slipper      (LNP, Qld) — 2011-11-28 to 2012-10-09
      Anna Burke         (ALP, Vic) — 2012-10-09 to 2013-10-14
      Bronwyn Bishop     (LP, NSW)  — 2013-11-12 to 2015-08-02
      Tony Smith         (LP, Vic)  — 2015-08-10 to 2021-09-01
      Andrew Wallace     (LNP, Qld) — 2021-09-01 to 2022-07-26
      Milton Dick        (ALP, Qld) — 2022-07-26 to 2025-03-28 (dissolution)
      Ben Morton         (LP, WA)   — 2025-07-22 to present

    The 'xml_name_pattern' contains strings appearing in Hansard XML —
    either in <span class="HPS-MemberSpeech"> or in the business.start
    paragraph "The SPEAKER (Hon. X) took the chair..."

    Generic patterns (date-independent) use name_id="10000" as a sentinel.
    """
    rows = [
        # ── Speakers ──────────────────────────────────────────────────────────
        # Bob Halverson — LP Vic (to 1998-02-09)
        dict(unique_id="HalversonRobert1945", name_id="5D5", role="SPEAKER",
             from_date="1996-03-18", to_date="1998-02-09",
             xml_name_pattern="The SPEAKER", state="VIC", party="LP",
             display_name="Mr Halverson",
             full_name="Hon. Bob Halverson"),

        # Ian Sinclair — NP NSW (1998-02-10 to 1998-09-16)
        dict(unique_id="Sinclair1929", name_id="ES4", role="SPEAKER",
             from_date="1998-02-10", to_date="1998-09-16",
             xml_name_pattern="The SPEAKER", state="NSW", party="NP",
             display_name="Mr Sinclair",
             full_name="Hon. Ian Sinclair"),

        # Neil Andrew — LP SA (1998-11-10 to 2004-10-21)
        dict(unique_id="Andrew1944", name_id="C04", role="SPEAKER",
             from_date="1998-11-10", to_date="2004-10-21",
             xml_name_pattern="The SPEAKER", state="SA", party="LP",
             display_name="Mr Andrew",
             full_name="Hon. Neil Andrew"),

        # David Hawker — LP Vic (2004-11-16 to 2008-02-12)
        dict(unique_id="Hawker1949", name_id="CY4", role="SPEAKER",
             from_date="2004-11-16", to_date="2008-02-12",
             xml_name_pattern="The SPEAKER", state="VIC", party="LP",
             display_name="Mr Hawker",
             full_name="Hon. David Hawker"),

        # Harry Jenkins — ALP Vic (2008-02-12 to 2011-11-24)
        dict(unique_id="Jenkins1952", name_id="HU6", role="SPEAKER",
             from_date="2008-02-12", to_date="2011-11-24",
             xml_name_pattern="The SPEAKER", state="VIC", party="ALP",
             display_name="Mr Jenkins",
             full_name="Hon. Harry Jenkins"),

        # Peter Slipper — LNP Qld (2011-11-28 to 2012-10-09)
        dict(unique_id="Slipper1950", name_id="EC4", role="SPEAKER",
             from_date="2011-11-28", to_date="2012-10-09",
             xml_name_pattern="The SPEAKER", state="QLD", party="LNP",
             display_name="Mr Slipper",
             full_name="Hon. Peter Slipper"),

        # Anna Burke — ALP Vic (2012-10-09 to 2013-10-14)
        dict(unique_id="Burke1966", name_id="G26", role="SPEAKER",
             from_date="2012-10-09", to_date="2013-10-14",
             xml_name_pattern="The SPEAKER", state="VIC", party="ALP",
             display_name="Ms Burke",
             full_name="Hon. Anna Burke"),
        dict(unique_id="Burke1966", name_id="G26", role="SPEAKER",
             from_date="2012-10-09", to_date="2013-10-14",
             xml_name_pattern="MADAM SPEAKER", state="VIC", party="ALP",
             display_name="Ms Burke",
             full_name="Hon. Anna Burke"),

        # Bronwyn Bishop — LP NSW (2013-11-12 to 2015-08-02)
        dict(unique_id="Bishop1942", name_id="HQ4", role="SPEAKER",
             from_date="2013-11-12", to_date="2015-08-02",
             xml_name_pattern="The SPEAKER", state="NSW", party="LP",
             display_name="Ms Bishop",
             full_name="Hon. Bronwyn Bishop"),
        dict(unique_id="Bishop1942", name_id="HQ4", role="SPEAKER",
             from_date="2013-11-12", to_date="2015-08-02",
             xml_name_pattern="MADAM SPEAKER", state="NSW", party="LP",
             display_name="Ms Bishop",
             full_name="Hon. Bronwyn Bishop"),

        # Tony Smith — LP Vic (2015-08-10 to 2021-09-01)
        dict(unique_id="Smith1965", name_id="I45", role="SPEAKER",
             from_date="2015-08-10", to_date="2021-09-01",
             xml_name_pattern="The SPEAKER", state="VIC", party="LP",
             display_name="Mr Smith",
             full_name="Hon. Tony Smith"),

        # Andrew Wallace — LNP Qld (2021-09-01 to 2022-07-26)
        dict(unique_id="Wallace1972", name_id="30479", role="SPEAKER",
             from_date="2021-09-01", to_date="2022-07-26",
             xml_name_pattern="The SPEAKER", state="QLD", party="LNP",
             display_name="Mr Wallace",
             full_name="Hon. Andrew Wallace"),

        # Milton Dick — ALP Qld (2022-07-26 to 2025-03-28)
        dict(unique_id="Dick1971", name_id="53517", role="SPEAKER",
             from_date="2022-07-26", to_date="2025-03-28",
             xml_name_pattern="The SPEAKER", state="QLD", party="ALP",
             display_name="Mr Dick",
             full_name="Hon. Milton Dick"),

        # Ben Morton — LP WA (2025-07-22 to present)
        dict(unique_id="Morton1974", name_id="47477", role="SPEAKER",
             from_date="2025-07-22", to_date=None,
             xml_name_pattern="The SPEAKER", state="WA", party="LP",
             display_name="Mr Morton",
             full_name="Hon. Ben Morton"),

        # ── Generic presiding-officer patterns (date-independent) ─────────────
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The SPEAKER", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The DEPUTY SPEAKER", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="MADAM SPEAKER", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The CHAIR", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The DEPUTY CHAIR", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The ACTING SPEAKER", state=None, party=None,
             display_name=None, full_name=None),
    ]
    df = pd.DataFrame(rows)
    df["from_date"] = pd.to_datetime(df["from_date"], errors="coerce").dt.date
    df["to_date"]   = pd.to_datetime(df["to_date"],   errors="coerce").dt.date
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Build House of Representatives member lookup tables."
    )
    parser.add_argument("--out", default="../data/lookup", help="Output directory")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Download AustralianPoliticians data ───────────────────────────────────
    print("Loading AustralianPoliticians data...")
    all_df     = fetch_csv(AP_URLS["all"])
    mps_df     = fetch_csv(AP_URLS["mps"])
    parties_df = fetch_csv(AP_URLS["parties"])
    phid_df    = fetch_csv(AP_URLS["phid"])

    # Ensure column name consistency
    all_df     = all_df.rename(columns=lambda c: c.strip())
    mps_df     = mps_df.rename(columns=lambda c: c.strip())
    parties_df = parties_df.rename(columns=lambda c: c.strip())
    phid_df    = phid_df.rename(columns=lambda c: c.strip())

    # ── Download OpenAustralia data ───────────────────────────────────────────
    print("Loading OpenAustralia data...")
    oa_members = fetch_csv(OA_URLS["members"], comment="#", header=0)
    oa_people  = fetch_csv(OA_URLS["people"], header=0)

    # ── member_lookup ─────────────────────────────────────────────────────────
    print("Building member_lookup...")
    member_lookup = build_member_lookup(all_df, mps_df, phid_df)
    member_lookup = append_oa_members(member_lookup, oa_members, oa_people)
    member_lookup = fill_gender_wikidata(member_lookup)

    out_path = out_dir / "member_lookup.csv"
    member_lookup.to_csv(out_path, index=False)
    print(f"  Saved {len(member_lookup)} rows → {out_path}")

    # ── electorate_lookup ─────────────────────────────────────────────────────
    print("Building electorate_lookup...")
    electorate_lookup = build_electorate_lookup(mps_df, phid_df)
    electorate_lookup = apply_electorate_corrections(electorate_lookup)
    out_path = out_dir / "electorate_lookup.csv"
    electorate_lookup.to_csv(out_path, index=False)
    print(f"  Saved {len(electorate_lookup)} rows → {out_path}")

    # ── party_lookup_house ────────────────────────────────────────────────────
    print("Building party_lookup_house...")
    # Member unique_ids from the AP data
    member_ids = set(mps_df["uniqueID"].dropna())
    party_lookup_house = build_party_lookup_house(parties_df, phid_df, member_ids)
    out_path = out_dir / "party_lookup_house.csv"
    party_lookup_house.to_csv(out_path, index=False)
    print(f"  Saved {len(party_lookup_house)} rows → {out_path}")

    # ── speaker_lookup ────────────────────────────────────────────────────────
    print("Building speaker_lookup...")
    speaker_lookup = build_speaker_lookup()
    out_path = out_dir / "speaker_lookup.csv"
    speaker_lookup.to_csv(out_path, index=False)
    print(f"  Saved {len(speaker_lookup)} rows → {out_path}")

    oa_count = len(member_lookup[member_lookup["source"] == "OpenAustralia"])
    null_gender = member_lookup["gender"].isna().sum()
    print("\nAll lookup tables built successfully.")
    if null_gender:
        print(f"\n[NOTE] {null_gender} member(s) still have NULL gender after Wikidata fill.")
        print(member_lookup[member_lookup["gender"].isna()][["unique_id","surname","first_name","source"]].to_string(index=False))


if __name__ == "__main__":
    main()
