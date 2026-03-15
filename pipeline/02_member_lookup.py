"""
02_member_lookup.py — Build senator member lookup tables.

Data sources (fetched programmatically):
  Primary (1901–Nov 2021): RohanAlexander/australian_politicians
    - australian_politicians-all.csv
    - australian_politicians-senators-by_state.csv
    - australian_politicians-all-by_party.csv
    - australian_politicians-uniqueID_to_aphID.csv

  Gap fill (Nov 2021–present): openaustralia/openaustralia-parser
    - data/senators.csv
    - data/people.csv

Produces:
  data/lookup/senator_lookup.csv      — one row per senator (name forms, gender, PHID)
  data/lookup/party_lookup.csv        — one row per party spell (date-aware)
  data/lookup/state_lookup.csv        — one row per senate term (date-aware)
  data/lookup/president_lookup.csv    — President/Deputy President name variants
  data/lookup/partyfacts_map.csv      — party abbreviation → PartyFacts ID

Usage:
    python 02_member_lookup.py --out ../data/lookup
"""

import argparse
import re
from pathlib import Path

import pandas as pd
import requests

# ── Wikidata gender fill ───────────────────────────────────────────────────────

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

_SPARQL_QUERY = """
SELECT DISTINCT ?person ?nameLabel ?genderLabel WHERE {
  ?person wdt:P39 wd:Q15646722 .
  ?person wdt:P21 ?gender .
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en" .
    ?person rdfs:label ?nameLabel .
    ?gender rdfs:label ?genderLabel .
  }
}
"""

def fill_gender_wikidata(senator_lookup: pd.DataFrame) -> pd.DataFrame:
    """Query Wikidata for gender of senators where gender is NULL.

    Matches by surname + first name (case-insensitive). Only fills where
    a unique unambiguous match is found.
    """
    null_mask = senator_lookup["gender"].isna()
    if not null_mask.any():
        return senator_lookup

    print(f"  Querying Wikidata for gender ({null_mask.sum()} senators with NULL gender)...")
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
        return senator_lookup

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
    for idx in senator_lookup.index[null_mask]:
        sur   = str(senator_lookup.at[idx, "surname"]    or "").strip().lower()
        first = str(senator_lookup.at[idx, "first_name"] or "").strip().split()[0].lower()
        match = wd_map.get((sur, first))
        if match:
            senator_lookup.at[idx, "gender"] = match
            filled += 1

    still_null = senator_lookup["gender"].isna().sum()
    print(f"  Wikidata filled {filled} gender(s); {still_null} still NULL")
    return senator_lookup

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
    "all": f"{AP_BASE}/australian_politicians-all.csv",
    "senators": f"{AP_BASE}/australian_politicians-senators-by_state.csv",
    "parties": f"{AP_BASE}/australian_politicians-all-by_party.csv",
    "phid": f"{AP_BASE}/australian_politicians-uniqueID_to_aphID.csv",
}

OA_URLS = {
    "senators": f"{OA_BASE}/senators.csv",
    "people": f"{OA_BASE}/people.csv",
}

# ── State abbreviation mapping ─────────────────────────────────────────────────

STATE_ABBREV_TO_FULL = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "Vic": "Victoria",
    "QLD": "Queensland",
    "Qld": "Queensland",
    "SA":  "South Australia",
    "WA":  "Western Australia",
    "TAS": "Tasmania",
    "Tas": "Tasmania",
    "Tas.": "Tasmania",
    "ACT": "Australian Capital Territory",
    "NT":  "Northern Territory",
}

STATE_FULL_TO_ABBREV = {v: k for k, v in STATE_ABBREV_TO_FULL.items() if "." not in k}
# Ensure single canonical abbreviation per state
CANONICAL_ABBREV = {
    "New South Wales": "NSW",
    "Victoria": "VIC",
    "Queensland": "QLD",
    "South Australia": "SA",
    "Western Australia": "WA",
    "Tasmania": "TAS",
    "Australian Capital Territory": "ACT",
    "Northern Territory": "NT",
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


def generate_name_forms(surname: str, first_name: str) -> dict:
    """
    Generate the five Senator name form variants used for matching
    against Hansard display text and body text.

    form1: "Senator FirstName Surname"
    form2: "Senator Surname"
    form3: "Senator FIRSTNAME SURNAME"
    form4: "Senator SURNAME"
    form5: "Senator FirstName SURNAME"
    """
    sur = _safe_str(surname)
    first = _safe_str(first_name)
    title = "Senator"
    return {
        "form1": f"{title} {first} {sur}".strip(),
        "form2": f"{title} {sur}".strip(),
        "form3": f"{title} {first.upper()} {sur.upper()}".strip(),
        "form4": f"{title} {sur.upper()}".strip(),
        "form5": f"{title} {first} {sur.upper()}".strip(),
    }


def parse_ap_metadata_name(metadata: str) -> tuple[str, str]:
    """
    Parse AustralianPoliticians display name format: "Surname, FirstName"
    (no "Sen" prefix in the all.csv displayName — that's the OA format).
    Also handles "Cook, Peter" directly.
    Returns (surname, first_name).
    """
    if not metadata or pd.isna(metadata):
        return "", ""
    parts = str(metadata).split(",", 1)
    surname = parts[0].strip()
    first = parts[1].strip() if len(parts) > 1 else ""
    # Strip "Sen " or "Sen. " prefix if present (from OA metadata format)
    first = re.sub(r"^Sen\.?\s+", "", first).strip()
    return surname, first


# ── Build senator_lookup ───────────────────────────────────────────────────────

def build_senator_lookup(
    all_df: pd.DataFrame,
    senators_df: pd.DataFrame,
    phid_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build senator_lookup.csv: one row per senator with name forms and PHID.

    Columns: unique_id, name_id, surname, first_name, common_name,
             display_name, gender, state_abbrev, birth_date, death_date,
             form1, form2, form3, form4, form5
    """
    # Keep only senators from the all.csv
    senators_bio = all_df[all_df["senator"] == 1].copy()

    # Join PHID
    phid_df_clean = phid_df.rename(columns={"uniqueID": "unique_id", "aphID": "name_id"})
    phid_df_clean["name_id"] = phid_df_clean["name_id"].astype(str).str.strip()

    senators_bio = senators_bio.rename(columns={"uniqueID": "unique_id"})
    df = senators_bio.merge(phid_df_clean[["unique_id", "name_id"]], on="unique_id", how="left")

    # State: take the most recent state from senators_df (for uniqueness)
    senators_df_clean = senators_df.rename(columns={"uniqueID": "unique_id"})
    # Most recent term's state per senator
    senators_df_sorted = senators_df_clean.sort_values("senatorFrom", ascending=False)
    state_latest = senators_df_sorted.drop_duplicates("unique_id")[["unique_id", "senatorsState"]]
    df = df.merge(state_latest, on="unique_id", how="left")

    # Generate name forms
    forms_list = []
    for _, row in df.iterrows():
        sur = _safe_str(row.get("surname", ""))
        # Use commonName if available, else firstName
        first = _safe_str(row.get("commonName", "")) or _safe_str(row.get("firstName", ""))
        forms_list.append(generate_name_forms(sur, first))
    forms_df = pd.DataFrame(forms_list)

    df = df.reset_index(drop=True)
    df = pd.concat([df, forms_df], axis=1)

    # Select and rename output columns
    out = df[[
        "unique_id", "name_id", "surname", "firstName", "commonName",
        "displayName", "gender", "senatorsState", "birthDate", "deathDate",
        "form1", "form2", "form3", "form4", "form5",
    ]].rename(columns={
        "firstName": "first_name",
        "commonName": "common_name",
        "displayName": "display_name",
        "senatorsState": "state_abbrev",
        "birthDate": "birth_date",
        "deathDate": "death_date",
    })

    # Normalise name_id to uppercase
    out["name_id"] = out["name_id"].str.upper()

    # Flag as AustralianPoliticians source
    out["source"] = "AustralianPoliticians"

    return out


# ── Append post-2021 senators from OpenAustralia ──────────────────────────────

def append_oa_senators(
    senator_lookup: pd.DataFrame,
    oa_senators: pd.DataFrame,
    oa_people: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add senators who entered parliament after November 2021 (absent from
    AustralianPoliticians data). These come from the OpenAustralia parser.

    Gender is not available in OA data; will be filled via Wikidata.
    """
    # Clean up OA senators CSV
    oa = oa_senators.copy()
    # Strip comment rows
    oa = oa[~oa.iloc[:, 0].astype(str).str.startswith("#")]
    # Rename columns (they have spaces)
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
    oa["date_ceased_iso"] = oa["date_ceased_to_be_a_member"].apply(parse_oa_date)

    # Post-2021 cutoff
    oa_post2021 = oa[
        oa["date_of_election_iso"].notna() &
        (oa["date_of_election_iso"] >= "2021-11-01")
    ].copy()

    # Clean state abbreviation
    oa_post2021["state_abbrev"] = (
        oa_post2021["state_territory"]
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.upper()
        .map(lambda x: {
            "TAS": "TAS", "NSW": "NSW", "VIC": "VIC", "QLD": "QLD",
            "SA": "SA", "WA": "WA", "ACT": "ACT", "NT": "NT",
        }.get(x, x))
    )

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

    # Look up PHID from oa_people
    oa_people_clean = oa_people.copy()
    oa_people_clean.columns = [
        c.strip().lower().replace(" ", "_") for c in oa_people_clean.columns
    ]
    # people.csv: person_count, aph_id, name, birthday, alt_name
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

    # Build unique_id as "Surname+Year" (approximate; no birth year in OA data)
    # Use a placeholder pattern; manual review will be needed
    oa_post2021["unique_id"] = (
        oa_post2021["surname"].str.replace(r"\W", "", regex=True)
        + "_OA_"
        + oa_post2021["date_of_election_iso"].str[:4]
    )

    # Generate name forms
    forms_list = [
        generate_name_forms(row["surname"], row["first_name"])
        for _, row in oa_post2021.iterrows()
    ]
    forms_df = pd.DataFrame(forms_list, index=oa_post2021.index)
    oa_post2021 = pd.concat([oa_post2021, forms_df], axis=1)

    # Select columns matching senator_lookup
    oa_out = oa_post2021[[
        "unique_id", "name_id", "surname", "first_name",
        "state_abbrev", "form1", "form2", "form3", "form4", "form5",
    ]].copy()
    oa_out["common_name"] = None
    oa_out["display_name"] = None
    oa_out["gender"] = None  # Must be filled manually
    oa_out["birth_date"] = None
    oa_out["death_date"] = None
    oa_out["source"] = "OpenAustralia"

    # Remove any that are already in the AustralianPoliticians lookup
    # (match on state + surname + first_name approximately)
    existing_names = set(
        senator_lookup["surname"].str.lower() + "_" +
        senator_lookup["first_name"].fillna("").str.lower()
    )
    oa_out["_key"] = (
        oa_out["surname"].str.lower() + "_" +
        oa_out["first_name"].str.lower()
    )
    oa_new = oa_out[~oa_out["_key"].isin(existing_names)].drop(columns=["_key"])

    if len(oa_new) > 0:
        print(f"  Adding {len(oa_new)} post-2021 senators from OpenAustralia (gender will be filled via Wikidata)")
        print(oa_new[["surname", "first_name", "state_abbrev", "source"]].to_string(index=False))

    result = pd.concat([senator_lookup, oa_new], ignore_index=True)
    return result


# ── Build state_lookup ────────────────────────────────────────────────────────

def build_state_lookup(
    senators_df: pd.DataFrame,
    phid_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build state_lookup.csv: one row per senate term.
    Used for date-aware state assignment.

    Columns: unique_id, name_id, state_abbrev, senator_from, senator_to,
             senator_end_reason, sec15_sel
    """
    df = senators_df.rename(columns={
        "uniqueID": "unique_id",
        "senatorsState": "state_abbrev",
        "senatorFrom": "senator_from",
        "senatorTo": "senator_to",
        "senatorEndReason": "senator_end_reason",
        "sec15Sel": "sec15_sel",
    }).copy()

    phid_clean = phid_df.rename(columns={"uniqueID": "unique_id", "aphID": "name_id"})
    phid_clean["name_id"] = phid_clean["name_id"].astype(str).str.strip().str.upper()
    df = df.merge(phid_clean[["unique_id", "name_id"]], on="unique_id", how="left")

    # Normalise dates
    df["senator_from"] = pd.to_datetime(df["senator_from"], errors="coerce").dt.date
    df["senator_to"] = pd.to_datetime(df["senator_to"], errors="coerce").dt.date

    return df[[
        "unique_id", "name_id", "state_abbrev",
        "senator_from", "senator_to", "senator_end_reason", "sec15_sel",
    ]]


# ── Build party_lookup ────────────────────────────────────────────────────────

def build_party_lookup(
    parties_df: pd.DataFrame,
    phid_df: pd.DataFrame,
    senator_unique_ids: set,
) -> pd.DataFrame:
    """
    Build party_lookup.csv: one row per party affiliation spell for senators.
    Used for date-aware party assignment.

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

    # Keep only senators
    df = df[df["unique_id"].isin(senator_unique_ids)]

    phid_clean = phid_df.rename(columns={"uniqueID": "unique_id", "aphID": "name_id"})
    phid_clean["name_id"] = phid_clean["name_id"].astype(str).str.strip().str.upper()
    df = df.merge(phid_clean[["unique_id", "name_id"]], on="unique_id", how="left")

    df["party_from"] = pd.to_datetime(df["party_from"], errors="coerce").dt.date
    df["party_to"] = pd.to_datetime(df["party_to"], errors="coerce").dt.date

    return df[[
        "unique_id", "name_id", "party_abbrev", "party_name",
        "party_from", "party_to", "party_changed_name", "party_simplified_name",
    ]]


# ── Build president_lookup ────────────────────────────────────────────────────

def build_president_lookup() -> pd.DataFrame:
    """
    Build president_lookup.csv: all President and Deputy President name
    variants from 1998 to present.

    This table is manually compiled from Senate records (confirmed by
    research in 06_member_data_sources.md and APH biographical pages).

    The 'xml_name_pattern' column contains the string that will appear in
    Hansard XML — either as a display name in <name role="display"> (v2.0/v2.1)
    or as a <span class="HPS-OfficeSpeech"> text (v2.2), or in the business.start
    paragraph ("The PRESIDENT (Senator the Hon. X) took the chair...").
    """
    rows = [
        # ── Senate Presidents ─────────────────────────────────────────────────
        # Margaret Reid 1996-08-20 to 2002-08-18
        dict(unique_id="ReidMargaret1935", name_id="VI4", role="PRESIDENT",
             from_date="1996-08-20", to_date="2002-08-18",
             xml_name_pattern="The PRESIDENT", state="ACT", party="LP",
             display_name="Senator Reid",
             full_name="Senator the Hon. Margaret Reid"),
        dict(unique_id="ReidMargaret1935", name_id="VI4", role="PRESIDENT",
             from_date="1996-08-20", to_date="2002-08-18",
             xml_name_pattern="PRESIDENT (Senator Reid)", state="ACT", party="LP",
             display_name="Senator Reid",
             full_name="Senator the Hon. Margaret Reid"),

        # Paul Calvert 2002-08-19 to 2007-08-14
        dict(unique_id="Calvert1940", name_id="5F4", role="PRESIDENT",
             from_date="2002-08-19", to_date="2007-08-14",
             xml_name_pattern="The PRESIDENT", state="TAS", party="LP",
             display_name="Senator Calvert",
             full_name="Senator the Hon. Paul Calvert"),

        # Alan Ferguson 2007-08-14 to 2008-08-25
        dict(unique_id="Ferguson1943", name_id="EP5", role="PRESIDENT",
             from_date="2007-08-14", to_date="2008-08-25",
             xml_name_pattern="The PRESIDENT", state="SA", party="LP",
             display_name="Senator Ferguson",
             full_name="Senator the Hon. Alan Ferguson"),

        # John Hogg 2008-08-26 to 2014-06-30
        dict(unique_id="Hogg1949", name_id="7L6", role="PRESIDENT",
             from_date="2008-08-26", to_date="2014-06-30",
             xml_name_pattern="The PRESIDENT", state="QLD", party="ALP",
             display_name="Senator Hogg",
             full_name="Senator the Hon. John Hogg"),

        # Stephen Parry 2014-07-07 to 2017-11-02
        dict(unique_id="Parry1960", name_id="E5V", role="PRESIDENT",
             from_date="2014-07-07", to_date="2017-11-02",
             xml_name_pattern="The PRESIDENT", state="TAS", party="LP",
             display_name="Senator Parry",
             full_name="Senator the Hon. Stephen Parry"),

        # Scott Ryan 2017-11-13 to 2021-10-13
        dict(unique_id="Ryan1973", name_id="I0Q", role="PRESIDENT",
             from_date="2017-11-13", to_date="2021-10-13",
             xml_name_pattern="The PRESIDENT", state="VIC", party="LP",
             display_name="Senator Ryan",
             full_name="Senator the Hon. Scott Ryan"),

        # Slade Brockman 2021-10-18 to 2022-07-26
        dict(unique_id="Brockman1970", name_id="30484", role="PRESIDENT",
             from_date="2021-10-18", to_date="2022-07-26",
             xml_name_pattern="The PRESIDENT", state="WA", party="LP",
             display_name="Senator Brockman",
             full_name="Senator Slade Brockman"),

        # Sue Lines 2022-07-26 to present
        dict(unique_id="Lines1953", name_id="112096", role="PRESIDENT",
             from_date="2022-07-26", to_date=None,
             xml_name_pattern="The PRESIDENT", state="WA", party="ALP",
             display_name="Senator Lines",
             full_name="Senator the Hon. Sue Lines"),
        # Madam President variant for Sue Lines
        dict(unique_id="Lines1953", name_id="112096", role="PRESIDENT",
             from_date="2022-07-26", to_date=None,
             xml_name_pattern="MADAM PRESIDENT", state="WA", party="ALP",
             display_name="Senator Lines",
             full_name="Senator the Hon. Sue Lines"),

        # ── Deputy Presidents ─────────────────────────────────────────────────
        # Sue West 1997-05-06 to 2002-06-30
        dict(unique_id="West1947", name_id="1X4", role="DEPUTY_PRESIDENT",
             from_date="1997-05-06", to_date="2002-06-30",
             xml_name_pattern="The DEPUTY PRESIDENT", state="NSW", party="ALP",
             display_name="Senator West",
             full_name="Senator Sue West"),

        # John Hogg 2002-08-19 to 2008-08-25
        dict(unique_id="Hogg1949", name_id="7L6", role="DEPUTY_PRESIDENT",
             from_date="2002-08-19", to_date="2008-08-25",
             xml_name_pattern="The DEPUTY PRESIDENT", state="QLD", party="ALP",
             display_name="Senator Hogg",
             full_name="Senator John Hogg"),

        # Alan Ferguson 2008-08-26 to 2011-06-30
        dict(unique_id="Ferguson1943", name_id="EP5", role="DEPUTY_PRESIDENT",
             from_date="2008-08-26", to_date="2011-06-30",
             xml_name_pattern="The DEPUTY PRESIDENT", state="SA", party="LP",
             display_name="Senator Ferguson",
             full_name="Senator Alan Ferguson"),

        # Stephen Parry 2011-07-04 to 2014-07-06
        dict(unique_id="Parry1960", name_id="E5V", role="DEPUTY_PRESIDENT",
             from_date="2011-07-04", to_date="2014-07-06",
             xml_name_pattern="The DEPUTY PRESIDENT", state="TAS", party="LP",
             display_name="Senator Parry",
             full_name="Senator Stephen Parry"),

        # Gavin Marshall 2014-07-07 to 2016-05-09
        dict(unique_id="Marshall1960", name_id="00AOP", role="DEPUTY_PRESIDENT",
             from_date="2014-07-07", to_date="2016-05-09",
             xml_name_pattern="The DEPUTY PRESIDENT", state="VIC", party="ALP",
             display_name="Senator Marshall",
             full_name="Senator Gavin Marshall"),

        # Sue Lines 2016-09-30 to 2022-07-26
        dict(unique_id="Lines1953", name_id="112096", role="DEPUTY_PRESIDENT",
             from_date="2016-09-30", to_date="2022-07-26",
             xml_name_pattern="The DEPUTY PRESIDENT", state="WA", party="ALP",
             display_name="Senator Lines",
             full_name="Senator Sue Lines"),

        # Andrew McLachlan 2022-07-26 to 2025-07-22
        # Note: PHID unknown in available sources; placeholder used
        dict(unique_id="McLachlan1966", name_id=None, role="DEPUTY_PRESIDENT",
             from_date="2022-07-26", to_date="2025-07-22",
             xml_name_pattern="The DEPUTY PRESIDENT", state="SA", party="LP",
             display_name="Senator McLachlan",
             full_name="Senator Andrew McLachlan"),

        # Slade Brockman 2025-07-22 to present
        dict(unique_id="Brockman1970", name_id="30484", role="DEPUTY_PRESIDENT",
             from_date="2025-07-22", to_date=None,
             xml_name_pattern="The DEPUTY PRESIDENT", state="WA", party="LP",
             display_name="Senator Brockman",
             full_name="Senator Slade Brockman"),

        # ── Generic presiding-officer patterns (date-independent) ─────────────
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The PRESIDENT", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The DEPUTY PRESIDENT", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The ACTING PRESIDENT", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The ACTING DEPUTY PRESIDENT", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The CHAIR", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="The TEMPORARY CHAIR OF COMMITTEES", state=None, party=None,
             display_name=None, full_name=None),
        dict(unique_id=None, name_id="10000", role="PRESIDING_OFFICER",
             from_date=None, to_date=None,
             xml_name_pattern="MADAM PRESIDENT", state=None, party=None,
             display_name=None, full_name=None),
    ]
    df = pd.DataFrame(rows)
    df["from_date"] = pd.to_datetime(df["from_date"], errors="coerce").dt.date
    df["to_date"] = pd.to_datetime(df["to_date"], errors="coerce").dt.date
    return df


# ── Build partyfacts_map ──────────────────────────────────────────────────────

def build_partyfacts_map() -> pd.DataFrame:
    """
    Return the PartyFacts ID mapping for Australian Senate parties.
    Based on the Katz/Alexander PartyFacts_map.csv, extended for Senate parties.
    PartyFacts IDs verified against https://partyfacts.herokuapp.com/data/
    (core dataset, filtered for Australia, accessed 2026-03-09).
    NULL = genuinely absent from the PartyFacts database.
    """
    rows = [
        dict(party_abbrev="ALP",    party_name="Australian Labor Party",                    partyfacts_id=1383),
        dict(party_abbrev="LP",     party_name="Liberal Party of Australia",                 partyfacts_id=1388),
        dict(party_abbrev="NATS",   party_name="The Nationals",                              partyfacts_id=1387),
        dict(party_abbrev="NP",     party_name="National Party of Australia",                partyfacts_id=1387),
        dict(party_abbrev="CP",     party_name="Australian Country Party",                   partyfacts_id=1387),
        dict(party_abbrev="AG",     party_name="Australian Greens",                          partyfacts_id=1386),
        dict(party_abbrev="GRN",    party_name="Australian Greens",                          partyfacts_id=1386),
        dict(party_abbrev="AD",     party_name="Australian Democrats",                       partyfacts_id=1384),
        dict(party_abbrev="CLP",    party_name="Country Liberal Party",                      partyfacts_id=143),
        dict(party_abbrev="LNP",    party_name="Liberal National Party of Queensland",       partyfacts_id=285),
        dict(party_abbrev="NatsWA", party_name="Nationals WA",                               partyfacts_id=None),
        dict(party_abbrev="IND",    party_name="Independent",                                partyfacts_id=None),
        dict(party_abbrev="UAP",    party_name="United Australia Party",                     partyfacts_id=1996),
        dict(party_abbrev="PHON",   party_name="Pauline Hanson's One Nation",                partyfacts_id=1162),
        dict(party_abbrev="JLN",    party_name="Jacqui Lambie Network",                      partyfacts_id=None),
        dict(party_abbrev="PRES",   party_name="(Senate President — non-party role)",        partyfacts_id=None),
        dict(party_abbrev="DPRES",  party_name="(Senate Deputy President — non-party role)", partyfacts_id=None),
        dict(party_abbrev="KAP",    party_name="Katter's Australian Party",                  partyfacts_id=1997),
        dict(party_abbrev="NXT",    party_name="Nick Xenophon Team",                         partyfacts_id=5453),
        dict(party_abbrev="CA",     party_name="Centre Alliance",                            partyfacts_id=5453),
        dict(party_abbrev="PUP",    party_name="Palmer United Party",                        partyfacts_id=1996),
        dict(party_abbrev="AV",     party_name="Australia's Voice",                          partyfacts_id=None),
        dict(party_abbrev="DLP",    party_name="Democratic Labor Party",                     partyfacts_id=1540),
        # Additional parties observed in corpus (not in original K&A map)
        dict(party_abbrev="GWA",    party_name="The Greens (WA)",                            partyfacts_id=1209),
        dict(party_abbrev="LDP",    party_name="Liberal Democratic Party",                   partyfacts_id=9149),
        dict(party_abbrev="FFP",    party_name="Family First Party",                         partyfacts_id=1263),
        dict(party_abbrev="TG",     party_name="The Greens (Tasmania)",                      partyfacts_id=1209),
        dict(party_abbrev="AUS",    party_name="Katter's Australian Party (pre-registration)",partyfacts_id=1997),
    ]
    return pd.DataFrame(rows)


# ── Patches: corrections to upstream data ────────────────────────────────────
#
# These correct known errors or gaps in the RohanAlexander/australian_politicians
# and OpenAustralia source data.  Each function is applied after the initial
# build so that re-running 02_member_lookup.py always produces the correct output.

def patch_senator_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply known name_id corrections to senator_lookup.

    These senators had missing or wrong APH IDs (PHIDs) in the upstream
    AustralianPoliticians data.  Verified against APH biographical pages.

    Gender is no longer patched here — it is filled automatically via the
    Wikidata SPARQL query in fill_gender_wikidata().
    """
    # ── name_id corrections ────────────────────────────────────────────────
    name_id_fixes = {
        "Thorpe1973":    "280304",  # Lidia Thorpe — missing in upstream
        "Patrick1967":   "144292",  # Rex Patrick — was 114292 (digit transposition)
        "Small1988":     "291406",  # Benjamin Small — missing in upstream
        "Cox1970":       "296215",  # Dorinda Cox — missing in upstream
        "Green1981":     "259819",  # Nita Green — missing in upstream
        "McLachlan1966": "287062",  # Andrew McLachlan — missing in upstream
    }
    for uid, nid in name_id_fixes.items():
        mask = (df["unique_id"] == uid) & df["name_id"].isna()
        df.loc[mask, "name_id"] = nid

    # Grogan unique_id is stored as bare "Grogan" in AP data (no birth year)
    grogan_mask = (df["surname"] == "Grogan") & (df["first_name"] == "Karen") & df["name_id"].isna()
    df.loc[grogan_mask, "name_id"] = "296331"

    # McLachlan1966: remove any residual null-name_id duplicate rows (AP data
    # may produce one row with NaN name_id; our fix above sets it to 287062,
    # but if multiple null rows exist for any reason, drop the extras)
    mclachlan_null = (df["unique_id"] == "McLachlan1966") & df["name_id"].isna()
    df = df[~mclachlan_null].copy()

    return df


def patch_party_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply known corrections to party_lookup.

    Len Harris (name_id 8HC) — party change PHON → IND
    ---------------------------------------------------
    Harris was elected as a Pauline Hanson's One Nation senator in 1998.
    He resigned from PHON and became an independent on or around 1999-08-09
    (first date he appears as IND in the Hansard XML without a PHON row).
    The upstream AustralianPoliticians data records him as PHON with no
    date range, which causes T4 validation failures for 1999–2002 dates.
    Replace with two date-ranged entries.
    """
    # Remove the open-ended PHON entry for Harris
    harris_open = (df["name_id"] == "8HC") & df["party_from"].isna() & df["party_to"].isna()
    df = df[~harris_open].copy()

    # Add date-ranged entries
    new_rows = pd.DataFrame([
        {
            "unique_id": "Harris1943", "name_id": "8HC",
            "party_abbrev": "PHON", "party_name": "Pauline Hansons One Nation",
            "party_from": "1998-07-01", "party_to": "1999-08-08",
            "party_changed_name": None, "party_simplified_name": "Pauline Hansons One Nation",
        },
        {
            "unique_id": "Harris1943", "name_id": "8HC",
            "party_abbrev": "IND", "party_name": "Independent",
            "party_from": "1999-08-09", "party_to": None,
            "party_changed_name": None, "party_simplified_name": "Independent",
        },
    ])
    return pd.concat([df, new_rows], ignore_index=True)


def patch_state_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply known corrections to state_lookup.

    Andrew Bartlett (name_id DT6) — second Senate term
    ---------------------------------------------------
    Bartlett's first term (1997–2008) is in the upstream data.  He was
    re-elected in 2017 to fill Scott Ludlam's vacancy (dual citizenship
    crisis) and served until June 2019.  The upstream data does not include
    this second term, causing T7 validation failures for 2017–2019 dates.
    """
    already = (
        (df["unique_id"] == "Bartlett1964") &
        (df["senator_from"].astype(str) == "2017-11-09")
    )
    if not already.any():
        new_row = pd.DataFrame([{
            "unique_id": "Bartlett1964", "name_id": "DT6",
            "state_abbrev": "QLD",
            "senator_from": "2017-11-09", "senator_to": "2019-06-30",
            "senator_end_reason": "Defeated", "sec15_sel": None,
        }])
        df = pd.concat([df, new_row], ignore_index=True)
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Build senator member lookup tables."
    )
    parser.add_argument("--out", default="../data/lookup", help="Output directory")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Download AustralianPoliticians data ───────────────────────────────────
    print("Loading AustralianPoliticians data...")
    all_df = fetch_csv(AP_URLS["all"])
    senators_df = fetch_csv(AP_URLS["senators"])
    parties_df = fetch_csv(AP_URLS["parties"])
    phid_df = fetch_csv(AP_URLS["phid"])

    # Ensure column name consistency
    all_df = all_df.rename(columns=lambda c: c.strip())
    senators_df = senators_df.rename(columns=lambda c: c.strip())
    parties_df = parties_df.rename(columns=lambda c: c.strip())
    phid_df = phid_df.rename(columns=lambda c: c.strip())

    # ── Download OpenAustralia data ───────────────────────────────────────────
    print("Loading OpenAustralia data...")
    oa_senators = fetch_csv(OA_URLS["senators"], comment="#", header=0)
    oa_people = fetch_csv(OA_URLS["people"], header=0)

    # ── senator_lookup ────────────────────────────────────────────────────────
    print("Building senator_lookup...")
    senator_lookup = build_senator_lookup(all_df, senators_df, phid_df)
    senator_lookup = append_oa_senators(senator_lookup, oa_senators, oa_people)
    senator_lookup = patch_senator_lookup(senator_lookup)
    senator_lookup = fill_gender_wikidata(senator_lookup)

    out_path = out_dir / "senator_lookup.csv"
    senator_lookup.to_csv(out_path, index=False)
    print(f"  Saved {len(senator_lookup)} rows → {out_path}")

    # ── state_lookup ──────────────────────────────────────────────────────────
    print("Building state_lookup...")
    senator_ids = set(senators_df["uniqueID"].dropna())
    state_lookup = build_state_lookup(senators_df, phid_df)
    state_lookup = patch_state_lookup(state_lookup)
    out_path = out_dir / "state_lookup.csv"
    state_lookup.to_csv(out_path, index=False)
    print(f"  Saved {len(state_lookup)} rows → {out_path}")

    # ── party_lookup ──────────────────────────────────────────────────────────
    print("Building party_lookup...")
    party_lookup = build_party_lookup(parties_df, phid_df, senator_ids)
    party_lookup = patch_party_lookup(party_lookup)
    out_path = out_dir / "party_lookup.csv"
    party_lookup.to_csv(out_path, index=False)
    print(f"  Saved {len(party_lookup)} rows → {out_path}")

    # ── president_lookup ──────────────────────────────────────────────────────
    print("Building president_lookup...")
    president_lookup = build_president_lookup()
    out_path = out_dir / "president_lookup.csv"
    president_lookup.to_csv(out_path, index=False)
    print(f"  Saved {len(president_lookup)} rows → {out_path}")

    # ── partyfacts_map ────────────────────────────────────────────────────────
    print("Building partyfacts_map...")
    pf_map = build_partyfacts_map()
    out_path = out_dir / "partyfacts_map.csv"
    pf_map.to_csv(out_path, index=False)
    print(f"  Saved {len(pf_map)} rows → {out_path}")

    print("\nAll lookup tables built successfully.")
    null_gender = senator_lookup[senator_lookup["gender"].isna()]
    if len(null_gender):
        print(f"\n[NOTE] {len(null_gender)} senator(s) still have NULL gender after Wikidata fill.")
        print(null_gender[["unique_id", "first_name", "surname", "source"]].to_string(index=False))


if __name__ == "__main__":
    main()
