"""
05b_validate_house.py — Automated validation tests for House of Representatives Hansard.

Adapted from 05_validate.py (Senate). Key differences:
  - No `state` column — House has `electorate` instead
  - No `senate_flag` — House has `fedchamb_flag`
  - T4 checks `electorate` instead of `state`
  - T5 uses member_lookup.csv (name_id column)
  - T6 uses birth_date/death_date from member_lookup.csv
  - T7 uses member_from/member_to from electorate_lookup.csv (one row per term)

Tests run:
  T1. Date in filename matches session_info_all.csv date
  T2. No two consecutive non-interjection rows have identical body text
  T3. Body containing "(Time expired)" is not immediately followed by more text
  T4. Each name_id has only one party and one electorate per sitting day
  T5. All name_ids exist in member_lookup.csv (or are known special IDs)
  T6. All unique_ids have birth dates before and death dates after sitting day
  T7. All unique_ids were House members on the sitting day (via electorate_lookup term dates)

Usage:
    python 05b_validate_house.py
        --daily-dir ../data/output/house/daily
        --lookup-dir ../data/lookup
        --session-info ../data/lookup/session_info_all.csv
        --error-log ../data/output/house/validation_errors.csv
        [--sequential]       # disable parallelism entirely
"""

import argparse
import logging
import re
from datetime import date
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from parallel_utils import eager_map

log = logging.getLogger(__name__)


# ── Placeholder/generic name_ids excluded from T5 (mirrors R script filter) ──
# Covers all forms seen in the XML: "10000", "1000", "0000", "UNKNOWN",
# "110000", "1010000", "10001", "2210000".  "20000" kept for backward-compat.
_T5_PLACEHOLDER_IDS = {
    "10000", "10001", "1000", "0000", "UNKNOWN", "110000", "1010000", "2210000",
}
# Broader set still used in T4 (covers presiding-officer placeholder rows).
SPECIAL_IDS = {"10000", "10001", "UNKNOWN", "20000"}

# ── Party abbreviation aliases ─────────────────────────────────────────────────
# All Liberal/National coalition variants map to "COAL" so that LP, LNP, NP, NAT
# etc. on the same sitting day don't trigger false T4 failures.  The Queensland
# LNP in particular appears under both LNP and LP/NP in the XML depending on
# which XML variant tagged the speaker.  Real cross-coalition changes (e.g.
# coalition → IND, ALP → IND) are still caught because they map to distinct keys.
PARTY_ALIASES: dict[str, str] = {
    # Coalition parties
    "LP":      "COAL",
    "LIB":     "COAL",
    "LNP":     "COAL",
    "NP":      "COAL",
    "NPA":     "COAL",
    "NAT":     "COAL",
    "NATS":    "COAL",
    "Nats":    "COAL",
    "NATS WA": "COAL",
    "NatsWA":  "COAL",
    "CLP":     "COAL",
    # Greens
    "AG":      "GRN",
    "TG":      "GRN",
    "G(WA)":   "GWA",
    # Others
    "ON":      "PHON",
    "Ind.":    "IND",
    "AUS":     "KAP",
}


# ── Individual tests ──────────────────────────────────────────────────────────

def test_1_date_matches_session(date_str: str,
                                session_info: pd.DataFrame) -> list[str]:
    """Date in filename matches session_info_all.csv date."""
    row = session_info[session_info["filename"] == date_str]
    if row.empty:
        return [f"T1: {date_str} not found in session_info_all.csv"]
    header_date = str(row.iloc[0].get("date", "")).strip()
    if header_date != date_str:
        return [f"T1: filename date {date_str} != session.header date {header_date}"]
    return []


# Phrases that legitimately appear on consecutive rows in the XML (procedural
# repetitions, group/named interjections, bill reading formulas).  Consecutive
# duplicates matching this pattern are NOT flagged by T2.
_T2_PROCEDURAL_RE = re.compile(
    r"interjecting|"                        # any "X interjecting—" form
    r"^to move[:\-—]|"                      # procedural motion prefix
    r"^by leave[—\-]|"                      # by-leave motion prefix
    r"—by leave—|"                          # em-dash by-leave prefix
    r"^to present a bill|"                  # bill presentation
    r"^I move:|"                            # motion prefix
    r"^That this bill be now read|"         # standard bill third-reading formula
    r"^That the \w[\w\s]+ bill be now read|"
    r"^That the bill be now read|"
    r"^That the amendments be agreed|"
    r"^Is leave granted|"
    r"^Leave not granted|"
    r"^Leave granted|"
    r"^Order[!.]?$|"
    r"^\(Time expired\)|"
    r"^Question |"                          # Question agreed to / negatived / etc.
    r"^Votes and Proceedings|"
    r"^That the Member be no longer heard|"
    r"^Document made a parliamentary paper|"
    r"^There is no point of order|"
    r"^An incident having occurred|"
    r"^The (?:PRESIDENT|SPEAKER|CHAIR|DEPUTY)|"  # presiding officer lines
    r"^Mr Speaker[—\-]?$|"                 # interjection-form presiding address
    r"^(?:Honourable|Opposition|Government) (?:members|senators):|"  # group responses
    r"^Yes\.$|^No\.$|"                     # short affirmation/negation
    r"^\d{2}:\d{2}:\d{2}|"                 # timestamp prefix (division records)
    r"^—I move:|"                           # em-dash variant of motion prefix
    r"^Notice Paper|"                       # procedural notice paper
    r"^Ordered that the report be made|"
    r"^The question is that the bill be now read|"
    r"^\(\s*\)\s*\(",                       # parsing artifact "( ) ("
    re.I,
)


def test_2_no_consecutive_duplicates(df: pd.DataFrame,
                                     date_str: str) -> list[str]:
    """No two consecutive non-interjection rows have identical body.

    Procedural phrases that legitimately repeat on consecutive rows
    (interjections, bill reading formulas, motion prefixes) are excluded
    via _T2_PROCEDURAL_RE.
    """
    non_int = df[df["interject"] == 0]["body"].reset_index(drop=True)
    dup_mask = (non_int == non_int.shift()) & non_int.notna() & (non_int != "")
    dup_mask &= ~non_int.str.contains(_T2_PROCEDURAL_RE, na=False)
    if dup_mask.any():
        examples = non_int[dup_mask].head(3).tolist()
        return [
            f"T2: {date_str}: {dup_mask.sum()} consecutive duplicate body rows: "
            f"{examples}"
        ]
    return []


_T3_PROCEDURAL_RE = re.compile(
    r"^(Question (resolved|negatived|agreed to|put)|"
    r"Honourable (senators|members) interjecting|"
    r"Opposition (senators|members) interjecting|"
    r"Government (senators|members) interjecting|"
    r"The (PRESIDENT|DEPUTY PRESIDENT|SPEAKER|DEPUTY SPEAKER|CHAIR|ACTING DEPUTY)|"
    r"Senator \w+ interjecting|"
    r"Mr \w+ interjecting|"
    r"Ms \w+ interjecting|"
    r"Leave granted|"
    r"Sitting suspended|"
    r"Debate adjourned|"
    r"House adjourned|"
    r"Senate adjourned|"
    r"Bill read a|"
    r"Proposed expenditure|"
    r"Motion \(by|"
    r"That the|"
    r"Consideration interrupted|"
    r"A division having been|"
    r"Debate \(on motion)",
    re.I,
)


def test_3_time_expired(df: pd.DataFrame, date_str: str) -> list[str]:
    """'(Time expired)' in body should not be followed by more substantive speech text.

    Procedural text (interjections, speaker remarks, procedural motions) is
    allowed to follow — these represent normal parliamentary proceedings that
    appear in the same XML text block.
    """
    errors = []
    te_rows = df[df["body"].str.contains(r"\(Time expired\)", na=False, regex=True)]
    for _, row in te_rows.iterrows():
        body = str(row["body"])
        pos = body.find("(Time expired)")
        remainder = body[pos + len("(Time expired)"):].strip()
        if len(remainder) <= 20:
            continue
        if _T3_PROCEDURAL_RE.match(remainder):
            continue
        errors.append(
            f"T3: {date_str} row {row.get('order')}: text after '(Time expired)': "
            f"'{remainder[:50]}'"
        )
    return errors


def test_4_one_party_per_name_id(df: pd.DataFrame, date_str: str) -> list[str]:
    """Each name_id should have only one party and one electorate on this sitting day.

    Implementation notes
    --------------------
    * Interjection rows (interject == 1) are excluded — the parser assigns the
      current speaker's party to interjection rows, so the interjecter's name_id
      appears with the wrong party.  This is a known pipeline artifact.
    * Party abbreviations are normalised via PARTY_ALIASES before counting
      distinct values to avoid false failures from alternate XML abbreviations.
      All Liberal/National coalition variants (LP, LIB, LNP, NP, NAT, NATS …)
      are collapsed to "COAL" so QLD LNP dual-tagging does not produce failures.
    """
    errors = []
    valid = df[
        df["name_id"].notna() &
        ~df["name_id"].isin(SPECIAL_IDS) &
        (df["interject"] == 0)
    ].copy()

    valid["_party_norm"] = valid["party"].map(
        lambda p: PARTY_ALIASES.get(p, p) if pd.notna(p) else p
    )

    multi_party = (
        valid.groupby("name_id")["_party_norm"]
        .nunique(dropna=True)
        .pipe(lambda s: s[s > 1])
    )
    if not multi_party.empty:
        errors.append(
            f"T4: {date_str}: multiple parties per name_id: "
            f"{multi_party.index.tolist()}"
        )

    if "electorate" in df.columns:
        multi_elec = (
            valid.groupby("name_id")["electorate"]
            .nunique(dropna=True)
            .pipe(lambda s: s[s > 1])
        )
        if not multi_elec.empty:
            errors.append(
                f"T4: {date_str}: multiple electorates per name_id: "
                f"{multi_elec.index.tolist()}"
            )
    else:
        errors.append(f"T4: {date_str}: WARNING — 'electorate' column not found in data")

    return errors


def test_5_known_name_ids(df: pd.DataFrame, date_str: str,
                          known_ids: set) -> list[str]:
    """name_ids that are not in member_lookup and not placeholder IDs are flagged,
    excluding rows that are clearly not House members (Senators appearing in
    cross-chamber references, foreign dignitaries, Governor-General, etc.).

    The original R-style "MP" substring filter was replaced because after name
    normalisation (06c) names no longer carry an "MP" suffix — the filter
    was matching 'Mr KEMP', 'Mr CHAMPION', etc. instead of MPs.
    """
    # Work on (name, name_id) pairs; drop rows without a name_id.
    pairs = df[df["name_id"].notna()][["name", "name_id"]].drop_duplicates()

    # Exclude presiding-officer rows (name ends with "SPEAKER").
    pairs = pairs[~pairs["name"].str.contains(r"SPEAKER$", na=False, regex=True)]

    # Exclude placeholder / generic IDs.
    pairs = pairs[~pairs["name_id"].isin(_T5_PLACEHOLDER_IDS)]

    # Exclude rows that are clearly non-House-member speakers:
    #   • Senators in cross-chamber references (name starts with "Senator")
    #   • Foreign heads of state / dignitaries (name contains "EXCELLENCY")
    #   • Commonwealth dignitaries ("Rt Hon.", "The HONOURABLE", "GOVERNOR")
    pairs = pairs[
        ~pairs["name"].str.startswith("Senator ", na=False) &
        ~pairs["name"].str.contains("EXCELLENCY", na=False, regex=False) &
        ~pairs["name"].str.startswith("Rt Hon.", na=False) &
        ~pairs["name"].str.contains("GOVERNOR", na=False, regex=False)
    ]

    # Flag any remaining name_id not in the valid set.
    unknown = set(pairs["name_id"].unique()) - known_ids
    if unknown:
        return [f"T5: {date_str}: unknown name_ids: {sorted(unknown)[:20]}"]
    return []


def test_6_birth_death_dates(df: pd.DataFrame, date_str: str,
                              member_bio: pd.DataFrame) -> list[str]:
    """Rows fail T6 if the sitting date is before the member's birth date, or
    after their death date (when known).

    Mirrors R pass_test logic:
      pass if no death_date AND date >= birth_date
      pass if death_date known AND birth_date <= date <= death_date
      fail otherwise (including date < birth_date, or date > death_date)
      skip entirely if birth_date is NULL
    """
    errors = []
    sitting = pd.to_datetime(date_str).date()

    # One row per unique_id present in this daily file.
    uids = df[df["unique_id"].notna()]["unique_id"].drop_duplicates()

    # Vectorised join: left join the daily unique_ids onto member_bio.
    merged = uids.to_frame().merge(
        member_bio[["unique_id", "birth_date", "death_date"]],
        on="unique_id",
        how="left",
    )

    # Skip rows where birth_date is NULL (don't penalise missing data).
    merged = merged[merged["birth_date"].notna()]
    if merged.empty:
        return []

    merged["_birth"] = pd.to_datetime(merged["birth_date"], errors="coerce").dt.date
    merged["_death"] = pd.to_datetime(merged["death_date"], errors="coerce").dt.date

    for _, row in merged.iterrows():
        uid   = row["unique_id"]
        birth = row["_birth"]
        death = row["_death"]

        if pd.isna(birth):   # unparseable birth date — skip
            continue

        # Determine pass/fail using R's combined logic.
        if pd.isna(death):
            passes = (sitting >= birth)
        else:
            passes = (sitting >= birth) and (sitting <= death)

        if not passes:
            # Build an informative message indicating which bound was violated.
            if sitting < birth:
                reason = f"birth date {birth} is after sitting date"
            else:
                reason = f"death date {death} is before sitting date"
            errors.append(f"T6: {date_str}: {uid} {reason}")

    return errors


def test_7_was_member_on_date(df: pd.DataFrame, date_str: str,
                               electorate_lookup: pd.DataFrame) -> list[str]:
    """All unique_ids should have had at least one active term covering the
    sitting date.  A member may have multiple terms (multiple rows in
    electorate_lookup); the test passes if ANY term covers the date.

    Mirrors R:
      left_join on unique_id (expands to one row per term)
      filter(!is.na(mpFrom))   ← skip if no term data at all
      pass_test per term: date>=mpFrom (open-ended) or mpFrom<=date<=mpTo
      group_by uniqueID: pass if any term passes
      flag uniqueIDs where no term passes
    """
    errors = []
    sitting = pd.to_datetime(date_str).date()

    # One row per unique_id present in this daily file.
    uids = df[df["unique_id"].notna()]["unique_id"].drop_duplicates()

    # Expand: left join onto electorate_lookup (may produce multiple rows per uid).
    el = electorate_lookup[["unique_id", "member_from", "member_to"]].copy()
    el["member_from"] = pd.to_datetime(el["member_from"], errors="coerce").dt.date
    el["member_to"]   = pd.to_datetime(el["member_to"],   errors="coerce").dt.date

    merged = uids.to_frame().merge(el, on="unique_id", how="left")

    # Skip rows where member_from is NULL (no term data — mirrors R filter).
    merged = merged[merged["member_from"].notna()]
    if merged.empty:
        return []

    # Per-term pass test.
    def _term_passes(row) -> bool:
        if pd.isna(row["member_to"]):
            return sitting >= row["member_from"]          # still serving
        return (sitting >= row["member_from"]) and (sitting <= row["member_to"])

    merged["_passes"] = merged.apply(_term_passes, axis=1)

    # Group by unique_id: flag if NO term passes.
    fails = (
        merged.groupby("unique_id")["_passes"]
        .any()
        .pipe(lambda s: s[~s])          # keep unique_ids where any(passes)==False
        .index.tolist()
    )

    for uid in fails:
        terms = merged[merged["unique_id"] == uid][["member_from", "member_to"]]
        errors.append(
            f"T7: {date_str}: {uid} was not a House member on {date_str} "
            f"(terms: {terms.to_dict('records')})"
        )
    return errors


# ── Per-file validation ───────────────────────────────────────────────────────

def validate_file(date_str: str, df: pd.DataFrame,
                  session_info: pd.DataFrame,
                  lookups: dict) -> list[str]:
    """Run T1–T7 on one daily House file. Returns list of error strings."""
    errors = []
    errors.extend(test_1_date_matches_session(date_str, session_info))
    errors.extend(test_2_no_consecutive_duplicates(df, date_str))
    errors.extend(test_3_time_expired(df, date_str))
    errors.extend(test_4_one_party_per_name_id(df, date_str))

    # T5: valid set is the raw name_id values from member_lookup (no uppercasing —
    #     the R script compares against ausPH$PHID as-is).
    known_ids = set(lookups["member"]["name_id"].dropna().unique())
    errors.extend(test_5_known_name_ids(df, date_str, known_ids))
    errors.extend(test_6_birth_death_dates(df, date_str, lookups["member"]))
    # T7: uses electorate_lookup (one row per term per member — the per-term
    #     table that carries member_from/member_to, equivalent to R's `mps`).
    errors.extend(test_7_was_member_on_date(df, date_str, lookups["electorate"]))

    return errors


# ── Worker functions (module-level for pickle compatibility) ──────────────────

# Global state loaded once per worker process via the pool initializer.
_g_session_info: pd.DataFrame | None = None
_g_lookups: dict | None = None


def _worker_init(session_info_path: str, lookup_dir: str) -> None:
    """Load shared lookup data once per worker process."""
    global _g_session_info, _g_lookups
    _g_session_info = pd.read_csv(session_info_path, dtype=str)
    ldir = Path(lookup_dir)
    _g_lookups = {
        "member":      pd.read_csv(ldir / "member_lookup.csv",     dtype=str),
        "electorate":  pd.read_csv(ldir / "electorate_lookup.csv", dtype=str),
    }


def _validate_worker(pq_path_str: str) -> tuple[str, list[str]]:
    """Validate one daily House parquet file. Runs inside a worker process."""
    pq_path = Path(pq_path_str)
    date_str = pq_path.stem
    df = pd.read_parquet(pq_path)
    errors = validate_file(date_str, df, _g_session_info, _g_lookups)
    return date_str, errors


# ── Main validation runner ────────────────────────────────────────────────────

def validate_all(daily_dir: Path,
                 session_info_path: Path,
                 lookup_dir: Path | None = None,
                 error_log: Path | None = None,
                 sequential: bool = False) -> dict:
    """Validate all daily House files. Returns summary dict."""
    parquet_files = sorted(daily_dir.glob("*.parquet"))
    print(f"Validating {len(parquet_files)} daily House files")

    # Resolve lookup_dir: default to session_info's parent directory.
    if lookup_dir is None:
        lookup_dir = session_info_path.parent

    all_errors: dict[str, list[str]] = {}
    n_clean = 0

    if sequential or not parquet_files:
        # ── Sequential fallback ───────────────────────────────────────────────
        session_info = pd.read_csv(session_info_path, dtype=str)
        lookups = {
            "member":     pd.read_csv(lookup_dir / "member_lookup.csv",     dtype=str),
            "electorate": pd.read_csv(lookup_dir / "electorate_lookup.csv", dtype=str),
        }
        for pq_path in tqdm(parquet_files, desc="Validating"):
            date_str = pq_path.stem
            df = pd.read_parquet(pq_path)
            errors = validate_file(date_str, df, session_info, lookups)
            if errors:
                all_errors[date_str] = errors
            else:
                n_clean += 1

    else:
        # ── Parallel path ─────────────────────────────────────────────────────
        # Configure logging so worker-count changes appear on the console.
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(name)s] %(message)s",
            datefmt="%H:%M:%S",
        )

        items = [str(p) for p in parquet_files]
        results = eager_map(
            _validate_worker,
            items,
            initializer=_worker_init,
            initargs=(str(session_info_path), str(lookup_dir)),
        )

        for item in results:
            if item is None:
                log.warning("A worker returned None (see error log above)")
                continue
            date_str, errors = item
            if errors:
                all_errors[date_str] = errors
            else:
                n_clean += 1

    n_errors = len(all_errors)
    total_issues = sum(len(v) for v in all_errors.values())

    # ── Per-test breakdown ────────────────────────────────────────────────────
    all_error_strs = [e for errs in all_errors.values() for e in errs]
    counts = {}
    for tag in ("T1", "T2", "T3", "T4", "T5", "T6", "T7"):
        counts[tag] = sum(1 for e in all_error_strs if e.startswith(tag + ":"))

    print(f"\nValidation complete.")
    print(f"  Files total:       {len(parquet_files)}")
    print(f"  Clean:             {n_clean}")
    print(f"  Files with issues: {n_errors}")
    print(f"  Total issues:      {total_issues}")
    print(f"\n  By test:")
    for tag, n in counts.items():
        if n:
            print(f"    {tag}: {n} issue(s)")

    if all_errors:
        print("\nFiles with errors (first 20):")
        for date_str, errs in list(all_errors.items())[:20]:
            print(f"  {date_str}: {len(errs)} issue(s)")
            for e in errs:
                print(f"    - {e}")

    if error_log and all_errors:
        error_log.parent.mkdir(parents=True, exist_ok=True)
        rows = [
            {"date": d, "error": e}
            for d, errs in all_errors.items()
            for e in errs
        ]
        pd.DataFrame(rows).to_csv(error_log, index=False)
        print(f"\nError log saved → {error_log}")

    return {
        "n_clean":         n_clean,
        "n_with_errors":   n_errors,
        "total_issues":    total_issues,
        "errors":          all_errors,
        "counts_by_test":  counts,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Validate parsed House of Representatives Hansard daily files."
    )
    parser.add_argument(
        "--daily-dir",
        default="../data/output/house/daily",
        help="Directory containing daily .parquet files",
    )
    parser.add_argument(
        "--lookup-dir",
        default="../data/lookup",
        help="Directory containing member_lookup.csv and electorate_lookup.csv",
    )
    parser.add_argument(
        "--session-info",
        default="../data/lookup/session_info_all.csv",
        help="Path to session_info_all.csv (covers both chambers)",
    )
    parser.add_argument(
        "--error-log",
        default="../data/output/house/validation_errors.csv",
        help="CSV path to write error log",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Disable parallelism (single-process, useful for debugging)",
    )
    args = parser.parse_args()

    validate_all(
        Path(args.daily_dir),
        Path(args.session_info),
        lookup_dir=Path(args.lookup_dir),
        error_log=Path(args.error_log) if args.error_log else None,
        sequential=args.sequential,
    )


if __name__ == "__main__":
    main()
