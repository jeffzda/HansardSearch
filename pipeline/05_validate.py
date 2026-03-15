"""
05_validate.py — Seven automated validation tests (adapted from Katz & Alexander).

Tests:
  1. Date in filename matches session.header date
  2. No two consecutive rows have identical body text (excl. interjections)
  3. Body containing "(Time expired)" is not immediately followed by more text
  4. Each name_id has only one party and state per sitting day
  5. All name_ids exist in senator_lookup (or are known special IDs)
  6. All unique_ids have birth dates before and death dates after sitting day
  7. All unique_ids were senators on the sitting day (via state_lookup term dates)

Usage:
    python 05_validate.py --daily-dir ../data/output/senate/daily
                          --lookup-dir ../data/lookup
                          --session-info ../data/lookup/session_info_all.csv
                          [--sequential]       # disable parallelism
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


# ── Special name_ids that are always allowed ──────────────────────────────────
# "1" = The Clerk / Deputy Clerk (procedural role, not a senator)
SPECIAL_IDS = {"10000", "10001", "UNKNOWN", "20000", "1"}

# ── Party abbreviation aliases ─────────────────────────────────────────────────
# All Liberal/National coalition variants collapse to "COAL" — see House validator
# for full rationale.  Senate has fewer LNP cases but the same logic applies.
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
    """Date in filename matches session.header date."""
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
    r"^Pause the clock|"
    r"^That the senator be no longer heard|"
    r"^Document made a parliamentary paper|"
    r"^There is no point of order|"
    r"^An incident having occurred|"
    r"^The (?:PRESIDENT|SPEAKER|CHAIR|DEPUTY)|"  # presiding officer lines
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
        return [f"T2: {date_str}: {dup_mask.sum()} consecutive duplicate body rows: "
                f"{examples}"]
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

    Procedural text (interjections, president remarks, procedural motions) is
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
    """Each name_id should have only one party and state on this sitting day.

    Implementation notes
    --------------------
    * Interjection rows (interject == 1) are excluded.  The parser assigns the
      *speaker's* party to interjection rows, so the interjecter's name_id ends
      up paired with the wrong party — a pipeline artifact, not a real conflict.
    * Party abbreviations are normalised via PARTY_ALIASES before counting
      distinct values.  The raw XML uses several alternate forms (e.g. "LP" for
      the Liberal Party whose canonical abbreviation is "LIB") that would
      otherwise create spurious multi-party failures.
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

    multi_state = (
        valid.groupby("name_id")["state"]
        .nunique(dropna=True)
        .pipe(lambda s: s[s > 1])
    )
    if not multi_state.empty:
        errors.append(
            f"T4: {date_str}: multiple states per name_id: "
            f"{multi_state.index.tolist()}"
        )
    return errors


def test_5_known_name_ids(df: pd.DataFrame, date_str: str,
                          known_ids: set) -> list[str]:
    """All name_ids should be in senator_lookup or be special IDs."""
    seen = set(df["name_id"].dropna().unique())
    unknown = seen - known_ids - SPECIAL_IDS
    if unknown:
        return [f"T5: {date_str}: unknown name_ids: {sorted(unknown)[:20]}"]
    return []


def test_6_birth_death_dates(df: pd.DataFrame, date_str: str,
                              senator_bio: pd.DataFrame) -> list[str]:
    """All unique_ids should have birth before and death (if any) after sitting day."""
    errors = []
    sitting = pd.to_datetime(date_str).date()
    valid = df[df["unique_id"].notna()].drop_duplicates("unique_id")

    for _, row in valid.iterrows():
        uid = row["unique_id"]
        bio = senator_bio[senator_bio["unique_id"] == uid]
        if bio.empty:
            continue
        bio_row = bio.iloc[0]

        birth_raw = bio_row.get("birth_date")
        if pd.notna(birth_raw):
            try:
                birth = pd.to_datetime(birth_raw).date()
                if birth > sitting:
                    errors.append(
                        f"T6: {date_str}: {uid} birth date {birth} is after sitting date"
                    )
            except Exception:
                pass

        death_raw = bio_row.get("death_date")
        if pd.notna(death_raw):
            try:
                death = pd.to_datetime(death_raw).date()
                if death < sitting:
                    errors.append(
                        f"T6: {date_str}: {uid} death date {death} is before sitting date"
                    )
            except Exception:
                pass

    return errors


def test_7_was_senator_on_date(df: pd.DataFrame, date_str: str,
                                state_lookup: pd.DataFrame) -> list[str]:
    """All unique_ids should have been senators on this sitting day."""
    errors = []
    sitting = pd.to_datetime(date_str).date()

    valid = df[
        df["unique_id"].notna() &
        ~df["unique_id"].str.endswith("_OA_", na=False)
    ].drop_duplicates("unique_id")

    sl = state_lookup.copy()
    sl["senator_from"] = pd.to_datetime(sl["senator_from"], errors="coerce").dt.date
    sl["senator_to"]   = pd.to_datetime(sl["senator_to"],   errors="coerce").dt.date

    for _, row in valid.iterrows():
        uid = row["unique_id"]
        if str(row.get("name", "")) in ("business start", "stage direction"):
            continue

        terms = sl[sl["unique_id"] == uid]
        if terms.empty:
            continue

        on_date = terms[
            (terms["senator_from"].isna() | (terms["senator_from"] <= sitting)) &
            (terms["senator_to"].isna()   | (terms["senator_to"]   >= sitting))
        ]
        if on_date.empty:
            errors.append(
                f"T7: {date_str}: {uid} was not a senator on {date_str} "
                f"(terms: {terms[['senator_from','senator_to']].to_dict('records')})"
            )
    return errors


# ── Per-file validation ───────────────────────────────────────────────────────

def validate_file(date_str: str, df: pd.DataFrame,
                  session_info: pd.DataFrame,
                  lookups: dict) -> list[str]:
    """Run all 7 tests on one daily file. Returns list of error strings."""
    errors = []

    errors.extend(test_1_date_matches_session(date_str, session_info))
    errors.extend(test_2_no_consecutive_duplicates(df, date_str))
    errors.extend(test_3_time_expired(df, date_str))
    errors.extend(test_4_one_party_per_name_id(df, date_str))

    known_ids = set(lookups["senator"]["name_id"].dropna().str.upper().unique())
    errors.extend(test_5_known_name_ids(df, date_str, known_ids))
    errors.extend(test_6_birth_death_dates(df, date_str, lookups["senator"]))
    errors.extend(test_7_was_senator_on_date(df, date_str, lookups["state"]))

    return errors


# ── Worker functions (module-level for pickle compatibility) ──────────────────

# Global state loaded once per worker process via the pool initializer.
_g_session_info: pd.DataFrame | None = None
_g_lookups: dict | None = None


def _worker_init(session_info_path: str, lookup_dir: str) -> None:
    """Load shared lookup data once per worker process at startup."""
    global _g_session_info, _g_lookups
    _g_session_info = pd.read_csv(session_info_path, dtype=str)
    ldir = Path(lookup_dir)
    _g_lookups = {
        "senator": pd.read_csv(ldir / "senator_lookup.csv", dtype=str),
        "state":   pd.read_csv(ldir / "state_lookup.csv",   dtype=str),
        "party":   pd.read_csv(ldir / "party_lookup.csv",   dtype=str),
    }


def _validate_worker(pq_path_str: str) -> tuple[str, list[str]]:
    """Validate one daily Senate parquet file. Runs inside a worker process."""
    pq_path = Path(pq_path_str)
    date_str = pq_path.stem
    df = pd.read_parquet(pq_path)
    errors = validate_file(date_str, df, _g_session_info, _g_lookups)
    return date_str, errors


# ── Main validation runner ────────────────────────────────────────────────────

def validate_all(daily_dir: Path, lookup_dir: Path,
                 session_info_path: Path,
                 error_log: Path | None = None,
                 sequential: bool = False) -> dict:
    """Validate all daily Senate files. Returns summary dict."""
    parquet_files = sorted(daily_dir.glob("*.parquet"))
    print(f"Validating {len(parquet_files)} daily files")

    all_errors: dict[str, list[str]] = {}
    n_clean = 0

    if sequential or not parquet_files:
        # ── Sequential fallback ───────────────────────────────────────────────
        session_info = pd.read_csv(session_info_path, dtype=str)
        lookups = {
            "senator": pd.read_csv(lookup_dir / "senator_lookup.csv", dtype=str),
            "state":   pd.read_csv(lookup_dir / "state_lookup.csv",   dtype=str),
            "party":   pd.read_csv(lookup_dir / "party_lookup.csv",   dtype=str),
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

    print(f"\nValidation complete.")
    print(f"  Clean: {n_clean}")
    print(f"  Files with issues: {n_errors}")
    print(f"  Total issues: {total_issues}")

    if all_errors:
        print("\nFiles with errors (first 20):")
        for date_str, errs in list(all_errors.items())[:20]:
            print(f"  {date_str}: {len(errs)} issue(s)")
            for e in errs:
                print(f"    - {e}")

    if error_log and all_errors:
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
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Validate parsed Senate Hansard daily files."
    )
    parser.add_argument("--daily-dir",    default="../data/output/senate/daily")
    parser.add_argument("--lookup-dir",   default="../data/lookup")
    parser.add_argument("--session-info", default="../data/lookup/session_info_all.csv")
    parser.add_argument("--error-log",    default=None,
                        help="Optional CSV path to write error log")
    parser.add_argument("--sequential",   action="store_true",
                        help="Disable parallelism (single-process, useful for debugging)")
    args = parser.parse_args()

    validate_all(
        Path(args.daily_dir),
        Path(args.lookup_dir),
        Path(args.session_info),
        error_log=Path(args.error_log) if args.error_log else None,
        sequential=args.sequential,
    )


if __name__ == "__main__":
    main()
