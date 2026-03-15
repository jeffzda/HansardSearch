"""
webapp/app.py — Local Hansard search web application.

Run:
    cd /home/jeffzda/Hansard/webapp
    pip install flask pandas pyarrow
    python app.py

Then open http://localhost:5000
"""

import sys
import re
import io
import csv
import json
import time
import sqlite3
import secrets as _secrets
import concurrent.futures
from collections import Counter
from datetime import datetime, timezone, timedelta, date
from functools import wraps
from pathlib import Path

import pandas as pd
from flask import Flask, request, jsonify, send_from_directory, Response, session, redirect
from werkzeug.security import generate_password_hash, check_password_hash

_THREAD_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=2)

# ── Activity log ───────────────────────────────────────────────────────────────
_LOG_PATH = Path(__file__).parent / "activity_log.jsonl"

def _log(entry: dict):
    entry["ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        with _LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass

# Import search logic from pipeline
sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))
from search_corpus import (
    parse_expression,
    collect_terms,
    _build_masks,
    _eval_tree,
    _apply_filters,
)

app = Flask(__name__, static_folder="static")


@app.errorhandler(Exception)
def handle_any_exception(e):
    import traceback
    app.logger.error("Unhandled exception: %s\n%s", e, traceback.format_exc())
    if request.path.startswith("/api/"):
        return jsonify({"error": "Server error", "detail": str(e)}), 500
    from werkzeug.exceptions import HTTPException
    if isinstance(e, HTTPException):
        return e
    return jsonify({"error": "Server error"}), 500


@app.errorhandler(404)
def handle_404(e):
    if request.path.startswith("/api/"):
        return jsonify({"error": "Not found"}), 404
    return e


# ── Secret key ─────────────────────────────────────────────────────────────────
_SECRET_KEY_PATH = Path(__file__).parent / ".secret_key"
if _SECRET_KEY_PATH.exists():
    app.secret_key = _SECRET_KEY_PATH.read_text().strip()
else:
    _key = _secrets.token_hex(32)
    _SECRET_KEY_PATH.write_text(_key)
    app.secret_key = _key
app.permanent_session_lifetime = timedelta(days=30)

# ── User database ──────────────────────────────────────────────────────────────
_DB_PATH = Path(__file__).parent / "users.db"

def _init_db():
    with sqlite3.connect(_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                email       TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                expires_at  TEXT,
                label       TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS access_requests (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT NOT NULL,
                email        TEXT NOT NULL,
                organisation TEXT,
                reason       TEXT,
                requested_at TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending'
            )
        """)
        conn.commit()

_init_db()


def _get_user(user_id: int):
    with sqlite3.connect(_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user_id = session.get("user_id")
        if not user_id:
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required", "redirect": "/login"}), 401
            return redirect("/login")
        row = _get_user(user_id)
        if not row:
            session.clear()
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required", "redirect": "/login"}), 401
            return redirect("/login")
        if row["expires_at"]:
            expires = datetime.fromisoformat(row["expires_at"])
            if datetime.utcnow() > expires:
                session.clear()
                if request.path.startswith("/api/"):
                    return jsonify({"error": "Your access has expired", "redirect": "/login?expired=1"}), 403
                return redirect("/login?expired=1")
        return f(*args, **kwargs)
    return decorated


BASE = Path(__file__).parent.parent
_SENATE_PATH = BASE / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2026.parquet"
_HOUSE_PATH = BASE / "data/output/house/corpus/house_hansard_corpus_1998_to_2026.parquet"


# ── Corpus loading ─────────────────────────────────────────────────────────────

def _load_corpus(path: Path, chamber: str) -> pd.DataFrame:
    if not path.exists():
        print(f"  WARNING: corpus file not found: {path}")
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if "body" in df.columns:
        df = df.drop(columns=["body"])   # body held on disk, loaded per-request
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["chamber"] = chamber
    df["_parquet_idx"] = range(len(df))  # preserve original parquet row position
    print(f"  Loaded {chamber}: {len(df):,} rows (body held on disk)")
    return df


# ── Name display normalisation ─────────────────────────────────────────────────

def _build_name_display_map(lookup_base: Path) -> dict:
    """Build name_id → 'Honorific SURNAME, Firstname' from senator and member lookups."""
    result: dict[str, str] = {}

    def _first(row, *cols):
        for c in cols:
            v = str(row.get(c, "") or "").strip()
            if v and v.lower() not in ("nan", "none", ""):
                return v
        return ""

    spath = lookup_base / "senator_lookup.csv"
    if spath.exists():
        sl = pd.read_csv(spath, dtype=str)
        for _, row in sl.iterrows():
            nid = str(row.get("name_id", "")).strip()
            if not nid or nid in ("nan", "10000"):
                continue
            surname = str(row.get("surname", "")).strip().upper()
            fname   = _first(row, "common_name", "first_name").title()
            if surname:
                result[nid] = f"Senator {surname}, {fname}" if fname else f"Senator {surname}"

    mpath = lookup_base / "member_lookup.csv"
    if mpath.exists():
        ml = pd.read_csv(mpath, dtype=str)
        for _, row in ml.iterrows():
            nid = str(row.get("name_id", "")).strip()
            if not nid or nid in ("nan", "10000"):
                continue
            title   = _first(row, "title") or "Mr"
            surname = str(row.get("surname", "")).strip().upper()
            fname   = _first(row, "common_name", "first_name").title()
            if surname:
                result[nid] = f"{title} {surname}, {fname}" if fname else f"{title} {surname}"

    return result


def _normalise_names(df: pd.DataFrame, nid_to_name: dict) -> pd.DataFrame:
    """Strip trailing colons and apply 'Honorific SURNAME, Firstname' format."""
    if df.empty or "name" not in df.columns:
        return df
    # Strip trailing colon/whitespace from all names
    df["name"] = df["name"].str.rstrip(": \t")
    # Apply formatted names for known name_ids (skip presiding officer sentinel)
    if "name_id" not in df.columns or not nid_to_name:
        return df
    mask = df["name_id"].astype(str) != "10000"
    mapped = df.loc[mask, "name_id"].map(nid_to_name)
    filled = mapped.notna()
    df.loc[mask & filled, "name"] = mapped[filled]
    return df


# ── Presiding officer resolution ───────────────────────────────────────────────

def _build_presiding_map(lookup_df: pd.DataFrame) -> list:
    """Build a list of dicts for date-ranged presiding officer resolution.

    Date-ranged (real-person) entries come first; generic fallbacks (name_id==10000)
    come last so they only match rows not already resolved.
    """
    entries = []
    fallbacks = []
    for _, row in lookup_df.iterrows():
        nid = str(row.get("name_id", "")).strip()
        pat = str(row.get("xml_name_pattern", "")).strip()
        if not pat:
            continue
        entry = {
            "pattern":      pat,
            "name_id":      nid,
            "display_name": str(row.get("display_name", "")).strip(),
            "party":        str(row.get("party", "")).strip(),
            "state":        str(row.get("state", "")).strip(),
            "from":         str(row.get("from_date", "")).strip() if pd.notna(row.get("from_date")) else "",
            "to":           str(row.get("to_date",   "")).strip() if pd.notna(row.get("to_date"))   else "",
        }
        if nid == "10000":
            fallbacks.append(entry)
        else:
            entries.append(entry)
    return entries + fallbacks


def _resolve_presiding_officers(df: pd.DataFrame, presiding_map: list, role_col: str,
                                nid_to_name: dict = None) -> pd.DataFrame:
    """Replace name_id='10000' rows with real person details via date-range lookup."""
    if df.empty or "name_id" not in df.columns:
        return df
    mask_10k = df["name_id"].astype(str) == "10000"
    if not mask_10k.any():
        return df
    resolved = 0
    for entry in presiding_map:
        if not mask_10k.any():
            break
        pat = entry["pattern"].lower()
        lo  = entry["from"] or ""
        hi  = entry["to"]   or "9999-99-99"
        ematch = (
            mask_10k
            & df["name"].str.lower().str.contains(pat, regex=False, na=False)
            & (df["date"] >= lo)
            & (df["date"] <= hi)
        )
        if not ematch.any():
            continue
        role_label = (
            df.loc[ematch, "name"]
            .str.replace(r"(?i)^the\s+", "", regex=True)
            .str.title()
        )
        formatted_base = (nid_to_name or {}).get(entry["name_id"]) or entry["display_name"]
        if formatted_base:
            df.loc[ematch, "name"] = formatted_base + " (" + role_label + ")"
        df.loc[ematch, "name_id"] = entry["name_id"]
        if entry["party"]:
            df.loc[ematch, "party"] = entry["party"]
        if entry["state"] and role_col == "state" and "state" in df.columns:
            df.loc[ematch, "state"] = entry["state"]
        if entry["state"] and role_col == "electorate" and "electorate" in df.columns:
            df.loc[ematch, "electorate"] = entry["state"]
        mask_10k = mask_10k & ~ematch
        resolved += ematch.sum()
    print(f"  Resolved {resolved:,} presiding officer rows (role_col={role_col})")
    return df


print("Loading corpora…")
_SENATE: pd.DataFrame = _load_corpus(_SENATE_PATH, "senate")
_HOUSE: pd.DataFrame = _load_corpus(_HOUSE_PATH, "house")

_LOOKUP_BASE = BASE / "data/lookup"
_NID_TO_NAME = _build_name_display_map(_LOOKUP_BASE)
print(f"  Name map: {len(_NID_TO_NAME):,} entries")

# Normalise all non-presiding-officer names first
if not _SENATE.empty:
    _SENATE = _normalise_names(_SENATE, _NID_TO_NAME)
if not _HOUSE.empty:
    _HOUSE = _normalise_names(_HOUSE, _NID_TO_NAME)

# Resolve presiding officers (uses formatted names from map)
_president_path = _LOOKUP_BASE / "president_lookup.csv"
_speaker_path   = _LOOKUP_BASE / "speaker_lookup.csv"
if _president_path.exists() and not _SENATE.empty:
    _PRES_LOOKUP = pd.read_csv(_president_path, dtype=str)
    _SENATE = _resolve_presiding_officers(_SENATE, _build_presiding_map(_PRES_LOOKUP), "state", _NID_TO_NAME)
if _speaker_path.exists() and not _HOUSE.empty:
    _SPKR_LOOKUP = pd.read_csv(_speaker_path, dtype=str)
    _HOUSE = _resolve_presiding_officers(_HOUSE, _build_presiding_map(_SPKR_LOOKUP), "electorate", _NID_TO_NAME)

print("Corpora ready.")


import threading as _threading, time as _time
_body_cache     : dict              = {}   # "senate" | "house" → (pd.Series, float)
_body_lock       = _threading.Lock()
_SCAN_SEMAPHORE  = _threading.Semaphore(1)  # serialise heavy scans on single-core VPS

def _get_body_col(chamber: str) -> pd.Series:
    """Load body column from parquet; TTL-cached for 3600s to avoid repeated disk reads."""
    ttl = 3600
    now = _time.monotonic()

    def _load(path):
        return (
            pd.read_parquet(path, columns=["body"], dtype_backend="pyarrow")
            ["body"]
            .fillna("")
        )

    with _body_lock:
        if chamber in ("senate", "house"):
            cached, ts = _body_cache.get(chamber, (None, 0))
            if cached is not None and now - ts < ttl:
                return cached
            s = _load(_SENATE_PATH if chamber == "senate" else _HOUSE_PATH)
            _body_cache[chamber] = (s, now)
            return s
        else:  # "both"
            s_cached, s_ts = _body_cache.get("senate", (None, 0))
            h_cached, h_ts = _body_cache.get("house",  (None, 0))
            if s_cached is None or now - s_ts >= ttl:
                s_cached = _load(_SENATE_PATH)
                _body_cache["senate"] = (s_cached, now)
            if h_cached is None or now - h_ts >= ttl:
                h_cached = _load(_HOUSE_PATH)
                _body_cache["house"] = (h_cached, now)
            return pd.concat([s_cached, h_cached], ignore_index=True)


def _prewarm_body_cache():
    _get_body_col("senate")
    _get_body_col("house")

_threading.Thread(target=_prewarm_body_cache, daemon=True, name="body-prewarm").start()


def _load_body_for_display(page_rows: pd.DataFrame) -> pd.Series:
    """Load body text for result display rows using the warm body cache."""
    senate_body = None
    house_body  = None
    bodies = []
    for _, row in page_rows.iterrows():
        ch   = str(row.get("chamber", "senate"))
        pidx = int(row.get("_parquet_idx", 0))
        if ch == "senate":
            if senate_body is None:
                senate_body = _get_body_col("senate")
            bodies.append(str(senate_body.iloc[pidx]))
        else:
            if house_body is None:
                house_body = _get_body_col("house")
            bodies.append(str(house_body.iloc[pidx]))
    return pd.Series(bodies, index=page_rows.index)


_DIV_TYPE_ORDER = ["Inner Metropolitan", "Outer Metropolitan", "Provincial", "Rural", "Other"]

# Known corpus electorate artifacts → normalised lookup key
_ELEC_NORM = {
    "dunkleydunkley": "dunkley",
    "lingiari.":       "lingiari",
    "maranoamaranoa":  "maranoa",
    "kingsford-smith": "kingsford smith",
}

# Electorate values that are artifacts/ambiguous — group under "Other"
_ELEC_SKIP = {"po", "unknown", "namadgi", "bonython", "charlton"}


_STATE_ABBREV_TO_FULL = {
    "ACT": "Australian Capital Territory",
    "NSW": "New South Wales",
    "NT":  "Northern Territory",
    "QLD": "Queensland",
    "SA":  "South Australia",
    "TAS": "Tasmania",
    "VIC": "Victoria",
    "WA":  "Western Australia",
}


def _build_speaker_maps():
    """Build senators-by-state and MPs-by-state/type maps from both corpora."""

    # Work on slim copies (drop body text) to avoid peak-memory spikes during groupby
    _SEN = _SENATE.drop(columns=["body"], errors="ignore") if not _SENATE.empty else _SENATE
    _HOU = _HOUSE.drop(columns=["body"], errors="ignore") if not _HOUSE.empty else _HOUSE

    _LOOKUP_DIR = BASE / "data/lookup"

    # ── Load AEC electorate classification ────────────────────────────────────
    _elec_class: dict[str, tuple[str, str]] = {}  # normalised_name → (state, div_type)
    _elec_class_path = _LOOKUP_DIR / "electorate_classification.csv"
    if _elec_class_path.exists():
        ec = pd.read_csv(_elec_class_path)
        for _, row in ec.iterrows():
            div = str(row.get("division", "")).strip()
            if div:
                _elec_class[div.lower()] = (
                    str(row["state"]).strip(),
                    str(row["division_type"]).strip(),
                )

    # ── Load official senator state lookup ────────────────────────────────────
    # Use both senator_lookup (primary) and state_lookup (supplements terms),
    # so every senator with an official record gets the right state.
    _senator_state: dict[str, str] = {}  # name_id → full state name
    for fname in ("senator_lookup.csv", "state_lookup.csv"):
        fpath = _LOOKUP_DIR / fname
        if not fpath.exists():
            continue
        sl = pd.read_csv(fpath)
        for _, row in sl.iterrows():
            nid = str(row.get("name_id", "")).strip()
            abbrev = str(row.get("state_abbrev", "")).strip().upper()
            full = _STATE_ABBREV_TO_FULL.get(abbrev, abbrev)
            if nid and full and nid not in _senator_state:
                _senator_state[nid] = full

    # ── Load official MP electorate lookup ────────────────────────────────────
    # Use the most recent electorate per member from member_lookup.
    _mp_electorate: dict[str, str] = {}  # name_id → electorate name
    _member_lookup_path = _LOOKUP_DIR / "member_lookup.csv"
    if _member_lookup_path.exists():
        ml = pd.read_csv(_member_lookup_path)
        for _, row in ml.iterrows():
            nid = str(row["name_id"]).strip()
            elec = str(row.get("electorate", "")).strip()
            if nid and elec and elec.lower() not in ("nan", "none", ""):
                _mp_electorate[nid] = elec  # later rows overwrite earlier ones

    # ── Build senator map from corpus (names) + official lookup (states) ──────
    sen_map: dict[str, dict[str, str]] = {}   # state → {nid → canonical_name}
    mp_map:  dict[tuple, dict]         = {}   # (state, div_type, elec) → {nid → name}

    for df in [_SEN, _HOU]:
        if df.empty:
            continue

        # Senators: name from corpus, state from official lookup
        if "state" in df.columns:
            sub = df[df["state"].notna() & df["name_id"].notna() & df["name"].notna()]
            for nid, grp in sub.groupby("name_id"):
                nid_str = str(nid)
                if nid_str not in _senator_state:
                    continue
                canonical_name = (
                    grp["name"].str.replace(r"\s*\([^)]+\)\s*$", "", regex=True)
                    .value_counts().index[0]
                )
                state = _senator_state[nid_str]
                sen_map.setdefault(state, {})[nid_str] = canonical_name

        # MPs: name from corpus, electorate from official lookup
        if "electorate" in df.columns:
            sub = df[df["electorate"].notna() & df["name_id"].notna() & df["name"].notna()]
            for nid, grp in sub.groupby("name_id"):
                nid_str = str(nid)
                if nid_str not in _mp_electorate:
                    continue
                canonical_name = (
                    grp["name"].str.replace(r"\s*\([^)]+\)\s*$", "", regex=True)
                    .value_counts().index[0]
                )
                elec_str = _mp_electorate[nid_str]
                elec_key = _ELEC_NORM.get(elec_str.lower(), elec_str.lower())
                if elec_key in _ELEC_SKIP:
                    continue
                state_val, div_type = _elec_class.get(elec_key, (None, None))
                if state_val is None:
                    state_val, div_type = "Other", "Other"
                key = (state_val, div_type, elec_str)
                mp_map.setdefault(key, {})[nid_str] = canonical_name

    # ── Format senator list ───────────────────────────────────────────────────
    senators_by_state = {
        state: sorted(
            [{"name_id": nid, "name": nm} for nid, nm in entries.items()],
            key=lambda x: x["name"],
        )
        for state, entries in sorted(sen_map.items())
    }

    # ── Format MP nested dict: state → div_type → electorate → [sorted list] ─
    mps_by_state_type: dict[str, dict[str, dict[str, list]]] = {}
    for (state, div_type, elec), entries in mp_map.items():
        dt_dict = mps_by_state_type.setdefault(state, {}).setdefault(div_type, {})
        el_list = dt_dict.setdefault(elec, [])
        for nid, name in entries.items():
            el_list.append({"name_id": nid, "name": name})
    for state in mps_by_state_type:
        for div_type in mps_by_state_type[state]:
            for elec in mps_by_state_type[state][div_type]:
                mps_by_state_type[state][div_type][elec].sort(key=lambda x: x["name"])

    # ── Build name_id → [parties] from corpus (≥10% of rows) ────────────────
    # Using corpus party values (match what the filter checkboxes use).
    # 10% threshold filters presiding-officer noise while keeping real changers.
    nid_parties: dict[str, list[str]] = {}
    _party_sets: dict[str, set] = {}
    for df in [_SEN, _HOU]:
        if df.empty or "party" not in df.columns:
            continue
        for nid, grp in df.groupby("name_id"):
            nid_str = str(nid).strip()
            if not nid_str or nid_str.lower() in ("nan", "none"):
                continue
            total = len(grp)
            for party, count in grp["party"].value_counts().items():
                p = str(party).strip()
                if p and p.lower() not in ("nan", "none", "") and count / total >= 0.10:
                    _party_sets.setdefault(nid_str, set()).add(p)
    nid_parties = {k: sorted(v) for k, v in _party_sets.items()}

    # ── Build parliament metadata from session_info_all.csv ───────────────────
    # v2.2 XML uses parliament_no 1/2/3 for what are actually the 46th/47th/48th
    # parliaments. Fix that mapping before computing ranges.
    _PARL_FIX = {1: 46, 2: 47, 3: 48}
    _si_path = _LOOKUP_DIR / "session_info_all.csv"
    parl_ranges: dict[int, tuple[str, str]] = {}  # parl_no → (start, end) ISO strings
    if _si_path.exists():
        si = pd.read_csv(_si_path)
        si["date"] = pd.to_datetime(si["date"])
        si["parliament_no"] = si["parliament_no"].map(
            lambda x: _PARL_FIX.get(int(x), int(x)) if pd.notna(x) else x
        )
        si = si[si["parliament_no"] >= 38]
        for pno, grp in si.groupby("parliament_no"):
            parl_ranges[int(pno)] = (
                grp["date"].min().strftime("%Y-%m-%d"),
                grp["date"].max().strftime("%Y-%m-%d"),
            )

    # Ordinal suffix helper
    def _ordinal(n: int) -> str:
        if 11 <= (n % 100) <= 13:
            return f"{n}th"
        return f"{n}{['th','st','nd','rd','th'][min(n % 10, 4)]}"

    parliament_meta = []
    for pno in sorted(parl_ranges, reverse=True):
        start, end = parl_ranges[pno]
        sy, ey = start[:4], end[:4]
        is_current = end >= date.today().isoformat()
        date_part  = f"{sy}–Present" if is_current else (f"{sy}–{ey}" if ey != sy else sy)
        label = f"{_ordinal(pno)} Parliament ({date_part})"
        # For the current parliament use today as the end so the date-range filter
        # covers all corpus dates, not just those already in session_info_all.csv
        effective_end = date.today().isoformat() if is_current else end
        parliament_meta.append({"no": pno, "label": label, "start": start, "end": effective_end})

    # ── Build nid → [parliaments] and party → [parliaments] ──────────────────
    # Vectorised: for each parliament range, create a boolean mask and collect nids/parties.
    nid_parl_sets:   dict[str, set] = {}
    party_parl_sets: dict[str, set] = {}
    for df in [_SEN, _HOU]:
        if df.empty:
            continue
        dates = pd.to_datetime(df["date"])
        for pno, (start, end) in parl_ranges.items():
            mask = (dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))
            sub = df[mask]
            for nid in sub["name_id"].dropna().unique():
                nid_str = str(nid).strip()
                if nid_str and nid_str.lower() not in ("nan", "none"):
                    nid_parl_sets.setdefault(nid_str, set()).add(pno)
            if "party" in sub.columns:
                for party in sub["party"].dropna().unique():
                    p = str(party).strip()
                    if p and p.lower() not in ("nan", "none", ""):
                        party_parl_sets.setdefault(p, set()).add(pno)

    nid_parliaments   = {k: sorted(v) for k, v in nid_parl_sets.items()}
    party_parliaments = {k: sorted(v) for k, v in party_parl_sets.items()}

    return senators_by_state, mps_by_state_type, nid_parties, parliament_meta, nid_parliaments, party_parliaments


print("Building speaker maps…")
(_SENATORS_BY_STATE, _MPS_BY_STATE_TYPE, _NID_PARTIES,
 _PARLIAMENT_META, _NID_PARLIAMENTS, _PARTY_PARLIAMENTS) = _build_speaker_maps()
print("Speaker maps ready.")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_corpus(chamber: str) -> pd.DataFrame:
    """Return the requested corpus without copying (read-only reference)."""
    if chamber == "senate":
        return _SENATE
    if chamber == "house":
        return _HOUSE
    # For "both", concatenate at request time — avoids keeping a third copy in RAM
    parts = [df for df in [_SENATE, _HOUSE] if not df.empty]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _safe_str(val) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return "" if s in ("nan", "None", "NaT") else s


def _safe_int(val, default=0) -> int:
    try:
        v = int(float(val))
        return v
    except (TypeError, ValueError):
        return default


def _safe_bool(val) -> bool:
    try:
        return bool(val)
    except Exception:
        return False


# ── Corpus seed extraction ─────────────────────────────────────────────────────

_SEED_STOPWORDS = {
    'about', 'above', 'after', 'again', 'against', 'also', 'although', 'among',
    'another', 'around', 'because', 'been', 'before', 'being', 'between', 'both',
    'could', 'does', 'doing', 'during', 'each', 'every', 'from', 'further',
    'have', 'having', 'here', 'however', 'into', 'itself', 'just', 'keep',
    'make', 'many', 'more', 'most', 'much', 'must', 'need', 'never', 'only',
    'other', 'over', 'same', 'should', 'since', 'some', 'still', 'such',
    'than', 'that', 'their', 'them', 'then', 'there', 'these', 'they',
    'this', 'those', 'through', 'under', 'until', 'upon', 'very', 'want',
    'well', 'were', 'what', 'when', 'where', 'which', 'while', 'will',
    'with', 'within', 'would', 'your', 'senator', 'minister', 'member',
    'government', 'opposition', 'parliament', 'australia', 'australian',
    'committee', 'question', 'answer', 'speaker', 'madam', 'president',
    'honourable', 'said', 'shall', 'order', 'time', 'made', 'says',
    'think', 'know', 'like', 'look', 'come', 'take', 'matter', 'point',
    'issue', 'debate', 'house', 'senate', 'today', 'important', 'support',
    'people', 'right', 'given', 'number', 'party', 'years', 'year',
    'first', 'second', 'third', 'whether', 'without', 'will', 'been',
    'that', 'this', 'with', 'they', 'their', 'from', 'also', 'very',
    'just', 'over', 'such', 'only', 'some', 'must', 'need', 'want',
}


def _normalise_expression(expr: str) -> str:
    """Auto-quote simple phrases that have no boolean operators or parens."""
    if not expr:
        return expr
    if not re.search(r'[&|!()]', expr):
        # Strip any stray quote characters and wrap the whole thing
        return "'" + re.sub(r"['\"]", "", expr) + "'"
    return expr


def _extract_seeds(matched_df: pd.DataFrame, n: int = 20) -> list:
    """Extract frequently co-occurring terms from matched speeches to seed alias suggestions."""
    sample = matched_df.head(100)
    all_words = []
    for body in sample["body"].fillna(""):
        all_words.extend(re.findall(r"\b[a-zA-Z]{4,}\b", body.lower()))

    # Bigrams from consecutive non-stopword words
    bigrams = []
    for i in range(len(all_words) - 1):
        w1, w2 = all_words[i], all_words[i + 1]
        if w1 not in _SEED_STOPWORDS and w2 not in _SEED_STOPWORDS:
            bigrams.append(f"{w1} {w2}")

    # Unigrams (5+ chars, not stopwords)
    unigrams = [w for w in all_words if w not in _SEED_STOPWORDS and len(w) >= 5]

    seeds, seen = [], set()
    for term, _ in Counter(bigrams).most_common(15):
        if term not in seen:
            seen.add(term)
            seeds.append(term)
    for term, _ in Counter(unigrams).most_common(30):
        if term not in seen and len(seeds) < n:
            seen.add(term)
            seeds.append(term)
    return seeds[:n]


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return send_from_directory("static", "index.html")


@app.route("/help")
def help_page():
    return send_from_directory("static", "help.html")


@app.route("/login")
def login_page():
    return send_from_directory("static", "login.html")


@app.route("/request-access")
def request_access_page():
    return send_from_directory("static", "request_access.html")


@app.route("/api/request-access", methods=["POST"])
def api_request_access():
    body = request.get_json(force=True, silent=True) or {}
    name  = (body.get("name") or "").strip()
    email = (body.get("email") or "").strip().lower()
    org   = (body.get("organisation") or "").strip()
    reason = (body.get("reason") or "").strip()
    if not name or not email:
        return jsonify({"error": "Name and email are required."}), 400
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    with sqlite3.connect(_DB_PATH) as conn:
        conn.execute(
            "INSERT INTO access_requests (name, email, organisation, reason, requested_at) VALUES (?,?,?,?,?)",
            (name, email, org, reason, now),
        )
        conn.commit()
    app.logger.info("Access request: %s <%s> org=%r", name, email, org)
    return jsonify({"ok": True})


@app.route("/api/login", methods=["POST"])
def api_login():
    body = request.get_json(force=True, silent=True) or {}
    email    = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""
    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400
    with sqlite3.connect(_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    if not row or not check_password_hash(row["password_hash"], password):
        _log({"event": "login_fail", "email": email, "found_user": bool(row)})
        return jsonify({"error": "Invalid email or password"}), 401
    if row["expires_at"]:
        if datetime.utcnow() > datetime.fromisoformat(row["expires_at"]):
            return jsonify({"error": "Your access has expired. Please contact Jeff to renew."}), 403
    session.permanent = True
    session["user_id"] = row["id"]
    _log({"event": "login", "email": email, "label": row["label"]})
    return jsonify({"ok": True})


@app.route("/api/me")
@login_required
def api_me():
    user = _get_user(session["user_id"])
    return jsonify({
        "email": user["email"],
        "label": user["label"],
        "expires_at": user["expires_at"],
    })


@app.route("/api/logout", methods=["POST"])
def api_logout():
    _log({"event": "logout", "user_id": session.get("user_id")})
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/metadata")
@login_required
def metadata():
    """Return unique filter values and row counts."""
    parties = set()
    states = set()
    electorates = set()
    dates = []

    for df in [_SENATE, _HOUSE]:
        if df.empty:
            continue
        if "party" in df.columns:
            parties.update(df["party"].dropna().unique().tolist())
        if "state" in df.columns:
            states.update(df["state"].dropna().unique().tolist())
        if "electorate" in df.columns:
            electorates.update(df["electorate"].dropna().unique().tolist())
        if "date" in df.columns:
            valid = df["date"].dropna()
            dates.extend(valid.tolist())

    def _clean(s): return s and str(s).strip() not in ("", "nan", "None")
    parties     = sorted(p for p in parties     if _clean(p))
    states      = sorted(s for s in states      if _clean(s))
    electorates = sorted(e for e in electorates if _clean(e))

    date_min = min(dates) if dates else ""
    date_max = max(dates) if dates else ""

    return jsonify({
        "parties": parties,
        "states": states,
        "electorates": electorates,
        "senators_by_state": _SENATORS_BY_STATE,
        "mps_by_state_type": _MPS_BY_STATE_TYPE,
        "nid_parties": _NID_PARTIES,
        "parliaments": _PARLIAMENT_META,
        "nid_parliaments": _NID_PARLIAMENTS,
        "party_parliaments": _PARTY_PARLIAMENTS,
        "date_range": {"min": date_min, "max": date_max},
        "senate_rows": len(_SENATE),
        "house_rows": len(_HOUSE),
    })


# Map frontend sort_col names to DataFrame column(s), with date as tiebreak
_SORT_MAP = {
    "date":               ["date", "order"],
    "chamber":            ["chamber", "date"],
    "name":               ["name", "date"],
    "party":              ["party", "date"],
    "in_gov":             ["in_gov", "date"],
    "state_or_electorate": None,   # resolved per-df below
}

def _sort_df(df: pd.DataFrame, sort_col: str, ascending: bool) -> pd.DataFrame:
    cols = _SORT_MAP.get(sort_col, ["date", "order"])
    if cols is None:
        geo = "state" if "state" in df.columns else "electorate" if "electorate" in df.columns else "date"
        cols = [geo, "date"]
    valid = [c for c in cols if c in df.columns]
    return df.sort_values(valid, ascending=ascending) if valid else df


def _compute_facet_counts(df: pd.DataFrame) -> dict:
    """Compute per-value counts from matched rows for sidebar filter facets."""
    if df.empty:
        return {}
    bad = {"nan", "none", ""}
    out: dict = {}

    for col, key in [("party", "party"), ("gender", "gender"), ("name_id", "name_id")]:
        if col in df.columns:
            out[key] = {
                str(k): int(v)
                for k, v in df[col].value_counts().items()
                if str(k).strip().lower() not in bad
            }

    if "in_gov" in df.columns:
        out["in_gov"] = {
            str(k): int(v)
            for k, v in df["in_gov"].dropna().value_counts().items()
        }

    if "question" in df.columns and "answer" in df.columns:
        out["row_type"] = {
            "speech":   int(((df["question"] == 0) & (df["answer"] == 0)).sum()),
            "exchange": int(((df["question"] == 1) | (df["answer"] == 1)).sum()),
        }

    for col, key in [
        ("first_speech", "first_speech"), ("question", "question"),
        ("answer", "answer"), ("q_in_writing", "q_in_writing"),
    ]:
        if col in df.columns:
            out[key] = int((df[col] == 1).sum())

    if "interject" in df.columns:
        out["no_interject"] = int((df["interject"] == 0).sum())
    if "has_embedded_interject" in df.columns:
        out["has_embedded_interject"] = int(
            df["has_embedded_interject"].fillna(False).astype(bool).sum()
        )
    if "fedchamb_flag" in df.columns:
        out["main_chamber"] = int((df["fedchamb_flag"] == 0).sum())
        out["fed_chamber"]  = int((df["fedchamb_flag"] == 1).sum())

    if "state" in df.columns:
        out["state"] = {
            str(k).strip(): int(v)
            for k, v in df["state"].dropna().value_counts().items()
            if str(k).strip() and str(k).strip().lower() not in bad
        }
    if "electorate" in df.columns:
        out["electorate"] = {
            str(k): int(v)
            for k, v in df["electorate"].dropna().value_counts().items()
            if str(k).strip() and str(k).strip().lower() not in bad
        }

    if "date" in df.columns:
        parl_counts: dict[int, int] = {}
        for pm in _PARLIAMENT_META:
            start = str(pm["start"])[:10]
            end   = str(pm["end"])[:10]
            count = int(((df["date"] >= start) & (df["date"] <= end)).sum())
            if count:
                parl_counts[int(pm["no"])] = count
        out["parliament"] = parl_counts

    if "chamber" in df.columns:
        out["chamber"] = {
            str(k): int(v)
            for k, v in df["chamber"].value_counts().items()
        }

    return out


@app.route("/api/search", methods=["POST"])
@login_required
def search():
    """Execute a search and return paginated results with context."""
    _t0 = time.perf_counter()
    _t1 = _t2 = _t3 = _t0  # defaults; overwritten by whichever branch runs
    body = request.get_json(force=True, silent=True) or {}

    expression = body.get("expression", "").strip()
    chamber = body.get("chamber", "both")
    page = max(1, int(body.get("page", 1)))
    page_size = max(1, min(200, int(body.get("page_size", 20))))
    filters = body.get("filters") or {}
    case_sensitive = bool(body.get("case_sensitive", False))
    sort_col = (body.get("sort_col") or "date").strip()
    sort_dir = (body.get("sort_dir") or "asc").strip()
    ascending = sort_dir != "desc"

    tree = None
    if expression:
        expression = _normalise_expression(expression)
        try:
            tree = parse_expression(expression)
        except SyntaxError as e:
            return jsonify({"error": f"Expression syntax error: {e}"}), 400

    terms = []
    corpus_seeds = []
    facet_counts: dict = {}

    # ── No-expression "both" path: never concat full corpora ───────────────────
    if tree is None and chamber == "both":
        s_src = _apply_filters(_SENATE, filters) if not _SENATE.empty else _SENATE
        h_src = _apply_filters(_HOUSE,  filters) if not _HOUSE.empty else _HOUSE

        orig_len     = len(_SENATE) + len(_HOUSE)
        filtered_len = len(s_src) + len(h_src)
        senate_count = len(s_src)
        house_count  = len(h_src)
        total        = filtered_len

        _t1 = _t2 = time.perf_counter()  # no expression scan in this branch

        # Timeseries — groupby on each, no concat
        yr_senate = s_src["date"].str[:4].value_counts().to_dict() if not s_src.empty else {}
        yr_house  = h_src["date"].str[:4].value_counts().to_dict() if not h_src.empty else {}
        all_years = sorted(set(yr_senate) | set(yr_house))
        timeseries = {
            "years":  all_years,
            "senate": [int(yr_senate.get(y, 0)) for y in all_years],
            "house":  [int(yr_house.get(y, 0))  for y in all_years],
        }

        # Sort each independently
        s_sorted = _sort_df(s_src, sort_col, ascending)
        h_sorted = _sort_df(h_src, sort_col, ascending)

        pages = max(1, (total + page_size - 1) // page_size) if total > 0 else 0
        page  = min(page, pages) if pages > 0 else 1
        start = (page - 1) * page_size
        end   = start + page_size

        # Merge-sort pagination: taking top `end` rows from each guarantees
        # we have all candidates for the correct global page slice (proof: any
        # row in the global top-end must rank ≤ end within its own chamber).
        s_cand = s_sorted.head(end) if not s_sorted.empty else s_sorted
        h_cand = h_sorted.head(end) if not h_sorted.empty else h_sorted

        if not (s_cand.empty and h_cand.empty):
            merged = pd.concat([s_cand, h_cand], ignore_index=True)
            merged = _sort_df(merged, sort_col, ascending)
            page_rows = merged.iloc[start:end]
        else:
            page_rows = pd.DataFrame()

        if not page_rows.empty:
            page_rows = page_rows.copy()
            page_rows["body"] = _load_body_for_display(page_rows)

        _meta_frames = [s_src, h_src]
        _facet_parts = [df for df in [s_src, h_src] if not df.empty]
        facet_counts = _compute_facet_counts(
            pd.concat(_facet_parts, ignore_index=True) if _facet_parts else pd.DataFrame()
        )

    # ── All other paths ─────────────────────────────────────────────────────────
    else:
        df = _get_corpus(chamber)
        if df.empty:
            return jsonify({
                "total": 0, "senate_count": 0, "house_count": 0,
                "filtered_rows": 0, "original_rows": 0,
                "page": page, "page_size": page_size, "pages": 0,
                "results": [],
            })

        orig_len = len(df)
        df = _apply_filters(df, filters)
        filtered_len = len(df)

        if tree is None:
            matched = df
        elif chamber == "both" and not _SENATE.empty and not _HOUSE.empty:
            terms  = collect_terms(tree)
            s_body = _get_body_col("senate")
            h_body = _get_body_col("house")
            _t1 = time.perf_counter()
            def _search_one(src, bdy, filt):
                # Always scan full body (zero-copy, fast regardless of filters)
                masks = _build_masks(pd.DataFrame({"body": bdy}), terms, case_sensitive)
                mask  = _eval_tree(tree, masks)
                res   = src[mask].copy()
                res["body"] = bdy[mask].values
                # Post-scan metadata filter — fast, result set is small
                if filt:
                    res = _apply_filters(res, filt)
                return res
            with _SCAN_SEMAPHORE:
                fs = _THREAD_POOL.submit(_search_one, _SENATE, s_body, filters)
                fh = _THREAD_POOL.submit(_search_one, _HOUSE,  h_body, filters)
                matched = pd.concat([fs.result(), fh.result()], ignore_index=True)
            _t2 = time.perf_counter()
        else:
            terms  = collect_terms(tree)
            body_s = _get_body_col(chamber)   # full body, always zero-copy
            _t1 = time.perf_counter()
            with _SCAN_SEMAPHORE:
                masks  = _build_masks(pd.DataFrame({"body": body_s}), terms, case_sensitive)
                mask   = _eval_tree(tree, masks)
            _t2 = time.perf_counter()
            matched = _get_corpus(chamber)[mask].copy()
            matched["body"] = body_s[mask].values
            if filters:
                matched = _apply_filters(matched, filters)

        total        = len(matched)
        senate_count = int((matched["chamber"] == "senate").sum()) if "chamber" in matched.columns else 0
        house_count  = int((matched["chamber"] == "house" ).sum()) if "chamber" in matched.columns else 0

        yr_senate = matched[matched["chamber"] == "senate"]["date"].str[:4].value_counts().to_dict() \
            if "chamber" in matched.columns else {}
        yr_house  = matched[matched["chamber"] == "house" ]["date"].str[:4].value_counts().to_dict() \
            if "chamber" in matched.columns else {}
        all_years = sorted(set(yr_senate) | set(yr_house))
        timeseries = {
            "years":  all_years,
            "senate": [int(yr_senate.get(y, 0)) for y in all_years],
            "house":  [int(yr_house.get(y, 0))  for y in all_years],
        }

        corpus_seeds = _extract_seeds(matched) if not matched.empty and "body" in matched.columns else []
        facet_counts = _compute_facet_counts(matched)
        matched = _sort_df(matched, sort_col, ascending)

        pages = max(1, (total + page_size - 1) // page_size) if total > 0 else 0
        page  = min(page, pages) if pages > 0 else 1
        start = (page - 1) * page_size
        page_rows = matched.iloc[start: start + page_size]

        if not page_rows.empty and "body" not in page_rows.columns:
            page_rows = page_rows.copy()
            page_rows["body"] = _load_body_for_display(page_rows)

        _meta_frames = [matched]

    # ── Result metadata (for panel filtering on the client) ────────────────────
    _nids_set  = set()
    _party_set = set()
    _all_dates: set[str] = set()
    for _f in _meta_frames:
        if _f.empty:
            continue
        _nids_set.update(str(n) for n in _f["name_id"].dropna().unique())
        if "party" in _f.columns:
            _party_set.update(str(p) for p in _f["party"].dropna().unique())
        if "date" in _f.columns:
            _all_dates.update(str(d) for d in _f["date"].dropna().unique())

    _bad = {"nan", "none", ""}
    result_nids    = sorted(n for n in _nids_set  if n.strip().lower() not in _bad)
    result_parties = sorted(p for p in _party_set if p.strip().lower() not in _bad)

    # State and electorate counts for choropleth maps
    _STATE_NORM = {
        "act": "Australian Capital Territory", "nsw": "New South Wales",
        "nt":  "Northern Territory",           "qld": "Queensland",
        "sa":  "South Australia",              "tas": "Tasmania",
        "vic": "Victoria",                     "wa":  "Western Australia",
    }
    state_counts: dict[str, int] = {}
    electorate_counts: dict[str, int] = {}
    for _f in _meta_frames:
        if _f.empty:
            continue
        if "state" in _f.columns:
            for s, cnt in _f["state"].dropna().value_counts().items():
                s = str(s).strip()
                s = _STATE_NORM.get(s.lower(), s)
                if s and s.lower() not in _bad:
                    state_counts[s] = state_counts.get(s, 0) + int(cnt)
        if "electorate" in _f.columns:
            for e, cnt in _f["electorate"].dropna().value_counts().items():
                e = str(e).strip()
                if e and e.lower() not in _bad:
                    electorate_counts[e] = electorate_counts.get(e, 0) + int(cnt)

    result_parl_nos = [
        pm["no"] for pm in _PARLIAMENT_META
        if any(str(pm["start"])[:10] <= d <= str(pm["end"])[:10] for d in _all_dates)
    ]

    _t3 = time.perf_counter()

    results = []
    for _, row in page_rows.iterrows():
        ch = _safe_str(row.get("chamber", ""))
        dt = _safe_str(row.get("date", ""))
        order_val = _safe_str(row.get("order", ""))
        match_id = f"{ch}-{dt}-{order_val}"

        # Determine which terms matched this row
        body_text = _safe_str(row.get("body", ""))
        matched_terms = [t for t in terms if t.lower() in body_text.lower()]

        body_preview = body_text[:280] + ("…" if len(body_text) > 280 else "")

        state_or_electorate = _safe_str(row.get("state")) or _safe_str(row.get("electorate"))

        results.append({
            "match_id": match_id,
            "date": dt,
            "chamber": ch,
            "name": _safe_str(row.get("name")),
            "name_id": _safe_str(row.get("name_id")),
            "party": _safe_str(row.get("party")),
            "state_or_electorate": state_or_electorate,
            "gender": _safe_str(row.get("gender")),
            "in_gov": _safe_int(row.get("in_gov")),
            "is_question": _safe_int(row.get("question")),
            "is_answer": _safe_int(row.get("answer")),
            "is_interject": _safe_int(row.get("interject")),
            "first_speech": _safe_int(row.get("first_speech")),
            "has_embedded_interject": _safe_bool(row.get("has_embedded_interject")),
            "body": body_text,
            "body_preview": body_preview,
            "matched_terms": matched_terms,
        })

    _t4 = time.perf_counter()
    print(
        f"[search timing] body={_t1-_t0:.3f}s scan={_t2-_t1:.3f}s "
        f"meta={_t3-_t2:.3f}s assemble={_t4-_t3:.3f}s total={_t4-_t0:.3f}s",
        flush=True,
    )

    return jsonify({
        "total": total,
        "senate_count": senate_count,
        "house_count": house_count,
        "filtered_rows": filtered_len,
        "original_rows": orig_len,
        "page": page,
        "page_size": page_size,
        "pages": pages,
        "timeseries": timeseries,
        "corpus_seeds": corpus_seeds,
        "result_nids":       result_nids,
        "result_parties":    result_parties,
        "result_parl_nos":   result_parl_nos,
        "state_counts":      state_counts,
        "electorate_counts": electorate_counts,
        "facet_counts":      facet_counts,
        "results": results,
    })

    _log({"event": "search", "expression": expression, "chamber": chamber,
          "filters": filters, "total": total,
          "senate_count": senate_count, "house_count": house_count})


@app.route("/api/day_context", methods=["POST"])
@login_required
def day_context():
    try:
        body = request.get_json(force=True, silent=True) or {}
        date_str = (body.get("date") or "").strip()
        chamber  = (body.get("chamber") or "senate").strip()
        if not date_str or chamber not in ("senate", "house"):
            return jsonify({"speeches": []}), 400

        df = _get_corpus(chamber)
        if df.empty:
            return jsonify({"speeches": []})

        day_rows = df[df["date"] == date_str].sort_values("order").copy()
        if day_rows.empty:
            return jsonify({"speeches": []})

        day_rows["body"] = _load_body_for_display(day_rows)

        speeches = []
        for _, row in day_rows.iterrows():
            ord_val = _safe_str(row.get("order", ""))
            speeches.append({
                "match_id":            f"{chamber}-{date_str}-{ord_val}",
                "order":               _safe_int(row.get("order")),
                "name":                _safe_str(row.get("name")),
                "gender":              _safe_str(row.get("gender")),
                "party":               _safe_str(row.get("party")),
                "state_or_electorate": _safe_str(row.get("state")) or _safe_str(row.get("electorate")),
                "in_gov":              _safe_int(row.get("in_gov")),
                "is_question":         _safe_int(row.get("question")),
                "is_answer":           _safe_int(row.get("answer")),
                "is_interject":        _safe_int(row.get("interject")),
                "body":                _safe_str(row.get("body")),
                "time_stamp":          _safe_str(row.get("time_stamp")),
                "time_est":            bool(row.get("time_est", False)),
            })

        return jsonify({"date": date_str, "chamber": chamber, "speeches": speeches})
    except Exception as e:
        app.logger.exception("day_context error")
        return jsonify({"error": "Failed to load day context", "detail": str(e)}), 500


_alias_cache: dict = {}  # cache_key -> list[str]


@app.route("/api/suggest_aliases", methods=["POST"])
@login_required
def suggest_aliases():
    """Use Claude to suggest alternative search terms, seeded by corpus co-occurrence."""
    import anthropic
    body = request.get_json(force=True, silent=True) or {}
    term = (body.get("term") or "").strip()
    seeds = [str(s) for s in (body.get("seeds") or []) if s][:20]
    if not term:
        return jsonify({"error": "No term supplied"}), 400

    # Cache key includes seeds so context-specific results are stored separately
    cache_key = term.lower() + "|" + ",".join(sorted(seeds))
    if cache_key in _alias_cache:
        _log({"event": "aliases", "term": term, "source": "cache", "aliases": _alias_cache[cache_key]})
        return jsonify({"aliases": _alias_cache[cache_key]})

    # Filter seeds: drop any that contain the original term and are longer (redundant superstrings)
    term_lower = term.lower()
    term_words = len(term.split())
    seeds = [s for s in seeds
             if not (term_lower in s.lower() and len(s.split()) > term_words)]

    seed_context = ""
    if seeds:
        seed_context = (
            f"\n\nThe following terms and phrases frequently appear in the parliamentary speeches "
            f"that matched this search query: {', '.join(repr(s) for s in seeds)}. "
            f"Use these as evidence of the actual language used in this corpus to inform your suggestions, "
            f"but also suggest broader related terms the researcher might want to explore."
        )

    try:
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            temperature=0,
            system=(
                "You are helping researchers search Australian Parliamentary Hansard debates (1998–2025). "
                "Given a search phrase, return alternative phrasings, abbreviations, acronyms, and synonyms "
                "that might appear in parliamentary speech. Focus on terms actually used in Australian political "
                "and parliamentary language. "
                "Important: do NOT suggest phrases that contain the original search term as a substring — "
                "for example, if the search phrase is 'climate change', do not suggest 'climate change policy' "
                "or 'on climate change', because any speech containing those phrases already contains the "
                "original term and would already be captured by it. Only suggest genuinely different terms. "
                "Respond with a JSON array of strings only — no explanation, no markdown, no extra text."
                + seed_context
            ),
            messages=[{"role": "user", "content": f"Search phrase: {term}"}],
        )
    except Exception as api_err:
        _log({"event": "aliases_error", "term": term, "error": str(api_err)})
        return jsonify({"aliases": [], "warning": "Suggestions unavailable"}), 200
    import json as _json, re as _re
    raw = msg.content[0].text.strip()
    # Strip markdown code fences if present
    raw = _re.sub(r"^```(?:json)?\s*", "", raw)
    raw = _re.sub(r"\s*```$", "", raw.strip())
    # Truncate to the closing bracket in case max_tokens cut it off
    if "[" in raw:
        raw = raw[:raw.rfind("]") + 1] if "]" in raw else raw
    try:
        aliases = _json.loads(raw)
        if not isinstance(aliases, list):
            aliases = []
    except Exception:
        aliases = []
    # Deduplicate, drop the original term, drop redundant superstrings, limit to 8
    term_lower = term.lower()
    term_words = len(term.split())
    seen = set()
    clean = []
    for a in aliases:
        a = str(a).strip()
        lo = a.lower()
        # Skip if same as original
        if not a or lo == term_lower or lo in seen:
            continue
        # Skip if alias contains the original term and has more words —
        # any match would already be captured by the original search term
        if term_lower in lo and len(a.split()) > term_words:
            continue
        seen.add(lo)
        clean.append(a)
        if len(clean) >= 8:
            break
    _alias_cache[cache_key] = clean
    _log({"event": "aliases", "term": term, "source": "sonnet", "aliases": clean})
    return jsonify({"aliases": clean})


@app.route("/api/download", methods=["POST"])
@login_required
def download():
    """Return CSV of all matches (no pagination)."""
    try:
        body = request.get_json(force=True, silent=True) or {}

        expression = body.get("expression", "").strip()
        chamber = body.get("chamber", "both")
        filters = body.get("filters") or {}

        if not expression:
            return jsonify({"error": "expression is required"}), 400

        expression = _normalise_expression(expression)
        try:
            tree = parse_expression(expression)
        except SyntaxError as e:
            return jsonify({"error": f"Expression syntax error: {e}"}), 400

        df = _get_corpus(chamber)
        if df.empty:
            csv_str = "date,chamber,name,party,state_or_electorate,in_gov,is_question,is_answer,is_interject,first_speech,body,name_id,unique_id,partyfacts_id,matched_terms\n"
            return Response(csv_str, mimetype="text/csv",
                            headers={"Content-Disposition": "attachment; filename=hansard_matches.csv"})

        df = _apply_filters(df, filters)

        if not df.empty:
            terms  = collect_terms(tree)
            body_s = _get_body_col(chamber).reindex(df.index).fillna("")
            masks  = _build_masks(pd.DataFrame({"body": body_s}), terms, False)
            mask   = _eval_tree(tree, masks)
            matched = df[mask].copy()
            matched["body"] = body_s[mask].values
        else:
            matched = df.copy()
            terms = collect_terms(tree)

        sort_cols = [c for c in ["date", "order"] if c in matched.columns]
        if sort_cols:
            matched = matched.sort_values(sort_cols)

        matched = matched.head(10)

        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow([
            "date", "chamber", "name", "party", "state_or_electorate",
            "in_gov", "is_question", "is_answer", "is_interject", "first_speech",
            "body", "name_id", "unique_id", "partyfacts_id", "matched_terms",
        ])

        for _, row in matched.iterrows():
            body_text = _safe_str(row.get("body", ""))
            matched_terms = "|".join(t for t in terms if t.lower() in body_text.lower())
            state_or_electorate = _safe_str(row.get("state")) or _safe_str(row.get("electorate"))
            writer.writerow([
                _safe_str(row.get("date")),
                _safe_str(row.get("chamber")),
                _safe_str(row.get("name")),
                _safe_str(row.get("party")),
                state_or_electorate,
                _safe_int(row.get("in_gov")),
                _safe_int(row.get("question")),
                _safe_int(row.get("answer")),
                _safe_int(row.get("interject")),
                _safe_int(row.get("first_speech")),
                body_text,
                _safe_str(row.get("name_id")),
                _safe_str(row.get("unique_id")),
                _safe_str(row.get("partyfacts_id")),
                matched_terms,
            ])

        csv_str = out.getvalue()
        return Response(
            csv_str,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=hansard_matches.csv"},
        )
    except SyntaxError:
        raise
    except Exception as e:
        app.logger.exception("Download error")
        return jsonify({"error": "Download failed", "detail": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, host="0.0.0.0", port=5000)
