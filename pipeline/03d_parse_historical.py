"""
03d_parse_historical.py — Parse pre-1998 Hansard XML from wragge/hansard-xml.

Two eras handled:

  Era A — 1901-1980 (lowercase v2.1 XML):
    Root tag: <hansard>  version="2.0" or "2.1"
    File naming: YYYYMMDD_reps_NN_horNN_vN.xml
    Strategy: delegate to existing parse_sitting_day_house / parse_sitting_day

  Era B — 1981-1997 (uppercase XML):
    Root tag: <HANSARD DATE="21/02/1985" ...>
    File naming: reps_YYYY-MM-DD.xml / senate_YYYY-MM-DD.xml
    Outer elements uppercase: DEBATE, DEBATE.SUB1-3, SPEECH, INTERJECT, PARA
    Inner attribution still lowercase: talk.start / talker / name / name.id
    Speeches: <SPEECH NAMEID="5J4" PARTY="ALP" ELECTORATE="CORIO" GOV="1">
    Interjections: <INTERJECT NAMEID="WD4" SPEAKER="Ms Fatin" CHAIR="0">

Output schema matches existing parsers (date inferred from filename):
  House:  date, name, order, speech_no, page_no, time_stamp, name_id,
          electorate, party, in_gov, first_speech, body, question, answer,
          q_in_writing, div_flag, gender, unique_id, interject,
          fedchamb_flag, partyfacts_id
  Senate: (same but electorate→state, fedchamb_flag→senate_flag)

Usage:
    # Parse a single file (chamber inferred from filename)
    python 03d_parse_historical.py --xml ../data/raw/historical/house/uppercase/reps_1985-02-21.xml

    # Batch: all files in a directory
    python 03d_parse_historical.py \\
        --xml-dir ../data/raw/historical/house/uppercase \\
        --out-dir ../data/output/historical/house/daily_raw \\
        --chamber house

    python 03d_parse_historical.py \\
        --xml-dir ../data/raw/historical/house/v21 \\
        --out-dir ../data/output/historical/house/daily_raw \\
        --chamber house
"""

import argparse
import importlib.util
import re
import sys
from pathlib import Path

import pandas as pd
from lxml import etree
from tqdm import tqdm

# ── Import existing parsers for Era A (1901-1980) ─────────────────────────────

_PIPELINE = Path(__file__).parent
sys.path.insert(0, str(_PIPELINE))

# ── Output schemas ─────────────────────────────────────────────────────────────

HOUSE_COLUMNS = [
    "date", "name", "order", "speech_no", "page_no", "time_stamp",
    "name_id", "electorate", "party", "in_gov", "first_speech",
    "body", "question", "answer", "q_in_writing", "div_flag",
    "gender", "unique_id", "interject", "fedchamb_flag", "partyfacts_id",
]

SENATE_COLUMNS = [
    "date", "name", "order", "speech_no", "page_no", "time_stamp",
    "name_id", "state", "party", "in_gov", "first_speech",
    "body", "question", "answer", "q_in_writing", "div_flag",
    "gender", "unique_id", "interject", "senate_flag", "partyfacts_id",
]

# ── Uppercase-era constants ────────────────────────────────────────────────────

# Elements that wrap debate sub-sections (any depth) — recurse into these
_DEBATE_TAGS = {
    "DEBATE", "DEBATE.SUB1", "DEBATE.SUB2", "DEBATE.SUB3",
    # Questions
    "QWN", "QUESTION.BLOCK", "ANSWER.TO.QON", "ANSWER.TO.QWN",
    "ANSWERS.TO.QUESTIONS", "QTS",
    # Other containers
    "ADJOURNMENT", "PETITION.GRP", "PRESENTER.BLOCK", "PETITION",
    "SSO", "RDI", "RDI.ITEM", "MAINCOMM.XSCRIPT",
    # 1992+ format: PROCTEXT wraps speech elements
    "PROCTEXT",
}
# All containers to recurse into when walking (superset of _DEBATE_TAGS)
# PARA is separate: we recurse into it during walking, but still extract body text from it
_WALK_CONTAINERS = _DEBATE_TAGS | {"PARA"}
# Elements that hold spoken content
_SPEECH_TAGS = {"SPEECH", "INTERJECT", "QUESTION", "ANSWER", "PRESENTER"}
# Procedural patterns — rows with only these get dropped
_PROCEDURAL = re.compile(
    r"^(The (?:Senate|House) (adjourned|met) at|"
    r"Sitting (suspended|resumed)|"
    r"The (PRESIDENT|CHAIR|SPEAKER|DEPUTY PRESIDENT) (took the chair|resumed the chair)|"
    r"Question (agreed to|negatived|put)|"
    r"[A-Z ]+\s+\d+)$",
    re.IGNORECASE,
)
_DATE_DMY = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def _date_from_hansard_root(root) -> str | None:
    """Parse DATE attribute from <HANSARD DATE="21/02/1985">."""
    raw = root.get("DATE", "").strip()
    m = _DATE_DMY.match(raw)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    return None


def _date_from_filename(xml_path: Path) -> str:
    """
    Extract YYYY-MM-DD from filename.
      reps_1985-02-21.xml      → 1985-02-21
      19750211_reps_...xml     → 1975-02-11
    """
    stem = xml_path.stem
    m = re.match(r"^(?:reps|senate)_(\d{4}-\d{2}-\d{2})$", stem)
    if m:
        return m.group(1)
    m = re.match(r"^(\d{8})", stem)
    if m:
        d = m.group(1)
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return stem


def _all_text(el) -> str:
    return " ".join("".join(el.itertext()).split())


def _para_text(node) -> str:
    """Collect text from all <PARA> children (not recursing into sub-speeches)."""
    parts = []
    for child in node:
        if child.tag == "PARA":
            t = _all_text(child).strip()
            if t:
                parts.append(t)
        # Skip TALK.START (already extracted inline para), TIME.STAMP, DIVISION etc.
    return " ".join(parts).strip()


def _shallow_para_text(para) -> str:
    """
    Collect text from a PARA element but skip any nested speech/debate children.
    This prevents body text from including text that belongs to nested speeches.
    """
    parts = []
    if para.text:
        t = para.text.strip()
        if t:
            parts.append(t)
    for child in para:
        if child.tag in _SPEECH_TAGS or child.tag in _DEBATE_TAGS:
            # Don't include text from nested speech/debate elements
            pass
        else:
            t = "".join(child.itertext()).strip()
            if t:
                parts.append(t)
        if child.tail:
            t = child.tail.strip()
            if t:
                parts.append(t)
    return " ".join(parts).strip()


def _parse_talker_upper(node) -> dict:
    """
    Extract speaker metadata from a <SPEECH> or <INTERJECT> element.

    Primary: attributes on the element itself (NAMEID, PARTY, ELECTORATE/STATE, GOV)
    Secondary: inner <talk.start>/<talker>/<name> lowercase structure
    """
    name_id  = (node.get("NAMEID") or "").strip().upper() or None
    party    = (node.get("PARTY") or "").strip() or None
    electorate = (node.get("ELECTORATE") or node.get("STATE") or "").strip() or None
    in_gov   = 1 if node.get("GOV", "0") == "1" else 0
    name     = (node.get("SPEAKER") or "").strip() or None

    # Try inner talk.start/talker structure for display name
    talk_start = node.find("talk.start")
    if talk_start is not None:
        talker = talk_start.find("talker")
        if talker is not None:
            name_el = talker.find("name")
            if name_el is not None and name_el.text:
                name = name_el.text.strip() or name
            # Also try name.id as fallback
            nid_el = talker.find("name.id")
            if nid_el is not None and nid_el.text:
                nid = nid_el.text.strip().upper()
                if nid and nid not in ("", "NONE", "NAN"):
                    name_id = name_id or nid
            # Electorate/state from talker
            for tag in ("electorate", "state"):
                el = talker.find(tag)
                if el is not None and el.text:
                    electorate = electorate or el.text.strip()
            # Party from talker
            party_el = talker.find("party")
            if party_el is not None and party_el.text:
                party = party or party_el.text.strip()

    if name_id in ("", "NONE", "NAN"):
        name_id = None

    return {
        "name": name,
        "name_id": name_id,
        "electorate": electorate,
        "party": party,
        "in_gov": in_gov,
    }


def _extract_body_upper(node) -> str:
    """
    Collect body text from a <SPEECH> or <INTERJECT> node.

    - Skip <TALK.START> first child (talker metadata + inline para handled separately)
    - Collect <PARA> text
    - Collect inline para from TALK.START if present
    """
    parts = []

    talk_start = node.find("talk.start")
    if talk_start is not None:
        # Inline para: <PARA IN-LINE="1"> inside talk.start
        for para in talk_start:
            if para.tag == "PARA":
                t = _all_text(para).strip()
                # Strip leading em-dash artifact
                t = re.sub(r"^[—–\-]+\s*", "", t)
                if t:
                    parts.append(t)

    # Remaining children of SPEECH (after TALK.START)
    for child in node:
        if child.tag in ("talk.start", "TALK.START"):
            continue
        if child.tag == "PARA":
            # Use shallow text to avoid including nested speech elements' text
            t = _shallow_para_text(child)
            if t:
                parts.append(t)
        elif child.tag in _SPEECH_TAGS:
            # Nested speech — will be extracted as its own row
            pass
        elif child.tag in _DEBATE_TAGS:
            # Nested debate container — speeches inside will be separate rows
            pass
        elif child.tag == "TIME.STAMP":
            pass  # skip
        elif child.tag == "DIVISION":
            pass  # could mark div_flag later

    return " ".join(parts).strip()


def _extract_time_upper(node) -> str | None:
    """Get timestamp from <TIME.STAMP> child."""
    ts = node.find("TIME.STAMP")
    if ts is not None and ts.text:
        return ts.text.strip()
    return None


def _get_page(node) -> str | None:
    pg = node.get("PAGE") or node.get("page")
    return pg.strip() if pg else None


def _walk_upper(root) -> list[dict]:
    """
    Recursively walk uppercase-era XML tree and yield raw speech row dicts.
    Handles nested DEBATE, DEBATE.SUB1-3, SPEECH, INTERJECT elements.
    """
    rows = []

    def _visit(node, depth=0):
        tag = node.tag
        if tag in _WALK_CONTAINERS:
            for child in node:
                _visit(child, depth + 1)

        elif tag in ("SPEECH", "QUESTION", "ANSWER", "PRESENTER"):
            talker = _parse_talker_upper(node)
            body   = _extract_body_upper(node)
            ts     = _extract_time_upper(node)
            page   = _get_page(node)
            is_div = 1 if node.find("DIVISION") is not None else 0
            is_q   = 1 if tag == "QUESTION" else 0
            is_a   = 1 if tag == "ANSWER" else 0
            # Questions on notice (inside ANSWER.TO.QON) are written
            parent_tag = node.getparent().tag if node.getparent() is not None else ""
            is_qw  = 1 if is_q and "QON" in parent_tag else 0

            rows.append({
                **talker,
                "body":         body,
                "time_stamp":   ts,
                "page_no":      page,
                "div_flag":     is_div,
                "interject":    0,
                "question":     is_q,
                "answer":       is_a,
                "q_in_writing": is_qw,
            })
            # Recurse into children that may contain nested speeches/debates
            for child in node:
                if child.tag in _SPEECH_TAGS or child.tag in _WALK_CONTAINERS:
                    _visit(child, depth + 1)

        elif tag == "INTERJECT":
            talker = _parse_talker_upper(node)
            body   = _extract_body_upper(node)
            ts     = _extract_time_upper(node)
            page   = _get_page(node)
            chair  = node.get("CHAIR", "0") == "1"

            rows.append({
                **talker,
                "body":         body,
                "time_stamp":   ts,
                "page_no":      page,
                "div_flag":     0,
                "interject":    0 if chair else 1,
                "question":     0,
                "answer":       0,
                "q_in_writing": 0,
            })
            # Recurse into children that may contain nested speeches
            for child in node:
                if child.tag in _SPEECH_TAGS or child.tag in _WALK_CONTAINERS:
                    _visit(child, depth + 1)

        elif tag in ("CHAMBER.XSCRIPT", "BUSINESS.START"):
            for child in node:
                _visit(child, depth)

    for child in root:
        _visit(child)

    return rows


def _assign_order(rows: list[dict]) -> list[dict]:
    for i, r in enumerate(rows):
        r["order"] = i + 1
    return rows


def _assign_speech_numbers(rows: list[dict]) -> list[dict]:
    speech_no = 0
    for r in rows:
        if not r.get("interject"):
            speech_no += 1
        r["speech_no"] = speech_no
    return rows


def _clean_to_output(rows: list[dict], date_str: str,
                     chamber: str) -> pd.DataFrame:
    columns = HOUSE_COLUMNS if chamber == "house" else SENATE_COLUMNS
    geo_col = "electorate" if chamber == "house" else "state"
    flag_col = "fedchamb_flag" if chamber == "house" else "senate_flag"

    records = []
    for r in rows:
        body = r.get("body") or ""
        if not body or not r.get("name_id") and not r.get("name"):
            if not body:
                continue  # drop empty body rows

        uid = f"{date_str}_{r.get('order', 0):05d}"
        records.append({
            "date":          date_str,
            "name":          r.get("name"),
            "order":         r.get("order"),
            "speech_no":     r.get("speech_no"),
            "page_no":       r.get("page_no"),
            "time_stamp":    r.get("time_stamp"),
            "name_id":       r.get("name_id"),
            geo_col:         r.get("electorate"),
            "party":         r.get("party"),
            "in_gov":        r.get("in_gov", 0),
            "first_speech":  0,
            "body":          body,
            "question":      r.get("question", 0),
            "answer":        r.get("answer", 0),
            "q_in_writing":  r.get("q_in_writing", 0),
            "div_flag":      r.get("div_flag", 0),
            "gender":        None,
            "unique_id":     uid,
            "interject":     r.get("interject", 0),
            flag_col:        0,
            "partyfacts_id": None,
        })

    df = pd.DataFrame(records, columns=columns)
    return df


# ── Era A (1901-1980): delegate to existing parsers ───────────────────────────

def _parse_era_a(xml_path: Path, chamber: str) -> pd.DataFrame:
    """
    Delegate 1901-1980 files to the existing v2.1 parsers.
    These files use lowercase tags identical to post-1998 v2.1.
    """
    if chamber == "house":
        from pipeline_03b import parse_sitting_day_house  # type: ignore
        return parse_sitting_day_house(xml_path)
    else:
        from pipeline_03 import parse_sitting_day  # type: ignore
        return parse_sitting_day(xml_path)


# ── Era B (1981-1997): uppercase parser ───────────────────────────────────────

def parse_historical_upper(xml_path: Path, chamber: str) -> pd.DataFrame:
    """Parse a single 1981-1997 uppercase-format XML file."""
    columns = HOUSE_COLUMNS if chamber == "house" else SENATE_COLUMNS
    date_str = _date_from_filename(xml_path)

    try:
        parser = etree.XMLParser(recover=True)
        tree   = etree.parse(str(xml_path), parser)
        root   = tree.getroot()
    except Exception as e:
        print(f"  [ERROR] XML parse failed {xml_path.name}: {e}")
        return pd.DataFrame(columns=columns)

    # Verify this is uppercase format
    if root.tag != "HANSARD":
        print(f"  [WARN] Expected uppercase <HANSARD> but got <{root.tag}> in "
              f"{xml_path.name} — treating as v2.1, skipping")
        return pd.DataFrame(columns=columns)

    # Try to get date from XML attribute (more reliable than filename for old files)
    xml_date = _date_from_hansard_root(root)
    if xml_date:
        date_str = xml_date

    rows = _walk_upper(root)
    rows = _assign_order(rows)
    rows = _assign_speech_numbers(rows)
    return _clean_to_output(rows, date_str, chamber)


# ── Auto-detect era ────────────────────────────────────────────────────────────

def parse_historical(xml_path: Path, chamber: str) -> pd.DataFrame:
    """
    Parse any pre-1998 XML file. Detects era from root element tag.
    """
    columns = HOUSE_COLUMNS if chamber == "house" else SENATE_COLUMNS

    try:
        # Peek at root tag only
        parser = etree.XMLParser(recover=True)
        tree   = etree.parse(str(xml_path), parser)
        root   = tree.getroot()
    except Exception as e:
        print(f"  [ERROR] {xml_path.name}: {e}")
        return pd.DataFrame(columns=columns)

    if root.tag == "HANSARD":
        # Era B: 1981-1997 uppercase
        return parse_historical_upper(xml_path, chamber)
    elif root.tag == "hansard":
        # Era A: 1901-1980 lowercase v2.1
        # Delegate to existing parsers if they can be imported
        try:
            if chamber == "house":
                spec = importlib.util.spec_from_file_location(
                    "parse_house", _PIPELINE / "03b_parse_house.py")
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return mod.parse_sitting_day_house(xml_path)
            else:
                spec = importlib.util.spec_from_file_location(
                    "parse_senate", _PIPELINE / "03_parse.py")
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return mod.parse_sitting_day(xml_path)
        except Exception as e:
            print(f"  [ERROR] Era-A delegation failed for {xml_path.name}: {e}")
            return pd.DataFrame(columns=columns)
    else:
        print(f"  [WARN] Unrecognised root <{root.tag}> in {xml_path.name}")
        return pd.DataFrame(columns=columns)


# ── Batch ─────────────────────────────────────────────────────────────────────

def parse_all_historical(
    xml_dir: Path,
    out_dir: Path,
    chamber: str,
    skip_existing: bool = True,
) -> None:
    xml_files = sorted(xml_dir.glob("*.xml"))
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Parsing {len(xml_files)} historical {chamber} XML files → {out_dir}")

    n_ok = n_empty = n_err = 0
    for xml_path in tqdm(xml_files, desc="Parsing"):
        stem = _date_from_filename(xml_path)
        out_csv     = out_dir / f"{stem}.csv"
        out_parquet = out_dir / f"{stem}.parquet"
        if skip_existing and out_csv.exists() and out_parquet.exists():
            n_ok += 1
            continue
        try:
            df = parse_historical(xml_path, chamber)
        except Exception as e:
            print(f"  [ERROR] {xml_path.name}: {e}")
            n_err += 1
            continue

        if df.empty:
            print(f"  [WARN] Empty: {xml_path.name}")
            n_empty += 1
            continue

        df.to_csv(out_csv, index=False)
        df.to_parquet(out_parquet, index=False)
        n_ok += 1

    print(f"\nDone. OK: {n_ok}  Empty: {n_empty}  Errors: {n_err}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xml", help="Single XML file to parse")
    ap.add_argument("--xml-dir", help="Directory of XML files (batch mode)")
    ap.add_argument("--out-dir", default=".", help="Output directory for batch mode")
    ap.add_argument("--chamber", choices=["house", "senate"], default="house",
                    help="Chamber (inferred from filename if possible)")
    ap.add_argument("--no-skip", action="store_true",
                    help="Overwrite existing output files")
    args = ap.parse_args()

    if args.xml:
        xml_path = Path(args.xml)
        # Infer chamber from filename
        chamber = args.chamber
        if xml_path.name.startswith("senate_") or "senate" in xml_path.parts:
            chamber = "senate"
        elif xml_path.name.startswith("reps_") or "hofreps" in xml_path.parts:
            chamber = "house"

        df = parse_historical(xml_path, chamber)
        print(f"Rows: {len(df)}")
        print(df.head(10).to_string())
        if not df.empty:
            stem = _date_from_filename(xml_path)
            out = Path(args.out_dir) / f"{stem}.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(out, index=False)
            df.to_csv(out.with_suffix(".csv"), index=False)
            print(f"Written → {out}")

    elif args.xml_dir:
        parse_all_historical(
            xml_dir=Path(args.xml_dir),
            out_dir=Path(args.out_dir),
            chamber=args.chamber,
            skip_existing=not args.no_skip,
        )
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
