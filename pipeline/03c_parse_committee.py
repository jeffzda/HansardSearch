"""
03c_parse_committee.py — Parse Australian Committee Hansard XML files.

Input:  XML files in data/raw/committee/ named {date}_{dataset}_{docid}.xml
Output: Per-file parquet in data/output/committee/daily_raw/

Two structural variants exist within the same <committee version="2.2"> root:

  1. Hearing format (commsen, commrep, commjnt, commbill):
       <committee>
         <committeeinfo>...</committeeinfo>
         <discussion>          ← one per witness panel
           <witness.group>...</witness.group>
           <talk.text><body>...</body></talk.text>
         </discussion>
         ...
       </committee>

  2. Estimates format (estimate):
       <committee>
         <committeeinfo>...</committeeinfo>
         <chamber.xscript>
           <debate>            ← one per portfolio
             <debateinfo><title>...</title></debateinfo>
             <debate.text><body>...</body></debate.text>
           </debate>
           ...
         </chamber.xscript>
       </committee>

Both formats use the same HPS v2.2 body markup for utterances. Speaker
attribution is by span class and anchor href:

  Members (senators/MPs on the committee):
    <a href="PHID" type="MemberContinuation">
      <span class="HPS-MemberContinuation">Senator NAME:</span>
    </a>
    <a href="PHID" type="MemberWitness">   ← public servant who is also an MP
      <span class="HPS-MemberWitness">...</span>
    </a>

  Committee roles without anchor:
    <span class="HPS-OfficeCommittee">CHAIR:</span>
    <span class="HPS-DeputyChair">DEPUTY CHAIR:</span>

  External witnesses:
    <span class="HPS-WitnessName">Mr SURNAME</span>
    <span class="HPS-GeneralBold">:</span>

  Witness introduction lines (skip — not speech):
    <p class="HPS-StartWitness">...</p>

Output schema (22 columns):

  date             — from committeeinfo/date (YYYY-MM-DD)
  name             — speaker display name
  order            — sequential row number within the file
  speech_no        — sequential utterance number within the current panel/debate
  panel_no         — 1-indexed discussion panel (hearing format) or
                     1-indexed debate block (estimates); NULL not used
  page_no          — from page.no if available (often 0)
  name_id          — PHID from <a href> (for members), NULL for witnesses
  party            — filled by 04c_fill_committee.py from lookup
  in_gov           — filled by 04c_fill_committee.py
  body             — speech text
  witness_flag     — 1 if external witness, 0 if member/chair
  gender           — filled by 04c_fill_committee.py
  unique_id        — filled by 04c_fill_committee.py
  partyfacts_id    — filled by 04c_fill_committee.py
  committee_name   — from committeeinfo/comm.name
  committee_chamber— from committeeinfo/chamber (Senate/Reps/Joint)
  hearing_type     — derived from filename dataset code
  reference        — from committeeinfo/reference (inquiry title or "Estimates")
  portfolio        — debate title for estimates; NULL for hearing format

Usage:
    python 03c_parse_committee.py --xml-dir ../data/raw/committee \\
        --out-dir ../data/output/committee/daily_raw

    # Single file:
    python 03c_parse_committee.py \\
        --xml-dir ../data/raw/committee \\
        --out-dir ../data/output/committee/daily_raw \\
        --file 2024-03-01_commsen_27718.xml
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
from lxml import etree
from tqdm import tqdm
from parallel_utils import eager_map

# ── Parallel worker state ─────────────────────────────────────────────────────

_WORKER_OUT_DIR: Path | None = None
_WORKER_NO_SKIP: bool = False


def _init_worker(args_tuple: tuple) -> None:
    """Per-process initializer: store output dir and no-skip flag in globals."""
    global _WORKER_OUT_DIR, _WORKER_NO_SKIP
    _WORKER_OUT_DIR, _WORKER_NO_SKIP = args_tuple


def _parse_one(xml_path: Path) -> str:
    """
    Worker function: parse one committee XML file and write Parquet output.
    Returns a status string: 'ok', 'skipped', 'empty', or 'error'.
    """
    out_path = _WORKER_OUT_DIR / xml_path.with_suffix(".parquet").name

    if not _WORKER_NO_SKIP and out_path.exists():
        return "skipped"

    try:
        df = _parse_file(xml_path)
    except Exception as e:
        print(f"  [ERROR] {xml_path.name}: {e}")
        return "error"

    if df.empty:
        return "empty"

    df.to_parquet(out_path, index=False)
    return "ok"

# ── Output schema ─────────────────────────────────────────────────────────────

OUTPUT_COLUMNS = [
    "date", "name", "order", "speech_no", "panel_no", "page_no",
    "name_id", "party", "in_gov",
    "body", "witness_flag",
    "gender", "unique_id", "partyfacts_id",
    "committee_name", "committee_chamber", "hearing_type",
    "reference", "portfolio",
]

# ── Constants ─────────────────────────────────────────────────────────────────

# Anchor types that introduce a new member speaker (with PHID)
MEMBER_ANCHOR_TYPES = {"MemberContinuation", "MemberWitness"}

# Span classes that are speaker attributions (not body text)
ATTRIBUTION_SPAN_CLASSES = {
    "HPS-MemberContinuation",
    "HPS-MemberWitness",
    "HPS-OfficeCommittee",
    "HPS-DeputyChair",
    "HPS-WitnessName",
    "HPS-Electorate",
    "HPS-Time",
    "HPS-GeneralBold",   # typically just ":" after a name — skip
    "HPS-StartWitness",
}

# HPS-OfficeCommittee span classes (no anchor — need context resolution)
OFFICE_CLASSES = {"HPS-OfficeCommittee", "HPS-DeputyChair"}

# Paragraph classes to skip entirely
SKIP_P_CLASSES = {"HPS-StartWitness", "HPS-Small"}


# ── Text helpers ──────────────────────────────────────────────────────────────

def _all_text(el: etree._Element) -> str:
    """Concatenate all text within an element (text + tails of descendants)."""
    return "".join(el.itertext())


def _normalise_time(s: str) -> str | None:
    """Convert time strings to HH:MM:SS or return None."""
    if not s:
        return None
    s = s.strip("() :")
    m = re.match(r"^(\d{1,2})[:\.](\d{2})(?:[:\.](\d{2}))?$", s)
    if m:
        h, mn, sec = m.group(1), m.group(2), m.group(3) or "00"
        return f"{int(h):02d}:{mn}:{sec}"
    return None


# ── Body text extraction ──────────────────────────────────────────────────────

def _extract_body_text(p: etree._Element) -> str:
    """
    Extract speech body text from a <p> element, skipping speaker attribution
    spans and returning only the actual speech content.
    """
    parts = []

    def visit(el: etree._Element, skip_self: bool = False):
        cls = el.get("class", "")

        # Member anchor: skip the span inside but keep tail (speech after ":")
        if el.tag == "a" and el.get("type", "") in MEMBER_ANCHOR_TYPES:
            # tail is typically "  text of speech..."
            tail = (el.tail or "").lstrip(" ):—")
            if tail.strip():
                parts.append(tail.strip())
            return

        # Attribution spans: skip content but keep tail
        if cls in ATTRIBUTION_SPAN_CLASSES:
            tail = (el.tail or "").lstrip(" ):—")
            if tail.strip():
                parts.append(tail.strip())
            return

        # Normal span or p: include text and recurse
        if el.text and el.text.strip():
            parts.append(el.text.strip())
        for child in el:
            visit(child)
        if el.tail and el.tail.strip():
            parts.append(el.tail.strip())

    for child in p:
        visit(child)

    return " ".join(parts).strip()


# ── Speaker detection ─────────────────────────────────────────────────────────

def _detect_speaker(p: etree._Element) -> dict | None:
    """
    Inspect the first speaker-attribution elements in a <p class="HPS-Normal">.
    Returns a dict with keys: name, name_id, witness_flag, time_stamp
    or None if no new speaker is found (continuation paragraph).
    """
    # Check for member anchor (MemberContinuation, MemberWitness)
    for a in p.findall(".//a"):
        atype = a.get("type", "")
        if atype in MEMBER_ANCHOR_TYPES:
            href = a.get("href", "").strip().upper()
            name_id = href if href and href not in ("", "CHAMBER") else None

            # Name text from span inside anchor
            name_parts = []
            for span in a:
                if span.get("class", "") in ("HPS-MemberContinuation", "HPS-MemberWitness"):
                    text = _all_text(span).strip().rstrip(":").strip()
                    if text:
                        name_parts.append(text)

            # Also check for OfficeCommittee role after anchor:
            # CHAIR (Senator NAME):  →  anchor has CHAIR, sibling spans have name
            parent = p
            for span in parent.findall(".//span"):
                sc = span.get("class", "")
                if sc in OFFICE_CLASSES:
                    role_text = _all_text(span).strip().rstrip(":").strip()
                    if role_text and role_text not in name_parts:
                        name_parts.insert(0, role_text)

            # Time from HPS-Time sibling
            ts = None
            for span in parent.findall(".//span"):
                if "HPS-Time" in span.get("class", ""):
                    ts = _normalise_time(_all_text(span))
                    break

            name = " ".join(name_parts).strip() or None
            return {
                "name": name,
                "name_id": name_id,
                "witness_flag": 0,
                "time_stamp": ts,
            }

    # Check for OfficeCommittee (CHAIR: etc.) without anchor
    for span in p.findall(".//span"):
        sc = span.get("class", "")
        if sc in OFFICE_CLASSES:
            name = _all_text(span).strip().rstrip(":").strip()
            if name:
                return {
                    "name": name,
                    "name_id": None,
                    "witness_flag": 0,
                    "time_stamp": None,
                }

    # Check for WitnessName span
    for span in p.findall(".//span"):
        if span.get("class", "") == "HPS-WitnessName":
            name = _all_text(span).strip().rstrip(":").strip()
            if name:
                return {
                    "name": name,
                    "name_id": None,
                    "witness_flag": 1,
                    "time_stamp": None,
                }

    return None  # continuation paragraph


# ── Parse a body element into utterances ──────────────────────────────────────

def _parse_body(body: etree._Element) -> list[dict]:
    """
    Parse a <body> element and return a list of utterance dicts:
    {name, name_id, witness_flag, body, time_stamp}
    """
    utterances = []
    current: dict | None = None

    for p in body:
        if p.tag != "p":
            continue

        p_class = p.get("class", "")

        # Skip witness introduction and small/footnote paragraphs
        if p_class in SKIP_P_CLASSES:
            continue

        speaker = _detect_speaker(p)

        if speaker is not None:
            # Flush previous utterance
            if current is not None and current.get("_parts"):
                current["body"] = " ".join(current["_parts"]).strip()
                del current["_parts"]
                utterances.append(current)

            body_text = _extract_body_text(p)
            current = {
                **speaker,
                "_parts": [body_text] if body_text else [],
            }
        else:
            # Continuation — plain paragraph text
            body_text = _all_text(p).strip()
            if body_text and p_class not in ("HPS-Line", "HPS-SODJobDate"):
                if current is not None:
                    current["_parts"].append(body_text)
                # else: orphan text before first speaker — silently skip

    # Flush last utterance
    if current is not None and current.get("_parts"):
        current["body"] = " ".join(current["_parts"]).strip()
        del current["_parts"]
        utterances.append(current)

    return utterances


# ── v2.1 parser ───────────────────────────────────────────────────────────────
# v2.1 uses <talk>/<talker> speaker attribution and plain <para> body text
# (no HPS v2.2 span/anchor markup).  Two structural variants:
#   Hearings:  <discussion> blocks containing <talk> elements
#   Estimates: <portfolio> blocks containing <talk> elements

_TIME_STAMP_RE = re.compile(r"^\[?(\d{1,2}[.:]\d{2}(?:\s*[ap]m)?)\]?$", re.IGNORECASE)
_DASH_STRIP_RE = re.compile(r"^[\u2014\-\s]+")


def _v21_para_text(el: etree._Element) -> str:
    """Collect all text from a <para> element including <inline> and <quote> children."""
    return " ".join(el.itertext()).strip()


def _v21_is_timestamp(text: str) -> bool:
    """Return True if the para is just a time marker like '[10.30 am]'."""
    return bool(_TIME_STAMP_RE.match(text.strip("[] ")))


def _v21_extract_talk(talk: etree._Element) -> tuple[dict | None, list[str]]:
    """
    Extract (speaker_info, body_parts) from a v2.1 <talk> element.

    speaker_info keys: name, name_id, witness_flag, page_no
    body_parts: list of paragraph text strings
    """
    talk_start = talk.find("talk.start")
    if talk_start is None:
        return None, []

    talker = talk_start.find("talker")
    if talker is None:
        return None, []

    raw_name     = (talker.findtext("name") or "").strip()
    raw_name_id  = (talker.findtext("name.id") or "").strip()
    witness_data = (talker.findtext("witness.data") or "0").strip()
    page_no_raw  = (talker.findtext("page.no") or "").strip()

    # Resolve name_id: 10000 and unknown are not real PHIDs
    name_id = None
    if raw_name_id and raw_name_id not in ("10000", "unknown", "0", ""):
        name_id = raw_name_id

    witness_flag = 1 if witness_data == "1" else 0

    try:
        page_no = int(page_no_raw) if page_no_raw else None
    except ValueError:
        page_no = None

    speaker = {
        "name":         raw_name,
        "name_id":      name_id,
        "witness_flag": witness_flag,
        "page_no":      page_no,
    }

    # Collect body paragraphs: from talk.start (after talker) and from talk siblings
    parts: list[str] = []

    def _add_para(para: etree._Element) -> None:
        text = _v21_para_text(para)
        if not text:
            return
        # Strip leading em-dash
        text = _DASH_STRIP_RE.sub("", text).strip()
        if not text:
            return
        # Skip pure time-stamp paras
        if _v21_is_timestamp(text):
            return
        parts.append(text)

    for child in talk_start:
        if child.tag == "para":
            _add_para(child)

    for child in talk:
        if child.tag == "para":
            _add_para(child)

    return speaker, parts


def _v21_talks_to_rows(
    talks: list[etree._Element],
    doc_date: str,
    comm_name: str,
    comm_chamb: str,
    hearing_type: str,
    reference: str,
    portfolio: str | None,
    panel_no: int,
    order_counter: int,
) -> tuple[list[dict], int]:
    """Convert a list of v2.1 <talk> elements into output row dicts."""
    rows: list[dict] = []
    speech_counter = 0

    for talk in talks:
        speaker, parts = _v21_extract_talk(talk)
        if speaker is None or not parts:
            continue
        body = " ".join(parts).strip()
        if not body:
            continue

        order_counter += 1
        speech_counter += 1
        rows.append({
            "date":              doc_date,
            "name":              speaker["name"],
            "order":             order_counter,
            "speech_no":         speech_counter,
            "panel_no":          panel_no,
            "page_no":           speaker["page_no"],
            "name_id":           speaker["name_id"],
            "party":             None,
            "in_gov":            None,
            "body":              body,
            "witness_flag":      speaker["witness_flag"],
            "gender":            None,
            "unique_id":         None,
            "partyfacts_id":     None,
            "committee_name":    comm_name,
            "committee_chamber": comm_chamb,
            "hearing_type":      hearing_type,
            "reference":         reference,
            "portfolio":         portfolio,
        })

    return rows, order_counter


def _parse_file_v21(
    xml_path: Path,
    root: etree._Element,
    file_date: str,
    dataset_code: str,
) -> pd.DataFrame:
    """Parse a v2.1 committee XML file."""
    import re as _re
    hearing_type = "estimates" if dataset_code == "estimate" else "committee"

    ci = root.find("committeeinfo")
    if ci is None:
        print(f"  [WARN] No <committeeinfo> in {xml_path.name}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    _raw_date  = (ci.findtext("date") or "").strip()
    doc_date   = _raw_date if _re.match(r"^\d{4}-\d{2}-\d{2}$", _raw_date) else file_date
    comm_name  = (ci.findtext("comm.name") or "").strip()
    comm_chamb = (ci.findtext("chamber") or "").strip()
    reference  = (ci.findtext("reference") or "").strip()

    rows: list[dict] = []
    order_counter = 0

    # ── Hearing format: <discussion> blocks ───────────────────────────────
    discussions = root.findall("discussion")
    if discussions:
        # Also capture top-level <talk> elements before discussions
        top_talks = [c for c in root if c.tag == "talk"]
        if top_talks:
            new_rows, order_counter = _v21_talks_to_rows(
                top_talks, doc_date, comm_name, comm_chamb,
                hearing_type, reference, None, 0, order_counter,
            )
            rows.extend(new_rows)

        for panel_idx, disc in enumerate(discussions, start=1):
            talks = disc.findall("talk")
            new_rows, order_counter = _v21_talks_to_rows(
                talks, doc_date, comm_name, comm_chamb,
                hearing_type, reference, None, panel_idx, order_counter,
            )
            rows.extend(new_rows)
        return _finalise(rows)

    # ── Estimates format: <portfolio> blocks ───────────────────────────────
    portfolios = root.findall("portfolio")
    if portfolios:
        # Top-level talks before portfolios
        top_talks = [c for c in root if c.tag == "talk"]
        if top_talks:
            new_rows, order_counter = _v21_talks_to_rows(
                top_talks, doc_date, comm_name, comm_chamb,
                hearing_type, reference, None, 0, order_counter,
            )
            rows.extend(new_rows)

        for panel_idx, portfolio_el in enumerate(portfolios, start=1):
            pi = portfolio_el.find("portfolioinfo")
            portfolio_title = (pi.findtext("title") if pi is not None else None) or ""
            talks = portfolio_el.findall("talk")
            new_rows, order_counter = _v21_talks_to_rows(
                talks, doc_date, comm_name, comm_chamb,
                hearing_type, reference, portfolio_title or None,
                panel_idx, order_counter,
            )
            rows.extend(new_rows)
        return _finalise(rows)

    # Fallback: scan all <talk> elements at any depth
    all_talks = root.findall(".//talk")
    if all_talks:
        new_rows, order_counter = _v21_talks_to_rows(
            all_talks, doc_date, comm_name, comm_chamb,
            hearing_type, reference, None, 1, order_counter,
        )
        rows.extend(new_rows)
        return _finalise(rows)

    print(f"  [WARN] v2.1: no parseable structure in {xml_path.name}")
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


# ── File-level parsing ────────────────────────────────────────────────────────

def _parse_file(xml_path: Path) -> pd.DataFrame:
    """
    Parse a single committee XML file. Returns a DataFrame with OUTPUT_COLUMNS.
    Returns an empty DataFrame if the file cannot be parsed.
    """
    # Derive date, dataset, doc_id from filename: {date}_{dataset}_{docid}.xml
    stem = xml_path.stem
    parts = stem.split("_", 2)
    if len(parts) != 3:
        print(f"  [WARN] Unexpected filename format: {xml_path.name} — skipping")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    file_date, dataset_code, doc_id = parts
    hearing_type = "estimates" if dataset_code == "estimate" else "committee"

    try:
        tree = etree.parse(str(xml_path))
    except etree.XMLSyntaxError as e:
        print(f"  [WARN] XML parse error in {xml_path.name}: {e}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    root = tree.getroot()

    if root.tag != "committee":
        print(f"  [WARN] Unexpected root tag <{root.tag}> in {xml_path.name}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Dispatch to v2.1 parser if applicable
    version = root.get("version", "2.2")
    if version == "2.1":
        return _parse_file_v21(xml_path, root, file_date, dataset_code)

    # ── Extract metadata from <committeeinfo> ──────────────────────────────
    ci = root.find("committeeinfo")
    if ci is None:
        print(f"  [WARN] No <committeeinfo> in {xml_path.name}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    doc_date   = (ci.findtext("date") or file_date).strip()
    comm_name  = (ci.findtext("comm.name") or "").strip()
    comm_chamb = (ci.findtext("chamber") or "").strip()
    reference  = (ci.findtext("reference") or "").strip()

    rows: list[dict] = []
    order_counter = 0

    # ── Hearing format: multiple <discussion> blocks ────────────────────────
    discussions = root.findall("discussion")
    if discussions:
        for panel_idx, disc in enumerate(discussions, start=1):
            speech_counter = 0
            talk_text = disc.find("talk.text")
            if talk_text is None:
                continue
            body = talk_text.find("body")
            if body is None:
                continue

            utterances = _parse_body(body)
            for utt in utterances:
                if not utt.get("body", "").strip():
                    continue
                order_counter += 1
                speech_counter += 1
                rows.append({
                    "date":              doc_date,
                    "name":              utt.get("name") or "",
                    "order":             order_counter,
                    "speech_no":         speech_counter,
                    "panel_no":          panel_idx,
                    "page_no":           None,
                    "name_id":           utt.get("name_id"),
                    "party":             None,
                    "in_gov":            None,
                    "body":              utt["body"],
                    "witness_flag":      utt.get("witness_flag", 1),
                    "gender":            None,
                    "unique_id":         None,
                    "partyfacts_id":     None,
                    "committee_name":    comm_name,
                    "committee_chamber": comm_chamb,
                    "hearing_type":      hearing_type,
                    "reference":         reference,
                    "portfolio":         None,
                })
        return _finalise(rows)

    # ── Estimates format: <chamber.xscript> outer wrapper ─────────────────
    # Structure (complex): debate → [debate.text, discussion, talk, subdebate.2]
    # subdebate.2 → [subdebate.text, discussion, talk, subdebate.3, ...]
    # Walk all <body> elements anywhere in the file, inferring portfolio context
    # from the nearest ancestor <debateinfo/title> and <subdebateinfo/title>.
    cxscript = root.find("chamber.xscript")
    if cxscript is not None:
        panel_idx = 0

        def _get_ancestor_context(body_el: etree._Element) -> tuple[str, str]:
            """Walk up the tree to find portfolio and subdebate titles."""
            portfolio_title = ""
            subdebate_title = ""
            el = body_el.getparent()
            while el is not None:
                tag = el.tag
                if tag == "debate":
                    di = el.find("debateinfo")
                    if di is not None:
                        portfolio_title = (di.findtext("title") or "").strip()
                elif tag in ("subdebate.1", "subdebate.2", "subdebate.3"):
                    di = el.find("subdebateinfo")
                    if di is not None and not subdebate_title:
                        subdebate_title = (di.findtext("title") or "").strip()
                el = el.getparent()
            return portfolio_title, subdebate_title

        # Find all <body> elements in document order, skipping duplicates
        # (a <body> can appear in debate.text, discussion/talk.text, subdebate.text, etc.)
        all_bodies = cxscript.findall(".//body")
        for body in all_bodies:
            utterances = _parse_body(body)
            if not utterances:
                continue

            portfolio_title, subdebate_title = _get_ancestor_context(body)
            portfolio_str = portfolio_title
            if subdebate_title:
                portfolio_str = f"{portfolio_title} — {subdebate_title}" if portfolio_title else subdebate_title

            panel_idx += 1
            speech_counter = 0
            for utt in utterances:
                if not utt.get("body", "").strip():
                    continue
                order_counter += 1
                speech_counter += 1
                rows.append({
                    "date":              doc_date,
                    "name":              utt.get("name") or "",
                    "order":             order_counter,
                    "speech_no":         speech_counter,
                    "panel_no":          panel_idx,
                    "page_no":           None,
                    "name_id":           utt.get("name_id"),
                    "party":             None,
                    "in_gov":            None,
                    "body":              utt["body"],
                    "witness_flag":      utt.get("witness_flag", 1),
                    "gender":            None,
                    "unique_id":         None,
                    "partyfacts_id":     None,
                    "committee_name":    comm_name,
                    "committee_chamber": comm_chamb,
                    "hearing_type":      hearing_type,
                    "reference":         reference,
                    "portfolio":         portfolio_str or None,
                })
        return _finalise(rows)

    print(f"  [WARN] No <discussion> or <chamber.xscript> in {xml_path.name}")
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def _finalise(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    # Coerce numeric columns
    for col in ("order", "speech_no", "panel_no", "witness_flag"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["in_gov"] = pd.to_numeric(df["in_gov"], errors="coerce")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Parse committee Hansard XML → per-file parquet."
    )
    ap.add_argument(
        "--xml-dir",
        required=True,
        help="Directory containing {date}_{dataset}_{docid}.xml files",
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for parquet files",
    )
    ap.add_argument(
        "--file",
        default=None,
        help="Parse a single file only (filename within --xml-dir)",
    )
    ap.add_argument(
        "--no-skip",
        action="store_true",
        help="Overwrite existing output parquets",
    )
    ap.add_argument(
        "--sequential",
        action="store_true",
        help="Disable parallel processing (useful for debugging)",
    )
    args = ap.parse_args()

    xml_dir = Path(args.xml_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.file:
        xml_files = [xml_dir / args.file]
    else:
        xml_files = sorted(xml_dir.glob("*.xml"))

    if not xml_files:
        print(f"No XML files found in {xml_dir}")
        sys.exit(1)

    print(f"Parsing {len(xml_files)} files → {out_dir}")

    if args.file or args.sequential:
        # Single-file mode or explicit sequential fallback
        stats = {"parsed": 0, "skipped": 0, "empty": 0, "errors": 0}
        for xml_path in tqdm(xml_files):
            out_path = out_dir / xml_path.with_suffix(".parquet").name
            if not args.no_skip and out_path.exists():
                stats["skipped"] += 1
                continue
            try:
                df = _parse_file(xml_path)
            except Exception as e:
                print(f"  [ERROR] {xml_path.name}: {e}")
                stats["errors"] += 1
                continue
            if df.empty:
                stats["empty"] += 1
                continue
            df.to_parquet(out_path, index=False)
            stats["parsed"] += 1
    else:
        init_args = (out_dir, args.no_skip)
        results = eager_map(
            _parse_one,
            xml_files,
            initializer=_init_worker,
            initargs=(init_args,),
            desc="Parsing",
        )
        stats = {
            "parsed":  results.count("ok"),
            "skipped": results.count("skipped"),
            "empty":   results.count("empty"),
            "errors":  results.count("error"),
        }

    print(
        f"\nDone. Parsed: {stats['parsed']}, Skipped: {stats['skipped']}, "
        f"Empty: {stats['empty']}, Errors: {stats['errors']}"
    )

    if args.file and stats["parsed"] == 1:
        # Print summary for single-file mode
        df = pd.read_parquet(out_dir / Path(args.file).with_suffix(".parquet").name)
        print(f"\nRows: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print(f"Witness rows: {df['witness_flag'].sum()} / {len(df)}")
        print(f"Member rows (name_id known): {df['name_id'].notna().sum()}")
        print(f"Panels: {df['panel_no'].max()}")
        print(f"Committee: {df['committee_name'].iloc[0]}")
        print(f"Reference: {df['reference'].iloc[0]}")
        print("\nSample rows:")
        cols = ["name", "name_id", "witness_flag", "panel_no", "body"]
        print(df[cols].head(15).to_string())


if __name__ == "__main__":
    main()
