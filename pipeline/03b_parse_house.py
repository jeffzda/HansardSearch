"""
03b_parse_house.py — Australian House of Representatives Hansard XML parser.

Handles five XML schema eras:
  Era 1: 1998–1999   — version "2.0" (or missing), maincomm.xscript,
                        day.start date field, old talker structure
  Era 2: 2000–2011   — version "2.0"/"2.1", still maincomm.xscript,
                        business.start may have "body" or "para" field
  Era 3: 2011–2012   — version "2.1", maincomm.xscript (transitional)
  Era 4: 2012–2022   — version "2.2" (OOXML), fedchamb.xscript,
                        HPS-class body paragraphs, <a type="..."> attribution
  Era 5: 2022–2025   — version "2.2" (newest OOXML), same fedchamb.xscript
                        structure with minor additions

Federation Chamber node names:
  - Pre-2012:  //maincomm.xscript
  - 2012+:     //fedchamb.xscript

Output schema (21 columns, exact order):
  date, name, order, speech_no, page_no, time_stamp, name_id, electorate,
  party, in_gov, first_speech, body, question, answer, q_in_writing,
  div_flag, gender, unique_id, interject, fedchamb_flag, partyfacts_id

Usage:
    python 03b_parse_house.py --xml ../data/raw/house/2020-02-04.xml
    python 03b_parse_house.py --xml-dir ../data/raw/house --out-dir ../data/output/house/daily
"""

import argparse
import re
from pathlib import Path

import pandas as pd
from lxml import etree
from tqdm import tqdm
from parallel_utils import eager_map

# ── Parallel worker state ─────────────────────────────────────────────────────

_WORKER_OUT_DIR: Path | None = None
_WORKER_SKIP_EXISTING: bool = True


def _init_worker(args_tuple: tuple) -> None:
    """Per-process initializer: store output dir and skip flag in globals."""
    global _WORKER_OUT_DIR, _WORKER_SKIP_EXISTING
    _WORKER_OUT_DIR, _WORKER_SKIP_EXISTING = args_tuple


def _parse_one(xml_path: Path) -> str:
    """
    Worker function: parse one House XML file and write CSV + Parquet output.
    Returns a status string: 'ok', 'skipped', or 'empty'.
    """
    out_csv = _WORKER_OUT_DIR / f"{xml_path.stem}.csv"
    out_parquet = _WORKER_OUT_DIR / f"{xml_path.stem}.parquet"

    if _WORKER_SKIP_EXISTING and out_csv.exists() and out_parquet.exists():
        return "skipped"

    df = parse_sitting_day_house(xml_path)

    if df.empty:
        print(f"  [WARN] Empty output for {xml_path.name}")
        return "empty"

    df.to_csv(out_csv, index=False)
    df.to_parquet(out_parquet, index=False)
    return "ok"

# ── Schema version constants ───────────────────────────────────────────────────

V2x = ("2.0", "2.1")   # legacy custom-markup versions
V22 = ("2.2",)          # OOXML-derived version

# ── Stage direction patterns (House-specific) ──────────────────────────────────

STAGE_PATTERNS_HOUSE = re.compile(
    "|".join([
        r"The House adjourned at",
        r"House adjourned",
        r"Federation Chamber adjourned",
        r"The (SPEAKER|DEPUTY SPEAKER|CHAIR|DEPUTY CHAIR) declared",
        r"The (SPEAKER|DEPUTY SPEAKER|CHAIR|DEPUTY CHAIR) took the chair",
        r"(MADAM |MR )(SPEAKER|DEPUTY SPEAKER|CHAIR)",
        r"The House divided\.",
        r"The committee divided\.",
        r"Question agreed to\.",
        r"Question negatived\.",
        r"Questions? so resolved in the (affirmative|negative)\.",
        r"Bill read a \w+( time)?",
        r"Bill read the \w+ time\.",
        r"Leave (was |not )?granted\.",
        r"Sitting suspended from",
        r"Debate (adjourned|resumed|interrupted|resumed from)",
        r"Time expired\.",
        r"A division having been called",
        r"The (House |committee )?rose",
        r"Ordered to be printed\.",
        r"Message from the Senate",
        r"\* denotes teller",
        r"Ayes \d+, Noes \d+",
        r"Quorum (formed|called)",
        r"(The )?amendment( proposed)? negatived",
        r"(The )?amendment agreed to",
        r"Pursuant to order",
        r"(Formal )?motion agreed to",
        r"Bills? received from the Senate",
        r"Amendment received from the Senate",
        r"Opposition members interjecting",
        r"Government members interjecting",
        r"Members? interjecting",
        r"Honourable members? interjecting",
        r"An? (honourable )?member[s]? interjecting",
    ]),
    re.IGNORECASE,
)

# ── Output schema columns ──────────────────────────────────────────────────────

OUTPUT_COLUMNS = [
    "date", "name", "order", "speech_no", "page_no", "time_stamp",
    "name_id", "electorate", "party", "in_gov", "first_speech",
    "body", "question", "answer", "q_in_writing", "div_flag",
    "gender", "unique_id", "interject", "fedchamb_flag", "partyfacts_id",
]

# ── HPS class / A-type maps (same as Senate v2.2) ─────────────────────────────

HPS_TYPE_MAP = {
    "HPS-MemberSpeech":       "speech",
    "HPS-OfficeSpeech":       "speech",
    "HPS-MemberContinuation": "continue",
    "HPS-MemberInterjecting": "interjection",
    "HPS-OfficeInterjecting": "interjection",
    "HPS-MemberQuestion":     "question",
    "HPS-MemberAnswer":       "answer",
}

A_TYPE_MAP = {
    "MemberSpeech":       "speech",
    "OfficeSpeech":       "speech",
    "MemberContinuation": "continue",
    "MemberInterjecting": "interjection",
    "OfficeInterjecting": "interjection",
    "MemberQuestion":     "question",
    "MemberAnswer":       "answer",
}

SPEAKER_ID_CLASSES = {
    "HPS-MemberSpeech", "HPS-OfficeSpeech",
    "HPS-MemberContinuation", "HPS-MemberInterjecting",
    "HPS-OfficeInterjecting", "HPS-MemberQuestion", "HPS-MemberAnswer",
    "HPS-Electorate", "HPS-MinisterialTitles", "HPS-Time",
}

# ── Utility functions ──────────────────────────────────────────────────────────

def _t(element, tag: str, default: str = "") -> str:
    """Get text from a named child element, stripped."""
    el = element.find(tag)
    if el is None:
        return default
    return (el.text or "").strip()


def _all_text(element) -> str:
    """Recursively collect all text content from element and descendants."""
    return "".join(element.itertext()).strip()


def _normalise_time(raw: str) -> str | None:
    """
    Normalise a time string to HH:MM:SS.
    Handles: '14:01:00', '14:01', '2.30 pm', '2.30 p.m.', '14.01', '9 am'.
    Returns None if the string cannot be parsed.
    """
    if not raw or not raw.strip():
        return None
    s = raw.strip()

    # Already HH:MM:SS
    if re.match(r"^\d{2}:\d{2}:\d{2}$", s):
        return s

    # HH:MM
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if m:
        return f"{int(m.group(1)):02d}:{m.group(2)}:00"

    # H.MM am/pm (legacy format)
    m = re.match(r"^(\d{1,2})[.,](\d{2})\s*([apAP]\.?[mM]\.?)$", s)
    if m:
        hour, minute = int(m.group(1)), int(m.group(2))
        ampm = m.group(3).lower().replace(".", "")
        if ampm == "pm" and hour < 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        return f"{hour:02d}:{minute:02d}:00"

    # H am/pm (no minutes)
    m = re.match(r"^(\d{1,2})\s*([apAP]\.?[mM]\.?)$", s)
    if m:
        hour = int(m.group(1))
        ampm = m.group(2).lower().replace(".", "")
        if ampm == "pm" and hour < 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        return f"{hour:02d}:00:00"

    return None


def _to_int(val: str | None) -> int | None:
    if val is None or val.strip() == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _is_stage_direction(body: str) -> bool:
    return bool(STAGE_PATTERNS_HOUSE.search(body))


def _parse_house_metadata_name(metadata: str) -> tuple[str, str]:
    """
    Parse House metadata name "Smith, Mr John" → ("Smith", "John").
    Handles special presiding officer names.
    """
    if not metadata:
        return "", ""
    meta_upper = metadata.upper()
    if meta_upper in ("SPEAKER, THE", "THE SPEAKER"):
        return "The SPEAKER", ""
    if meta_upper in ("DEPUTY SPEAKER, THE", "THE DEPUTY SPEAKER"):
        return "The DEPUTY SPEAKER", ""
    if meta_upper in ("CHAIR, THE", "THE CHAIR"):
        return "The CHAIR", ""
    if meta_upper in ("DEPUTY CHAIR, THE", "THE DEPUTY CHAIR"):
        return "The DEPUTY CHAIR", ""
    parts = metadata.split(",", 1)
    surname = parts[0].strip()
    first_raw = parts[1].strip() if len(parts) > 1 else ""
    # Strip title prefixes like "Mr ", "Ms ", "Dr ", "Hon. " etc.
    first = re.sub(r"^(Mr\.?|Mrs\.?|Ms\.?|Miss\.?|Dr\.?|Prof\.?|Hon\.?)\s+", "", first_raw).strip()
    return surname, first


# ── v2.0 / v2.1 House parser ──────────────────────────────────────────────────

class HouseParserV21:
    """
    Parser for House of Representatives Hansard XML schema versions 2.0 and 2.1
    (approximately 1998–2011).

    Handles both main chamber (chamber.xscript, fedchamb_flag=0) and the
    Federation Chamber / Main Committee (maincomm.xscript, fedchamb_flag=1).

    Text model: <para> / <inline> within speech elements.
    Interjections: separate sibling <interjection> elements inside <speech>.
    """

    SPEECH_TAGS = {"speech", "question", "answer", "interjection", "continue"}

    def __init__(self, root: etree._Element, date_str: str):
        self.root = root
        self.date_str = date_str
        self._rows: list[dict] = []

    def parse(self) -> list[dict]:
        # Parse main chamber
        chamber = self.root.find(".//chamber.xscript")
        if chamber is not None:
            self._parse_chamber_root(chamber, fedchamb_flag=0)
        else:
            raise ValueError(f"No chamber.xscript in {self.date_str}")

        # Parse Federation Chamber / Main Committee
        # Pre-2012 House uses maincomm.xscript
        fedchamb = self.root.find(".//maincomm.xscript")
        if fedchamb is not None:
            self._parse_chamber_root(fedchamb, fedchamb_flag=1)

        # answers.to.questions is a root-level sibling (not child of chamber)
        atq = self.root.find("answers.to.questions")
        if atq is not None:
            self._parse_answers_to_questions(atq, fedchamb_flag=0)

        return self._rows

    def _parse_chamber_root(self, chamber: etree._Element,
                            fedchamb_flag: int) -> None:
        # Business start
        bs = chamber.find("business.start")
        if bs is not None:
            row = self._parse_business_start(bs, fedchamb_flag)
            self._rows.append(row)

        # Debates and adjournments
        for child in chamber:
            tag = child.tag
            if tag == "debate":
                self._parse_debate(child, fedchamb_flag=fedchamb_flag)
            elif tag == "adjournment":
                self._parse_adjournment_element(child, fedchamb_flag)
            elif tag in ("answers.to.questions",):
                self._parse_answers_to_questions(child, fedchamb_flag)

    # ── Business start ─────────────────────────────────────────────────────

    def _parse_business_start(self, bs: etree._Element,
                               fedchamb_flag: int) -> dict:
        # Try para first, then body, then day.start
        body_parts = []

        day_start_el = bs.find("day.start")
        if day_start_el is not None:
            text = _all_text(day_start_el).strip()
            if text:
                body_parts.append(text)

        for el in bs:
            if el.tag in ("para", "body"):
                text = _all_text(el).strip()
                if text:
                    body_parts.append(text)
            elif el.tag not in ("day.start",):
                # Catch any other text elements
                text = _all_text(el).strip()
                if text:
                    body_parts.append(text)

        body = " ".join(body_parts)

        # Extract start time from the chair-taking sentence
        time_match = re.search(
            r"\b(\d{1,2})[.,](\d{2})\s*([apAP]\.?[mM]\.?)"
            r"|\b(\d{1,2}):(\d{2})\b"
            r"|\b(\d{1,2})\s*([apAP]\.?[mM]\.?)\b",
            body,
        )
        ts = None
        if time_match:
            ts = _normalise_time(time_match.group(0))

        return self._make_row(
            name="business start",
            body=body,
            time_stamp=ts,
            row_type="business_start",
            fedchamb_flag=fedchamb_flag,
        )

    # ── Adjournment ────────────────────────────────────────────────────────

    def _parse_adjournment_element(self, adj: etree._Element,
                                   fedchamb_flag: int) -> None:
        info = adj.find("adjournmentinfo")
        ts = None
        if info is not None:
            ts = _normalise_time(_t(info, "time.stamp"))
        paras = [_all_text(p) for p in adj.iter("para")]
        body = " ".join(p for p in paras if p)
        self._rows.append(self._make_row(
            name="stage direction",
            body=body,
            time_stamp=ts,
            row_type="stage_direction",
            fedchamb_flag=fedchamb_flag,
        ))

    # ── Answers to questions in writing ────────────────────────────────────

    def _parse_answers_to_questions(self, node: etree._Element,
                                    fedchamb_flag: int) -> None:
        """Parse questions/answers in writing (q_in_writing=1)."""
        for debate in node.iter("debate"):
            for subdebate in debate:
                if subdebate.tag == "subdebate.1":
                    for child in subdebate:
                        if child.tag == "question":
                            self._parse_speech_node(
                                child, context={},
                                fedchamb_flag=fedchamb_flag,
                                q_in_writing=1,
                            )
                        elif child.tag == "answer":
                            self._parse_speech_node(
                                child, context={},
                                fedchamb_flag=fedchamb_flag,
                                q_in_writing=1,
                            )

    # ── Debate walking ─────────────────────────────────────────────────────

    def _parse_debate(self, debate: etree._Element,
                      debate_context: dict | None = None,
                      fedchamb_flag: int = 0) -> None:
        info = debate.find("debateinfo")
        context = dict(debate_context or {})
        if info is not None:
            context["debate_title"] = _t(info, "title")
            context["debate_type"] = _t(info, "type")

        for child in debate:
            tag = child.tag
            if tag == "subdebate.1":
                self._parse_subdebate(child, level=1, context=context,
                                      fedchamb_flag=fedchamb_flag)
            elif tag in self.SPEECH_TAGS:
                self._parse_speech_node(child, context,
                                        fedchamb_flag=fedchamb_flag)
            elif tag == "division":
                self._parse_division(child, fedchamb_flag=fedchamb_flag)
            elif tag == "adjournment":
                self._parse_adjournment_element(child, fedchamb_flag)
            elif tag == "motionnospeech":
                self._parse_motionnospeech(child, context,
                                           fedchamb_flag=fedchamb_flag)

    def _parse_subdebate(self, sub: etree._Element, level: int,
                         context: dict, fedchamb_flag: int = 0) -> None:
        info = sub.find("subdebateinfo")
        ctx = dict(context)
        if info is not None:
            ctx[f"sub{level}_title"] = _t(info, "title")

        for child in sub:
            tag = child.tag
            if tag == "subdebate.2":
                self._parse_subdebate(child, level=2, context=ctx,
                                      fedchamb_flag=fedchamb_flag)
            elif tag in self.SPEECH_TAGS:
                self._parse_speech_node(child, ctx,
                                        fedchamb_flag=fedchamb_flag)
            elif tag == "division":
                self._parse_division(child, fedchamb_flag=fedchamb_flag)
            elif tag == "motionnospeech":
                self._parse_motionnospeech(child, ctx,
                                           fedchamb_flag=fedchamb_flag)

    # ── Speech node parsing ────────────────────────────────────────────────

    def _parse_speech_node(self, node: etree._Element, context: dict,
                           fedchamb_flag: int = 0,
                           q_in_writing: int = 0) -> None:
        tag = node.tag

        talk_start = node.find("talk.start")
        if talk_start is None:
            return

        talker_data = self._parse_talker(talk_start.find("talker"))

        # Body text from talk.start (non-talker children) + speech node children
        body_parts = []
        for el in talk_start:
            if el.tag not in ("talker",):
                text = _all_text(el)
                if text:
                    body_parts.append(text)

        for el in node:
            if el.tag not in ("talk.start", "interjection", "continue"):
                text = _all_text(el)
                if text:
                    body_parts.append(text)

        body_raw = " ".join(body_parts)
        body = re.sub(r"^[\u2014\-]\s*", "", body_raw).strip()

        is_question = 1 if tag == "question" else 0
        is_answer = 1 if tag == "answer" else 0
        is_interject = 1 if tag == "interjection" else 0

        row = self._make_row(
            body=body,
            row_type=tag,
            interject_flag=is_interject,
            question=is_question,
            answer=is_answer,
            q_in_writing=q_in_writing,
            fedchamb_flag=fedchamb_flag,
            **talker_data,
        )
        self._rows.append(row)

        # Recurse into <interjection> and <continue> children
        for child in node:
            if child.tag in ("interjection", "continue"):
                self._parse_speech_node(child, context,
                                        fedchamb_flag=fedchamb_flag,
                                        q_in_writing=q_in_writing)

    def _parse_talker(self, talker: etree._Element | None) -> dict:
        if talker is None:
            return {}

        def txt(tag):
            return _t(talker, tag)

        # Two <name> elements: role="metadata" and role="display"
        name_display = None
        name_metadata = None
        for name_el in talker.findall("name"):
            role = name_el.get("role", "")
            val = (name_el.text or "").strip()
            if role == "display":
                name_display = val
            elif role == "metadata":
                name_metadata = val

        # Derive display name from metadata if display is missing
        if not name_display and name_metadata:
            sur, _ = _parse_house_metadata_name(name_metadata)
            name_display = sur if sur else name_metadata

        name_id = txt("name.id") or None
        if name_id:
            name_id = name_id.strip().upper()
            if name_id in ("", "NONE", "NAN"):
                name_id = None

        return {
            "name": name_display,
            "name_id": name_id,
            "electorate": txt("electorate") or None,
            "party": txt("party") or None,
            "in_gov": _to_int(txt("in.gov")),
            "first_speech": _to_int(txt("first.speech")),
            "page_no": txt("page.no") or None,
            "time_stamp": _normalise_time(txt("time.stamp")),
        }

    # ── Motionnospeech ─────────────────────────────────────────────────────

    def _parse_motionnospeech(self, node: etree._Element, context: dict,
                              fedchamb_flag: int = 0) -> None:
        name_text = _t(node, "name")
        ts = _normalise_time(_t(node, "time.stamp"))

        parts = []
        for el in node:
            if el.tag not in ("name", "electorate", "role", "time.stamp"):
                text = _all_text(el)
                if text:
                    parts.append(text)
        body = " ".join(parts).strip()

        self._rows.append(self._make_row(
            name=name_text or "stage direction",
            body=body,
            time_stamp=ts,
            row_type="motionnospeech",
            fedchamb_flag=fedchamb_flag,
        ))

    # ── Division ───────────────────────────────────────────────────────────

    def _parse_division(self, div: etree._Element,
                        fedchamb_flag: int = 0) -> None:
        header = div.find("division.header")
        ts = None
        header_text = ""
        if header is not None:
            ts = _normalise_time(_t(header, "time.stamp"))
            header_text = _all_text(header)

        result_el = div.find("division.result")
        result_text = _all_text(result_el) if result_el is not None else ""

        chair_text = ""
        for p in div.findall("para"):
            txt_val = _all_text(p).strip()
            if txt_val.startswith("(") or "Chair" in txt_val or "Speaker" in txt_val:
                chair_text = txt_val
                break

        body = " ".join(filter(None, [header_text, chair_text, result_text])).strip()

        self._rows.append(self._make_row(
            name="stage direction",
            body=body,
            time_stamp=ts,
            row_type="division",
            div_flag=1,
            fedchamb_flag=fedchamb_flag,
        ))

    # ── Row factory ────────────────────────────────────────────────────────

    def _make_row(self, *, name: str | None = None, body: str = "",
                  time_stamp: str | None = None, row_type: str = "speech",
                  question: int = 0, answer: int = 0, interject_flag: int = 0,
                  div_flag: int = 0, name_id: str | None = None,
                  electorate: str | None = None, party: str | None = None,
                  in_gov: int | None = None, first_speech: int | None = None,
                  page_no: str | None = None, q_in_writing: int = 0,
                  fedchamb_flag: int = 0, **_) -> dict:
        return {
            "name": name,
            "body": body,
            "time_stamp": time_stamp,
            "row_type": row_type,
            "question": question,
            "answer": answer,
            "_interject_flag": interject_flag,
            "div_flag": div_flag,
            "name_id": name_id,
            "electorate": electorate,
            "party": party,
            "in_gov": in_gov,
            "first_speech": first_speech,
            "page_no": page_no,
            "q_in_writing": q_in_writing,
            "gender": None,
            "unique_id": None,
            "interject": None,
            "speech_no": None,
            "order": None,
            "fedchamb_flag": fedchamb_flag,
            "partyfacts_id": None,
        }


# ── v2.2 House parser ─────────────────────────────────────────────────────────

class HouseParserV22:
    """
    Parser for House of Representatives Hansard XML schema version 2.2
    (approximately 2012–2025).

    Handles both main chamber (chamber.xscript, fedchamb_flag=0) and the
    Federation Chamber (fedchamb.xscript, fedchamb_flag=1).

    Text model: <talk.text>/<body>/<p class="HPS-*">/<span> structure.
    Speaker type encoded in <a type="..."> and <span class="HPS-*"> within
    the body text.
    """

    def __init__(self, root: etree._Element, date_str: str):
        self.root = root
        self.date_str = date_str
        self._rows: list[dict] = []

    def parse(self) -> list[dict]:
        # Parse main chamber
        chamber = self.root.find(".//chamber.xscript")
        if chamber is not None:
            self._parse_chamber_root(chamber, fedchamb_flag=0)
        else:
            raise ValueError(f"No chamber.xscript in {self.date_str}")

        # Parse Federation Chamber (2012+ uses fedchamb.xscript)
        fedchamb = self.root.find(".//fedchamb.xscript")
        if fedchamb is None:
            # Fall back to maincomm.xscript for transitional files
            fedchamb = self.root.find(".//maincomm.xscript")
        if fedchamb is not None:
            self._parse_chamber_root(fedchamb, fedchamb_flag=1)

        # answers.to.questions is a root-level sibling of chamber/fedchamb
        # (not a child of either), so check root directly
        atq = self.root.find("answers.to.questions")
        if atq is not None:
            self._parse_answers_to_questions(atq, fedchamb_flag=0)

        return self._rows

    def _parse_chamber_root(self, chamber: etree._Element,
                            fedchamb_flag: int) -> None:
        bs = chamber.find("business.start")
        if bs is not None:
            self._rows.append(self._parse_business_start(bs, fedchamb_flag))

        for child in chamber:
            tag = child.tag
            if tag == "debate":
                self._parse_debate(child, fedchamb_flag=fedchamb_flag)
            elif tag in ("answers.to.questions",):
                self._parse_answers_to_questions(child, fedchamb_flag)

    # ── Business start ─────────────────────────────────────────────────────

    def _parse_business_start(self, bs: etree._Element,
                               fedchamb_flag: int) -> dict:
        body_parts = []
        ts = None
        for p in bs.iter("p"):
            text = _all_text(p).strip()
            if text:
                body_parts.append(text)
                if ts is None:
                    m = re.search(
                        r"\b(\d{1,2}[.,:]\d{2})\s*([apAP]\.?[mM]\.?)?",
                        text,
                    )
                    if m:
                        ts = _normalise_time(m.group(0))

        # Fallback: try para text if no <p> found
        if not body_parts:
            for el in bs:
                text = _all_text(el).strip()
                if text:
                    body_parts.append(text)
                    if ts is None:
                        m = re.search(r"\b(\d{1,2}[.,:]\d{2})\b", text)
                        if m:
                            ts = _normalise_time(m.group(0))

        body = " ".join(body_parts)
        return self._make_row(
            name="business start",
            body=body,
            time_stamp=ts,
            row_type="business_start",
            fedchamb_flag=fedchamb_flag,
        )

    # ── Answers to questions in writing ────────────────────────────────────

    def _parse_answers_to_questions(self, node: etree._Element,
                                    fedchamb_flag: int) -> None:
        """Parse questions/answers in writing (q_in_writing=1)."""
        for debate in node.iter("debate"):
            for child in debate:
                if child.tag == "subdebate.1":
                    for item in child:
                        if item.tag in ("question", "answer"):
                            self._parse_speech_v22(
                                item,
                                is_question=(item.tag == "question"),
                                is_answer=(item.tag == "answer"),
                                fedchamb_flag=fedchamb_flag,
                                q_in_writing=1,
                            )

    # ── Debate walking ─────────────────────────────────────────────────────

    def _parse_debate(self, debate: etree._Element,
                      fedchamb_flag: int = 0) -> None:
        for child in debate:
            tag = child.tag
            if tag in ("subdebate.1", "subdebate.2"):
                self._parse_subdebate(child, fedchamb_flag=fedchamb_flag)
            elif tag == "speech":
                self._parse_speech_v22(child, is_question=False,
                                       is_answer=False,
                                       fedchamb_flag=fedchamb_flag)
            elif tag == "question":
                self._parse_speech_v22(child, is_question=True,
                                       is_answer=False,
                                       fedchamb_flag=fedchamb_flag)
            elif tag == "answer":
                self._parse_speech_v22(child, is_question=False,
                                       is_answer=True,
                                       fedchamb_flag=fedchamb_flag)
            elif tag == "division":
                self._parse_division_v22(child, fedchamb_flag=fedchamb_flag)
            elif tag == "subdebate.text":
                self._parse_subdebate_text(child)

    def _parse_subdebate(self, sub: etree._Element,
                         fedchamb_flag: int = 0) -> None:
        for child in sub:
            tag = child.tag
            if tag in ("subdebate.1", "subdebate.2"):
                self._parse_subdebate(child, fedchamb_flag=fedchamb_flag)
            elif tag == "speech":
                self._parse_speech_v22(child, is_question=False,
                                       is_answer=False,
                                       fedchamb_flag=fedchamb_flag)
            elif tag == "question":
                self._parse_speech_v22(child, is_question=True,
                                       is_answer=False,
                                       fedchamb_flag=fedchamb_flag)
            elif tag == "answer":
                self._parse_speech_v22(child, is_question=False,
                                       is_answer=True,
                                       fedchamb_flag=fedchamb_flag)
            elif tag == "division":
                self._parse_division_v22(child, fedchamb_flag=fedchamb_flag)
            elif tag == "subdebate.text":
                self._parse_subdebate_text(child)

    def _parse_subdebate_text(self, node: etree._Element) -> None:
        """Emit a stage direction row for a <subdebate.text> procedural block."""
        parts = [_all_text(p).strip() for p in node.iter("p")]
        body = " ".join(p for p in parts if p).strip()
        if body:
            self._rows.append(self._make_row(
                name="stage direction",
                body=body,
                row_type="stage_direction",
            ))

    # ── Speech parsing (v2.2) ──────────────────────────────────────────────

    def _parse_speech_v22(self, node: etree._Element,
                          is_question: bool, is_answer: bool,
                          fedchamb_flag: int = 0,
                          q_in_writing: int = 0) -> None:
        """
        Parse a single <speech>/<question>/<answer> element in v2.2 format.

        The <talk.start>/<talker> provides name_id and metadata name.
        The <talk.text>/<body>/<p> elements provide the body text and
        speaker identification via <a type="..."> elements.

        A single <speech> element may contain multiple attributed utterances
        (interjections embedded via <a type="MemberInterjecting">). We split
        these into separate rows.
        """
        talk_start = node.find("talk.start")
        talker_data = {}
        if talk_start is not None:
            talker = talk_start.find("talker")
            if talker is not None:
                talker_data = self._parse_talker_v22(talker)

        talk_text = node.find("talk.text")
        if talk_text is None:
            # No text content — skip; emitting a body="" row adds noise with no value
            return

        utterances = self._extract_utterances_v22(talk_text, talker_data)

        for utt in utterances:
            row = self._make_row(
                body=utt["body"],
                name=utt.get("name") or talker_data.get("name"),
                name_id=utt.get("name_id") or talker_data.get("name_id"),
                time_stamp=utt.get("time_stamp") or talker_data.get("time_stamp"),
                electorate=utt.get("electorate") or talker_data.get("electorate"),
                party=talker_data.get("party"),
                in_gov=talker_data.get("in_gov"),
                first_speech=talker_data.get("first_speech"),
                page_no=talker_data.get("page_no"),
                question=1 if (is_question or utt.get("row_type") == "question") else 0,
                answer=1 if (is_answer or utt.get("row_type") == "answer") else 0,
                interject_flag=1 if utt.get("row_type") == "interjection" else 0,
                row_type=utt.get("row_type", "speech"),
                q_in_writing=q_in_writing,
                fedchamb_flag=fedchamb_flag,
            )
            self._rows.append(row)

    def _extract_utterances_v22(self, talk_text: etree._Element,
                                 talker_data: dict) -> list[dict]:
        """
        Walk all <p> elements within talk_text. Split into utterances when an
        <a type="Member*|Office*"> introduces a new speaker attribution.

        Returns a list of dicts with keys: body, name, name_id, time_stamp,
        electorate, row_type.
        """
        utterances = []
        current = {
            "name": talker_data.get("name"),
            "name_id": talker_data.get("name_id"),
            "time_stamp": talker_data.get("time_stamp"),
            "electorate": talker_data.get("electorate"),
            "row_type": "speech",
            "body_parts": [],
        }

        for p in talk_text.iter("p"):
            p_class = p.get("class", "")
            if "HPS-DivisionPreamble" in p_class or "HPS-DivisionFooter" in p_class:
                div_text = _all_text(p).strip()
                if div_text:
                    utterances.append({**current, "body": " ".join(current["body_parts"])})
                    utterances.append({
                        "name": "stage direction",
                        "name_id": None,
                        "time_stamp": None,
                        "electorate": None,
                        "row_type": "stage_direction",
                        "body": div_text,
                    })
                    current["body_parts"] = []
                continue

            a_elements = p.findall(".//a")
            has_attribution = any(
                a.get("type", "") in A_TYPE_MAP for a in a_elements
            )

            if has_attribution:
                for a in a_elements:
                    a_type = a.get("type", "")
                    row_type = A_TYPE_MAP.get(a_type)
                    if row_type is None:
                        continue

                    # Flush current utterance — only emit if there is body text
                    body_so_far = " ".join(current["body_parts"]).strip()
                    if body_so_far:
                        utterances.append({**current, "body": body_so_far})

                    # Name from text inside <a>
                    name_text = _all_text(a).strip()
                    # PHID from href
                    href = a.get("href", "").strip().upper()
                    name_id = href if href and href not in ("CHAMBER", "") else None

                    # Time from sibling <span class="HPS-Time">
                    ts = None
                    parent = a.getparent()
                    if parent is not None:
                        for span in parent.findall(".//span"):
                            if "HPS-Time" in span.get("class", ""):
                                ts = _normalise_time(_all_text(span).strip("() :"))
                                break

                    # Electorate from <span class="HPS-Electorate">
                    electorate = None
                    if parent is not None:
                        electorate_parts = [
                            "".join(s.itertext())
                            for s in parent.findall(".//span")
                            if "HPS-Electorate" in s.get("class", "")
                        ]
                        if electorate_parts:
                            raw = "".join(electorate_parts).strip(" ()").strip()
                            electorate = " ".join(raw.split())

                    current = {
                        "name": name_text,
                        "name_id": name_id or current.get("name_id"),
                        "time_stamp": ts,
                        "electorate": electorate or current.get("electorate"),
                        "row_type": row_type,
                        "body_parts": [],
                    }

                # Extract body text for this paragraph, skipping speaker-ID spans
                para_body = self._extract_body_text_v22(p)
                if para_body:
                    current["body_parts"].append(para_body)
            else:
                # Regular paragraph — add to current utterance body
                para_body = _all_text(p).strip()
                if para_body and p_class not in ("HPS-Line", "HPS-SODJobDate"):
                    current["body_parts"].append(para_body)

        # Flush last utterance — only emit if there is body text
        body_so_far = " ".join(current["body_parts"]).strip()
        if body_so_far:
            utterances.append({**current, "body": body_so_far})

        return utterances

    def _extract_body_text_v22(self, p: etree._Element) -> str:
        """
        Extract body text from a <p> element, excluding speaker-ID spans.
        Speaker attribution is in <span class="HPS-MemberSpeech|..."> and
        <a type="..."> elements; also skip <span class="HPS-Time"> and
        <span class="HPS-Electorate">.
        """
        parts = []

        def visit(el, in_attribution: bool):
            cls = el.get("class", "")
            if el.tag == "a" and el.get("type", "") in A_TYPE_MAP:
                # Tail may contain speech content (MemberContinuation, MemberInterjecting).
                # Strip leading attribution punctuation chars (opening parens, dashes etc.)
                # that are part of the "(Electorate—Title) (HH:MM): " format.
                if el.tail:
                    tail = re.sub(r"^[():\s\u2014\-]+", "", el.tail).strip()
                    if tail:
                        parts.append(tail)
                return
            if cls in SPEAKER_ID_CLASSES:
                # HPS-Time tail: "):  SPEECH CONTENT" — strip the closing-paren+colon prefix.
                # Other attribution spans (Electorate, MinisterialTitles, MemberSpeech, etc.)
                # also carry speech content in their tails after stripping attribution punctuation.
                if el.tail:
                    tail = re.sub(r"^[():\s\u2014\-]+", "", el.tail).strip()
                    if tail:
                        parts.append(tail)
                return
            if el.text and not in_attribution:
                t = el.text.strip()
                if t:
                    parts.append(t)
            for child in el:
                visit(child, in_attribution)
            if el.tail:
                t = el.tail.strip()
                if t:
                    parts.append(t)

        if p.text:
            t = p.text.strip()
            if t and t not in (":", "(", ")"):
                parts.append(t)
        for child in p:
            visit(child, False)

        body = " ".join(parts)
        body = re.sub(r"\s+", " ", body).strip()
        body = re.sub(r"^[\u2014\-]\s*", "", body)
        return body

    def _parse_talker_v22(self, talker: etree._Element) -> dict:
        """Parse <talker> element in v2.2 format for House."""

        def txt(tag):
            return _t(talker, tag)

        name_metadata = None
        for name_el in talker.findall("name"):
            if name_el.get("role") == "metadata":
                name_metadata = (name_el.text or "").strip()
                break

        # Construct display name from metadata
        name_display = None
        if name_metadata:
            meta_upper = name_metadata.upper()
            if meta_upper in ("SPEAKER, THE", "THE SPEAKER"):
                name_display = "The SPEAKER"
            elif meta_upper in ("DEPUTY SPEAKER, THE", "THE DEPUTY SPEAKER"):
                name_display = "The DEPUTY SPEAKER"
            elif meta_upper in ("CHAIR, THE", "THE CHAIR"):
                name_display = "The CHAIR"
            elif meta_upper in ("DEPUTY CHAIR, THE", "THE DEPUTY CHAIR"):
                name_display = "The DEPUTY CHAIR"
            else:
                sur, _ = _parse_house_metadata_name(name_metadata)
                name_display = sur if sur else name_metadata

        name_id = txt("name.id") or None
        if name_id:
            name_id = name_id.strip().upper()
            if name_id in ("", "NONE", "NAN"):
                name_id = None

        return {
            "name": name_display,
            "name_id": name_id,
            "electorate": txt("electorate") or None,
            "party": txt("party") or None,
            "in_gov": _to_int(txt("in.gov")),
            "first_speech": _to_int(txt("first.speech")),
            "page_no": txt("page.no") or None,
            "time_stamp": _normalise_time(txt("time.stamp")),
        }

    # ── Division (v2.2) ────────────────────────────────────────────────────

    def _parse_division_v22(self, div: etree._Element,
                             fedchamb_flag: int = 0) -> None:
        parts = []
        ts = None
        for p in div.iter("p"):
            cls = p.get("class", "")
            text = _all_text(p).strip()
            if text:
                parts.append(text)
                if ts is None and ("HPS-DivisionPreamble" in cls or "HPS-Time" in cls):
                    m = re.search(r"\b(\d{2}:\d{2})\b", text)
                    if m:
                        ts = _normalise_time(m.group(1))

        body = " ".join(parts)
        self._rows.append(self._make_row(
            name="stage direction",
            body=body,
            time_stamp=ts,
            row_type="division",
            div_flag=1,
            fedchamb_flag=fedchamb_flag,
        ))

    # ── Row factory ────────────────────────────────────────────────────────

    def _make_row(self, *, name: str | None = None, body: str = "",
                  time_stamp: str | None = None, row_type: str = "speech",
                  question: int = 0, answer: int = 0, interject_flag: int = 0,
                  div_flag: int = 0, name_id: str | None = None,
                  electorate: str | None = None, party: str | None = None,
                  in_gov: int | None = None, first_speech: int | None = None,
                  page_no: str | None = None, q_in_writing: int = 0,
                  fedchamb_flag: int = 0, **_) -> dict:
        return {
            "name": name,
            "body": body,
            "time_stamp": time_stamp,
            "row_type": row_type,
            "question": question,
            "answer": answer,
            "_interject_flag": interject_flag,
            "div_flag": div_flag,
            "name_id": name_id,
            "electorate": electorate,
            "party": party,
            "in_gov": in_gov,
            "first_speech": first_speech,
            "page_no": page_no,
            "q_in_writing": q_in_writing,
            "gender": None,
            "unique_id": None,
            "interject": None,
            "speech_no": None,
            "order": None,
            "fedchamb_flag": fedchamb_flag,
            "partyfacts_id": None,
        }


# ── Shared post-processing ────────────────────────────────────────────────────

def _assign_speech_numbers(rows: list[dict]) -> list[dict]:
    """
    Assign speech_no: each primary speech/question/answer/business_start
    starts a new group. Interjections and continuations belong to the
    immediately preceding primary row's group.
    Stage directions get speech_no = None.
    """
    counter = 0
    current_speech = None
    for row in rows:
        rt = row.get("row_type", "speech")
        if rt in ("speech", "question", "answer", "business_start", "motionnospeech"):
            counter += 1
            current_speech = counter
        elif rt in ("stage_direction", "division"):
            current_speech = None
        row["speech_no"] = current_speech
    return rows


def _flag_interjections(rows: list[dict]) -> list[dict]:
    """
    Set interject flag:
      0 — first row in a speech group, stage directions, presiding officers
      1 — all other rows within a speech group (interjections, continuations
          by different members, etc.)
    """
    if not rows:
        return rows

    from collections import defaultdict
    groups = defaultdict(list)
    for i, row in enumerate(rows):
        sno = row.get("speech_no")
        if sno is not None:
            groups[sno].append((i, row))

    for sno, group_rows in groups.items():
        if not group_rows:
            continue
        first_idx, first_row = group_rows[0]
        primary_name_id = first_row.get("name_id")

        for j, (idx, row) in enumerate(group_rows):
            rt = row.get("row_type", "speech")
            nid = row.get("name_id")

            if j == 0:
                rows[idx]["interject"] = 0
                continue

            # Explicit interjection from XML structure
            if row.get("_interject_flag") == 1:
                rows[idx]["interject"] = 1
                continue

            # Continuation by primary speaker: interject=0
            if rt == "continue" and nid == primary_name_id:
                rows[idx]["interject"] = 0
                continue

            # Different speaker mid-speech: interjection
            if nid and nid != primary_name_id:
                rows[idx]["interject"] = 1
                continue

            rows[idx]["interject"] = 0

    # Stage directions and divisions always get interject=0
    for row in rows:
        if row.get("row_type") in ("stage_direction", "division", "business_start"):
            row["interject"] = 0
        elif row.get("interject") is None:
            row["interject"] = 0

    return rows


def _assign_order(rows: list[dict]) -> list[dict]:
    for i, row in enumerate(rows, 1):
        row["order"] = i
    return rows


def _clean_to_output(rows: list[dict], date_str: str) -> pd.DataFrame:
    """Convert row list to DataFrame with final output columns."""
    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(rows)

    # Add date column
    df["date"] = date_str

    # Replace name for stage directions
    if "body" in df.columns:
        mask_sd = df["row_type"].isin(["stage_direction", "division"])
        df.loc[mask_sd, "name"] = "stage direction"

    # Drop internal columns
    if "_interject_flag" in df.columns:
        df = df.drop(columns=["_interject_flag"])

    for col in ["row_type", "debate_title", "debate_type",
                "sub1_title", "sub2_title"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Coerce numeric columns
    for col in ("in_gov", "first_speech", "question", "answer",
                "q_in_writing", "div_flag", "interject", "fedchamb_flag"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ("page_no",):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure all output columns exist
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None

    return df[OUTPUT_COLUMNS]


# ── Date extraction from XML ───────────────────────────────────────────────────

def _extract_date_from_xml(root: etree._Element, xml_path: Path) -> str:
    """
    Try to extract the sitting date from XML content.
    Falls back to the filename stem (YYYY-MM-DD) if not found.
    """
    # Try business.start day.start element (v2.0/v2.1)
    for day_el in root.iter("day.start"):
        text = (day_el.text or "").strip()
        if re.match(r"\d{4}-\d{2}-\d{2}", text):
            return text[:10]

    # Try <p class="HPS-SODJobDate"> in business.start (v2.2)
    for bs in root.iter("business.start"):
        for p in bs.iter("p"):
            cls = p.get("class", "")
            if "SODJobDate" in cls or "JobDate" in cls:
                text = _all_text(p).strip()
                # Parse "Wednesday, 4 February 2020" style
                m = re.search(
                    r"\b(\d{1,2})\s+(January|February|March|April|May|June|"
                    r"July|August|September|October|November|December)\s+(\d{4})\b",
                    text,
                )
                if m:
                    try:
                        from datetime import datetime
                        dt = datetime.strptime(
                            f"{m.group(1)} {m.group(2)} {m.group(3)}",
                            "%d %B %Y",
                        )
                        return dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass

    # Fall back to filename stem
    return xml_path.stem


# ── Top-level parse function ───────────────────────────────────────────────────

def parse_sitting_day_house(xml_path: Path) -> pd.DataFrame:
    """
    Parse one House of Representatives sitting-day XML file into a
    rectangular DataFrame.

    Args:
        xml_path: Path to a YYYY-MM-DD.xml file.

    Returns:
        DataFrame with OUTPUT_COLUMNS. Returns an empty DataFrame on failure.
    """
    date_str = xml_path.stem  # default from filename

    try:
        parser = etree.XMLParser(recover=True, encoding=None)
        tree = etree.parse(str(xml_path), parser)
        root = tree.getroot()
    except Exception as e:
        print(f"  [ERROR] Failed to parse {xml_path.name}: {e}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Try to get actual date from XML content
    date_str = _extract_date_from_xml(root, xml_path)

    version = root.get("version", "").strip()
    if not version:
        # Some v2.0 files may omit version; default to v2.1 parser
        version = "2.0"

    try:
        if version in V2x:
            rows = HouseParserV21(root, date_str).parse()
        elif version in V22:
            rows = HouseParserV22(root, date_str).parse()
        else:
            print(f"  [WARN] Unknown schema version '{version}' in {xml_path.name}; "
                  f"trying v2.1 parser")
            rows = HouseParserV21(root, date_str).parse()
    except Exception as e:
        print(f"  [ERROR] Parsing failed for {xml_path.name}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    rows = _assign_speech_numbers(rows)
    rows = _flag_interjections(rows)
    rows = _assign_order(rows)
    return _clean_to_output(rows, date_str)


# ── Batch processing ──────────────────────────────────────────────────────────

def parse_all_house(
    xml_dir: Path,
    out_dir: Path,
    skip_existing: bool = True,
    sequential: bool = False,
) -> None:
    """Parse all XML files in xml_dir and write daily CSV + Parquet files."""
    xml_files = sorted(xml_dir.glob("*.xml"))
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Parsing {len(xml_files)} House XML files → {out_dir}")

    if sequential:
        n_ok = 0
        n_empty = 0
        n_err = 0
        for xml_path in tqdm(xml_files, desc="Parsing"):
            out_csv = out_dir / f"{xml_path.stem}.csv"
            out_parquet = out_dir / f"{xml_path.stem}.parquet"
            if skip_existing and out_csv.exists() and out_parquet.exists():
                n_ok += 1
                continue
            df = parse_sitting_day_house(xml_path)
            if df.empty:
                n_empty += 1
                print(f"  [WARN] Empty output for {xml_path.name}")
                continue
            df.to_csv(out_csv, index=False)
            df.to_parquet(out_parquet, index=False)
            n_ok += 1
        print(f"\nDone. OK: {n_ok}, Empty: {n_empty}, Errors: {n_err}")
        return

    init_args = (out_dir, skip_existing)
    results = eager_map(
        _parse_one,
        xml_files,
        initializer=_init_worker,
        initargs=(init_args,),
        desc="Parsing",
    )
    n_ok = results.count("ok") + results.count("skipped")
    n_empty = results.count("empty")
    n_err = len(results) - n_ok - n_empty
    print(f"\nDone. OK: {n_ok}, Empty: {n_empty}, Errors: {n_err}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Parse House of Representatives Hansard XML files into CSV/Parquet."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--xml", help="Parse a single XML file (prints summary)")
    group.add_argument("--xml-dir", help="Parse all XMLs in this directory")
    parser.add_argument(
        "--out-dir",
        default="../data/output/house/daily_raw",
        help="Output directory for daily files (used with --xml-dir)",
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Re-parse files that already have output",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Disable parallel processing (useful for debugging)",
    )
    args = parser.parse_args()

    if args.xml:
        xml_path = Path(args.xml)
        df = parse_sitting_day_house(xml_path)
        print(f"\n{xml_path.name}: {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        if not df.empty:
            print("\nFirst 5 rows:")
            print(df.head().to_string())
            print("\nRow type breakdown:")
            if "interject" in df.columns:
                print(f"  Interjections: {df['interject'].sum()}")
            print(f"  Questions: {df['question'].sum()}")
            print(f"  Answers: {df['answer'].sum()}")
            print(f"  Q in writing: {df['q_in_writing'].sum()}")
            print(f"  Stage directions: {(df['name'] == 'stage direction').sum()}")
            print(f"  Main chamber rows: {(df['fedchamb_flag'] == 0).sum()}")
            print(f"  Federation Chamber rows: {(df['fedchamb_flag'] == 1).sum()}")
    else:
        parse_all_house(
            Path(args.xml_dir),
            Path(args.out_dir),
            skip_existing=not args.no_skip,
            sequential=args.sequential,
        )


if __name__ == "__main__":
    main()
