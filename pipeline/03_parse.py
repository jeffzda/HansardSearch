"""
03_parse.py — Core Australian Senate Hansard XML parser.

Handles three schema versions:
  v2.0 (1998–~2003):  Custom XML markup, <para>/<inline>
  v2.1 (~2004–2013):  Same as v2.0 with minor additions (<separator>, <list>)
  v2.2 (2014–present): OOXML-derived HTML/CSS-class model

The parser uses direct node-iteration over lxml.etree trees (not the
text-blob-split approach used in the Katz/Alexander R pipeline).

Usage:
    python 03_parse.py --xml ../data/raw/senate/2020-02-04.xml
    python 03_parse.py --xml-dir ../data/raw/senate --out-dir ../data/output/senate/daily
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
    Worker function: parse one Senate XML file and write CSV + Parquet output.
    Returns a status string: 'ok', 'skipped', or 'empty'.
    """
    out_csv = _WORKER_OUT_DIR / f"{xml_path.stem}.csv"
    out_parquet = _WORKER_OUT_DIR / f"{xml_path.stem}.parquet"

    if _WORKER_SKIP_EXISTING and out_csv.exists() and out_parquet.exists():
        return "skipped"

    df = parse_sitting_day(xml_path)

    if df.empty:
        return "empty"

    df.to_csv(out_csv, index=False)
    df.to_parquet(out_parquet, index=False)
    return "ok"

# ── Schema version constants ───────────────────────────────────────────────────

V2x = ("2.0", "2.1")   # legacy custom-markup versions
V22 = ("2.2",)          # OOXML-derived version

# ── Stage direction patterns (Senate-specific) ─────────────────────────────────

STAGE_PATTERNS_21 = re.compile(
    "|".join([
        r"Senate adjourned at \d+[.:\u2019]\d+\s*[apAP]\.?[mM]\.?",
        r"Senate adjourned",
        r"The Senate divided\.",
        r"The committee divided\.",
        r"Question agreed to\.",
        r"Question negatived\.",
        r"Questions? so resolved in the (affirmative|negative)\.",
        r"Bill read a \w+( time)?",
        r"Bill read the \w+ time\.",
        r"Leave (was |not )?granted\.",
        r"The PRESIDENT declared",
        r"The ACTING (DEPUTY )?PRESIDENT declared",
        r"Sitting suspended from",
        r"Debate (adjourned|resumed|interrupted|resumed from)\.",
        r"Time expired\.",
        r"A division having been called",
        r"The (Senate |committee )?rose",
        r"Ordered to be printed\.",
        r"Message from the House of Representatives",
        r"\* denotes teller",
        r"Ayes \d+, Noes \d+",
        r"The PRESIDENT took the chair",
        r"MADAM PRESIDENT took the chair",
        r"The DEPUTY PRESIDENT took the chair",
        r"Quorum (formed|called)",
        r"Senate resumed",
        r"(The )?amendment( proposed)? negatived",
        r"(The )?amendment agreed to",
        r"Pursuant to order",
        r"The petition was tabled",
        r"Oath of Allegiance",
        r"(Formal )?motion agreed to",
        r"Bills? received from the House of Representatives",
        r"Amendment received from the House of Representatives",
    ]),
    re.IGNORECASE,
)

# ── Output schema columns ──────────────────────────────────────────────────────

OUTPUT_COLUMNS = [
    "name", "order", "speech_no", "page_no", "time_stamp",
    "name_id", "state", "party", "in_gov", "first_speech",
    "body", "question", "answer", "q_in_writing", "div_flag",
    "gender", "unique_id", "interject", "senate_flag",
]

# ── Utility functions ──────────────────────────────────────────────────────────

def _t(element, tag: str, default: str = "") -> str:
    """Get text from a named child element, stripped."""
    el = element.find(tag)
    if el is None:
        return default
    return (el.text or "").strip()


def _all_text(element) -> str:
    """
    Recursively collect all text content from an element and its descendants,
    equivalent to R's xml_text() on a node.
    """
    return "".join(element.itertext()).strip()


def _normalise_time(raw: str) -> str | None:
    """
    Normalise a time string to HH:MM:SS.
    Handles formats: '14:01:00', '14:01', '2.30 pm', '2.30 p.m.', '14.01'.
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


def _is_stage_direction(body: str) -> bool:
    return bool(STAGE_PATTERNS_21.search(body))


# ── v2.0 / v2.1 parser ────────────────────────────────────────────────────────

class ParserV21:
    """
    Parser for Senate Hansard XML schema versions 2.0 and 2.1 (1998–~2013).

    Text model: <para> / <inline> within speech elements.
    Interjections: separate sibling <interjection> elements.
    """

    SPEECH_TAGS = {"speech", "question", "answer", "interjection", "continue"}

    def __init__(self, root: etree._Element, date_str: str):
        self.root = root
        self.date_str = date_str
        self._rows: list[dict] = []

    def parse(self) -> list[dict]:
        chamber = self.root.find(".//chamber.xscript")
        if chamber is None:
            raise ValueError(f"No chamber.xscript in {self.date_str}")

        # Business start
        bs = chamber.find("business.start")
        if bs is not None:
            self._rows.append(self._parse_business_start(bs))

        # Debates, petitions, and top-level adjournment for v2.0/v2.1
        for child in chamber:
            if child.tag == "debate":
                self._parse_debate(child)
            elif child.tag == "adjournment":
                self._parse_adjournment_element(child)
            elif child.tag == "petition.group":
                self._parse_petition_group(child)

        # Answers to questions on notice (v2.1 era: sibling of chamber.xscript)
        atq = self.root.find("answers.to.questions")
        if atq is not None:
            self._parse_answers_to_questions(atq)

        return self._rows

    # ── Business start ─────────────────────────────────────────────────────

    def _parse_business_start(self, bs: etree._Element) -> dict:
        paras = [_all_text(p) for p in bs.iter("para")]
        body = " ".join(p for p in paras if p)

        # Extract start time from the chair-taking sentence
        time_match = re.search(
            r"\b(\d{1,2})[.,](\d{2})\s*([apAP]\.?[mM]\.?)"
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
        )

    # ── Answers to questions on notice (v2.1: root-level sibling) ─────────

    def _parse_answers_to_questions(self, node: etree._Element) -> None:
        """Parse <answers.to.questions> section with q_in_writing=1."""
        for debate in node.iter("debate"):
            for subdebate in debate:
                if subdebate.tag == "subdebate.1":
                    for child in subdebate:
                        if child.tag in ("question", "answer"):
                            self._parse_speech_node(
                                child, context={}, q_in_writing=1,
                            )

    # ── Adjournment (v2.0/v2.1 top-level element) ─────────────────────────

    def _parse_adjournment_element(self, adj: etree._Element) -> None:
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
        ))

    # ── Petitions ──────────────────────────────────────────────────────────

    def _parse_petition_group(self, node: etree._Element) -> None:
        """Parse <petition.group>: Clerk's interjection + individual petitions."""
        for child in node:
            if child.tag == "interjection":
                self._parse_speech_node(child, context={})
            elif child.tag == "petition":
                self._parse_petition(child)

    def _parse_petition(self, node: etree._Element) -> None:
        """Emit one row per petition, attributed to the presenting senator."""
        info = node.find("petitioninfo")
        page_no = _t(info, "page.no") or None if info is not None else None

        # Presenter name / name_id from <presenter>/<talk.start>/<talker>
        talker_data: dict = {}
        presenter = node.find("presenter")
        if presenter is not None:
            ts_node = presenter.find("talk.start")
            if ts_node is not None:
                talker_data = self._parse_talker(ts_node.find("talker"))

        # Petition body: title + all <para> children of <petition> itself
        parts = []
        if info is not None:
            title = _t(info, "title")
            if title:
                parts.append(title)
        for child in node:
            if child.tag == "para":
                text = _all_text(child).strip()
                if text:
                    parts.append(text)
        # Presenter's statement (e.g. "Senator Forshaw (from 33 citizens)")
        if presenter is not None:
            ts_node = presenter.find("talk.start")
            if ts_node is not None:
                for el in ts_node:
                    if el.tag != "talker":
                        text = _all_text(el).strip()
                        if text:
                            parts.append(text)

        body = " ".join(parts).strip()
        if not body:
            return

        self._rows.append(self._make_row(
            name=talker_data.get("name") or "stage direction",
            name_id=talker_data.get("name_id"),
            body=body,
            page_no=page_no or talker_data.get("page_no"),
            time_stamp=talker_data.get("time_stamp"),
            row_type="petition",
        ))

    # ── Cognates ───────────────────────────────────────────────────────────

    def _parse_cognate(self, node: etree._Element) -> None:
        """Emit a stage direction row for a <cognate> header."""
        info = node.find("cognateinfo")
        if info is None:
            return
        title = _t(info, "title")
        if not title:
            return
        self._rows.append(self._make_row(
            name="stage direction",
            body=title,
            row_type="stage_direction",
        ))

    # ── Debate walking ─────────────────────────────────────────────────────

    def _parse_debate(self, debate: etree._Element,
                      debate_context: dict | None = None) -> None:
        info = debate.find("debateinfo")
        context = dict(debate_context or {})
        if info is not None:
            context["debate_title"] = _t(info, "title")
            context["debate_type"] = _t(info, "type")

        for child in debate:
            tag = child.tag
            if tag == "subdebate.1":
                self._parse_subdebate(child, level=1, context=context)
            elif tag in self.SPEECH_TAGS:
                self._parse_speech_node(child, context)
            elif tag == "division":
                self._parse_division(child)
            elif tag == "adjournment":
                self._parse_adjournment_element(child)
            elif tag == "motionnospeech":
                self._parse_motionnospeech(child, context)
            elif tag == "cognate":
                self._parse_cognate(child)
            # debateinfo, amendments: skip remaining structural elements

    def _parse_subdebate(self, sub: etree._Element, level: int,
                         context: dict) -> None:
        info = sub.find("subdebateinfo")
        ctx = dict(context)
        if info is not None:
            ctx[f"sub{level}_title"] = _t(info, "title")

        for child in sub:
            tag = child.tag
            if tag == "subdebate.2":
                self._parse_subdebate(child, level=2, context=ctx)
            elif tag in self.SPEECH_TAGS:
                self._parse_speech_node(child, ctx)
            elif tag == "division":
                self._parse_division(child)
            elif tag == "motionnospeech":
                self._parse_motionnospeech(child, ctx)

    # ── Speech node parsing ────────────────────────────────────────────────

    def _parse_speech_node(self, node: etree._Element,
                           context: dict,
                           q_in_writing: int = 0) -> None:
        tag = node.tag

        talk_start = node.find("talk.start")
        if talk_start is None:
            return

        talker_data = self._parse_talker(talk_start.find("talker"))

        # Body text: all <para> children of talk.start plus
        # all <para> direct children of the speech node
        body_parts = []

        # Paragraphs inside talk.start (after the talker element)
        for el in talk_start:
            if el.tag not in ("talker",):
                text = _all_text(el)
                if text:
                    body_parts.append(text)

        # Paragraphs, motions, quotes directly on the speech node
        for el in node:
            if el.tag not in ("talk.start", "interjection", "continue"):
                text = _all_text(el)
                if text:
                    body_parts.append(text)

        # Clean leading em-dash
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
            **talker_data,
        )
        self._rows.append(row)

        # Recurse into <interjection> and <continue> children
        for child in node:
            if child.tag in ("interjection", "continue"):
                self._parse_speech_node(child, context,
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
            # Format: "Cook, Sen Peter" → "Senator COOK"
            sur, _ = _parse_metadata_name(name_metadata)
            name_display = f"Senator {sur.upper()}" if sur else name_metadata

        name_id = txt("name.id") or None

        # Normalise PHID to uppercase
        if name_id:
            name_id = name_id.strip().upper()
            if name_id in ("", "NONE", "NAN"):
                name_id = None

        return {
            "name": name_display,
            "name_id": name_id,
            "state": txt("electorate") or None,
            "party": txt("party") or None,
            "in_gov": _to_int(txt("in.gov")),
            "first_speech": _to_int(txt("first.speech")),
            "page_no": txt("page.no") or None,
            "time_stamp": _normalise_time(txt("time.stamp")),
        }

    # ── Motionnospeech ─────────────────────────────────────────────────────

    def _parse_motionnospeech(self, node: etree._Element,
                              context: dict) -> None:
        name_text = _t(node, "name")
        ts = _normalise_time(_t(node, "time.stamp"))

        # Collect body from inline, motion, para
        parts = []
        for el in node:
            if el.tag not in ("name", "electorate", "role", "time.stamp"):
                text = _all_text(el)
                if text:
                    parts.append(text)
        body = " ".join(parts).strip()

        # Derive display name: "Senator FERRIS" from raw "Senator FERRIS"
        # or construct from the text
        if name_text:
            if not name_text.startswith("Senator"):
                name_text = f"Senator {name_text.split()[-1].upper()}"

        self._rows.append(self._make_row(
            name=name_text or "stage direction",
            body=body,
            time_stamp=ts,
            row_type="motionnospeech",
        ))

    # ── Division ───────────────────────────────────────────────────────────

    def _parse_division(self, div: etree._Element) -> None:
        header = div.find("division.header")
        ts = None
        header_text = ""
        if header is not None:
            ts = _normalise_time(_t(header, "time.stamp"))
            header_text = _all_text(header)

        result_el = div.find("division.result")
        result_text = _all_text(result_el) if result_el is not None else ""

        # Chair para
        chair_text = ""
        for p in div.findall("para"):
            txt_val = _all_text(p).strip()
            if txt_val.startswith("(") or "Chair" in txt_val or "President" in txt_val:
                chair_text = txt_val
                break

        body = " ".join(filter(None, [header_text, chair_text, result_text])).strip()

        self._rows.append(self._make_row(
            name="stage direction",
            body=body,
            time_stamp=ts,
            row_type="division",
            div_flag=1,
        ))

    # ── Row factory ────────────────────────────────────────────────────────

    def _make_row(self, *, name: str | None = None, body: str = "",
                  time_stamp: str | None = None, row_type: str = "speech",
                  question: int = 0, answer: int = 0, interject_flag: int = 0,
                  div_flag: int = 0, name_id: str | None = None,
                  state: str | None = None, party: str | None = None,
                  in_gov: int | None = None, first_speech: int | None = None,
                  page_no: str | None = None, q_in_writing: int = 0,
                  **_) -> dict:
        return {
            "name": name,
            "body": body,
            "time_stamp": time_stamp,
            "row_type": row_type,
            "question": question,
            "answer": answer,
            "_interject_flag": interject_flag,  # provisional; overwritten later
            "div_flag": div_flag,
            "name_id": name_id,
            "state": state,
            "party": party,
            "in_gov": in_gov,
            "first_speech": first_speech,
            "page_no": page_no,
            # Placeholders filled later
            "q_in_writing": q_in_writing,
            "gender": None,
            "unique_id": None,
            "interject": None,
            "speech_no": None,
            "order": None,
            "senate_flag": 1,
        }


# ── v2.2 parser ───────────────────────────────────────────────────────────────

# HPS class → speech type mapping
HPS_TYPE_MAP = {
    "HPS-MemberSpeech":      "speech",
    "HPS-OfficeSpeech":      "speech",
    "HPS-MemberContinuation":"continue",
    "HPS-MemberInterjecting":"interjection",
    "HPS-OfficeInterjecting":"interjection",
    "HPS-MemberQuestion":    "question",
    "HPS-MemberAnswer":      "answer",
}

# <a type="..."> → row type mapping
A_TYPE_MAP = {
    "MemberSpeech":      "speech",
    "OfficeSpeech":      "speech",
    "MemberContinuation":"continue",
    "MemberInterjecting":"interjection",
    "OfficeInterjecting":"interjection",
    "MemberQuestion":    "question",
    "MemberAnswer":      "answer",
}

# Span classes that represent speaker identification (not body text)
SPEAKER_ID_CLASSES = {
    "HPS-MemberSpeech", "HPS-OfficeSpeech",
    "HPS-MemberContinuation", "HPS-MemberInterjecting",
    "HPS-OfficeInterjecting", "HPS-MemberQuestion", "HPS-MemberAnswer",
    "HPS-Electorate", "HPS-MinisterialTitles", "HPS-Time",
}


class ParserV22:
    """
    Parser for Senate Hansard XML schema version 2.2 (2014–present).

    Text model: <talk.text>/<body>/<p class="HPS-*">/<span> structure.
    Speaker type encoded in <a type="..."> and <span class="HPS-*"> within
    the body text.
    """

    def __init__(self, root: etree._Element, date_str: str):
        self.root = root
        self.date_str = date_str
        self._rows: list[dict] = []

    def parse(self) -> list[dict]:
        chamber = self.root.find(".//chamber.xscript")
        if chamber is None:
            raise ValueError(f"No chamber.xscript in {self.date_str}")

        bs = chamber.find("business.start")
        if bs is not None:
            self._rows.append(self._parse_business_start(bs))

        for child in chamber:
            if child.tag == "debate":
                self._parse_debate(child)

        # Also handle answers.to.questions as root-level sibling
        # (edge case in some v2.2 transitional files)
        atq = self.root.find("answers.to.questions")
        if atq is not None:
            self._parse_atq_root(atq)

        return self._rows

    # ── Answers to questions (root-level, edge cases) ──────────────────────

    def _parse_atq_root(self, node: etree._Element) -> None:
        """Parse <answers.to.questions> section at root level (q_in_writing=1)."""
        for debate in node.iter("debate"):
            for child in debate:
                if child.tag == "subdebate.1":
                    for item in child:
                        if item.tag in ("question", "answer", "speech"):
                            self._parse_speech_v22(
                                item,
                                is_question=(item.tag == "question"),
                                is_answer=(item.tag == "answer"),
                                q_in_writing=1,
                            )

    # ── Business start ─────────────────────────────────────────────────────

    def _parse_business_start(self, bs: etree._Element) -> dict:
        # v2.2: business.start contains a <body> with <p class="HPS-SODJobDate">
        # and <p class="HPS-Normal"> for the chair-taking sentence.
        body_parts = []
        ts = None
        for p in bs.iter("p"):
            text = _all_text(p).strip()
            if text:
                body_parts.append(text)
                if ts is None:
                    # Try to extract time from the chair-taking paragraph
                    m = re.search(
                        r"\b(\d{1,2}[.,:]\d{2})\s*([apAP]\.?[mM]\.?)?",
                        text,
                    )
                    if m:
                        ts = _normalise_time(m.group(0))

        body = " ".join(body_parts)
        return self._make_row(
            name="business start",
            body=body,
            time_stamp=ts,
            row_type="business_start",
        )

    # ── Debate walking ─────────────────────────────────────────────────────

    # Debate types that indicate questions/answers on notice (written questions)
    _QON_TYPES = frozenset({
        "QUESTIONS ON NOTICE",
        "ANSWERS TO QUESTIONS ON NOTICE",
        "Questions on Notice",
        "Answers to Questions on Notice",
    })

    def _parse_debate(self, debate: etree._Element) -> None:
        info = debate.find("debateinfo")
        debate_type = ""
        if info is not None:
            debate_type = _t(info, "type")

        q_in_writing = 1 if debate_type in self._QON_TYPES else 0

        for child in debate:
            tag = child.tag
            if tag in ("subdebate.1", "subdebate.2"):
                self._parse_subdebate(child, q_in_writing=q_in_writing)
            elif tag == "speech":
                self._parse_speech_v22(child, is_question=False, is_answer=False,
                                       q_in_writing=q_in_writing)
            elif tag == "question":
                self._parse_speech_v22(child, is_question=True, is_answer=False,
                                       q_in_writing=q_in_writing)
            elif tag == "answer":
                self._parse_speech_v22(child, is_question=False, is_answer=True,
                                       q_in_writing=q_in_writing)
            elif tag == "division":
                self._parse_division_v22(child)
            elif tag == "subdebate.text":
                self._parse_subdebate_text(child)

    def _parse_subdebate(self, sub: etree._Element,
                         q_in_writing: int = 0) -> None:
        for child in sub:
            tag = child.tag
            if tag in ("subdebate.1", "subdebate.2"):
                self._parse_subdebate(child, q_in_writing=q_in_writing)
            elif tag == "speech":
                self._parse_speech_v22(child, is_question=False, is_answer=False,
                                       q_in_writing=q_in_writing)
            elif tag == "question":
                self._parse_speech_v22(child, is_question=True, is_answer=False,
                                       q_in_writing=q_in_writing)
            elif tag == "answer":
                self._parse_speech_v22(child, is_question=False, is_answer=True,
                                       q_in_writing=q_in_writing)
            elif tag == "division":
                self._parse_division_v22(child)
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
                          q_in_writing: int = 0) -> None:
        """
        Parse a single <speech> element in v2.2 format.

        The <talk.start>/<talker> provides name_id and metadata name.
        The <talk.text>/<body>/<p> elements provide the body text
        and speaker identification via <a type="..."> elements.

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
            # No body — emit a minimal row if we have talker data
            if talker_data.get("name_id"):
                self._rows.append(self._make_row(
                    body="",
                    question=1 if is_question else 0,
                    answer=1 if is_answer else 0,
                    q_in_writing=q_in_writing,
                    **talker_data,
                ))
            return

        # Extract attributed utterances from the talk.text body
        utterances = self._extract_utterances_v22(talk_text, talker_data)

        for utt in utterances:
            row = self._make_row(
                body=utt["body"],
                name=utt.get("name") or talker_data.get("name"),
                name_id=utt.get("name_id") or talker_data.get("name_id"),
                time_stamp=utt.get("time_stamp") or talker_data.get("time_stamp"),
                state=utt.get("state") or talker_data.get("state"),
                party=talker_data.get("party"),
                in_gov=talker_data.get("in_gov"),
                first_speech=talker_data.get("first_speech"),
                page_no=talker_data.get("page_no"),
                question=1 if (is_question or utt.get("row_type") == "question") else 0,
                answer=1 if (is_answer or utt.get("row_type") == "answer") else 0,
                interject_flag=1 if utt.get("row_type") == "interjection" else 0,
                row_type=utt.get("row_type", "speech"),
                q_in_writing=q_in_writing,
            )
            self._rows.append(row)

    def _extract_utterances_v22(self, talk_text: etree._Element,
                                talker_data: dict) -> list[dict]:
        """
        Walk all <p> elements within talk_text. Split into utterances when an
        <a type="Member*|Office*"> introduces a new speaker attribution.

        Returns a list of dicts with keys: body, name, name_id, time_stamp,
        state, row_type.
        """
        utterances = []
        current = {
            "name": talker_data.get("name"),
            "name_id": talker_data.get("name_id"),
            "time_stamp": talker_data.get("time_stamp"),
            "state": talker_data.get("state"),
            "row_type": "speech",
            "body_parts": [],
        }

        for p in talk_text.iter("p"):
            p_class = p.get("class", "")
            if "HPS-DivisionPreamble" in p_class or "HPS-DivisionFooter" in p_class:
                # Division text inside a speech — emit as stage direction
                div_text = _all_text(p).strip()
                if div_text:
                    utterances.append({**current, "body": " ".join(current["body_parts"])})
                    utterances.append({
                        "name": "stage direction",
                        "name_id": None,
                        "time_stamp": None,
                        "state": None,
                        "row_type": "stage_direction",
                        "body": div_text,
                    })
                    current["body_parts"] = []
                continue

            # Scan <a> elements in this paragraph
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

                    # Flush current utterance (only if it has body content — an
                    # empty flush here would emit a spurious row from the
                    # initial <talk.start> metadata before any body is seen)
                    body_so_far = " ".join(current["body_parts"]).strip()
                    if body_so_far:
                        utterances.append({**current, "body": body_so_far})

                    # Start new utterance
                    # Name from HPS class span inside <a>
                    name_text = _all_text(a).strip()
                    # PHID from href (lowercase → uppercase)
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

                    # State from <span class="HPS-Electorate">
                    # The state name may be split across consecutive HPS-Electorate
                    # spans (e.g. "South " + "Australia").  Do NOT strip individual
                    # parts — trailing spaces are significant separators.
                    state = None
                    if parent is not None:
                        state_parts = [
                            "".join(s.itertext())
                            for s in parent.findall(".//span")
                            if "HPS-Electorate" in s.get("class", "")
                        ]
                        if state_parts:
                            raw = "".join(state_parts).strip(" ()").strip()
                            state = " ".join(raw.split())

                    current = {
                        "name": name_text,
                        "name_id": name_id or current.get("name_id"),
                        "time_stamp": ts,
                        "state": state or current.get("state"),
                        "row_type": row_type,
                        "body_parts": [],
                    }

                # Now extract body text for this paragraph, skipping
                # speaker-ID spans
                para_body = self._extract_body_text_v22(p)
                if para_body:
                    current["body_parts"].append(para_body)
            else:
                # Regular paragraph — add to current utterance body
                para_body = _all_text(p).strip()
                # Skip HPS-Line (separator) and HPS-SODJobDate paragraphs
                if para_body and p_class not in ("HPS-Line", "HPS-SODJobDate"):
                    current["body_parts"].append(para_body)

        # Flush last utterance (only emit if there is body content)
        body_so_far = " ".join(current["body_parts"]).strip()
        if body_so_far:
            utterances.append({**current, "body": body_so_far})

        return utterances

    def _extract_body_text_v22(self, p: etree._Element) -> str:
        """
        Extract body text from a <p> element, excluding speaker-ID spans.
        Speaker attribution is in <span class="HPS-MemberSpeech|..."> elements
        inside <a> elements; also skip <span class="HPS-Time"> and
        <span class="HPS-Electorate">.
        """
        parts = []

        def visit(el, in_attribution: bool):
            cls = el.get("class", "")
            if el.tag == "a" and el.get("type", "") in A_TYPE_MAP:
                # Tail may contain speech content (MemberContinuation, MemberInterjecting).
                # Strip leading attribution punctuation: "(Electorate—Title) (HH:MM): "
                if el.tail:
                    tail = re.sub(r"^[():\s\u2014\-]+", "", el.tail).strip()
                    if tail:
                        parts.append(tail)
                return
            if cls in SPEAKER_ID_CLASSES:
                # HPS-Time tail: "):  SPEECH CONTENT" — strip closing-paren+colon prefix.
                # Other attribution spans also carry speech content after stripping punctuation.
                if el.tail:
                    tail = re.sub(r"^[():\s\u2014\-]+", "", el.tail).strip()
                    if tail:
                        parts.append(tail)
                return
            # Collect text
            if el.text and not in_attribution:
                t = el.text.strip()
                if t:
                    parts.append(t)
            for child in el:
                visit(child, in_attribution)
            # Tail text
            if el.tail:
                t = el.tail.strip()
                if t:
                    parts.append(t)

        # Process the paragraph's direct content
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
        """Parse <talker> element in v2.2 (no <name role="display"> or <role>)."""

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
            if name_metadata.upper() == "PRESIDENT, THE":
                name_display = "The PRESIDENT"
            else:
                sur, _ = _parse_metadata_name(name_metadata)
                name_display = f"Senator {sur.upper()}" if sur else name_metadata

        name_id = txt("name.id") or None
        if name_id:
            name_id = name_id.strip().upper()
            if name_id in ("", "NONE", "NAN"):
                name_id = None

        return {
            "name": name_display,
            "name_id": name_id,
            "state": txt("electorate") or None,
            "party": txt("party") or None,
            "in_gov": _to_int(txt("in.gov")),
            "first_speech": _to_int(txt("first.speech")),
            "page_no": txt("page.no") or None,
            "time_stamp": _normalise_time(txt("time.stamp")),
        }

    # ── Division (v2.2) ────────────────────────────────────────────────────

    def _parse_division_v22(self, div: etree._Element) -> None:
        """
        In v2.2, division data is embedded in <p class="HPS-DivisionPreamble">
        and <p class="HPS-DivisionFooter"> elements.
        """
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
        ))

    # ── Row factory ────────────────────────────────────────────────────────

    def _make_row(self, *, name: str | None = None, body: str = "",
                  time_stamp: str | None = None, row_type: str = "speech",
                  question: int = 0, answer: int = 0, interject_flag: int = 0,
                  div_flag: int = 0, name_id: str | None = None,
                  state: str | None = None, party: str | None = None,
                  in_gov: int | None = None, first_speech: int | None = None,
                  page_no: str | None = None, q_in_writing: int = 0,
                  **_) -> dict:
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
            "state": state,
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
            "senate_flag": 1,
        }


# ── Shared post-processing ────────────────────────────────────────────────────

def _parse_metadata_name(metadata: str) -> tuple[str, str]:
    """
    Parse Senate metadata name "Cook, Sen Peter" → ("Cook", "Peter").
    Also handles "PRESIDENT, The" → ("The PRESIDENT", "").
    """
    if not metadata:
        return "", ""
    # Special: presiding officers
    if metadata.upper() in ("PRESIDENT, THE", "THE PRESIDENT"):
        return "The PRESIDENT", ""
    parts = metadata.split(",", 1)
    surname = parts[0].strip()
    first_raw = parts[1].strip() if len(parts) > 1 else ""
    # Strip "Sen " or "Sen. " prefix
    first = re.sub(r"^Sen\.?\s+", "", first_raw).strip()
    return surname, first


def _to_int(val: str | None) -> int | None:
    if val is None or val.strip() == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


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
        # interjection / continue inherit current_speech
        row["speech_no"] = current_speech
    return rows


def _flag_interjections(rows: list[dict]) -> list[dict]:
    """
    Set interject flag:
      0 — first row in a speech group, stage directions, presiding officers
      1 — all other rows within a speech group (interjections, continuations
          by different senators, etc.)

    Uses the provisional _interject_flag set during parsing, then
    refines using speech_no grouping.
    """
    if not rows:
        return rows

    # Group rows by speech_no
    # Within each group, the first row is interject=0; subsequent rows
    # by a different name_id are interject=1; continuations by the same
    # name_id are interject=0.
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

            # First row: never an interjection
            if j == 0:
                rows[idx]["interject"] = 0
                continue

            # Presiding officer (name_id = 10000) never flagged as interjection
            if nid == "10000":
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

            # Default
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


def _clean_to_output(rows: list[dict]) -> pd.DataFrame:
    """Convert row list to DataFrame with final output columns."""
    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(rows)

    # Replace name for stage directions
    if "body" in df.columns:
        mask_sd = df["row_type"].isin(["stage_direction", "division"])
        df.loc[mask_sd, "name"] = "stage direction"

    # Rename _interject_flag — no longer needed after flagging
    if "_interject_flag" in df.columns:
        df = df.drop(columns=["_interject_flag"])

    # Drop internal columns
    for col in ["row_type", "debate_title", "debate_type",
                "sub1_title", "sub2_title"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Coerce numeric
    for col in ("in_gov", "first_speech", "question", "answer",
                "q_in_writing", "div_flag", "interject", "senate_flag"):
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


# ── Top-level parse function ───────────────────────────────────────────────────

def parse_sitting_day(xml_path: Path) -> pd.DataFrame:
    """
    Parse one Senate sitting-day XML file into a rectangular DataFrame.

    Args:
        xml_path: Path to a YYYY-MM-DD.xml file.

    Returns:
        DataFrame with OUTPUT_COLUMNS + ['order', 'speech_no'] columns.
        Returns an empty DataFrame on parse failure (with error logged).
    """
    date_str = xml_path.stem

    try:
        # Use recover=True to handle minor XML malformation
        parser = etree.XMLParser(recover=True, encoding=None)
        tree = etree.parse(str(xml_path), parser)
        root = tree.getroot()
    except Exception as e:
        print(f"  [ERROR] Failed to parse {xml_path.name}: {e}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    version = root.get("version", "").strip()
    if not version:
        # Some v2.0 files may omit version; default to v2.1 parser
        version = "2.0"

    try:
        if version in V2x:
            rows = ParserV21(root, date_str).parse()
        elif version in V22:
            rows = ParserV22(root, date_str).parse()
        else:
            print(f"  [WARN] Unknown schema version '{version}' in {xml_path.name}; "
                  f"trying v2.1 parser")
            rows = ParserV21(root, date_str).parse()
    except Exception as e:
        print(f"  [ERROR] Parsing failed for {xml_path.name}: {e}")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    rows = _assign_speech_numbers(rows)
    rows = _flag_interjections(rows)
    rows = _assign_order(rows)
    return _clean_to_output(rows)


# ── Batch processing ──────────────────────────────────────────────────────────

def parse_all(
    xml_dir: Path,
    out_dir: Path,
    skip_existing: bool = True,
    sequential: bool = False,
) -> None:
    """Parse all XML files in xml_dir and write daily CSV + Parquet files."""
    xml_files = sorted(xml_dir.glob("*.xml"))
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Parsing {len(xml_files)} XML files → {out_dir}")

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
            df = parse_sitting_day(xml_path)
            if df.empty:
                n_empty += 1
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
        description="Parse Senate Hansard XML files into CSV/Parquet."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--xml", help="Parse a single XML file (prints summary)")
    group.add_argument("--xml-dir", help="Parse all XMLs in this directory")
    parser.add_argument(
        "--out-dir",
        default="../data/output/senate/daily",
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
        df = parse_sitting_day(xml_path)
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
            print(f"  Stage directions: {(df['name'] == 'stage direction').sum()}")
    else:
        parse_all(
            Path(args.xml_dir),
            Path(args.out_dir),
            skip_existing=not args.no_skip,
            sequential=args.sequential,
        )


if __name__ == "__main__":
    main()
