#!/usr/bin/env python3
"""
search_corpus.py — Boolean string search across Hansard corpora.

Extract passages containing target strings from the Senate and/or House of
Representatives Hansard corpora, with rich contextual metadata for each match.

Boolean expression syntax
-------------------------
  Literal terms must be quoted (single or double quotes):
      'World Wildlife Fund'
      "WWF"

  Operators (equal precedence, evaluated strictly left-to-right):
      |   or   OR     union        rows matching either operand
      &   or   AND    intersection rows matching both operands

  Parentheses override left-to-right evaluation:
      ('WWF' | 'World Wildlife Fund') & 'Australia'

  Without parentheses A | B & C  ≡  (A | B) & C

Context enrichment
------------------
  For each matched row the script captures:
    - All corpus metadata fields (party, electorate/state, gender, etc.)
    - The debate heading inferred by scanning backwards for the most recent
      stage-direction or business-start row in the same sitting day
    - N rows of surrounding speech context (speakers + text) from the same
      sitting day, formatted as readable text
    - Which search terms triggered the match

Output
------
  case_studies/<NAME>/
      matches.csv     direct-match rows NOT part of any Q&A exchange
                      (consumed by enrich_case_study.py)
      exchanges.csv   all rows that form Q&A pairs (question + answer rows),
                      including pulled-in paired rows
                      (consumed by enrich_exchanges.py)
      summary.txt     aggregate statistics across all rows (speakers, parties, years)
  case_studies/logic.md   maps folder names → search expressions

Examples
--------
  python search_corpus.py "'WWF'"
  python search_corpus.py "'WWF' | 'World Wildlife Fund'"
  python search_corpus.py --name WWF \\
      "'WWF' | 'World Wildlife Fund' | 'World Wide Fund for Nature'"
  python search_corpus.py --context 3 --chamber senate "'climate change'"
  python search_corpus.py --party ALP GRN --date-from 2010-01-01 "'climate change'"
  python search_corpus.py --in-gov --no-interjections "'budget'"
"""

from __future__ import annotations

import argparse
import re
import sys
import textwrap
from datetime import datetime
from pathlib import Path

import pandas as pd


# ── Paths ──────────────────────────────────────────────────────────────────────

HERE = Path(__file__).resolve().parent          # pipeline/
ROOT = HERE.parent                               # project root

SENATE_CORPUS    = ROOT / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.parquet"
HOUSE_CORPUS     = ROOT / "data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet"
COMMITTEE_CORPUS = ROOT / "data/output/committee/corpus/committee_hansard_corpus_2010_to_2025.parquet"
CASE_STUDIES     = ROOT / "case_studies"
LOGIC_FILE       = CASE_STUDIES / "logic.md"


# ── Tokeniser ──────────────────────────────────────────────────────────────────

_TT_TERM   = "TERM"
_TT_AND    = "AND"
_TT_OR     = "OR"
_TT_LPAREN = "LPAREN"
_TT_RPAREN = "RPAREN"
_TT_EOF    = "EOF"


def _tokenize(expr: str) -> list[tuple[str, str]]:
    """Tokenize a boolean search expression into (type, value) pairs."""
    tokens: list[tuple[str, str]] = []
    i, n = 0, len(expr)
    while i < n:
        c = expr[i]
        if c in " \t\n\r":
            i += 1
        elif c == "(":
            tokens.append((_TT_LPAREN, c)); i += 1
        elif c == ")":
            tokens.append((_TT_RPAREN, c)); i += 1
        elif c == "|":
            tokens.append((_TT_OR, c)); i += 1
        elif c == "&":
            tokens.append((_TT_AND, c)); i += 1
        elif c in ('"', "'"):
            # quoted string — consume until matching quote
            q = c; i += 1; j = i
            while j < n and expr[j] != q:
                j += 1
            tokens.append((_TT_TERM, expr[i:j]))
            i = j + 1
        else:
            # unquoted word — could be OR/AND keyword or bare term
            j = i
            while j < n and expr[j] not in " \t\n\r()|&'\"":
                j += 1
            word = expr[i:j]
            upper = word.upper()
            if upper == "OR":
                tokens.append((_TT_OR, word))
            elif upper == "AND":
                tokens.append((_TT_AND, word))
            else:
                tokens.append((_TT_TERM, word))
            i = j
    tokens.append((_TT_EOF, ""))
    return tokens


# ── Parser — recursive descent, strictly left-to-right equal precedence ────────

class _Parser:
    """
    Grammar
    -------
        expr := atom (OP atom)*
        atom := TERM | '(' expr ')'
        OP   := '|' | '&' | OR | AND

    All operators have equal precedence and associate left-to-right.
    Parentheses override evaluation order.
    """

    def __init__(self, tokens: list[tuple[str, str]]) -> None:
        self._tok = tokens
        self._pos = 0

    def _peek(self) -> tuple[str, str]:
        return self._tok[self._pos]

    def _eat(self) -> tuple[str, str]:
        t = self._tok[self._pos]; self._pos += 1; return t

    def parse_expr(self) -> dict:
        node = self._parse_atom()
        while self._peek()[0] in (_TT_AND, _TT_OR):
            op_tok = self._eat()
            op     = "AND" if op_tok[0] == _TT_AND else "OR"
            right  = self._parse_atom()
            node   = {"op": op, "left": node, "right": right}
        return node

    def _parse_atom(self) -> dict:
        t, v = self._peek()
        if t == _TT_TERM:
            self._eat()
            return {"op": "TERM", "term": v}
        elif t == _TT_LPAREN:
            self._eat()
            node = self.parse_expr()
            if self._peek()[0] != _TT_RPAREN:
                raise SyntaxError(f"Expected ')' near token {self._pos}")
            self._eat()
            return node
        else:
            raise SyntaxError(f"Unexpected token {t!r}={v!r} at position {self._pos}")

    def parse(self) -> dict:
        node = self.parse_expr()
        if self._peek()[0] != _TT_EOF:
            raise SyntaxError(f"Trailing tokens starting at {self._peek()!r}")
        return node


def parse_expression(expr: str) -> dict:
    """Parse a boolean search expression into a nested-dict AST."""
    return _Parser(_tokenize(expr)).parse()


def collect_terms(tree: dict) -> list[str]:
    """Return all literal search terms from the AST (may contain duplicates)."""
    if tree["op"] == "TERM":
        return [tree["term"]]
    return collect_terms(tree["left"]) + collect_terms(tree["right"])


def first_term(tree: dict) -> str:
    """Return the leftmost literal term in the AST (used as default folder name)."""
    if tree["op"] == "TERM":
        return tree["term"]
    return first_term(tree["left"])


# ── Mask evaluation ────────────────────────────────────────────────────────────

def _build_masks(df: pd.DataFrame, terms: list[str], case_sensitive: bool = False) -> dict[str, pd.Series]:
    """Return a substring boolean mask for each unique term using PyArrow.

    Single-threaded: the chamber-level _THREAD_POOL in app.py provides the
    appropriate parallelism for the "both" path. A nested ThreadPoolExecutor
    here causes thread-storm overhead on small VPS cores.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    body_col = df["body"]
    if hasattr(body_col, "array") and hasattr(body_col.array, "_pa_array"):
        arr = body_col.array._pa_array   # ChunkedArray, zero-copy; pc.match_substring handles it
    else:
        arr = pa.array(body_col)
    return {
        t: pd.Series(
            pc.match_substring(arr, t, ignore_case=not case_sensitive)
              .to_numpy(zero_copy_only=False),
            index=df.index, dtype=bool,
        )
        for t in set(terms)
    }


def _eval_tree(tree: dict, masks: dict[str, pd.Series]) -> pd.Series:
    """Recursively evaluate the AST against precomputed term masks."""
    if tree["op"] == "TERM":
        return masks[tree["term"]]
    left  = _eval_tree(tree["left"],  masks)
    right = _eval_tree(tree["right"], masks)
    return (left | right) if tree["op"] == "OR" else (left & right)


# ── Metadata pre-filters ───────────────────────────────────────────────────────

def _apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """Apply metadata pre-filters before text search."""
    if not filters:
        return df
    orig = len(df)
    mask = pd.Series(True, index=df.index)

    if "date_ranges" in filters and filters["date_ranges"]:
        ranges = filters["date_ranges"]
        date_mask = pd.Series(False, index=df.index)
        for r in ranges:
            lo, hi = r.get("from", ""), r.get("to", "")
            if lo and hi:
                date_mask |= (df["date"] >= lo) & (df["date"] <= hi)
            elif lo:
                date_mask |= df["date"] >= lo
            elif hi:
                date_mask |= df["date"] <= hi
        mask &= date_mask
    elif "date_from" in filters or "date_to" in filters:
        if "date_from" in filters:
            mask &= df["date"] >= filters["date_from"]
        if "date_to" in filters:
            mask &= df["date"] <= filters["date_to"]

    if "party" in filters and "party" in df.columns:
        vals = [v.upper() for v in filters["party"]]
        mask &= df["party"].str.upper().isin(vals)

    if "gender" in filters and "gender" in df.columns:
        mask &= df["gender"].str.lower() == filters["gender"].lower()

    # Exact-match integer/bool flag columns
    for col in ("in_gov", "interject", "question", "answer", "first_speech",
                "q_in_writing", "fedchamb_flag", "div_flag"):
        if col in filters and col in df.columns:
            mask &= pd.to_numeric(df[col], errors="coerce") == filters[col]

    if "has_embedded_interject" in filters and "has_embedded_interject" in df.columns:
        mask &= df["has_embedded_interject"].astype(bool) == filters["has_embedded_interject"]

    if "row_type" in filters:
        q = pd.to_numeric(df.get("question", 0), errors="coerce").fillna(0)
        a = pd.to_numeric(df.get("answer",   0), errors="coerce").fillna(0)
        is_exchange = (q == 1) | (a == 1)
        if filters["row_type"] == "exchange":
            mask &= is_exchange
        elif filters["row_type"] == "speech":
            mask &= ~is_exchange

    # Substring matches
    if "speaker" in filters and "name" in df.columns:
        mask &= df["name"].str.contains(filters["speaker"], case=False, na=False, regex=False)

    if "electorate" in filters and "electorate" in df.columns:
        mask &= df["electorate"].str.contains(filters["electorate"], case=False, na=False, regex=False)

    if "state" in filters and "state" in df.columns:
        mask &= df["state"].str.contains(filters["state"], case=False, na=False, regex=False)

    if "name_id" in filters and "name_id" in df.columns:
        nids = filters["name_id"]
        if isinstance(nids, list):
            mask &= df["name_id"].isin(nids)
        else:
            mask &= df["name_id"] == nids

    if "unique_id" in filters and "unique_id" in df.columns:
        mask &= df["unique_id"] == filters["unique_id"]

    df = df[mask].copy()
    print(f"  Filters applied: {orig:,} → {len(df):,} rows")
    return df


# ── Context helpers ────────────────────────────────────────────────────────────

_CTX_SEP = " ¶ "   # separator between context turns within a single CSV cell
_NL_RE   = re.compile(r"\s*\n\s*")


def _safe(series: pd.Series, key: str, default: str = "") -> str:
    """Return a clean string value from a Series, treating NaN/None as empty."""
    v = series.get(key, None)
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    return s if s not in ("nan", "None", "NaT") else default


def _flatten(text: str) -> str:
    """Replace all newlines (and surrounding whitespace) with a single space.

    Keeps each string value on one line so that CSV fields never contain
    embedded newlines, preventing row-alignment breaks in spreadsheet tools.
    """
    return _NL_RE.sub(" ", text).strip()


def _fmt_ctx_row(row: pd.Series) -> str:
    """Format a single corpus row as a one-line context string."""
    name  = _safe(row, "name")
    body  = _flatten(_safe(row, "body"))
    party = _safe(row, "party")
    geo   = _safe(row, "state") or _safe(row, "electorate")
    ts    = _safe(row, "time_stamp")

    if party and geo:
        meta = f"({party}, {geo})"
    elif party:
        meta = f"({party})"
    elif geo:
        meta = f"({geo})"
    else:
        meta = ""

    ts_prefix = f"[{ts}] " if ts else ""
    parts = [p for p in [ts_prefix + name, meta] if p]
    header = " ".join(parts)
    return f"{header}: {body}" if body else header


_HEADING_SKIP_RE = re.compile(
    r"took the chair|adjourned|resumed|interrupted|suspended|"
    r"the chair was taken|presiding|in the chair|"
    r"the (?:house|senate) divided|question so resolved|"
    r"question negatived|bill read|ordered that",
    re.IGNORECASE,
)

def _find_debate_heading(day_df: pd.DataFrame, pos: int) -> str:
    """
    Scan backwards from position `pos` in the sorted day DataFrame to find
    the most recent stage-direction or business-start row that represents a
    real debate topic.  Procedural opening phrases (e.g. 'took the chair',
    'adjourned', 'suspended') are skipped.
    """
    for i in range(pos - 1, -1, -1):
        name = _safe(day_df.iloc[i], "name").lower()
        if name in ("stage direction", "business start"):
            body = _safe(day_df.iloc[i], "body")
            if body and not _HEADING_SKIP_RE.search(body):
                return body
    return ""


# ── Per-corpus search ──────────────────────────────────────────────────────────

def search_corpus(
    corpus_path: Path,
    tree: dict,
    chamber: str,
    context_n: int,
    filters: dict | None = None,
) -> pd.DataFrame:
    """
    Load *corpus_path*, evaluate the boolean expression, and return an enriched
    DataFrame — one row per match — with debate heading and context columns.
    """
    print(f"  Loading {chamber} corpus … ", end="", flush=True)
    df = pd.read_parquet(corpus_path)
    df["body"] = df["body"].fillna("").astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    print(f"{len(df):,} rows loaded.")

    # Apply metadata pre-filters
    df = _apply_filters(df, filters or {})

    # Evaluate boolean expression
    terms = collect_terms(tree)
    masks = _build_masks(df, terms)
    matched = df[_eval_tree(tree, masks)].copy()
    print(f"  {chamber.title()}: {len(matched):,} match(es) for term(s): {sorted(set(terms))}")

    if matched.empty:
        return pd.DataFrame()

    # Build per-date sorted DataFrames (only dates that have matches)
    match_dates = set(matched["date"].unique())
    day_lookup: dict[str, pd.DataFrame] = {
        str(date): grp.sort_values("order").reset_index(drop=True)
        for date, grp in df[df["date"].isin(match_dates)].groupby("date")
    }

    # Enrich each matched row
    enriched: list[dict] = []
    for orig_idx, row in matched.iterrows():
        date_val  = str(row["date"])
        order_val = row.get("order")
        day_df    = day_lookup.get(date_val, pd.DataFrame())

        # Position of this row within the sorted day
        pos: int | None = None
        if not day_df.empty and order_val is not None:
            hits = (day_df["order"] == order_val).values.nonzero()[0]
            if len(hits):
                pos = int(hits[0])

        # Debate heading — scan backwards for nearest stage direction
        debate_heading = _find_debate_heading(day_df, pos) if pos is not None else ""

        # N context rows before and after
        ctx_before: list[str] = []
        ctx_after:  list[str] = []
        if pos is not None:
            for i in range(max(0, pos - context_n), pos):
                ctx_before.append(_fmt_ctx_row(day_df.iloc[i]))
            for i in range(pos + 1, min(len(day_df), pos + context_n + 1)):
                ctx_after.append(_fmt_ctx_row(day_df.iloc[i]))

        # Which terms triggered this particular row (case-sensitive)
        body_text  = str(row.get("body") or "")
        hit_terms  = sorted({t for t in set(terms) if t in body_text})

        # Unified geography column (Senate → state, House → electorate)
        geo = _safe(row, "state") or _safe(row, "electorate")

        enriched.append({
            # Identification
            "match_id":            f"{chamber}-{date_val}-{_safe(row, 'order')}",
            "chamber":             chamber,
            "date":                _safe(row, "date"),
            "time_stamp":          _safe(row, "time_stamp"),
            "page_no":             _safe(row, "page_no"),
            "speech_no":           _safe(row, "speech_no"),
            "row_order":           _safe(row, "order"),
            # Speaker metadata
            "speaker_name":        _safe(row, "name"),
            "name_id":             _safe(row, "name_id"),
            "unique_id":           _safe(row, "unique_id"),
            "party":               _safe(row, "party"),
            "partyfacts_id":       _safe(row, "partyfacts_id"),
            "state_or_electorate": geo,
            "gender":              _safe(row, "gender"),
            "in_gov":              _safe(row, "in_gov"),
            "first_speech":        _safe(row, "first_speech"),
            # Speech type flags
            "is_question":         _safe(row, "question"),
            "is_answer":           _safe(row, "answer"),
            "q_in_writing":        _safe(row, "q_in_writing"),
            "is_interjection":     _safe(row, "interject"),
            "div_flag":            _safe(row, "div_flag"),
            # Match content
            "body":                _flatten(body_text),
            "debate_heading":      _flatten(debate_heading),
            "matched_terms":       " | ".join(hit_terms),
            # Surrounding context (formatted speech turns)
            "context_before":      _CTX_SEP.join(ctx_before),
            "context_after":       _CTX_SEP.join(ctx_after),
        })

    # Tag all direct matches; initialise exchange_id to None
    for rec in enriched:
        rec["match_source"] = "search"
        rec["exchange_id"]  = None

    # ── Pull in paired Q&A rows and assign exchange_id ────────────────────────
    # (a) Matched question  → pull in the next answer row
    # (b) Matched answer    → scan back to find the question that generated it
    # exchange_id = "{chamber}-{date}-xch{question_order}" — keyed to the
    # question row so it is deterministic and human-readable.
    already_matched = {
        (r["chamber"], r["date"], r["row_order"]): r
        for r in enriched
    }

    def _make_paired_row(paired_row: pd.Series, paired_pos: int,
                         source: str, exchange_id: str) -> dict:
        ctx_before: list[str] = []
        ctx_after:  list[str] = []
        date_val = str(paired_row.get("date") or "")
        day_df   = day_lookup.get(date_val, pd.DataFrame())
        for i in range(max(0, paired_pos - context_n), paired_pos):
            ctx_before.append(_fmt_ctx_row(day_df.iloc[i]))
        for i in range(paired_pos + 1, min(len(day_df), paired_pos + context_n + 1)):
            ctx_after.append(_fmt_ctx_row(day_df.iloc[i]))
        debate_heading = _find_debate_heading(day_df, paired_pos)
        geo = _safe(paired_row, "state") or _safe(paired_row, "electorate")
        label = "(answer to question)" if source == "question_answer" else "(question for answer)"
        return {
            "match_id":            f"{chamber}-{date_val}-{_safe(paired_row, 'order')}",
            "chamber":             chamber,
            "date":                _safe(paired_row, "date"),
            "time_stamp":          _safe(paired_row, "time_stamp"),
            "page_no":             _safe(paired_row, "page_no"),
            "speech_no":           _safe(paired_row, "speech_no"),
            "row_order":           _safe(paired_row, "order"),
            "speaker_name":        _safe(paired_row, "name"),
            "name_id":             _safe(paired_row, "name_id"),
            "unique_id":           _safe(paired_row, "unique_id"),
            "party":               _safe(paired_row, "party"),
            "partyfacts_id":       _safe(paired_row, "partyfacts_id"),
            "state_or_electorate": geo,
            "gender":              _safe(paired_row, "gender"),
            "in_gov":              _safe(paired_row, "in_gov"),
            "first_speech":        _safe(paired_row, "first_speech"),
            "is_question":         _safe(paired_row, "question"),
            "is_answer":           _safe(paired_row, "answer"),
            "q_in_writing":        _safe(paired_row, "q_in_writing"),
            "is_interjection":     _safe(paired_row, "interject"),
            "div_flag":            _safe(paired_row, "div_flag"),
            "body":                _flatten(str(paired_row.get("body") or "")),
            "debate_heading":      _flatten(debate_heading),
            "matched_terms":       label,
            "context_before":      _CTX_SEP.join(ctx_before),
            "context_after":       _CTX_SEP.join(ctx_after),
            "match_source":        source,
            "exchange_id":         exchange_id,
        }

    for rec in list(enriched):  # iterate over a snapshot
        date_val  = rec["date"]
        order_val = rec.get("row_order")
        day_df    = day_lookup.get(date_val, pd.DataFrame())
        if day_df.empty or order_val is None:
            continue

        hits = (day_df["order"] == int(order_val)).values.nonzero()[0]
        if not len(hits):
            continue
        pos = int(hits[0])

        is_q = str(rec.get("is_question", "0")) == "1"
        is_a = str(rec.get("is_answer",   "0")) == "1"

        if is_q:
            # exchange_id anchored to this question row
            exchange_id = f"{chamber}-{date_val}-xch{order_val}"
            rec["exchange_id"] = exchange_id

            # Scan forward for the next answer row
            for i in range(pos + 1, len(day_df)):
                candidate = day_df.iloc[i]
                if str(candidate.get("answer", "0")) == "1":
                    key = (chamber, date_val, str(candidate.get("order")))
                    if key in already_matched:
                        # Answer already matched directly — just set its exchange_id
                        already_matched[key]["exchange_id"] = exchange_id
                    else:
                        new_row = _make_paired_row(candidate, i, "question_answer", exchange_id)
                        already_matched[key] = new_row
                        enriched.append(new_row)
                    break

        elif is_a:
            # Scan backward for the most recent question row
            for i in range(pos - 1, -1, -1):
                candidate = day_df.iloc[i]
                if str(candidate.get("question", "0")) == "1":
                    # exchange_id anchored to the question row
                    q_order = candidate.get("order")
                    exchange_id = f"{chamber}-{date_val}-xch{q_order}"
                    rec["exchange_id"] = exchange_id

                    key = (chamber, date_val, str(q_order))
                    if key in already_matched:
                        already_matched[key]["exchange_id"] = exchange_id
                    else:
                        new_row = _make_paired_row(candidate, i, "answer_question", exchange_id)
                        already_matched[key] = new_row
                        enriched.append(new_row)
                    break

    result = pd.DataFrame(enriched)
    return result


# ── Committee corpus search (no Q&A pairing; committee-specific columns) ───────

def search_corpus_committee(
    corpus_path: Path,
    tree: dict,
    context_n: int,
    filters: dict | None = None,
) -> pd.DataFrame:
    """
    Search the committee Hansard corpus.  No Q&A exchange pairing (committee
    hearings don't use question/answer flags).  Adds committee-specific
    columns: committee_name, committee_chamber, hearing_type, reference,
    portfolio, witness_flag.
    """
    print(f"  Loading committee corpus … ", end="", flush=True)
    df = pd.read_parquet(corpus_path)
    df["body"] = df["body"].fillna("").astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    print(f"{len(df):,} rows loaded.")

    # Apply metadata pre-filters
    df = _apply_filters(df, filters or {})

    terms = collect_terms(tree)
    masks = _build_masks(df, terms)
    matched = df[_eval_tree(tree, masks)].copy()
    print(f"  Committee: {len(matched):,} match(es) for term(s): {sorted(set(terms))}")

    if matched.empty:
        return pd.DataFrame()

    match_dates = set(matched["date"].unique())
    day_lookup: dict[str, pd.DataFrame] = {
        str(date): grp.sort_values("order").reset_index(drop=True)
        for date, grp in df[df["date"].isin(match_dates)].groupby("date")
    }

    enriched: list[dict] = []
    for orig_idx, row in matched.iterrows():
        date_val  = str(row["date"])
        order_val = row.get("order")
        day_df    = day_lookup.get(date_val, pd.DataFrame())

        pos: int | None = None
        if not day_df.empty and order_val is not None:
            hits = (day_df["order"] == order_val).values.nonzero()[0]
            if len(hits):
                pos = int(hits[0])

        ctx_before: list[str] = []
        ctx_after:  list[str] = []
        if pos is not None:
            for i in range(max(0, pos - context_n), pos):
                ctx_before.append(_fmt_ctx_row(day_df.iloc[i]))
            for i in range(pos + 1, min(len(day_df), pos + context_n + 1)):
                ctx_after.append(_fmt_ctx_row(day_df.iloc[i]))

        body_text = str(row.get("body") or "")
        hit_terms = sorted({t for t in set(terms) if t in body_text})

        enriched.append({
            "match_id":            f"committee-{date_val}-{_safe(row, 'order')}",
            "chamber":             "committee",
            "date":                _safe(row, "date"),
            "time_stamp":          None,
            "page_no":             _safe(row, "page_no"),
            "speech_no":           _safe(row, "speech_no"),
            "row_order":           _safe(row, "order"),
            "speaker_name":        _safe(row, "name"),
            "name_id":             _safe(row, "name_id"),
            "unique_id":           _safe(row, "unique_id"),
            "party":               _safe(row, "party"),
            "partyfacts_id":       _safe(row, "partyfacts_id"),
            "state_or_electorate": None,
            "gender":              _safe(row, "gender"),
            "in_gov":              _safe(row, "in_gov"),
            "first_speech":        None,
            "is_question":         None,
            "is_answer":           None,
            "q_in_writing":        None,
            "is_interjection":     None,
            "div_flag":            None,
            # Committee-specific
            "witness_flag":        _safe(row, "witness_flag"),
            "committee_name":      _safe(row, "committee_name"),
            "committee_chamber":   _safe(row, "committee_chamber"),
            "hearing_type":        _safe(row, "hearing_type"),
            "reference":           _safe(row, "reference"),
            "portfolio":           _safe(row, "portfolio"),
            # Content
            "body":                _flatten(body_text),
            "debate_heading":      "",
            "matched_terms":       " | ".join(hit_terms),
            "context_before":      _CTX_SEP.join(ctx_before),
            "context_after":       _CTX_SEP.join(ctx_after),
            "match_source":        "search",
            "exchange_id":         None,
        })

    return pd.DataFrame(enriched)


# ── Summary statistics ─────────────────────────────────────────────────────────

def _write_summary(
    results: pd.DataFrame,
    out_path: Path,
    expr: str,
    folder_name: str,
    filters: dict | None = None,
) -> None:
    senate    = results[results["chamber"] == "senate"]
    house     = results[results["chamber"] == "house"]
    committee = results[results["chamber"] == "committee"]

    lines = [
        f"Search summary — {folder_name}",
        f"Expression :  {expr}",
    ]
    if filters:
        for k, v in filters.items():
            lines.append(f"Filter     :  {k} = {v}")
    lines += [
        f"Generated  :  {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "─" * 64,
        f"Total matches :  {len(results):>7,}",
        f"  Senate       :  {len(senate):>7,}",
        f"  House        :  {len(house):>7,}",
        f"  Committee    :  {len(committee):>7,}",
        "",
    ]

    # Date range
    dates = pd.to_datetime(results["date"], errors="coerce").dropna()
    if not dates.empty:
        lines += [
            f"Date range    :  {dates.min().date()} → {dates.max().date()}",
            f"Unique sittings: {results['date'].nunique():>5,}",
            "",
        ]

    # Matched-term breakdown
    all_terms: dict[str, int] = {}
    for ts in results["matched_terms"].dropna():
        for t in ts.split(" | "):
            t = t.strip()
            if t:
                all_terms[t] = all_terms.get(t, 0) + 1
    if all_terms:
        lines += ["Matched terms", "─" * 44, ""]
        for t, c in sorted(all_terms.items(), key=lambda x: -x[1]):
            lines.append(f"  {t!r:<40}  {c:>6,}")
        lines.append("")

    # Top speakers
    top_speakers = (
        results.groupby("speaker_name").size()
        .sort_values(ascending=False)
        .head(15)
    )
    lines += ["Top 15 speakers", "─" * 54, ""]
    for name, count in top_speakers.items():
        lines.append(f"  {str(name):<48}  {count:>5,}")
    lines.append("")

    # Top parties
    top_parties = (
        results[results["party"].notna() & (results["party"] != "")]
        .groupby("party").size()
        .sort_values(ascending=False)
        .head(10)
    )
    lines += ["Top 10 parties", "─" * 34, ""]
    for party, count in top_parties.items():
        lines.append(f"  {str(party):<24}  {count:>5,}")
    lines.append("")

    # Mentions by year (ASCII bar chart)
    results2 = results.copy()
    results2["year"] = pd.to_datetime(results2["date"], errors="coerce").dt.year
    by_year = results2.groupby("year").size().sort_index()
    max_count = int(by_year.max()) if not by_year.empty else 1
    bar_scale = min(1.0, 40 / max_count)
    lines += ["Mentions by year", "─" * 54, ""]
    for year, count in by_year.items():
        bar = "█" * max(1, int(count * bar_scale))
        lines.append(f"  {int(year)}  {count:>5,}  {bar}")
    lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Summary → {out_path}")


# ── logic.md ───────────────────────────────────────────────────────────────────

def _update_logic_md(folder_name: str, expr: str, match_count: int, filters: dict | None = None) -> None:
    """Append or create the case_studies/logic.md entry for this search."""
    LOGIC_FILE.parent.mkdir(parents=True, exist_ok=True)
    ts    = datetime.now().strftime("%Y-%m-%d")
    filter_str = f" | filters: {filters}" if filters else ""
    entry = f"| `{folder_name}` | `{expr}`{filter_str} | {match_count:,} | {ts} |\n"

    if not LOGIC_FILE.exists():
        LOGIC_FILE.write_text(
            "# Case Studies — Search Logic\n\n"
            "Each row maps an output folder to the boolean expression used to generate it.\n\n"
            "| Folder | Expression | Matches | Date |\n"
            "|--------|-----------|---------|------|\n"
            + entry,
            encoding="utf-8",
        )
    else:
        content = LOGIC_FILE.read_text(encoding="utf-8")
        LOGIC_FILE.write_text(content + entry, encoding="utf-8")

    print(f"  logic.md → {LOGIC_FILE}")


# ── Folder naming ──────────────────────────────────────────────────────────────

def _make_output_dir(preferred: str) -> tuple[str, Path]:
    """
    Create a unique sub-directory under CASE_STUDIES using *preferred* as the
    base name.  Sanitise the name and append _2, _3, … if it already exists.
    """
    CASE_STUDIES.mkdir(parents=True, exist_ok=True)
    name = re.sub(r"[^\w\-]", "_", preferred).strip("_") or "search"
    candidate = CASE_STUDIES / name
    if not candidate.exists():
        candidate.mkdir(parents=True)
        return name, candidate
    i = 2
    while True:
        indexed_name = f"{name}_{i}"
        candidate = CASE_STUDIES / indexed_name
        if not candidate.exists():
            candidate.mkdir(parents=True)
            return indexed_name, candidate
        i += 1


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        prog="search_corpus.py",
        description="Boolean string search across Hansard corpora.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python search_corpus.py "'WWF'"
              python search_corpus.py "'WWF' | 'World Wildlife Fund'"
              python search_corpus.py --name WWF \\
                  "'WWF' | 'World Wildlife Fund' | 'World Wide Fund for Nature'"
              python search_corpus.py --context 3 --chamber senate "'climate change'"
        """),
    )
    ap.add_argument(
        "expression",
        help="Boolean search expression over quoted string literals.",
    )
    ap.add_argument(
        "--name", default=None, metavar="NAME",
        help="Output folder name (default: first term in expression).",
    )
    ap.add_argument(
        "--chamber", choices=["senate", "house", "both", "committee"], default="both",
        help="Which corpus to search: senate, house, both, or committee (default: both).",
    )
    ap.add_argument(
        "--committee", action="store_true",
        help="Also search the committee Hansard corpus (use --chamber committee to search only committee).",
    )
    ap.add_argument(
        "--context", type=int, default=5, metavar="N",
        help="Speech turns to capture before/after each match (default: 5).",
    )
    ap.add_argument(
        "--senate-corpus", type=Path, default=SENATE_CORPUS, metavar="PATH",
        help=f"Senate corpus parquet path (default: auto).",
    )
    ap.add_argument(
        "--house-corpus", type=Path, default=HOUSE_CORPUS, metavar="PATH",
        help=f"House corpus parquet path (default: auto).",
    )
    ap.add_argument(
        "--committee-corpus", type=Path, default=COMMITTEE_CORPUS, metavar="PATH",
        help=f"Committee corpus parquet path (default: auto).",
    )

    fg = ap.add_argument_group("Metadata filters")
    fg.add_argument("--party", nargs="+", metavar="ABBREV",
        help="Keep rows where party matches one of these abbreviations (e.g. ALP GRN LP).")
    fg.add_argument("--gender", choices=["male", "female"], metavar="GENDER",
        help="Keep rows where gender matches (male or female).")
    fg.add_argument("--speaker", metavar="SUBSTRING",
        help="Keep rows where speaker name contains this substring (case-insensitive).")
    fg.add_argument("--electorate", metavar="SUBSTRING",
        help="Keep rows where electorate contains this substring (House only).")
    fg.add_argument("--state", metavar="ABBREV",
        help="Keep rows where state contains this substring (Senate only).")
    fg.add_argument("--name-id", metavar="PHID",
        help="Keep rows with this exact name_id (ausPH PHID).")
    fg.add_argument("--unique-id", metavar="ID",
        help="Keep rows with this exact unique_id (AusPol uniqueID).")
    fg.add_argument("--date-from", metavar="YYYY-MM-DD",
        help="Earliest sitting date to include (inclusive).")
    fg.add_argument("--date-to", metavar="YYYY-MM-DD",
        help="Latest sitting date to include (inclusive).")
    fg.add_argument("--in-gov", action="store_true", default=None,
        help="Keep only rows where speaker is from the governing party.")
    fg.add_argument("--not-in-gov", action="store_true", default=None,
        help="Keep only rows where speaker is from opposition.")
    fg.add_argument("--questions-only", action="store_true",
        help="Keep only question rows (question=1).")
    fg.add_argument("--answers-only", action="store_true",
        help="Keep only answer rows (answer=1).")
    fg.add_argument("--interjections-only", action="store_true",
        help="Keep only interjection rows (interject=1).")
    fg.add_argument("--no-interjections", action="store_true",
        help="Exclude interjection rows (interject=0).")
    fg.add_argument("--first-speech", action="store_true",
        help="Keep only first-speech rows.")
    fg.add_argument("--fedchamb-only", action="store_true",
        help="Keep only Federation Chamber rows (House only).")
    fg.add_argument("--main-chamber-only", action="store_true",
        help="Keep only main chamber rows (fedchamb_flag=0, House only).")
    fg.add_argument("--written-only", action="store_true",
        help="Keep only written questions/answers (q_in_writing=1).")
    fg.add_argument("--exclude-written", action="store_true",
        help="Exclude written questions/answers (q_in_writing=0).")
    fg.add_argument("--has-embedded-interject", action="store_true",
        help="Keep only rows that contain inline interjection markers in body.")
    fg.add_argument("--row-type", choices=["speech", "exchange", "both"], default=None,
        help="speech = not a question/answer; exchange = question or answer; both = no filter (default).")

    args = ap.parse_args()

    # Build metadata filters dict
    filters: dict = {}
    if args.party:
        filters["party"] = args.party
    if args.gender:
        filters["gender"] = args.gender
    if args.speaker:
        filters["speaker"] = args.speaker
    if args.electorate:
        filters["electorate"] = args.electorate
    if args.state:
        filters["state"] = args.state
    if args.name_id:
        filters["name_id"] = args.name_id
    if args.unique_id:
        filters["unique_id"] = args.unique_id
    if args.date_from:
        filters["date_from"] = args.date_from
    if args.date_to:
        filters["date_to"] = args.date_to
    if args.in_gov:
        filters["in_gov"] = 1
    if args.not_in_gov:
        filters["in_gov"] = 0
    if args.questions_only:
        filters["question"] = 1
    if args.answers_only:
        filters["answer"] = 1
    if args.interjections_only:
        filters["interject"] = 1
    if args.no_interjections:
        filters["interject"] = 0
    if args.first_speech:
        filters["first_speech"] = 1
    if args.fedchamb_only:
        filters["fedchamb_flag"] = 1
    if args.main_chamber_only:
        filters["fedchamb_flag"] = 0
    if args.written_only:
        filters["q_in_writing"] = 1
    if args.exclude_written:
        filters["q_in_writing"] = 0
    if args.has_embedded_interject:
        filters["has_embedded_interject"] = True
    if args.row_type and args.row_type != "both":
        filters["row_type"] = args.row_type

    # Parse and display the expression
    try:
        tree = parse_expression(args.expression)
    except SyntaxError as e:
        print(f"Expression syntax error: {e}", file=sys.stderr)
        sys.exit(1)

    terms = collect_terms(tree)
    print(f"\nExpression : {args.expression}")
    print(f"Terms      : {sorted(set(terms))}")
    print(f"Context    : ±{args.context} rows")
    print(f"Chamber(s) : {args.chamber}" + (" + committee" if args.committee else "") + "\n")
    if filters:
        filter_desc = ", ".join(
            f"{k}={v}" for k, v in filters.items()
        )
        print(f"Filters    : {filter_desc}")

    # Resolve output folder
    name_pref    = args.name or first_term(tree)
    folder_name, out_dir = _make_output_dir(name_pref)
    print(f"Output dir : {out_dir}\n")

    # Run searches
    all_results: list[pd.DataFrame] = []

    if args.chamber in ("senate", "both"):
        if not args.senate_corpus.exists():
            print(f"WARNING: Senate corpus not found: {args.senate_corpus}", file=sys.stderr)
        else:
            r = search_corpus(args.senate_corpus, tree, "senate", args.context, filters=filters)
            if not r.empty:
                all_results.append(r)

    if args.chamber in ("house", "both"):
        if not args.house_corpus.exists():
            print(f"WARNING: House corpus not found: {args.house_corpus}", file=sys.stderr)
        else:
            r = search_corpus(args.house_corpus, tree, "house", args.context, filters=filters)
            if not r.empty:
                all_results.append(r)

    if args.committee or args.chamber == "committee":
        if not args.committee_corpus.exists():
            print(f"WARNING: Committee corpus not found: {args.committee_corpus}", file=sys.stderr)
        else:
            r = search_corpus_committee(args.committee_corpus, tree, args.context, filters=filters)
            if not r.empty:
                all_results.append(r)

    if not all_results:
        print("\nNo matches found.")
        # Still update logic.md so the search is recorded
        _update_logic_md(folder_name, args.expression, 0, filters=filters or None)
        return

    combined = (
        pd.concat(all_results, ignore_index=True)
        .sort_values(["date", "chamber", "row_order"])
        .reset_index(drop=True)
    )

    # Split: non-exchange rows → matches.csv; exchange rows → exchanges.csv
    has_exchange = combined["exchange_id"].notna()
    matches_df   = combined[~has_exchange].copy()
    exchanges_df = combined[has_exchange].copy()

    _EXCEL_CELL_LIMIT = 32_000

    def _excel_cap(v: object) -> object:
        return v[:_EXCEL_CELL_LIMIT] + "…" if isinstance(v, str) and len(v) > _EXCEL_CELL_LIMIT else v

    # Write matches.csv (for enrich_case_study.py)
    matches_path = out_dir / "matches.csv"
    matches_df.to_csv(matches_path, index=False, encoding="utf-8")
    print(f"\n  matches.csv  → {matches_path}  ({len(matches_df):,} rows — non-exchange)")

    # Write exchanges.csv (for enrich_exchanges.py)
    if not exchanges_df.empty:
        exchanges_path = out_dir / "exchanges.csv"
        exchanges_df.to_csv(exchanges_path, index=False, encoding="utf-8")
        n_xch = exchanges_df["exchange_id"].nunique()
        print(f"  exchanges.csv → {exchanges_path}  ({len(exchanges_df):,} rows — {n_xch} exchanges)")

    # Write Excel of all combined rows (for human inspection)
    xlsx_path = out_dir / "matches.xlsx"
    xlsx_df = combined.copy()
    for col in ("body", "context_before", "context_after", "debate_heading"):
        xlsx_df[col] = xlsx_df[col].apply(_excel_cap)
    xlsx_df.to_excel(xlsx_path, index=False, engine="openpyxl")
    print(f"  matches.xlsx → {xlsx_path}  (all {len(combined):,} rows)")

    # Write summary
    _write_summary(combined, out_dir / "summary.txt", args.expression, folder_name, filters=filters or None)

    # Update logic.md
    _update_logic_md(folder_name, args.expression, len(combined), filters=filters or None)

    n_xch_total = exchanges_df["exchange_id"].nunique() if not exchanges_df.empty else 0
    print(f"\nDone. {len(combined):,} total rows "
          f"({len(matches_df):,} matches + {len(exchanges_df):,} exchange rows "
          f"/ {n_xch_total} exchanges) → {out_dir}\n")


if __name__ == "__main__":
    main()
