#!/usr/bin/env python3
"""
generate_cited_report.py — Produce a fully cited HTML analytical report
from a matches_enriched.csv using the Claude API.

Outputs (to case_studies/<NAME>/):
  reference_lookup.csv        — every source record + Hansard URL + ref_id
  report_with_ref_ids.html    — draft report with [ref_id] square-bracket citations
  report_final_cited.html     — final report with superscript citations + reference list

Subject name is inferred from the input path (parent directory) unless
overridden with --subject / --subject-full-name.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, date
from pathlib import Path

import anthropic
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
HERE  = Path(__file__).resolve().parent    # pipeline/
ROOT  = HERE.parent                        # project root

# ── ParlInfo URL templates ─────────────────────────────────────────────────────
#   Matches the pattern used in 00_download.py.
#   Lands on the ParlInfo page for that sitting day — the closest available
#   deep link without reconstructing session-level UUID/integer identifiers.
_PARLINFO_SEARCH = (
    "https://parlinfo.aph.gov.au/parlInfo/search/display/display.w3p"
    ";adv=yes;orderBy=_fragment_number,doc_date-rev;page=0"
    ";query=Dataset%3A{datasets}%20Date%3A{day}%2F{month}%2F{year}"
    ";rec=0;resCount=Default"
)
_DATASETS = {
    "senate": "hansardS%2ChansardS80",
    "house":  "hansardr%2Chansardr80",
}


def _hansard_url(chamber: str, date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return _PARLINFO_SEARCH.format(
        datasets=_DATASETS.get(chamber, "hansardS%2ChansardS80"),
        day=d.day, month=d.month, year=d.year,
    )


# ── Reference short label ──────────────────────────────────────────────────────
def _short_label(row: pd.Series) -> str:
    ch      = "Senate" if row["chamber"] == "senate" else "House"
    speaker = str(row.get("speaker_name", "") or "Unknown Speaker").strip()
    heading = str(row.get("debate_heading", "") or "").strip()
    if not heading or heading == "nan":
        # Fall back to what_happened summary
        heading = str(row.get("what_happened", "") or "").strip()
    # Cap at 80 chars regardless of source to keep reference list readable
    if len(heading) > 80:
        heading = heading[:80].rstrip() + "…"
    return f"{ch} Hansard, {row['date']}, {speaker}, {heading}"


# ── STEP 1 — Build reference lookup ───────────────────────────────────────────
def build_lookup(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Assign stable ref_ids (1-based) and build the lookup table.
    Returns (lookup_df, record_id → ref_id mapping).
    """
    rows, id_map = [], {}
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        rid = r["match_id"]
        id_map[rid] = i
        rows.append({
            "ref_id":        i,
            "record_id":     rid,
            "date":          r["date"],
            "chamber":       r["chamber"],
            "speaker":       str(r.get("speaker_name", "") or "").strip(),
            "party":         str(r.get("party", "") or "").strip(),
            "in_gov":        int(r.get("in_gov", 0)),
            "debate_heading": str(r.get("debate_heading", "") or "").strip(),
            "url":           _hansard_url(r["chamber"], r["date"]),
            "short_label":   _short_label(r),
        })
    return pd.DataFrame(rows), id_map


# ── Field auto-detection ──────────────────────────────────────────────────────

def detect_fields(df: pd.DataFrame) -> dict[str, str]:
    """
    Auto-detect the subject-specific column names from the enriched CSV.

    Framing column  — any column ending in '_framing'
                      (e.g. wwf_framing, arena_framing, grdc_framing)
    Valence column  — first match from:
                      1. column starting with 'speaker_valence_toward_'
                         (e.g. speaker_valence_toward_wwf)
                      2. 'mention_valence'  (used by the ARENA taxonomy)

    Falls back to None for either if not found; callers must handle None gracefully.
    """
    cols = list(df.columns)
    framing = next((c for c in cols if c.endswith("_framing")), None)
    valence = (
        next((c for c in cols if c.startswith("speaker_valence_toward_")), None)
        or ("mention_valence" if "mention_valence" in cols else None)
    )
    return {"framing": framing, "valence": valence}


# ── STEP 2a — Pre-compute statistics ──────────────────────────────────────────
def _party_group(p) -> str:
    if pd.isna(p):
        return "Other"
    p = str(p).strip()
    if p in ("LP", "LNP", "CLP"): return "Coalition"
    if p == "ALP":                 return "Labor"
    if p == "AG":                  return "Greens"
    if p in ("NP", "NATS"):        return "Nationals"
    if p == "AD":                  return "Democrats"
    return "Other"


def compute_stats(df: pd.DataFrame, subject: str, fields: dict[str, str | None]) -> str:
    d = df.copy()
    d["party_group"] = d["party"].apply(_party_group)
    d["year"] = pd.to_datetime(d["date"], errors="coerce").dt.year

    total  = len(d)
    senate = (d["chamber"] == "senate").sum()
    house  = (d["chamber"] == "house").sum()
    yr_min = int(d["year"].min())
    yr_max = int(d["year"].max())
    top_yr     = int(d["year"].value_counts().idxmax())
    top_yr_cnt = int(d["year"].value_counts().max())
    gov_n  = int((d["in_gov"] == 1).sum())
    opp_n  = int((d["in_gov"] == 0).sum())

    framing_col = fields["framing"]
    valence_col = fields["valence"]

    framing  = d[framing_col].value_counts().to_dict() if framing_col else {}
    valence  = d[valence_col].value_counts().to_dict() if valence_col else {}
    rhet     = d["rhetorical_function"].value_counts().to_dict() if "rhetorical_function" in d else {}
    domains  = d["policy_domain"].value_counts().to_dict() if "policy_domain" in d else {}
    central  = d["mention_centrality"].value_counts().to_dict() if "mention_centrality" in d else {}
    parties  = d["party_group"].value_counts().to_dict()

    # Framing by party — most useful crosstab
    framing_x_party = (
        pd.crosstab(d[framing_col], d["party_group"]).to_string()
        if framing_col and d[framing_col].notna().any() else "(no framing column)"
    )

    # Valence by party
    valence_x_party = (
        pd.crosstab(d[valence_col], d["party_group"]).to_string()
        if valence_col and d[valence_col].notna().any() else "(no valence column)"
    )

    # Top speakers
    top_sp = (
        d[~d["speaker_name"].str.lower().str.startswith("stage", na=True)]
        .groupby("speaker_name").size()
        .sort_values(ascending=False).head(15).to_string()
    )

    # Year series
    year_ser = d.groupby("year").size().sort_index().to_string()

    # Framing by in-gov
    framing_x_gov = (
        pd.crosstab(d[framing_col], d["in_gov"].map({1: "in_gov", 0: "opposition"})).to_string()
        if framing_col and d[framing_col].notna().any() else "(no framing column)"
    )

    framing_label = framing_col or "framing"
    valence_label = valence_col or "valence"

    return f"""=== DATASET STATISTICS (pre-computed from {total} records) ===

OVERVIEW:
  Total mentions: {total}
  Senate: {senate} | House: {house}
  Date range: {yr_min}–{yr_max}   |   Unique sittings: {d['date'].nunique()}
  Peak year: {top_yr} ({top_yr_cnt} mentions)
  Government speakers: {gov_n} | Opposition/crossbench: {opp_n}

{subject.upper()} FRAMING [{framing_label}] (how speaker characterises {subject}):
  {framing}

SPEAKER VALENCE [{valence_label}] (attitude toward {subject}):
  {valence}

RHETORICAL FUNCTION (argumentative work of the {subject} mention):
  {rhet}

POLICY DOMAIN:
  {domains}

MENTION CENTRALITY:
  {central}

PARTY GROUP BREAKDOWN (total mentions):
  {parties}

FRAMING BY PARTY GROUP (rows=framing, cols=party):
{framing_x_party}

VALENCE BY PARTY GROUP (rows=valence, cols=party):
{valence_x_party}

FRAMING BY GOVERNMENT STATUS (rows=framing, cols=in_gov/opposition):
{framing_x_gov}

TOP 15 SPEAKERS (by mention count):
{top_sp}

MENTIONS BY YEAR:
{year_ser}"""


# ── STEP 2b — Build compact record table for Claude ────────────────────────────
def build_compact_records(
    df: pd.DataFrame,
    id_map: dict[str, int],
    fields: dict[str, str | None],
) -> str:
    """
    One entry per record. Focal records get a truncated body excerpt
    (for quotable language); others get only the what_happened summary.
    """
    framing_col = fields["framing"]
    valence_col = fields["valence"]

    lines: list[str] = []
    for _, r in df.iterrows():
        rid     = id_map[r["match_id"]]
        ch      = r["chamber"]
        sp      = str(r.get("speaker_name", "") or "?")
        party   = str(r.get("party", "") or "?")
        gov_tag = "gov" if r.get("in_gov", 0) == 1 else "opp"
        frm     = str(r.get(framing_col, "") or "") if framing_col else ""
        val     = str(r.get(valence_col, "") or "") if valence_col else ""
        rfn     = str(r.get("rhetorical_function", "") or "")
        dom     = str(r.get("policy_domain", "") or "")
        cen     = str(r.get("mention_centrality", "") or "")
        wh      = str(r.get("what_happened", "") or "").strip()

        header = (
            f"[{rid}] {r['date']} | {ch} | {sp} ({party},{gov_tag}) | "
            f"{frm} | {val} | {rfn} | {dom} | {cen}"
        )
        lines.append(header)
        lines.append(f"  {wh}")

        # Focal records: include a body excerpt so Claude can quote specific language
        if cen == "focal":
            body = str(r.get("body", "") or "").strip()
            if body:
                excerpt = body[:600].replace("\n", " ") + ("…" if len(body) > 600 else "")
                lines.append(f"  BODY_EXCERPT: {excerpt}")
        lines.append("")

    return "\n".join(lines)


# ── STEP 2c — Claude API call ──────────────────────────────────────────────────

def build_system_prompt(
    subject: str,
    subject_full: str,
    fields: dict[str, str | None],
    df: pd.DataFrame,
) -> str:
    """Build a subject-specific system prompt for the report generation call."""
    framing_col = fields["framing"] or "framing"
    valence_col = fields["valence"] or "valence"

    # Derive the top framing and valence values from the data for the prompt
    framing_vals = (
        list(df[framing_col].dropna().value_counts().head(6).index)
        if framing_col in df.columns else []
    )
    valence_vals = (
        list(df[valence_col].dropna().value_counts().index)
        if valence_col in df.columns else ["positive", "neutral", "negative", "instrumental"]
    )
    framing_list = " · ".join(framing_vals) if framing_vals else "(see data)"
    valence_list = " · ".join(valence_vals)

    return f"""\
You are an expert parliamentary analyst specialising in Australian parliamentary politics
and organisational government relations. Write for a senior government relations analyst
at {subject_full}.

You will produce a structured analytical HTML report from the supplied parliamentary records.

CRITICAL CITATION RULES:
- When making a factual or interpretive claim grounded in specific records, cite using
  square-bracket ref IDs from the supplied list: e.g. [12] or [15, 38] or [7, 12, 44]
- Use AT MOST 3 citation IDs per claim
- Only cite records you have actually read in the supplied data
- Do NOT cite a record that only weakly supports the claim
- Reserve citations for load-bearing factual/interpretive claims — not every sentence
- If you cannot find strong record support for a claim, soften or remove the claim
- Prefer diverse citations (different speakers, chambers) over repeating one speaker
- When citing chronological claims, prefer to cite records from the relevant year(s)

OUTPUT FORMAT:
- Return clean HTML only — no markdown, no code fences, no backticks
- Do NOT include <html>, <head>, <body>, or <style> tags
- Start with the first <h2> heading
- Citation format: [ref_id] or [ref_id1, ref_id2, ref_id3] — integers in square brackets

REPORT SECTIONS (produce all of these):

<h2>At a Glance</h2>
Key statistical summary using a <table> with 2 columns (metric | value).
Include: total mentions, Senate vs House split, date range, peak year,
dominant framing, dominant valence, most common rhetorical function,
top policy domain, government vs opposition speaker split.

<h2>Volume and Trend, 1998–2025</h2>
How mention frequency evolved. Identify distinct eras or peaks.
Explain what drove changes. Cite specific records as evidence of key episodes.
Approximately 200 words. Cite 4–6 records across the period.

<h2>Framing Analysis</h2>
How {subject} is characterised — dominant framings, what drives variation.
The framing values present in the data are: {framing_list}.
Note which framings dominate and which are marginal.
Cite specific records illustrating different framings.
Approximately 250 words. Cite 6–8 records.

<h2>Rhetorical Patterns</h2>
What argumentative work {subject} citations perform. Analyse the rhetorical_function
values present in the data (e.g. evidence_citation, authority_endorsement,
discredit_source, deflection_rebuttal, attack_opponent, coalition_building).
What do these patterns reveal about how {subject} is invoked or contested?
Approximately 200 words. Cite 4–6 records.

<h2>Policy Domain Breakdown</h2>
Which policy areas generate {subject} mentions and what that reveals about
{subject}'s parliamentary footprint. Cover at least the top 3 domains with examples.
Approximately 150 words. Cite 3–5 records.

<h2>Party Dynamics</h2>
How Coalition, Labor, Greens, Nationals and minor parties differ in their
framing and valence toward {subject}. The valence values are: {valence_list}.
Use specific examples from the data.
Where do government and opposition framings diverge most sharply?
Approximately 300 words. Cite 6–8 records.

<h2>Critical and Negative Framing Patterns</h2>
Detailed analysis: who uses the most critical or dismissive framings of {subject};
when; in what contexts; what it reveals about {subject}'s political vulnerabilities.
Include specific quoted examples from focal/body records.
Approximately 250 words. Cite 5–7 records.

<h2>Strategic Implications for {subject}</h2>
Four to five concrete, evidence-based implications for {subject}'s government relations
and communications strategy, drawn from the parliamentary record.
Use a numbered <ol>. Cite 1–2 records per implication as evidence.
Approximately 200 words.

STYLE:
- Professional, analytical, specific — name speakers, dates, contexts
- Do not say "the AI found" or use hollow phrases like "the data clearly shows"
- Avoid generic conclusions not grounded in the supplied records
- Keep prose readable — do not reduce the report to a citation list
- Total prose length: approximately 1,600–2,200 words"""


def _user_tmpl(framing_col: str, valence_col: str) -> str:
    return f"""\
{{stats}}

=== ALL RECORDS ===
Format: [ref_id] date | chamber | speaker (party,gov/opp) | {framing_col} | {valence_col} | rhetorical_function | policy_domain | mention_centrality
  what_happened summary
  BODY_EXCERPT (focal records only)

{{records}}

Now produce the analytical HTML report following the section structure specified.
Use [ref_id] inline citations throughout.
"""


def call_claude(
    stats: str,
    compact_records: str,
    model: str,
    system: str,
    framing_col: str,
    valence_col: str,
) -> tuple[str, int, int, float]:
    client = anthropic.Anthropic()
    user_msg = _user_tmpl(framing_col, valence_col).format(stats=stats, records=compact_records)
    print(f"  Prompt ~{len(user_msg.split()):,} words → Claude ({model})…", flush=True)

    resp = client.messages.create(
        model=model,
        max_tokens=8192,
        system=system,
        messages=[{"role": "user", "content": user_msg}],
    )

    html = resp.content[0].text
    in_tok  = resp.usage.input_tokens
    out_tok = resp.usage.output_tokens

    _PRICE = {
        "claude-opus-4-6":   {"in": 15.0, "out": 75.0},
        "claude-sonnet-4-6": {"in":  3.0, "out": 15.0},
    }
    p = _PRICE.get(model, _PRICE["claude-sonnet-4-6"])
    cost = in_tok / 1e6 * p["in"] + out_tok / 1e6 * p["out"]
    return html, in_tok, out_tok, cost


# ── HTML page wrapper ──────────────────────────────────────────────────────────
_HTML_WRAPPER = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{subject} in Australian Parliament — Hansard Analysis 1998–2025</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    background: #fff;
    color: #1a1a1a;
    line-height: 1.7;
    font-size: 15px;
  }}
  header {{
    background: #1a1a1a;
    color: #fff;
    padding: 2.5rem 3rem;
    border-bottom: 4px solid #FF6200;
  }}
  header h1 {{ font-size: 1.75rem; font-weight: 700; letter-spacing: -.02em; }}
  header p  {{ margin-top: .5rem; color: #bbb; font-size: .88rem; }}

  .container {{ max-width: 960px; margin: 0 auto; padding: 2.5rem 2rem 5rem; }}

  h2 {{
    font-size: 1.25rem; font-weight: 700; color: #1a1a1a;
    border-left: 4px solid #FF6200; padding-left: .75rem;
    margin: 2.75rem 0 1rem;
  }}
  h3 {{ font-size: 1.0rem; font-weight: 600; margin: 1.5rem 0 .5rem; color: #333; }}
  p  {{ margin-bottom: .9rem; color: #222; }}
  ol, ul {{ margin: .5rem 0 .9rem 1.5rem; color: #222; }}
  li {{ margin-bottom: .4rem; }}

  /* Summary table */
  table {{
    width: 100%; border-collapse: collapse;
    font-size: .85rem; margin: 1rem 0 1.75rem;
  }}
  th {{
    background: #1a1a1a; color: #fff;
    padding: .45rem .9rem; text-align: left;
  }}
  td {{ padding: .4rem .9rem; border-bottom: 1px solid #e0e0e0; }}
  tr:nth-child(even) {{ background: #f9f9f9; }}
  td:last-child {{ text-align: right; font-weight: 500; }}

  /* Superscript citations */
  sup.cite {{ font-size: .68em; font-weight: 600; vertical-align: super; line-height: 0; }}
  sup.cite a {{ color: #FF6200; text-decoration: none; }}
  sup.cite a:hover {{ text-decoration: underline; }}

  /* References section */
  .references {{
    margin-top: 3rem; padding-top: 1.75rem;
    border-top: 2px solid #e0e0e0;
  }}
  .references h2 {{
    border-left: none; padding-left: 0;
    font-size: 1.15rem; margin-top: 0;
  }}
  .ref-list {{ list-style: none; padding: 0; margin-top: .75rem; }}
  .ref-list li {{
    padding: .4rem 0;
    font-size: .82rem; color: #444;
    border-bottom: 1px solid #f0f0f0;
    display: flex; gap: .6rem;
  }}
  .ref-num {{ font-weight: 700; color: #1a1a1a; min-width: 2.2rem; flex-shrink: 0; }}
  .ref-list a {{ color: #FF6200; text-decoration: none; }}
  .ref-list a:hover {{ text-decoration: underline; }}

  footer {{
    background: #f5f5f5; border-top: 1px solid #ddd;
    padding: 1.2rem 3rem; font-size: .79rem; color: #666;
  }}
</style>
</head>
<body>
<header>
  <h1>{subject} in Australian Parliament</h1>
  <p>Hansard Analysis 1998–2025 &nbsp;·&nbsp;
     Senate &amp; House of Representatives &nbsp;·&nbsp;
     N={total} matched speech turns</p>
</header>
<div class="container">
{body}
</div>
<footer>
  Analysis covers Australian Senate and House of Representatives Hansard, 1998–2025.
  Source: {source}. Generated {generated}.
</footer>
</body>
</html>"""


def wrap_html(body: str, total: int, subject: str, source: str) -> str:
    return _HTML_WRAPPER.format(
        body=body,
        total=total,
        subject=subject,
        source=source,
        generated=date.today().strftime("%d %B %Y"),
    )


# ── Taxonomy appendix ─────────────────────────────────────────────────────────

_VALUE_COLOURS: dict[str, str] = {
    "scientific_authority":   "#2196F3",
    "policy_ally":            "#4CAF50",
    "neutral_reference":      "#9E9E9E",
    "lobby_group":            "#FF9800",
    "activist_adversary":     "#F44336",
    "hypocrite_self_serving": "#B71C1C",
    "authority_endorsement":  "#03A9F4",
    "positive":               "#4CAF50",
    "neutral":                "#9E9E9E",
    "negative":               "#F44336",
    "instrumental":           "#FF9800",
    "focal":                  "#2196F3",
    "supporting":             "#FF9800",
    "incidental":             "#9E9E9E",
    "evidence_citation":      "#2196F3",
    "coalition_building":     "#4CAF50",
    "deflection_rebuttal":    "#00BCD4",
    "attack_opponent":        "#F44336",
    "discredit_source":       "#B71C1C",
    "narrative_colour":       "#9E9E9E",
    "strengthen_regulation":  "#4CAF50",
    "defend_status_quo":      "#9E9E9E",
    "weaken_regulation":      "#F44336",
    "support_legislation":    "#2196F3",
    "oppose_legislation":     "#FF9800",
    "no_clear_action":        "#bdbdbd",
}
_DEFAULT_COLOUR = "#607D8B"

_APPENDIX_CSS = """
  /* ── Taxonomy appendix ──────────────────────────────────────────────── */
  .appendix {
    margin-top: 3rem; padding-top: 1.75rem;
    border-top: 2px solid #e0e0e0;
  }
  .appendix h2 { border-left: none; padding-left: 0; font-size: 1.15rem; margin-top: 0; }
  .appendix h3 {
    font-size: 1rem; font-weight: 700; color: #1a1a1a;
    margin: 1.75rem 0 .75rem;
    border-bottom: 1px solid #e0e0e0; padding-bottom: .3rem;
  }
  .appendix-intro { font-size: .88rem; color: #444; margin-bottom: 1.25rem; }
  .tax-summary-table td:last-child { text-align: center; font-weight: 400; }
  .tax-type-badge {
    display: inline-block; padding: .1rem .45rem; border-radius: 3px;
    font-size: .72rem; font-weight: 600; letter-spacing: .03em;
    color: #fff; vertical-align: middle;
  }
  .tax-type-extractable { background: #1976D2; }
  .tax-type-inferred    { background: #7B1FA2; }
  .tax-field-card {
    border: 1px solid #e8e8e8; border-radius: 6px;
    padding: 1rem 1.25rem; margin-bottom: 1rem; background: #fafafa;
  }
  .tax-field-header {
    display: flex; align-items: center; gap: .6rem;
    margin-bottom: .6rem; flex-wrap: wrap;
  }
  .tax-field-name {
    font-size: .95rem; font-weight: 700; color: #1a1a1a;
    background: #f0f0f0; padding: .15rem .45rem; border-radius: 3px;
  }
  .tax-fillrate { font-size: .75rem; color: #888; margin-left: auto; }
  .tax-purpose  { font-size: .86rem; color: #333; margin-bottom: .65rem; }
  .tax-free-text { font-size: .83rem; color: #777; font-style: italic; }
  .tax-val-list {
    list-style: none; padding: 0; margin: .5rem 0 .75rem;
    display: flex; flex-direction: column; gap: .35rem;
  }
  .tax-val-item  { display: flex; align-items: baseline; gap: .55rem; }
  .tax-badge {
    display: inline-block; padding: .12rem .5rem; border-radius: 3px;
    font-size: .73rem; font-weight: 600; color: #fff;
    white-space: nowrap; flex-shrink: 0;
  }
  .tax-val-desc { font-size: .82rem; color: #555; }
  .tax-rule {
    font-size: .81rem; color: #555; border-left: 3px solid #e0e0e0;
    padding-left: .75rem; margin-top: .6rem; font-style: italic;
  }
  .tax-rule strong { color: #333; font-style: normal; }
"""


def _parse_taxonomy(taxonomy_path: Path) -> list[dict]:
    """Parse a taxonomy_proposal.md into a list of field dicts."""
    text = taxonomy_path.read_text(encoding="utf-8")

    field_pat = re.compile(
        r"### \d+\. `([^`]+)`\n(.*?)(?=\n### |\n## |\Z)", re.DOTALL
    )

    def _get(block: str, label: str) -> str:
        m = re.search(
            r"\*\*" + re.escape(label) + r":\*\*\s*(.*?)(?=\n\*\*|\n---|\Z)",
            block, re.DOTALL,
        )
        return re.sub(r"\s+", " ", m.group(1)).strip() if m else ""

    def _get_values(block: str) -> list[tuple[str, str]]:
        m = re.search(
            r"\*\*allowed values:\*\*\s*\n((?:- .*\n?)+)", block, re.IGNORECASE
        )
        if not m:
            return []
        items = []
        for line in m.group(1).strip().split("\n"):
            line = line.strip().lstrip("- ")
            parts = line.split(" — ", 1)
            name = re.sub(r"`", "", parts[0]).strip()
            desc = parts[1].strip() if len(parts) > 1 else ""
            if name:
                items.append((name, desc))
        return items

    fields = []
    for name, block in field_pat.findall(text):
        fields.append({
            "name":      name,
            "type":      _get(block, "type"),
            "purpose":   _get(block, "purpose"),
            "values":    _get_values(block),
            "rule":      _get(block, "rule"),
            "fill_rate": _get(block, "expected fill rate"),
        })
    return fields


def build_taxonomy_appendix(taxonomy_path: Path, subject: str) -> tuple[str, str]:
    """
    Build an HTML appendix section and companion CSS from a taxonomy_proposal.md.
    Returns (appendix_html, css_block).
    """
    fields = _parse_taxonomy(taxonomy_path)

    def _badge(val_name: str, val_desc: str) -> str:
        colour = _VALUE_COLOURS.get(val_name, _DEFAULT_COLOUR)
        label  = val_name.replace("_", " ")
        html   = (
            f'<span class="tax-badge" style="background:{colour}">{label}</span>'
            f'<span class="tax-val-desc">{val_desc}</span>'
        )
        return f'<li class="tax-val-item">{html}</li>'

    summary_rows = "".join(
        f"<tr>"
        f"<td><code>{f['name']}</code></td>"
        f'<td><span class="tax-type-badge tax-type-{f["type"]}">{f["type"]}</span></td>'
        f"<td>{f['purpose'][:120]}{'…' if len(f['purpose']) > 120 else ''}</td>"
        f'<td style="text-align:center">{f["fill_rate"]}</td>'
        f"</tr>"
        for f in fields
    )

    field_cards = ""
    for f in fields:
        if f["values"]:
            items = "\n".join(_badge(v, d) for v, d in f["values"])
            values_html = f'<ul class="tax-val-list">{items}</ul>'
        else:
            values_html = '<p class="tax-free-text">Free text — no fixed allowed values.</p>'

        rule_html = (
            f'<p class="tax-rule"><strong>Annotation rule:</strong> {f["rule"]}</p>'
            if f["rule"] else ""
        )

        field_cards += f"""
<div class="tax-field-card" id="tax-{f['name']}">
  <div class="tax-field-header">
    <code class="tax-field-name">{f['name']}</code>
    <span class="tax-type-badge tax-type-{f['type']}">{f['type']}</span>
    <span class="tax-fillrate">fill rate: {f['fill_rate']}</span>
  </div>
  <p class="tax-purpose">{f['purpose']}</p>
  {values_html}
  {rule_html}
</div>"""

    appendix_html = f"""
<div class="appendix">
<h2>Appendix: Annotation Taxonomy</h2>
<p class="appendix-intro">
  Each speech turn in the dataset was annotated using a lean schema purpose-built
  for analysing how {subject} is invoked in Australian parliamentary discourse.
  Fields marked
  <span class="tax-type-badge tax-type-extractable">extractable</span>
  are directly determinable from the speech text; fields marked
  <span class="tax-type-badge tax-type-inferred">inferred</span>
  require interpretive judgement. A <code>null</code> value indicates the text
  did not support classification at medium confidence or above.
</p>

<h3>Summary table</h3>
<table class="tax-summary-table">
  <thead>
    <tr><th>Field</th><th>Type</th><th>Purpose (summary)</th><th>Fill rate</th></tr>
  </thead>
  <tbody>{summary_rows}</tbody>
</table>

<h3>Field definitions</h3>
{field_cards}
</div>"""

    return appendix_html, _APPENDIX_CSS


# ── STEP 3 — Transform [N] citations into superscripts ────────────────────────
_BRACKET_RE = re.compile(r'\[(\d+(?:\s*,\s*\d+)*)\]')


def assign_sequential_numbers(html: str) -> dict[int, int]:
    """
    Scan html for [N] bracket citations in document order.
    Return mapping: ref_id → sequential display number (1, 2, 3 …).
    The first ref_id encountered in the text gets display number 1, etc.
    """
    seq_map: dict[int, int] = {}
    counter = 1
    for m in _BRACKET_RE.finditer(html):
        for tok in m.group(1).split(","):
            tok = tok.strip()
            if tok.isdigit():
                rid = int(tok)
                if rid not in seq_map:
                    seq_map[rid] = counter
                    counter += 1
    return seq_map


def transform_citations(html: str, seq_map: dict[int, int]) -> str:
    """
    Convert [12] and [12, 38, 44] into superscript anchor links.
    Display numbers come from seq_map (first-appearance order).
    Adjacent superscripts are separated by a hair space (&#8202;).
    """
    def _replace(m: re.Match) -> str:
        raw = m.group(1)
        ids: list[int] = []
        seen: set[int] = set()
        for tok in raw.split(","):
            tok = tok.strip()
            if tok.isdigit():
                rid = int(tok)
                if rid not in seen:
                    seen.add(rid)
                    ids.append(rid)
        parts = [
            f'<sup class="cite" id="citeref-{rid}-{i}">'
            f'<a href="#ref-{rid}">{seq_map.get(rid, rid)}</a></sup>'
            for i, rid in enumerate(ids)
        ]
        return "&#8202;".join(parts)   # hair space between adjacent superscripts
    return _BRACKET_RE.sub(_replace, html)


# ── STEP 4 — Append references section ────────────────────────────────────────
def append_references(
    html: str,
    lookup_df: pd.DataFrame,
    seq_map: dict[int, int],
) -> tuple[str, list[int]]:
    """
    Extract all cited ref_ids from the transformed HTML,
    build a reference list, and append it before </div>.
    """
    # Collect cited ref_ids
    cited: set[int] = set()
    for m in re.finditer(r'href="#ref-(\d+)"', html):
        cited.add(int(m.group(1)))

    # Sort by sequential display number (first-appearance in document)
    cited_sorted = sorted(cited, key=lambda rid: seq_map.get(rid, rid))

    items: list[str] = []
    for ref_id in cited_sorted:
        rows = lookup_df[lookup_df["ref_id"] == ref_id]
        if rows.empty:
            continue
        r          = rows.iloc[0]
        label      = r["short_label"]
        url        = r["url"]
        seq_num    = seq_map.get(ref_id, ref_id)   # sequential display number
        items.append(
            f'<li id="ref-{ref_id}">'
            f'<span class="ref-num">{seq_num}.</span>'
            f'<span>{label} &nbsp;'
            f'<a href="{url}" target="_blank" rel="noopener">[Hansard↗]</a>'
            f'</span></li>'
        )

    ref_section = (
        '\n<div class="references">\n'
        '<h2>References</h2>\n'
        '<ol class="ref-list">\n'
        + "\n".join(items)
        + "\n</ol>\n</div>\n"
    )

    # Append to body HTML; wrap_html will place this inside the container.
    return html + ref_section, cited_sorted


# ── QC checks ─────────────────────────────────────────────────────────────────
def quality_check(
    final_html: str,
    draft_html: str,
    lookup_df: pd.DataFrame,
    cited_ids: list[int],
) -> list[str]:
    msgs: list[str] = []

    # 1. No raw bracket citations remain
    remaining = _BRACKET_RE.findall(final_html)
    if remaining:
        msgs.append(f"WARN: {len(remaining)} raw bracket citation(s) still present: {remaining[:5]}")
    else:
        msgs.append("OK:   No raw [N] bracket citations remain.")

    # 2. All cited ref_ids exist in lookup
    max_ref = int(lookup_df["ref_id"].max())
    cited_in_html = {int(m.group(1)) for m in re.finditer(r'href="#ref-(\d+)"', final_html)}
    invalid = [r for r in cited_in_html if r < 1 or r > max_ref]
    if invalid:
        msgs.append(f"ERROR: ref_ids not in lookup: {invalid}")
    else:
        msgs.append(f"OK:   All {len(cited_in_html)} cited ref_ids are valid (1–{max_ref}).")

    # 3. Reference list entries match cited refs
    in_reflist = {int(m.group(1)) for m in re.finditer(r'id="ref-(\d+)"', final_html)}
    if not in_reflist:
        msgs.append("ERROR: Reference list is EMPTY — references section was not inserted.")
    else:
        orphaned = in_reflist - cited_in_html
        missing  = cited_in_html - in_reflist
        if orphaned:
            msgs.append(f"WARN: Refs in list but not cited in text: {sorted(orphaned)}")
        if missing:
            msgs.append(f"WARN: Cited refs missing from reference list: {sorted(missing)}")
        if not orphaned and not missing:
            msgs.append(f"OK:   Reference list ({len(in_reflist)} entries) matches cited set exactly.")

    # 4. Check each bracket citation group in the draft respects max-3 rule
    draft_brackets = re.findall(r'\[(\d+(?:\s*,\s*\d+)*)\]', draft_html)
    over3 = [b for b in draft_brackets if len(b.split(",")) > 3]
    if over3:
        msgs.append(f"WARN: {len(over3)} citation group(s) in draft exceed 3 IDs: {over3[:5]}")
    else:
        msgs.append("OK:   All individual citation groups respect the max-3 rule.")

    # 5. Counts
    msgs.append(f"INFO: {len(cited_in_html)} unique ref_ids cited.")
    msgs.append(f"INFO: {len(cited_ids)} entries in reference list.")
    msgs.append(f"INFO: Draft had {len(_BRACKET_RE.findall(draft_html))} citation clusters.")

    return msgs


# ── CLI & main ─────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate a traceable cited HTML report from matches_enriched.csv."
    )
    ap.add_argument(
        "--input", type=Path, required=True,
        help="Path to matches_enriched.csv.",
    )
    ap.add_argument(
        "--subject", default=None,
        help="Short name of the subject organisation (e.g. WWF, ARENA, GRDC). "
             "Defaults to the parent directory name of --input.",
    )
    ap.add_argument(
        "--subject-full-name", default=None,
        help="Full name of the subject organisation for use in the report prose. "
             "Defaults to --subject.",
    )
    ap.add_argument(
        "--outdir", type=Path, default=None,
        help="Output directory (default: same folder as --input).",
    )
    ap.add_argument(
        "--model", default="claude-opus-4-6",
        choices=["claude-opus-4-6", "claude-sonnet-4-6"],
        help="Claude model for the narrative (default: claude-opus-4-6).",
    )
    ap.add_argument(
        "--taxonomy", type=Path, default=None,
        help="Path to taxonomy_proposal.md for the appendix "
             "(default: <outdir>/taxonomy_proposal.md).",
    )
    args = ap.parse_args()

    # Resolve subject names
    subject      = args.subject or args.input.parent.name
    subject_full = args.subject_full_name or subject

    outdir = args.outdir or args.input.parent
    outdir.mkdir(parents=True, exist_ok=True)

    # ── Load data ──────────────────────────────────────────────────────────────
    print(f"\nLoading {args.input}…")
    df = pd.read_csv(args.input)
    print(f"  {len(df):,} records loaded.")

    # ── Detect subject-specific field names ────────────────────────────────────
    fields = detect_fields(df)
    print(f"  Detected framing column : {fields['framing']}")
    print(f"  Detected valence column : {fields['valence']}")

    # ── STEP 1: Lookup table ───────────────────────────────────────────────────
    print("\nSTEP 1 — Building reference lookup…")
    lookup_df, id_map = build_lookup(df)
    lookup_path = outdir / "reference_lookup.csv"
    lookup_df.to_csv(lookup_path, index=False, encoding="utf-8")
    print(f"  {len(lookup_df):,} entries → {lookup_path}")

    # ── STEP 2: Generate draft report ─────────────────────────────────────────
    print("\nSTEP 2 — Generating draft report via Claude API…")
    stats   = compute_stats(df, subject, fields)
    records = build_compact_records(df, id_map, fields)
    system  = build_system_prompt(subject, subject_full, fields, df)
    body_draft, in_tok, out_tok, cost = call_claude(
        stats, records, args.model, system,
        framing_col=fields["framing"] or "framing",
        valence_col=fields["valence"] or "valence",
    )

    print(f"  Tokens: {in_tok:,} in + {out_tok:,} out  (≈ ${cost:.4f})")

    source     = str(args.input)
    draft_html = wrap_html(body_draft, total=len(df), subject=subject, source=source)
    draft_path = outdir / "report_with_ref_ids.html"
    draft_path.write_text(draft_html, encoding="utf-8")
    print(f"  Draft → {draft_path}  ({draft_path.stat().st_size // 1024} KB)")

    # ── STEP 3: Transform citations ────────────────────────────────────────────
    print("\nSTEP 3 — Post-processing citations…")
    seq_map    = assign_sequential_numbers(body_draft)
    body_cited = transform_citations(body_draft, seq_map)

    # ── STEP 4: Append references ──────────────────────────────────────────────
    print("STEP 4 — Appending references section…")
    final_html_inner, cited_ids = append_references(body_cited, lookup_df, seq_map)

    # ── STEP 4b: Taxonomy appendix ────────────────────────────────────────────
    taxonomy_path = args.taxonomy or (outdir / "taxonomy_proposal.md")
    if taxonomy_path.exists():
        print("STEP 4b — Building taxonomy appendix…")
        app_html, app_css = build_taxonomy_appendix(taxonomy_path, subject)
        final_html_inner = final_html_inner + app_html
    else:
        app_css = ""
        print(f"  WARN: taxonomy file not found at {taxonomy_path} — appendix skipped.")

    final_html = wrap_html(final_html_inner, total=len(df), subject=subject, source=source)

    # Inject appendix CSS before </style>
    if app_css:
        final_html = final_html.replace("</style>", app_css + "\n</style>", 1)

    final_path = outdir / "report_final_cited.html"
    final_path.write_text(final_html, encoding="utf-8")
    print(f"  Final → {final_path}  ({final_path.stat().st_size // 1024} KB)")

    # ── QC ─────────────────────────────────────────────────────────────────────
    print("\nQC CHECKS:")
    issues = quality_check(final_html, draft_html, lookup_df, cited_ids)
    for msg in issues:
        print(f"  {msg}")

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"""
─────────────────────────────────────────────────
Records loaded         : {len(df):,}
References in lookup   : {len(lookup_df):,}
References cited       : {len(cited_ids)}
API cost (estimated)   : ${cost:.4f}
Output files:
  {lookup_path}
  {draft_path}
  {final_path}
─────────────────────────────────────────────────
""")


if __name__ == "__main__":
    main()
