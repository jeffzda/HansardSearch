#!/usr/bin/env python3
"""
analyse_combined.py — Combined row-level + exchange-level Hansard case study report.

Reads both matches_enriched.csv (row-level enrichment) and
exchanges_enriched.csv (exchange-level enrichment) and produces a single
self-contained HTML report with statistical charts and an LLM narrative
covering both layers of analysis.

Usage:
    python analyse_combined.py \\
        --matches   ../case_studies/solar_thermal/matches_enriched.csv \\
        --exchanges ../case_studies/solar_thermal/exchanges_enriched.csv \\
        --out       ../case_studies/solar_thermal/report.html \\
        --subject   solar_thermal
"""

import argparse
import base64
import csv
import datetime
import io
import json
import re
import textwrap
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
csv.field_size_limit(10_000_000)

# ── Party helpers ──────────────────────────────────────────────────────────────

PARTY_COLOURS = {
    "Coalition": "#003087",
    "Labor":     "#E3231F",
    "Greens":    "#009B55",
    "Nationals": "#006633",
    "Other":     "#888888",
}
PARTY_ORDER = ["Coalition", "Labor", "Greens", "Nationals", "Other"]

def map_party_group(party) -> str:
    if pd.isna(party):
        return "Other"
    p = str(party).strip()
    if p in ("LP", "LNP", "CLP"):
        return "Coalition"
    if p == "ALP":
        return "Labor"
    if p == "AG":
        return "Greens"
    if p in ("NP", "NATS"):
        return "Nationals"
    return "Other"


def year_to_parliament(year: int) -> str:
    if year <= 1999: return "39th"
    elif year <= 2001: return "40th"
    elif year <= 2004: return "41st"
    elif year <= 2007: return "42nd"
    elif year <= 2010: return "43rd"
    elif year <= 2013: return "44th"
    elif year <= 2016: return "45th"
    elif year <= 2019: return "46th"
    elif year <= 2022: return "47th"
    else: return "48th"


# ── Subject configurations ─────────────────────────────────────────────────────

SUBJECT_CONFIGS = {
    "solar_thermal": {
        "subject_name":     "Solar Thermal Power",
        "subject_full":     "solar thermal power (concentrated solar power / CSP)",
        "primary_colour":   "#1a2a3a",
        "accent_colour":    "#F9A825",
        "framing_col":      "org_framing",
        "valence_col":      "speaker_valence",
        "framing_order":    [
            "proven_solution", "promising_frontier", "policy_instrument",
            "regional_asset", "political_football", "failed_or_inferior",
            "neutral_reference",
        ],
        "framing_colours":  {
            "proven_solution":   "#1565C0",
            "promising_frontier":"#43A047",
            "policy_instrument": "#00ACC1",
            "regional_asset":    "#7B1FA2",
            "political_football":"#EF6C00",
            "failed_or_inferior":"#C62828",
            "neutral_reference": "#9E9E9E",
        },
        "valence_order":    ["positive", "neutral", "negative", "mixed", "instrumental"],
        "valence_colours":  {
            "positive":     "#4CAF50",
            "neutral":      "#9E9E9E",
            "negative":     "#F44336",
            "mixed":        "#FF9800",
            "instrumental": "#2196F3",
        },
        "negative_framings": ["failed_or_inferior", "political_football"],
        "fp_pattern":        r"false positive|stage direction|formatting artefact",
        "secondary_col":     "technology_context",
    },
}


# ── Chart helpers ──────────────────────────────────────────────────────────────

def fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    buf.seek(0)
    data = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return data


def apply_clean_style():
    plt.rcParams.update({
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "axes.grid":         True,
        "axes.grid.axis":    "x",
        "grid.alpha":        0.3,
        "font.family":       "sans-serif",
        "font.size":         11,
    })


# ── Row-level charts ───────────────────────────────────────────────────────────

def chart_year_framing(df: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    years = sorted(df["year"].dropna().unique())
    framing_order   = cfg["framing_order"]
    framing_colours = cfg["framing_colours"]
    framings = [f for f in framing_order if f in df[cfg["framing_col"]].values]

    data = {f: [df[(df["year"] == y) & (df[cfg["framing_col"]] == f)].shape[0]
                for y in years] for f in framings}

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom = np.zeros(len(years))
    for f in framings:
        vals = np.array(data[f], dtype=float)
        ax.bar(years, vals, bottom=bottom,
               color=framing_colours[f], label=f.replace("_", " ").title(), width=0.7)
        bottom += vals

    ax.set_xlabel("Year"); ax.set_ylabel("Mentions")
    ax.set_title(f"{cfg['subject_name']} — Mentions per Year by Framing", fontsize=13, fontweight="bold")
    ax.set_xticks(years)
    ax.set_xticklabels([str(int(y)) for y in years], rotation=45, ha="right", fontsize=9)
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_framing_by_party(df: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    plt.rcParams["axes.grid.axis"] = "x"
    framings = [f for f in cfg["framing_order"] if f in df[cfg["framing_col"]].values]
    parties  = [p for p in PARTY_ORDER if p in df["party_group"].values]
    matrix   = pd.crosstab(df[cfg["framing_col"]], df["party_group"]).reindex(
        index=framings, columns=parties, fill_value=0)

    fig, ax = plt.subplots(figsize=(12, 5))
    y = np.arange(len(framings))
    bar_h = 0.12
    offsets = np.linspace(-(len(parties)-1)/2, (len(parties)-1)/2, len(parties)) * bar_h
    for i, party in enumerate(parties):
        vals = matrix[party].values if party in matrix.columns else np.zeros(len(framings))
        ax.barh(y + offsets[i], vals, height=bar_h * 0.9,
                color=PARTY_COLOURS[party], label=party)

    ax.set_yticks(y)
    ax.set_yticklabels([f.replace("_", " ").title() for f in framings])
    ax.set_xlabel("Mentions")
    ax.set_title("Framing Type by Party Group", fontsize=13, fontweight="bold")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_ingov_framing(df: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    framings = [f for f in cfg["framing_order"] if f in df[cfg["framing_col"]].values]
    in_gov_counts  = df[df["in_gov"] == 1][cfg["framing_col"]].value_counts()
    out_gov_counts = df[df["in_gov"] == 0][cfg["framing_col"]].value_counts()
    in_pct  = (in_gov_counts.reindex(framings, fill_value=0) /
               max(in_gov_counts.sum(), 1) * 100)
    out_pct = (out_gov_counts.reindex(framings, fill_value=0) /
               max(out_gov_counts.sum(), 1) * 100)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
    colors = [cfg["framing_colours"][f] for f in framings]
    for ax, pct, title in zip(axes, [in_pct, out_pct], ["In Government", "In Opposition"]):
        ax.barh([f.replace("_", " ").title() for f in framings], pct.values, color=colors)
        ax.set_xlabel("% of mentions")
        ax.set_title(title, fontsize=12, fontweight="bold")
        for s in ["top", "right"]: ax.spines[s].set_visible(False)
    fig.suptitle("Framing Distribution: Government vs Opposition",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_rhetorical_function(df: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    plt.rcParams["axes.grid.axis"] = "x"
    counts = df["rhetorical_function"].value_counts().sort_values()
    fig, ax = plt.subplots(figsize=(12, 5))
    colors = [cfg["accent_colour"] if v == counts.idxmax() else "#555555" for v in counts.values]
    ax.barh([v.replace("_", " ").title() for v in counts.index], counts.values, color=colors)
    ax.set_xlabel("Mentions")
    ax.set_title("Rhetorical Function", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_policy_domain(df: pd.DataFrame) -> str:
    counts = df["policy_domain"].value_counts()
    fig, ax = plt.subplots(figsize=(8, 6))
    labels = [v.replace("_", " ").title() for v in counts.index]
    colors_cycle = ["#1565C0","#43A047","#EF6C00","#9E9E9E",
                    "#C62828","#00ACC1","#7B1FA2","#006633"][:len(counts)]
    ax.pie(counts.values, labels=labels, autopct="%1.1f%%",
           colors=colors_cycle, startangle=140,
           pctdistance=0.8, textprops={"fontsize": 9})
    ax.set_title("Policy Domain Distribution", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_valence_time(df: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    valences = [v for v in cfg["valence_order"] if v in df[cfg["valence_col"]].values]
    years    = sorted(df["year"].dropna().unique())
    vc       = pd.crosstab(df["year"], df[cfg["valence_col"]]).reindex(
        index=years, columns=valences, fill_value=0).astype(float)
    rolled   = vc.rolling(5, center=True, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom  = np.zeros(len(years))
    for v in valences:
        vals = rolled[v].values
        ax.fill_between(years, bottom, bottom + vals,
                        color=cfg["valence_colours"][v], alpha=0.75, label=v.title())
        bottom += vals
    ax.set_xlabel("Year"); ax.set_ylabel("Mentions (5-yr rolling)")
    ax.set_title(f"Speaker Valence Toward {cfg['subject_name']} Over Time (5-year rolling)",
                 fontsize=13, fontweight="bold")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_secondary(df: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    col  = cfg.get("secondary_col", "technology_context")
    sub  = df.dropna(subset=[col, "year"])
    if sub.empty:
        return ""
    years = sorted(sub["year"].dropna().unique())
    cats  = sub[col].value_counts().head(6).index.tolist()
    colors_cycle = ["#1565C0","#43A047","#EF6C00","#9E9E9E","#C62828","#00ACC1"]

    cross  = pd.crosstab(sub["year"], sub[col]).reindex(
        index=years, columns=cats, fill_value=0).astype(float)
    rolled = cross.rolling(3, center=True, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom = np.zeros(len(years))
    for i, c in enumerate(cats):
        vals = rolled[c].values
        ax.fill_between(years, bottom, bottom + vals,
                        color=colors_cycle[i % len(colors_cycle)], alpha=0.75,
                        label=c.replace("_", " ").title())
        bottom += vals
    ax.set_xlabel("Year"); ax.set_ylabel("Mentions (3-yr rolling)")
    ax.set_title(f"Technology Context Over Time (3-year rolling)", fontsize=13, fontweight="bold")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


def build_top_speakers_table(df: pd.DataFrame, cfg: dict) -> str:
    _stage = r"(?i)^stage direction$|^the (president|speaker|chair|deputy|acting|clerk)"
    df = df[~df["speaker_name"].fillna("").str.match(_stage, na=False)]
    grp = df.groupby("speaker_name")
    counts      = grp.size().rename("count")
    party       = grp["party_group"].agg(lambda x: x.mode()[0] if len(x) else "")
    def ingov_label(x):
        g, o = int((x==1).sum()), int((x==0).sum())
        if g > 0 and o > 0: return f"Both ({g}g/{o}o)"
        return "Gov" if g > 0 else "Opp"
    ingov       = grp["in_gov"].agg(ingov_label)
    dom_framing = grp[cfg["framing_col"]].agg(
        lambda x: x.dropna().mode()[0] if len(x.dropna()) else "")
    dom_valence = grp[cfg["valence_col"]].agg(
        lambda x: x.dropna().mode()[0] if len(x.dropna()) else "")

    tbl = pd.DataFrame({
        "Speaker":          counts.index,
        "Party Group":      party.values,
        "In Gov":           ingov.values,
        "Mentions":         counts.values,
        "Dominant Framing": dom_framing.values,
        "Dominant Valence": dom_valence.values,
    }).sort_values("Mentions", ascending=False).head(20)

    rows = ""
    for _, r in tbl.iterrows():
        fk = str(r["Dominant Framing"]).lower().replace(" ", "_")
        vk = str(r["Dominant Valence"]).lower()
        fc = cfg["framing_colours"].get(fk, "#888")
        vc = cfg["valence_colours"].get(vk, "#888")
        rows += f"""
        <tr>
          <td>{r['Speaker']}</td>
          <td><span class="party-badge" style="background:{PARTY_COLOURS.get(r['Party Group'],'#888')}">{r['Party Group']}</span></td>
          <td>{r['In Gov']}</td>
          <td style="text-align:center;font-weight:bold">{r['Mentions']}</td>
          <td><span class="badge" style="background:{fc}">{r['Dominant Framing'].replace('_',' ').title()}</span></td>
          <td><span class="badge" style="background:{vc}">{r['Dominant Valence'].title()}</span></td>
        </tr>"""

    return f"""
    <table class="data-table">
      <thead><tr>
        <th>Speaker</th><th>Party Group</th><th>Gov Status</th>
        <th>Mentions</th><th>Dominant Framing</th><th>Dominant Valence</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


# ── Exchange charts ────────────────────────────────────────────────────────────

def chart_exchange_answered_time(ex: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    plt.rcParams["axes.grid.axis"] = "y"
    sub = ex.dropna(subset=["year", "answered"])
    sub = sub.copy()
    sub["answered_bool"] = sub["answered"].astype(str).str.lower().isin(["true", "1", "yes"])
    years = sorted(sub["year"].dropna().unique())

    yes_vals = [sub[(sub["year"]==y) & sub["answered_bool"]].shape[0] for y in years]
    no_vals  = [sub[(sub["year"]==y) & ~sub["answered_bool"]].shape[0] for y in years]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.bar(years, yes_vals, label="Answered",     color="#43A047", width=0.6)
    ax.bar(years, no_vals,  label="Not answered", color="#C62828", width=0.6,
           bottom=yes_vals)
    ax.set_xlabel("Year"); ax.set_ylabel("Exchanges")
    ax.set_title("Q&A Exchanges: Answered vs Not Answered by Year", fontsize=13, fontweight="bold")
    ax.set_xticks(years)
    ax.set_xticklabels([str(int(y)) for y in years], rotation=45, ha="right", fontsize=9)
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_answer_quality(ex: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    plt.rcParams["axes.grid.axis"] = "x"
    order  = ["substantive", "partial", "deflection", "non_answer"]
    counts = ex["answer_quality"].value_counts().reindex(order).dropna()
    colors = {"substantive": "#1565C0", "partial": "#43A047",
              "deflection": "#EF6C00", "non_answer": "#C62828"}
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh([v.replace("_", " ").title() for v in counts.index],
            counts.values,
            color=[colors.get(v, "#888") for v in counts.index])
    ax.set_xlabel("Exchanges")
    ax.set_title("Answer Quality Distribution", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_question_intent(ex: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    plt.rcParams["axes.grid.axis"] = "x"
    order  = ["factual", "accountability", "hostile", "supportive", "rhetorical", "clarifying"]
    counts = ex["question_intent"].value_counts().reindex(order).dropna()
    colors_map = {
        "factual": "#2196F3", "accountability": "#FF9800", "hostile": "#C62828",
        "supportive": "#43A047", "rhetorical": "#9E9E9E", "clarifying": "#00ACC1",
    }
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh([v.replace("_", " ").title() for v in counts.index],
            counts.values,
            color=[colors_map.get(v, "#888") for v in counts.index])
    ax.set_xlabel("Exchanges")
    ax.set_title("Question Intent Distribution", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_intent_vs_quality(ex: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    intent_order  = ["factual", "accountability", "hostile", "supportive", "rhetorical", "clarifying"]
    quality_order = ["substantive", "partial", "deflection", "non_answer"]
    sub = ex.dropna(subset=["question_intent", "answer_quality"])
    if sub.empty:
        return ""

    matrix = pd.crosstab(sub["question_intent"], sub["answer_quality"]).reindex(
        index=[i for i in intent_order if i in sub["question_intent"].values],
        columns=[q for q in quality_order if q in sub["answer_quality"].values],
        fill_value=0)

    quality_colors = {"substantive": "#1565C0", "partial": "#43A047",
                      "deflection":  "#EF6C00",  "non_answer": "#C62828"}

    fig, ax = plt.subplots(figsize=(10, 5))
    y_labels = [i.replace("_", " ").title() for i in matrix.index]
    y_pos    = np.arange(len(matrix.index))
    bar_h    = 0.18
    cols_present = list(matrix.columns)
    offsets  = np.linspace(-(len(cols_present)-1)/2, (len(cols_present)-1)/2,
                           len(cols_present)) * bar_h

    for i, col in enumerate(cols_present):
        ax.barh(y_pos + offsets[i], matrix[col].values, height=bar_h * 0.9,
                color=quality_colors.get(col, "#888"),
                label=col.replace("_", " ").title())

    ax.set_yticks(y_pos); ax.set_yticklabels(y_labels)
    ax.set_xlabel("Exchanges")
    ax.set_title("Question Intent vs Answer Quality", fontsize=13, fontweight="bold")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_evasion_technique(ex: pd.DataFrame, cfg: dict) -> str:
    apply_clean_style()
    plt.rcParams["axes.grid.axis"] = "x"
    unanswered = ex[ex["answered"].astype(str).str.lower().isin(["false", "0", "no"])]
    if unanswered.empty:
        return ""
    counts = unanswered["evasion_technique"].value_counts().sort_values()
    if counts.empty:
        return ""
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh([v.replace("_", " ").title() for v in counts.index], counts.values,
            color=cfg["accent_colour"])
    ax.set_xlabel("Exchanges")
    ax.set_title("Evasion Techniques (unanswered exchanges only)", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


def build_exchange_table(ex: pd.DataFrame, cfg: dict, n: int = 20) -> str:
    """Table of notable exchanges (sorted by date)."""
    cols_needed = ["exchange_id", "date", "chamber", "question_speaker", "question_party",
                   "answer_speaker", "answer_party", "question_intent",
                   "answer_quality", "answered", "summary"]
    present = [c for c in cols_needed if c in ex.columns]
    sub = ex[present].sort_values("date").head(n)

    INTENT_COLOURS = {
        "factual": "#2196F3", "accountability": "#FF9800", "hostile": "#C62828",
        "supportive": "#43A047", "rhetorical": "#9E9E9E", "clarifying": "#00ACC1",
    }
    QUALITY_COLOURS = {
        "substantive": "#1565C0", "partial": "#43A047",
        "deflection": "#EF6C00", "non_answer": "#C62828",
    }

    rows = ""
    for _, r in sub.iterrows():
        intent  = str(r.get("question_intent", "")).replace("_", " ").title()
        quality = str(r.get("answer_quality", "")).replace("_", " ").title()
        ic = INTENT_COLOURS.get(str(r.get("question_intent", "")).lower(), "#888")
        qc = QUALITY_COLOURS.get(str(r.get("answer_quality", "")).lower(), "#888")
        answered_str = str(r.get("answered", "")).lower()
        ans_icon = "✓" if answered_str in ("true", "1", "yes") else "✗"
        ans_col  = "#43A047" if ans_icon == "✓" else "#C62828"
        rows += f"""
        <tr>
          <td style="font-size:0.8rem">{str(r.get('date',''))[:10]}</td>
          <td>{r.get('chamber','').title()}</td>
          <td>{r.get('question_speaker','')}<br><small>{r.get('question_party','')}</small></td>
          <td>{r.get('answer_speaker','')}<br><small>{r.get('answer_party','')}</small></td>
          <td><span class="badge" style="background:{ic}">{intent}</span></td>
          <td><span class="badge" style="background:{qc}">{quality}</span></td>
          <td style="color:{ans_col};font-weight:bold;text-align:center">{ans_icon}</td>
          <td style="font-size:0.8rem;max-width:280px">{str(r.get('summary',''))[:200]}</td>
        </tr>"""

    return f"""
    <table class="data-table">
      <thead><tr>
        <th>Date</th><th>Chamber</th><th>Questioner</th><th>Minister</th>
        <th>Intent</th><th>Answer Quality</th><th>Ans?</th><th>Summary</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


# ── LLM narrative ──────────────────────────────────────────────────────────────

def get_llm_narrative(df: pd.DataFrame, ex: pd.DataFrame,
                      cfg: dict, model: str) -> tuple[str, int, int, float]:
    try:
        import anthropic
        client = anthropic.Anthropic()

        # Row-level summary table (compact)
        row_cols = ["year", "chamber", "party", "in_gov",
                    cfg["framing_col"], cfg["valence_col"],
                    "rhetorical_function", "policy_domain", "summary"]
        row_cols = [c for c in row_cols if c in df.columns]
        row_table = df[row_cols].copy()
        if "summary" in row_table.columns:
            row_table["summary"] = row_table["summary"].fillna("").str[:180]
        row_csv = row_table.to_csv(index=False)

        # Exchange summary table (compact)
        ex_cols = ["date", "chamber", "question_speaker", "question_party",
                   "answer_speaker", "answer_party",
                   "question_intent", "answer_quality", "answered",
                   "evasion_technique", "summary"]
        ex_cols = [c for c in ex_cols if c in ex.columns]
        ex_table = ex[ex_cols].copy()
        if "summary" in ex_table.columns:
            ex_table["summary"] = ex_table["summary"].fillna("").str[:180]
        ex_csv = ex_table.to_csv(index=False)

        subject   = cfg["subject_name"]
        subject_f = cfg["subject_full"]

        system_prompt = textwrap.dedent(f"""
            You are an expert political analyst specialising in Australian parliamentary politics
            and energy policy. You will analyse two datasets of parliamentary references to
            {subject_f} drawn from Australian Hansard (1998–2025):

            1. ROW-LEVEL DATA — all speech turns mentioning {subject} (direct matches not
               part of Q&A exchanges)
            2. EXCHANGE DATA — Q&A pairs where {subject} was mentioned in either question
               or answer (35 exchanges)

            Produce a structured analytical report in clean HTML (no markdown, no code fences,
            no backticks) using the exact section headings specified. Write for a senior
            parliamentary researcher or energy policy analyst — analytical, precise, grounded
            in the data, with specific examples. Do not include <html>, <head>, or <body>
            tags — only the section content beginning with the first <h2>.
        """).strip()

        user_prompt = f"""Below are two datasets.

=== ROW-LEVEL DATA ({len(df)} speech turns, non-exchange direct matches) ===
{row_csv}

=== EXCHANGE DATA ({len(ex)} exchanges) ===
{ex_csv}

Produce the following sections in HTML:

<h2>Executive Summary</h2>
Three paragraphs for a senior energy policy researcher summarising the overall picture across
both datasets: volume and temporal trends, dominant framings, party dynamics, and the
accountability story told by the exchange data.

<h2>The Parliamentary Arc, 1998–2025</h2>
Chronological narrative in 4–6 eras identifying the political events that drove changes in
how {subject} was framed. Use specific year ranges, speakers, and policy events where
visible in the data. Draw on both the speech-turn framing patterns and the exchange data
to show how interest peaked, what drove it, and why it faded.

<h2>The Accountability Record</h2>
Detailed analysis of the exchange data: what types of questions were asked, how ministers
responded, which exchanges were substantively answered vs deflected or evaded, and what
this reveals about government accountability on {subject} across different administrations.
Identify specific exchanges (by date and speakers) that best illustrate patterns.
Note: analyse the period before and after the Solar Flagships program failure separately
where the data supports it.

<h2>Party Dynamics</h2>
How Coalition, Labor, and Greens used {subject} differently in non-exchange speech turns,
with specific framing examples. Cross-reference with the exchange data to show whether
the same party patterns hold in Q&A settings.

<h2>Strategic Observations</h2>
Four to five concrete observations for an energy policy researcher or advocacy organisation
tracking {subject} in parliament. What does the framing trajectory suggest about the
technology's political prospects? Use a numbered list (<ol>).
"""

        print(f"  Calling {model} for narrative…", flush=True)
        response = client.messages.create(
            model=model,
            max_tokens=8192,
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
        )
        usage = response.usage
        cost  = (usage.input_tokens / 1e6 * 15) + (usage.output_tokens / 1e6 * 75)
        print(f"  Done. {usage.input_tokens:,} in / {usage.output_tokens:,} out / ${cost:.3f}", flush=True)
        return response.content[0].text, usage.input_tokens, usage.output_tokens, cost

    except Exception as e:
        print(f"  WARNING: narrative failed: {e}", flush=True)
        placeholder = f"""
        <h2>Executive Summary</h2><p><em>[Narrative generation failed: {str(e)[:200]}]</em></p>
        <h2>The Parliamentary Arc, 1998–2025</h2><p><em>Not available.</em></p>
        <h2>The Accountability Record</h2><p><em>Not available.</em></p>
        <h2>Party Dynamics</h2><p><em>Not available.</em></p>
        <h2>Strategic Observations</h2><p><em>Not available.</em></p>
        """
        return placeholder, 0, 0, 0.0


# ── HTML assembly ──────────────────────────────────────────────────────────────

def build_html(row_stats: dict, ex_stats: dict, charts: dict,
               speakers_table: str, exchange_table: str,
               narrative: str, cfg: dict) -> str:

    today   = datetime.date.today().strftime("%d %B %Y")
    primary = cfg["primary_colour"]
    accent  = cfg["accent_colour"]
    subject = cfg["subject_name"]

    def stat_box(label, value):
        return f"""
        <div class="stat-box">
          <div class="stat-value">{value}</div>
          <div class="stat-label">{label}</div>
        </div>"""

    row_stat_boxes = "".join([
        stat_box("Speech turns (direct)", row_stats["total"]),
        stat_box("Senate / House", f"{row_stats['senate']} / {row_stats['house']}"),
        stat_box("Date range", row_stats["date_range"]),
        stat_box("Positive / Negative valence",
                 f"{row_stats['pct_positive']:.0f}% / {row_stats['pct_negative']:.0f}%"),
        stat_box("Top framing",
                 row_stats["top_framing"].replace("_", " ").title()),
        stat_box("Top policy domain",
                 row_stats["top_domain"].replace("_", " ").title()),
    ])

    ex_stat_boxes = "".join([
        stat_box("Q&A exchanges", ex_stats["total"]),
        stat_box("Answered", f"{ex_stats['answered']}/{ex_stats['total']} ({ex_stats['pct_answered']:.0f}%)"),
        stat_box("Hostile/accountability", f"{ex_stats['n_hostile_accountability']}"),
        stat_box("H/A answered",
                 f"{ex_stats['hostile_accountability_answered']}/{ex_stats['n_hostile_accountability']}"),
        stat_box("Most common intent",
                 ex_stats["top_intent"].replace("_", " ").title()),
        stat_box("Most common quality",
                 ex_stats["top_quality"].replace("_", " ").title()),
    ])

    def section(title, chart_b64, caption, extra_html="", chart_id=""):
        img_tag = (f'<img src="data:image/png;base64,{chart_b64}" '
                   f'alt="{title}" style="max-width:100%;height:auto;">'
                   if chart_b64 else "")
        return f"""
        <section class="chart-section" id="{chart_id}">
          <h3>{title}</h3>
          {"<div class='chart-wrap'>" + img_tag + "</div>" if img_tag else ""}
          <p class="caption">{caption}</p>
          {extra_html}
        </section>"""

    html = f"""<!DOCTYPE html>
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
    color: {primary};
    line-height: 1.6;
  }}
  header {{
    background: {primary};
    color: #fff;
    padding: 2.5rem 3rem;
    border-bottom: 4px solid {accent};
  }}
  header h1 {{ font-size: 1.8rem; font-weight: 700; letter-spacing: -0.02em; }}
  header p  {{ margin-top: 0.5rem; color: #ccc; font-size: 0.95rem; }}
  .container {{ max-width: 1100px; margin: 0 auto; padding: 2rem 2rem 4rem; }}
  .stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 1rem;
    margin: 1.5rem 0;
  }}
  .stat-box {{
    background: #f5f5f5;
    border-left: 4px solid {accent};
    padding: 1rem 1.25rem;
    border-radius: 4px;
  }}
  .stat-value {{ font-size: 1.45rem; font-weight: 700; line-height: 1.2; }}
  .stat-label {{ font-size: 0.78rem; color: #666; margin-top: 0.25rem;
                 text-transform: uppercase; letter-spacing: 0.05em; }}
  .chart-section {{
    background: #f5f5f5;
    border-radius: 8px;
    padding: 1.5rem 2rem;
    margin: 1.5rem 0;
  }}
  .chart-section h3 {{
    font-size: 1.1rem; font-weight: 600; margin-bottom: 1rem;
    color: {primary}; border-bottom: 2px solid {accent}; padding-bottom: 0.4rem;
  }}
  .chart-wrap {{ text-align: center; margin-bottom: 0.75rem; }}
  .caption {{ font-size: 0.85rem; color: #555; font-style: italic; }}
  .narrative-section {{
    background: #fff; border: 1px solid #e0e0e0;
    border-radius: 8px; padding: 2rem 2.5rem; margin: 2rem 0;
  }}
  .narrative-content h2 {{
    font-size: 1.2rem; font-weight: 700; color: {primary};
    border-bottom: 2px solid {accent}; padding-bottom: 0.4rem;
    margin: 1.75rem 0 0.75rem;
  }}
  .narrative-content h2:first-child {{ margin-top: 0; }}
  .narrative-content p {{ margin-bottom: 0.9rem; color: #333; line-height: 1.7; }}
  .narrative-content ol, .narrative-content ul {{
    margin: 0.5rem 0 0.9rem 1.5rem; color: #333;
  }}
  .narrative-content li {{ margin-bottom: 0.4rem; }}
  .data-table {{ width: 100%; border-collapse: collapse; font-size: 0.83rem; margin-top: 1rem; }}
  .data-table th {{
    background: {primary}; color: #fff;
    padding: 0.5rem 0.75rem; text-align: left; font-weight: 600;
  }}
  .data-table td {{ padding: 0.45rem 0.75rem; border-bottom: 1px solid #e0e0e0; }}
  .data-table tr:nth-child(even) {{ background: #f9f9f9; }}
  .data-table tr:hover {{ background: #fffde7; }}
  .badge {{
    display: inline-block; padding: 0.15rem 0.5rem; border-radius: 3px;
    font-size: 0.75rem; color: #fff; font-weight: 500;
  }}
  .party-badge {{
    display: inline-block; padding: 0.15rem 0.5rem; border-radius: 3px;
    font-size: 0.75rem; color: #fff; font-weight: 600;
  }}
  .section-heading {{
    font-size: 1.4rem; font-weight: 700; color: {primary};
    margin: 2.5rem 0 1rem; padding-left: 0.75rem;
    border-left: 4px solid {accent};
  }}
  .subsection-heading {{
    font-size: 1.15rem; font-weight: 600; color: {primary};
    margin: 2rem 0 0.75rem; padding-left: 0.5rem;
    border-left: 3px solid #ccc;
  }}
  footer {{
    background: #f5f5f5; border-top: 1px solid #ddd;
    padding: 1.25rem 3rem; font-size: 0.82rem; color: #666;
  }}
  @media (max-width: 600px) {{
    .stats-grid {{ grid-template-columns: 1fr 1fr; }}
    .stat-value {{ font-size: 1.1rem; }}
  }}
</style>
</head>
<body>
<header>
  <h1>{subject} in Australian Parliament</h1>
  <p>Combined speech-turn &amp; exchange analysis &nbsp;·&nbsp; Hansard 1998–2025 &nbsp;·&nbsp; Senate &amp; House of Representatives</p>
</header>
<div class="container">

  <h2 class="section-heading">At a Glance — Speech Turns ({row_stats['total']} direct matches)</h2>
  <div class="stats-grid">{row_stat_boxes}</div>

  <h2 class="section-heading">At a Glance — Q&amp;A Exchanges ({ex_stats['total']} exchanges)</h2>
  <div class="stats-grid">{ex_stat_boxes}</div>

  <h2 class="section-heading">Analytical Narrative</h2>
  <section class="narrative-section">
    <div class="narrative-content">{narrative}</div>
  </section>

  <h2 class="section-heading">Speech-Turn Analysis</h2>

  {section("Mentions Per Year by Framing Type", charts.get("year_framing",""),
           f"Peak year was {row_stats['top_year']['year']} ({row_stats['top_year']['count']} mentions). "
           f"Dominant framing: {row_stats['top_framing'].replace('_',' ').title()}.", chart_id="chart-a")}

  {section("Framing Type by Party Group", charts.get("framing_party",""),
           "Framing distribution across party groups.", chart_id="chart-b")}

  {section("Framing: Government vs Opposition", charts.get("ingov_framing",""),
           "Normalised % of mentions by framing, split by government membership at time of speech.",
           chart_id="chart-c")}

  {section("Rhetorical Function", charts.get("rhetorical_function",""),
           "How solar thermal mentions function rhetorically within broader parliamentary argument.",
           chart_id="chart-d")}

  {section("Policy Domain Distribution", charts.get("policy_domain",""),
           "The policy arena in which solar thermal is invoked.", chart_id="chart-e")}

  {section("Speaker Valence Over Time (5-year rolling)", charts.get("valence_time",""),
           "Smoothed trend of speaker disposition toward solar thermal.", chart_id="chart-f")}

  {section("Technology Context Over Time (3-year rolling)", charts.get("secondary",""),
           "How the technological frame shifts — from CSP-specific advocacy to generic "
           "renewables list citations and storage/baseload claims.",
           chart_id="chart-t")}

  {section("Top 20 Speakers", "", "Ranked by total mentions; dominant framing and valence shown.",
           extra_html=speakers_table, chart_id="chart-g")}

  <h2 class="section-heading">Exchange (Q&amp;A) Analysis</h2>

  {section("Exchanges: Answered vs Not Answered by Year", charts.get("answered_time",""),
           f"{ex_stats['answered']}/{ex_stats['total']} exchanges answered "
           f"({ex_stats['pct_answered']:.0f}%). "
           f"Hostile/accountability questions answered: "
           f"{ex_stats['hostile_accountability_answered']}/{ex_stats['n_hostile_accountability']}.",
           chart_id="chart-x1")}

  {section("Question Intent Distribution", charts.get("question_intent",""),
           "What questioners were trying to achieve.", chart_id="chart-x2")}

  {section("Answer Quality Distribution", charts.get("answer_quality",""),
           "How substantively ministers responded across all exchanges.", chart_id="chart-x3")}

  {section("Question Intent vs Answer Quality", charts.get("intent_vs_quality",""),
           "Whether answer quality varies by question type.", chart_id="chart-x4")}

  {section("Evasion Techniques (unanswered exchanges only)", charts.get("evasion",""),
           "How ministers avoided direct answers when they did not address the question.",
           chart_id="chart-x5")}

  {section("Exchange Sample (first 20 by date)", "", "Chronological sample of Q&A exchanges.",
           extra_html=exchange_table, chart_id="chart-x6")}

</div>
<footer>
  Analysis covers Australian Senate and House of Representatives Hansard, 1998–2025.
  Speech-turn layer: N={row_stats['total']} direct matches (after false-positive exclusion: N={row_stats['n_valid']}).
  Exchange layer: N={ex_stats['total']} Q&amp;A exchanges.
  Generated {today}.
</footer>
</body>
</html>"""
    return html


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Combined row-level + exchange-level Hansard case study report."
    )
    ap.add_argument("--matches",   required=True, help="matches_enriched.csv path")
    ap.add_argument("--exchanges", required=True, help="exchanges_enriched.csv path")
    ap.add_argument("--out",       required=True, help="Output HTML path")
    ap.add_argument("--subject",   default="solar_thermal",
                    choices=list(SUBJECT_CONFIGS.keys()),
                    help="Subject configuration key")
    ap.add_argument("--model",     default="claude-opus-4-6",
                    help="Claude model for narrative (default: claude-opus-4-6)")
    args = ap.parse_args()

    cfg = SUBJECT_CONFIGS[args.subject]

    # ── Load data ──────────────────────────────────────────────────────────────
    print(f"Loading {args.matches}…", flush=True)
    df = pd.read_csv(args.matches)
    print(f"  {len(df)} rows")

    print(f"Loading {args.exchanges}…", flush=True)
    ex = pd.read_csv(args.exchanges)
    print(f"  {len(ex)} exchanges")

    # ── Prepare row-level DF ───────────────────────────────────────────────────
    df["date"]        = pd.to_datetime(df["date"], errors="coerce")
    df["year"]        = df["date"].dt.year.astype("Int64")
    df["party_group"] = df["party"].apply(map_party_group)

    # False positive filter
    fp_mask = (
        df[cfg["framing_col"]].isna() |
        (df[cfg["framing_col"]].fillna("") == "")
    ) & df.get("summary", df.get("what_happened", pd.Series([""] * len(df)))).fillna(
        "").str.contains(cfg["fp_pattern"], case=False, regex=True)
    df_valid = df[~fp_mask].copy()
    print(f"  FP excluded: {fp_mask.sum()}, valid: {len(df_valid)}")

    # ── Prepare exchange DF ────────────────────────────────────────────────────
    ex["date"] = pd.to_datetime(ex["date"], errors="coerce")
    ex["year"] = ex["date"].dt.year.astype("Int64")
    ex_valid   = ex[ex["enrich_error"].fillna("") == ""].copy() if "enrich_error" in ex.columns else ex.copy()

    # ── Row-level stats ────────────────────────────────────────────────────────
    total  = len(df_valid)
    senate = (df_valid["chamber"] == "senate").sum()
    house  = (df_valid["chamber"] == "house").sum()
    dates  = df_valid["date"].dropna()
    date_range = f"{dates.min().year}–{dates.max().year}" if not dates.empty else "N/A"

    vc = df_valid[cfg["valence_col"]].value_counts()
    pct_pos = vc.get("positive", 0) / max(vc.sum(), 1) * 100
    pct_neg = vc.get("negative", 0) / max(vc.sum(), 1) * 100

    fc = df_valid[cfg["framing_col"]].value_counts()
    top_framing = fc.idxmax() if not fc.empty else "N/A"

    dc = df_valid["policy_domain"].value_counts() if "policy_domain" in df_valid.columns else pd.Series()
    top_domain = dc.idxmax() if not dc.empty else "N/A"

    year_counts = df_valid["year"].value_counts()
    top_year_val = int(year_counts.idxmax()) if not year_counts.empty else 0
    top_year_cnt = int(year_counts.max())    if not year_counts.empty else 0

    row_stats = {
        "total":       total,
        "senate":      senate,
        "house":       house,
        "date_range":  date_range,
        "pct_positive": pct_pos,
        "pct_negative": pct_neg,
        "top_framing":  top_framing,
        "top_domain":   top_domain,
        "top_year":     {"year": top_year_val, "count": top_year_cnt},
        "n_valid":      len(df_valid),
    }

    # ── Exchange stats ─────────────────────────────────────────────────────────
    ex_total = len(ex_valid)
    answered_mask = ex_valid["answered"].astype(str).str.lower().isin(["true", "1", "yes"])
    n_answered = answered_mask.sum()
    pct_answered = n_answered / max(ex_total, 1) * 100

    hostile_acc = ex_valid["question_intent"].isin(["hostile", "accountability"])
    n_ha = hostile_acc.sum()
    ha_answered = (hostile_acc & answered_mask).sum()

    intent_counts  = ex_valid["question_intent"].value_counts()
    quality_counts = ex_valid["answer_quality"].value_counts()

    ex_stats = {
        "total":                    ex_total,
        "answered":                 n_answered,
        "pct_answered":             pct_answered,
        "n_hostile_accountability": n_ha,
        "hostile_accountability_answered": ha_answered,
        "top_intent":  intent_counts.idxmax() if not intent_counts.empty else "N/A",
        "top_quality": quality_counts.idxmax() if not quality_counts.empty else "N/A",
    }

    print("Row stats:", row_stats)
    print("Exchange stats:", ex_stats)

    # ── Charts ─────────────────────────────────────────────────────────────────
    print("Generating charts…", flush=True)
    charts = {}

    charts["year_framing"]       = chart_year_framing(df_valid, cfg)
    print("  A: year×framing done")
    charts["framing_party"]      = chart_framing_by_party(df_valid, cfg)
    print("  B: framing×party done")
    charts["ingov_framing"]      = chart_ingov_framing(df_valid, cfg)
    print("  C: gov vs opp done")
    charts["rhetorical_function"] = chart_rhetorical_function(df_valid, cfg)
    print("  D: rhetorical function done")
    charts["policy_domain"]      = chart_policy_domain(df_valid)
    print("  E: policy domain done")
    charts["valence_time"]       = chart_valence_time(df_valid, cfg)
    print("  F: valence over time done")
    charts["secondary"]          = chart_secondary(df_valid, cfg)
    print("  T: technology context done")

    charts["answered_time"]      = chart_exchange_answered_time(ex_valid, cfg)
    print("  X1: answered over time done")
    charts["question_intent"]    = chart_question_intent(ex_valid, cfg)
    print("  X2: question intent done")
    charts["answer_quality"]     = chart_answer_quality(ex_valid, cfg)
    print("  X3: answer quality done")
    charts["intent_vs_quality"]  = chart_intent_vs_quality(ex_valid, cfg)
    print("  X4: intent vs quality done")
    charts["evasion"]            = chart_evasion_technique(ex_valid, cfg)
    print("  X5: evasion techniques done")

    speakers_table  = build_top_speakers_table(df_valid, cfg)
    print("  speakers table done")
    exchange_table  = build_exchange_table(ex_valid, cfg)
    print("  exchange table done")

    # ── LLM narrative ──────────────────────────────────────────────────────────
    print(f"Requesting narrative from {args.model}…", flush=True)
    narrative, in_toks, out_toks, cost = get_llm_narrative(df_valid, ex_valid, cfg, args.model)

    # ── Assemble HTML ──────────────────────────────────────────────────────────
    print("Assembling HTML…", flush=True)
    html = build_html(row_stats, ex_stats, charts,
                      speakers_table, exchange_table, narrative, cfg)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    size_kb = out_path.stat().st_size / 1024
    print(f"\nReport → {out_path}  ({size_kb:.0f} KB)")
    if in_toks:
        print(f"API usage: {in_toks:,} in + {out_toks:,} out ≈ ${cost:.3f}")


if __name__ == "__main__":
    main()
