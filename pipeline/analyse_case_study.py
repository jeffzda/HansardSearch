#!/usr/bin/env python3
"""
analyse_case_study.py
Analyses an enriched parliamentary case study CSV and produces a self-contained HTML report.
"""

import argparse
import base64
import datetime
import io
import re
import sys
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

# ── Colour palettes ──────────────────────────────────────────────────────────
FRAMING_COLOURS = {
    "scientific_authority":   "#2196F3",
    "policy_ally":            "#4CAF50",
    "neutral_reference":      "#9E9E9E",
    "lobby_group":            "#FF9800",
    "activist_adversary":     "#F44336",
    "hypocrite_self_serving": "#B71C1C",
    "authority_endorsement":  "#03A9F4",
}
PARTY_COLOURS = {
    "Coalition":  "#003087",
    "Labor":      "#E3231F",
    "Greens":     "#009B55",
    "Nationals":  "#006633",
    "Other":      "#888888",
}
VALENCE_COLOURS = {
    "positive":     "#4CAF50",
    "neutral":      "#9E9E9E",
    "negative":     "#F44336",
    "instrumental": "#FF9800",
}
WWF_PRIMARY = "#1a1a1a"
WWF_ACCENT  = "#FF6200"

FRAMING_ORDER = [
    "scientific_authority", "policy_ally", "neutral_reference",
    "lobby_group", "activist_adversary", "hypocrite_self_serving",
    "authority_endorsement",
]
VALENCE_ORDER  = ["positive", "neutral", "negative", "instrumental"]
PARTY_ORDER    = ["Coalition", "Labor", "Greens", "Nationals", "Other"]

ARENA_FRAMING_COLOURS = {
    "essential_public_institution": "#2196F3",
    "policy_instrument":            "#4CAF50",
    "innovation_enabler":           "#00BCD4",
    "bureaucratic_waste":           "#FF9800",
    "political_target":             "#F44336",
    "neutral_reference":            "#9E9E9E",
    "discredited_or_captured":      "#B71C1C",
}
ARENA_FRAMING_ORDER = [
    "essential_public_institution", "policy_instrument", "innovation_enabler",
    "neutral_reference", "bureaucratic_waste", "political_target", "discredited_or_captured",
]
ARENA_VALENCE_ORDER = ["positive", "neutral", "negative", "mixed"]
ARENA_VALENCE_COLOURS = {
    "positive": "#4CAF50",
    "neutral":  "#9E9E9E",
    "negative": "#F44336",
    "mixed":    "#FF9800",
}
ARENA_PRIMARY = "#1a1a1a"
ARENA_ACCENT  = "#2196F3"

GRDC_FRAMING_COLOURS = {
    "public_research_institution": "#2196F3",
    "policy_instrument":           "#4CAF50",
    "sector_exemplar":             "#00BCD4",
    "accountability_target":       "#FF9800",
    "governance_failure":          "#F44336",
    "neutral_reference":           "#9E9E9E",
}
GRDC_FRAMING_ORDER = [
    "public_research_institution", "policy_instrument", "sector_exemplar",
    "neutral_reference", "accountability_target", "governance_failure",
]
GRDC_VALENCE_ORDER = ["positive", "neutral", "negative", "instrumental"]
GRDC_VALENCE_COLOURS = {
    "positive":     "#4CAF50",
    "neutral":      "#9E9E9E",
    "negative":     "#F44336",
    "instrumental": "#FF9800",
}
GRDC_PRIMARY = "#1a1a1a"
GRDC_ACCENT  = "#4CAF50"

# Global config dict — populated in main() from --subject
CFG: dict = {}


# ── Parliament number mapping ─────────────────────────────────────────────────
def year_to_parliament(year: int) -> str:
    # Election dates → parliament start
    if year <= 1999:
        return "39th"
    elif year <= 2001:
        return "40th"
    elif year <= 2004:
        return "41st"
    elif year <= 2007:
        return "42nd"
    elif year <= 2010:
        return "43rd"
    elif year <= 2013:
        return "44th"
    elif year <= 2016:
        return "45th"
    elif year <= 2019:
        return "46th"
    elif year <= 2022:
        return "47th"
    else:
        return "48th"


def map_party_group(party: str) -> str:
    if pd.isna(party):
        return "Other"
    party = str(party).strip()
    if party in ("LP", "LNP", "CLP"):
        return "Coalition"
    if party == "ALP":
        return "Labor"
    if party == "AG":
        return "Greens"
    if party in ("NP", "NATS"):
        return "Nationals"
    return "Other"


# ── Chart helpers ─────────────────────────────────────────────────────────────
def fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    buf.seek(0)
    data = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return data


def apply_clean_style():
    plt.rcParams.update({
        "axes.spines.top":    False,
        "axes.spines.right":  False,
        "axes.grid":          True,
        "axes.grid.axis":     "x",
        "grid.alpha":         0.3,
        "font.family":        "sans-serif",
        "font.size":          11,
    })


# ── Chart A: Year series stacked bar by framing ───────────────────────────────
def chart_year_framing(df_valid: pd.DataFrame):
    apply_clean_style()
    years = sorted(df_valid["year"].dropna().unique())
    framing_col = CFG["framing_col"]
    framing_order = CFG["framing_order"]
    framing_colours = CFG["framing_colours"]
    framings = [f for f in framing_order if f in df_valid[framing_col].values]

    data = {}
    for f in framings:
        data[f] = [df_valid[(df_valid["year"] == y) & (df_valid[framing_col] == f)].shape[0]
                   for y in years]

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom = np.zeros(len(years))
    for f in framings:
        vals = np.array(data[f], dtype=float)
        ax.bar(years, vals, bottom=bottom, color=framing_colours[f],
               label=f.replace("_", " ").title(), width=0.7)
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Mentions")
    ax.set_title(f"{CFG['subject_name']} Mentions per Year by Framing Type", fontsize=13, fontweight="bold")
    ax.set_xticks(years)
    ax.set_xticklabels([str(y) for y in years], rotation=45, ha="right", fontsize=9)
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Chart B: Framing by party group horizontal bar ───────────────────────────
def chart_framing_by_party(df_valid: pd.DataFrame):
    apply_clean_style()
    plt.rcParams["axes.grid.axis"] = "x"
    framing_col = CFG["framing_col"]
    framings = [f for f in CFG["framing_order"] if f in df_valid[framing_col].values]
    parties  = [p for p in PARTY_ORDER if p in df_valid["party_group"].values]

    # Build matrix: rows=framing, cols=party
    matrix = pd.crosstab(df_valid[framing_col], df_valid["party_group"]).reindex(
        index=framings, columns=parties, fill_value=0
    )

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
    ax.set_xlabel("Number of mentions")
    ax.set_title("Framing Type by Party Group", fontsize=13, fontweight="bold")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Chart C: In-government framing comparison ────────────────────────────────
def chart_ingov_framing(df_valid: pd.DataFrame):
    apply_clean_style()
    framing_col = CFG["framing_col"]
    framing_colours = CFG["framing_colours"]
    framings = [f for f in CFG["framing_order"] if f in df_valid[framing_col].values]

    in_gov_counts  = df_valid[df_valid["in_gov"] == 1][framing_col].value_counts()
    out_gov_counts = df_valid[df_valid["in_gov"] == 0][framing_col].value_counts()

    # Normalise to %
    in_gov_pct  = (in_gov_counts.reindex(framings, fill_value=0) /
                   in_gov_counts.sum() * 100)
    out_gov_pct = (out_gov_counts.reindex(framings, fill_value=0) /
                   out_gov_counts.sum() * 100)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
    for ax, pct, title in zip(axes, [in_gov_pct, out_gov_pct],
                              ["In Government", "In Opposition"]):
        colors = [framing_colours[f] for f in framings]
        ax.barh([f.replace("_", " ").title() for f in framings],
                pct.values, color=colors)
        ax.set_xlabel("% of mentions")
        ax.set_title(title, fontsize=12, fontweight="bold")
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)

    fig.suptitle("Framing Distribution: Government vs Opposition Members",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Chart D: Rhetorical function bar ─────────────────────────────────────────
def chart_rhetorical_function(df_valid: pd.DataFrame):
    apply_clean_style()
    plt.rcParams["axes.grid.axis"] = "x"
    counts = df_valid["rhetorical_function"].value_counts().sort_values()
    fig, ax = plt.subplots(figsize=(12, 5))
    colors = [CFG["accent_colour"] if v == counts.idxmax() else "#555555" for v in counts.values]
    ax.barh([v.replace("_", " ").title() for v in counts.index], counts.values,
            color=colors)
    ax.set_xlabel("Number of mentions")
    ax.set_title("Rhetorical Function", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Chart E: Policy domain pie ────────────────────────────────────────────────
def chart_policy_domain(df_valid: pd.DataFrame):
    counts = df_valid["policy_domain"].value_counts()
    fig, ax = plt.subplots(figsize=(8, 6))
    labels = [v.replace("_", " ").title() for v in counts.index]
    colors_cycle = ["#2196F3", "#4CAF50", "#FF9800", "#9E9E9E",
                    "#F44336", "#03A9F4", "#B71C1C", "#006633"][:len(counts)]
    wedges, texts, autotexts = ax.pie(
        counts.values, labels=labels, autopct="%1.1f%%",
        colors=colors_cycle, startangle=140,
        pctdistance=0.8, textprops={"fontsize": 9}
    )
    for t in autotexts:
        t.set_fontsize(8)
    ax.set_title("Policy Domain Distribution", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Chart F: Valence over time stacked area (5-yr rolling) ───────────────────
def chart_valence_over_time(df_valid: pd.DataFrame):
    apply_clean_style()
    valence_col = CFG["valence_col"]
    valence_order = CFG["valence_order"]
    valence_colours = CFG["valence_colours"]
    valences = [v for v in valence_order if v in df_valid[valence_col].values]
    years    = sorted(df_valid["year"].dropna().unique())

    # Annual counts per valence
    year_val = pd.crosstab(df_valid["year"], df_valid[valence_col]).reindex(
        index=years, columns=valences, fill_value=0
    ).astype(float)

    # 5-year rolling mean
    rolled = year_val.rolling(5, center=True, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom = np.zeros(len(years))
    for v in valences:
        vals = rolled[v].values
        ax.fill_between(years, bottom, bottom + vals,
                        color=valence_colours[v], alpha=0.75,
                        label=v.title())
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Mentions (5-year rolling mean)")
    ax.set_title(f"Speaker Valence Toward {CFG['subject_name']} Over Time (5-year rolling)", fontsize=13, fontweight="bold")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Chart G: Top 20 speakers table (returned as HTML, no chart) ───────────────
def build_top_speakers_table(df_valid: pd.DataFrame) -> str:
    # Exclude rows where speaker_name looks like a stage direction
    df_valid = df_valid[~df_valid["speaker_name"].fillna("").str.match(
        r"(?i)^stage direction$|^the (president|speaker|chair|deputy|acting|clerk)",
        na=False,
    )]
    grp = df_valid.groupby("speaker_name")
    counts = grp.size().rename("count")
    party  = grp["party_group"].agg(lambda x: x.mode()[0] if len(x) else "")
    def ingov_label(x):
        g = int((x == 1).sum())
        o = int((x == 0).sum())
        if g > 0 and o > 0:
            return f"Both ({g}g/{o}o)"
        return "Gov" if g > 0 else "Opp"

    ingov  = grp["in_gov"].agg(ingov_label)
    dom_framing = grp[CFG["framing_col"]].agg(
        lambda x: x.dropna().mode()[0] if len(x.dropna()) else "")
    dom_valence = grp[CFG["valence_col"]].agg(
        lambda x: x.dropna().mode()[0] if len(x.dropna()) else "")

    tbl = pd.DataFrame({
        "Speaker":    counts.index,
        "Party Group": party.values,
        "In Gov":     ingov.values,
        "Mentions":   counts.values,
        "Dominant Framing": [v.replace("_", " ").title() for v in dom_framing.values],
        "Dominant Valence": [v.title() if isinstance(v, str) else v for v in dom_valence.values],
    }).sort_values("Mentions", ascending=False).head(20)

    rows = ""
    for _, r in tbl.iterrows():
        # Colour framing cell
        framing_key = r["Dominant Framing"].lower().replace(" ", "_")
        fc = CFG["framing_colours"].get(framing_key, "#eee")
        valence_key = r["Dominant Valence"].lower()
        vc = CFG["valence_colours"].get(valence_key, "#eee")
        rows += f"""
        <tr>
          <td>{r['Speaker']}</td>
          <td><span class="party-badge" style="background:{PARTY_COLOURS.get(r['Party Group'],'#888')}">{r['Party Group']}</span></td>
          <td>{r['In Gov']}</td>
          <td style="text-align:center;font-weight:bold">{r['Mentions']}</td>
          <td><span class="badge" style="background:{fc}">{r['Dominant Framing']}</span></td>
          <td><span class="badge" style="background:{vc}">{r['Dominant Valence']}</span></td>
        </tr>"""

    return f"""
    <table class="data-table">
      <thead>
        <tr>
          <th>Speaker</th><th>Party Group</th><th>Gov Status</th>
          <th>Mentions</th><th>Dominant Framing</th><th>Dominant Valence</th>
        </tr>
      </thead>
      <tbody>{rows}
      </tbody>
    </table>"""


# ── Chart H: Name-form evolution stacked area ─────────────────────────────────
def chart_nameform_evolution(df: pd.DataFrame):
    if not CFG.get("has_nameform"):
        return None
    nameform_col = CFG["secondary_col"]
    apply_clean_style()
    sub = df.dropna(subset=[nameform_col, "year"]).copy()
    sub["name_form_clean"] = sub[nameform_col].apply(
        lambda x: x if x in ["GRDC", "Grains Research and Development Corporation",
                              "WWF", "World Wildlife Fund", "World Wide Fund for Nature",
                              "mixed", "WWF Australia", "WWF-Australia",
                              "World Wildlife Fund for Nature"] else "Other"
    )

    years = sorted(sub["year"].dropna().unique())
    forms = sub["name_form_clean"].value_counts().index.tolist()

    cross = pd.crosstab(sub["year"], sub["name_form_clean"]).reindex(
        index=years, columns=forms, fill_value=0
    ).astype(float)

    rolled = cross.rolling(3, center=True, min_periods=1).mean()

    form_colors = {
        "GRDC":                                    "#1a1a1a",
        "Grains Research and Development Corporation": "#4CAF50",
        "mixed":                                   "#9E9E9E",
        "WWF":                                     "#1a1a1a",
        "World Wildlife Fund":                     "#FF6200",
        "World Wide Fund for Nature":              "#2196F3",
        "WWF Australia":                           "#4CAF50",
        "WWF-Australia":                           "#006633",
        "World Wildlife Fund for Nature":          "#FF9800",
        "Other":                                   "#888888",
    }

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom = np.zeros(len(years))
    for f in forms:
        vals = rolled[f].values
        ax.fill_between(years, bottom, bottom + vals,
                        color=form_colors.get(f, "#888"), alpha=0.75, label=f)
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Mentions (3-year rolling mean)")
    ax.set_title("Name Form Evolution Over Time", fontsize=13, fontweight="bold")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_secondary_evolution(df: pd.DataFrame):
    """For ARENA: stacked bar of speech_act_type by year."""
    apply_clean_style()
    col = CFG.get("secondary_col", "speech_act_type")
    sub = df.dropna(subset=[col, "year"])
    years = sorted(sub["year"].dropna().unique())
    cats = sub[col].value_counts().head(6).index.tolist()
    colors_cycle = ["#2196F3","#4CAF50","#FF9800","#9E9E9E","#F44336","#00BCD4"]

    cross = pd.crosstab(sub["year"], sub[col]).reindex(
        index=years, columns=cats, fill_value=0
    ).astype(float)
    rolled = cross.rolling(3, center=True, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom = np.zeros(len(years))
    for i, c in enumerate(cats):
        vals = rolled[c].values
        ax.fill_between(years, bottom, bottom + vals,
                        color=colors_cycle[i % len(colors_cycle)], alpha=0.75,
                        label=c.replace("_", " ").title())
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Mentions (3-year rolling mean)")
    ax.set_title(f"Speech Act Type Over Time ({CFG['subject_name']} mentions)", fontsize=13, fontweight="bold")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── LLM narrative ─────────────────────────────────────────────────────────────
def get_llm_narrative(df_valid: pd.DataFrame, model: str, grounded: bool = False,
                      topic: str | None = None) -> str:
    try:
        import anthropic
        client = anthropic.Anthropic()

        # Build compact table
        summary_col = CFG.get("summary_col", "what_happened")
        cols = ["year", "chamber", "party", "in_gov", CFG["framing_col"],
                CFG["valence_col"], "rhetorical_function",
                "policy_domain", summary_col]
        sub = df_valid[cols].copy()
        sub[summary_col] = sub[summary_col].fillna("").str[:200]

        # CSV-like text
        table_text = sub.to_csv(index=False)

        subject_name = CFG["subject_name"]
        subject_full = CFG["subject_full"]

        source_note = CFG.get("source_note", "")
        source_note_block = f"\n\n{source_note}" if source_note else ""

        if grounded:
            system_prompt = textwrap.dedent(f"""
                You are a data analyst. You will analyse a dataset of parliamentary mentions of
                {subject_full} drawn from Australian Hansard
                (1998–2025). Produce a structured analytical report in clean HTML (no markdown,
                no code fences, no backticks) using the exact section headings specified. Write for
                a senior {subject_name} government relations director — analytical, precise, strictly
                grounded in the data provided.{source_note_block}

                STRICT GROUNDING RULES — you MUST follow these without exception:
                - Every claim must be directly supported by patterns visible in the CSV data.
                - Do NOT use your training knowledge of Australian political history, elections,
                  party leaders, policy events, or external context of any kind.
                - Do NOT name specific legislation, elections, political figures, or events that are
                  not mentioned in the summary column of the data.
                - If you notice a trend, attribute it to what the data shows (e.g. "the data shows
                  an increase in negative framing from 2013") — do not explain it using external
                  political context.
                - Treat the dataset as your sole source of truth.

                Do not include <html>, <head>, or <body> tags — only section content beginning
                with the first <h2>.
            """).strip()
        else:
            system_prompt = textwrap.dedent(f"""
                You are an expert political analyst specialising in Australian parliamentary politics
                and NGO government relations. You will analyse a dataset of parliamentary mentions of
                {subject_full} drawn from Australian Hansard
                (1998–2025). Produce a structured analytical report in clean HTML (no markdown,
                no code fences, no backticks) using the exact section headings specified. Write for
                a senior {subject_name} government relations director — analytical, precise, grounded
                in the data, with specific examples.{source_note_block} Do not include <html>, <head>,
                or <body> tags — only the section content beginning with the first <h2>.
            """).strip()

        if topic:
            date_range = f"{df_valid['date'].min()} to {df_valid['date'].max()}" \
                if hasattr(df_valid['date'].min(), 'strftime') else "2024–2025"
            user_prompt = f"""Below is a CSV of all parliamentary speech turns mentioning {subject_name}
in the context of {topic}, drawn from Australian Hansard ({date_range}). Each row is one speech turn.

{table_text}

Produce the following sections in HTML:

<h2>Executive Summary</h2>
Three paragraphs summarising the parliamentary debate around {topic} and {subject_name}'s role:
the volume and distribution of mentions, dominant framings, which parties were advocates vs critics.

<h2>The Legislative Debate</h2>
A detailed chronological narrative of how {subject_name} featured in the {topic} debate:
what role the government assigned to {subject_name}, how the opposition framed its criticism,
and what the crossbench/Greens contributed. Use specific examples from the what_happened column.

<h2>Party Positions</h2>
How each party group (Labor, Coalition, Greens, others) used {subject_name} in this debate,
with specific quoted examples from the data illustrating each party's distinctive argument.

<h2>The Opposition Attack Lines</h2>
Detailed analysis of the negative framing — what specific criticisms were made, by whom,
and what rhetorical functions they served (opponent_attack, policy_critique, etc.).

<h2>Strategic Implications for {subject_name}</h2>
Four to five concrete implications for {subject_name}'s government relations and communications
strategy arising from this legislative episode. Use a numbered list (<ol>).
"""
        else:
            _src = "committee hearings (Senate, House and Joint committees, and Senate Estimates)" \
                if CFG.get("source_note") else "Australian Hansard from 1998 to 2025"
            user_prompt = f"""Below is a CSV of all non-false-positive mentions of {subject_name}
in {_src}. Each row is one speech turn or witness statement.

{table_text}

Produce the following sections in HTML:

<h2>Executive Summary</h2>
Three paragraphs for a {subject_name} government relations director summarising the overall picture:
volume trends, dominant framings, which committees and party groups are most active,
and the balance between member scrutiny and witness/official testimony.

<h2>The Committee Arc</h2>
Chronological narrative identifying how the pattern of {subject_name} mentions in committee hearings
has evolved — which committees drove early attention, how accountability scrutiny compares to
routine reference, and what the data suggests about the organisation's relationship with parliament.

<h2>Scrutiny vs Advocacy: Member Framing Patterns</h2>
How committee members (by party group) frame {subject_name} — who presses accountability questions,
who cites it positively as a policy instrument, with specific examples from the summary column.

<h2>The Accountability Record</h2>
Analysis of accountability_target and governance_failure framings — what the hearing record shows
about parliamentary scrutiny of {subject_name}, which committees drove it, and what it reveals.

<h2>Strategic Implications</h2>
Four to five concrete implications for {subject_name}'s government relations strategy arising
from the committee hearing record. Use a numbered list (<ol>).
"""

        print("  Calling Claude API for narrative...", flush=True)
        response = client.messages.create(
            model=model,
            max_tokens=8192,
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
        )

        usage = response.usage
        input_tokens  = usage.input_tokens
        output_tokens = usage.output_tokens

        # Approximate cost for claude-opus-4-6:
        # Input: $15/M tokens, Output: $75/M tokens (as of 2025 pricing)
        cost_usd = (input_tokens / 1_000_000 * 15) + (output_tokens / 1_000_000 * 75)
        print(f"  API call complete. Input tokens: {input_tokens:,}, "
              f"Output tokens: {output_tokens:,}, "
              f"Estimated cost: ${cost_usd:.4f}", flush=True)

        return response.content[0].text, input_tokens, output_tokens, cost_usd

    except Exception as e:
        print(f"  WARNING: LLM narrative failed: {e}", flush=True)
        placeholder = f"""
        <h2>Executive Summary</h2>
        <p><em>[Narrative generation failed: {str(e)[:200]}]</em></p>
        <h2>The Parliamentary Arc, 1998–2025</h2><p><em>Not available.</em></p>
        <h2>Party Dynamics</h2><p><em>Not available.</em></p>
        <h2>The Negative Framing Pattern</h2><p><em>Not available.</em></p>
        <h2>Strategic Implications</h2><p><em>Not available.</em></p>
        """
        return placeholder, 0, 0, 0.0


# ── HTML assembly ─────────────────────────────────────────────────────────────
def build_html(stats: dict, charts: dict, speakers_table: str,
               narrative: str, fp_count: int, n_valid: int) -> str:

    today = datetime.date.today().strftime("%d %B %Y")

    def stat_box(label, value):
        return f"""
        <div class="stat-box">
          <div class="stat-value">{value}</div>
          <div class="stat-label">{label}</div>
        </div>"""

    stat_boxes = "".join([
        stat_box("Total matches", stats["total"]),
        stat_box("Senate / House", f"{stats['senate']} / {stats['house']}"),
        stat_box("Date range", stats["date_range"]),
        stat_box("False positives excluded", stats["fp_count"]),
        stat_box("Positive / Negative valence",
                 f"{stats['pct_positive']:.0f}% / {stats['pct_negative']:.0f}%"),
        stat_box("Top framing", stats["top_framing"].replace("_", " ").title()),
    ])

    def section(title, chart_b64, caption, extra_html="", chart_id=""):
        img_tag = (f'<img src="data:image/png;base64,{chart_b64}" '
                   f'alt="{title}" style="max-width:100%;height:auto;">')
        return f"""
        <section class="chart-section" id="{chart_id}">
          <h3>{title}</h3>
          <div class="chart-wrap">{img_tag}</div>
          <p class="caption">{caption}</p>
          {extra_html}
        </section>"""

    primary_colour = CFG["primary_colour"]
    accent_colour  = CFG["accent_colour"]
    subject_name   = CFG["subject_name"]

    # ── Captions ──────────────────────────────────────────────────────────────
    top_year = stats["top_year"]
    cap_a = (f"Peak mention year was {top_year['year']} ({top_year['count']} mentions). "
             f"Framing distribution across all years shown above.")

    coal_neg = stats["coalition_neg_pct"]
    cap_b = (f"Coalition members account for {coal_neg:.0f}% of negative framings combined.")

    cap_c = (f"Government members show a markedly different framing profile from opposition "
             f"members.")

    top_rf = stats["top_rhetorical_function"].replace("_", " ").title()
    cap_d  = f"The most common rhetorical function is {top_rf}, reflecting {subject_name}'s role as an evidential source."

    top_pd = stats["top_policy_domain"].replace("_", " ").title()
    cap_e  = f"{top_pd} accounts for the largest share of {subject_name} mentions."

    cap_f = ("Positive valence has generally grown over time, while negative framing "
             "shows episodic spikes tied to specific political controversies.")

    cap_g = "Top 20 speakers by total mentions, with dominant framing and valence."

    cap_h = (f"Secondary evolution of {subject_name} mentions over time (3-year rolling mean).")

    # ── Narrative block ───────────────────────────────────────────────────────
    narrative_html = f"""
    <section class="narrative-section">
      <div class="narrative-content">
        {narrative}
      </div>
    </section>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{subject_name} in Australian Parliament: Hansard Analysis 1998–2025</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    background: #ffffff;
    color: {primary_colour};
    line-height: 1.6;
  }}
  header {{
    background: {primary_colour};
    color: #fff;
    padding: 2.5rem 3rem;
    border-bottom: 4px solid {accent_colour};
  }}
  header h1 {{
    font-size: 1.8rem;
    font-weight: 700;
    letter-spacing: -0.02em;
  }}
  header p {{
    margin-top: 0.5rem;
    color: #ccc;
    font-size: 0.95rem;
  }}
  .container {{
    max-width: 1100px;
    margin: 0 auto;
    padding: 2rem 2rem 4rem;
  }}
  /* At-a-glance boxes */
  .stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 1rem;
    margin: 2rem 0;
  }}
  .stat-box {{
    background: #f5f5f5;
    border-left: 4px solid {accent_colour};
    padding: 1rem 1.25rem;
    border-radius: 4px;
  }}
  .stat-value {{
    font-size: 1.6rem;
    font-weight: 700;
    color: {primary_colour};
    line-height: 1.2;
  }}
  .stat-label {{
    font-size: 0.78rem;
    color: #666;
    margin-top: 0.25rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }}
  /* Chart sections */
  .chart-section {{
    background: #f5f5f5;
    border-radius: 8px;
    padding: 1.5rem 2rem;
    margin: 1.5rem 0;
  }}
  .chart-section h3 {{
    font-size: 1.1rem;
    font-weight: 600;
    margin-bottom: 1rem;
    color: {primary_colour};
    border-bottom: 2px solid {accent_colour};
    padding-bottom: 0.4rem;
  }}
  .chart-wrap {{
    text-align: center;
    margin-bottom: 0.75rem;
  }}
  .caption {{
    font-size: 0.85rem;
    color: #555;
    font-style: italic;
  }}
  /* Narrative */
  .narrative-section {{
    background: #fff;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 2rem 2.5rem;
    margin: 2rem 0;
  }}
  .narrative-content h2 {{
    font-size: 1.25rem;
    font-weight: 700;
    color: {primary_colour};
    border-bottom: 2px solid {accent_colour};
    padding-bottom: 0.4rem;
    margin: 1.75rem 0 0.75rem;
  }}
  .narrative-content h2:first-child {{
    margin-top: 0;
  }}
  .narrative-content p {{
    margin-bottom: 0.9rem;
    color: #333;
    line-height: 1.7;
  }}
  .narrative-content ol, .narrative-content ul {{
    margin: 0.5rem 0 0.9rem 1.5rem;
    color: #333;
  }}
  .narrative-content li {{
    margin-bottom: 0.4rem;
  }}
  /* Table */
  .data-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.83rem;
    margin-top: 1rem;
  }}
  .data-table th {{
    background: {primary_colour};
    color: #fff;
    padding: 0.5rem 0.75rem;
    text-align: left;
    font-weight: 600;
  }}
  .data-table td {{
    padding: 0.45rem 0.75rem;
    border-bottom: 1px solid #e0e0e0;
  }}
  .data-table tr:nth-child(even) {{ background: #f9f9f9; }}
  .data-table tr:hover {{ background: #fff3e0; }}
  .badge {{
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    font-size: 0.75rem;
    color: #fff;
    font-weight: 500;
  }}
  .party-badge {{
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    font-size: 0.75rem;
    color: #fff;
    font-weight: 600;
  }}
  /* Section headings */
  .section-heading {{
    font-size: 1.4rem;
    font-weight: 700;
    color: {primary_colour};
    margin: 2.5rem 0 1rem;
    padding-left: 0.75rem;
    border-left: 4px solid {accent_colour};
  }}
  footer {{
    background: #f5f5f5;
    border-top: 1px solid #ddd;
    padding: 1.25rem 3rem;
    font-size: 0.82rem;
    color: #666;
  }}
  @media (max-width: 600px) {{
    .stats-grid {{ grid-template-columns: 1fr 1fr; }}
    .stat-value {{ font-size: 1.2rem; }}
  }}
</style>
</head>
<body>
<header>
  <h1>{subject_name} in Australian Parliament</h1>
  <p>Hansard Analysis 1998–2025 &nbsp;·&nbsp; Senate &amp; House of Representatives</p>
</header>
<div class="container">

  <h2 class="section-heading">At a Glance</h2>
  <div class="stats-grid">
    {stat_boxes}
  </div>

  <h2 class="section-heading">Analytical Narrative</h2>
  {narrative_html}

  <h2 class="section-heading">Statistical Analysis</h2>

  {section("Mentions Per Year by Framing Type", charts["year_framing"], cap_a, chart_id="chart-a")}
  {section("Framing Type by Party Group", charts["framing_party"], cap_b, chart_id="chart-b")}
  {section("Framing Distribution: Government vs Opposition", charts["ingov_framing"], cap_c, chart_id="chart-c")}
  {section("Rhetorical Function", charts["rhetorical_function"], cap_d, chart_id="chart-d")}
  {section("Policy Domain Distribution", charts["policy_domain"], cap_e, chart_id="chart-e")}
  {section("Speaker Valence Over Time (5-year rolling)", charts["valence_time"], cap_f, chart_id="chart-f")}
  {section("Top 20 Speakers", "", cap_g, extra_html=speakers_table, chart_id="chart-g")}
  {section("Secondary Evolution Over Time (3-year rolling)", charts["secondary"], cap_h, chart_id="chart-h")}

</div>
<footer>
  Analysis covers Australian Senate and House of Representatives Hansard, 1998–2025.
  N={stats['total']} matched speech turns (N={n_valid} after false positive exclusion).
  Generated {today}.
</footer>
</body>
</html>"""

    return html


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Analyse an enriched parliamentary case study CSV and produce HTML report."
    )
    parser.add_argument("--enriched", required=True, help="Path to matches_enriched.csv")
    parser.add_argument("--out",      required=True, help="Output HTML path")
    parser.add_argument("--model",    default="claude-opus-4-6",
                        help="Claude model for narrative (default: claude-opus-4-6)")
    parser.add_argument("--grounded", action="store_true",
                        help="Use a strictly data-grounded narrative prompt (no external political context).")
    parser.add_argument("--subject", default="WWF", choices=["WWF", "ARENA", "GRDC"],
                        help="Case study subject")
    parser.add_argument("--narrative-topic", default=None,
                        help="Focus the LLM narrative on a specific legislative episode "
                             "(e.g. 'the Future Made in Australia Bill 2024'). "
                             "When set, the prompt sections are rewritten for a topic-specific report.")
    args = parser.parse_args()

    global CFG
    if args.subject == "GRDC":
        CFG = {
            "subject_name":     "GRDC",
            "subject_full":     "Grains Research and Development Corporation (GRDC)",
            "source_note":      (
                "IMPORTANT CONTEXT: This dataset is drawn exclusively from Australian "
                "parliamentary COMMITTEE Hansard (2010–2025) — Senate, House and Joint "
                "committee hearings and Senate Estimates — NOT from chamber floor debates. "
                "Speakers are a mix of committee members (senators and members) and "
                "witnesses (industry representatives, officials, researchers). The "
                "'party' and 'in_gov' columns reflect committee members only; witness "
                "rows have no party. The 'chamber' column is 'committee' for all rows. "
                "Framing patterns, accountability dynamics, and party positions should "
                "be interpreted in the context of committee hearings, not floor speeches."
            ),
            "framing_col":      "org_framing",
            "valence_col":      "speaker_valence",
            "summary_col":      "summary",
            "framing_order":    GRDC_FRAMING_ORDER,
            "framing_colours":  GRDC_FRAMING_COLOURS,
            "valence_order":    GRDC_VALENCE_ORDER,
            "valence_colours":  GRDC_VALENCE_COLOURS,
            "primary_colour":   GRDC_PRIMARY,
            "accent_colour":    GRDC_ACCENT,
            "negative_framings": ["accountability_target", "governance_failure"],
            "fp_pattern":       r"false positive|stage direction|formatting artefact",
            "has_nameform":     True,
            "secondary_col":    "org_name_form",
        }
    elif args.subject == "ARENA":
        CFG = {
            "subject_name":     "ARENA",
            "subject_full":     "Australian Renewable Energy Agency (ARENA)",
            "framing_col":      "arena_framing",
            "valence_col":      "mention_valence",
            "summary_col":      "what_happened",
            "framing_order":    ARENA_FRAMING_ORDER,
            "framing_colours":  ARENA_FRAMING_COLOURS,
            "valence_order":    ARENA_VALENCE_ORDER,
            "valence_colours":  ARENA_VALENCE_COLOURS,
            "primary_colour":   ARENA_PRIMARY,
            "accent_colour":    ARENA_ACCENT,
            "negative_framings": ["bureaucratic_waste", "political_target", "discredited_or_captured"],
            "fp_pattern":       r"false positive|stage direction|formatting artefact",
            "has_nameform":     False,
            "secondary_col":    "speech_act_type",
        }
    else:  # WWF default
        CFG = {
            "subject_name":     "WWF",
            "subject_full":     "World Wildlife Fund / WWF (World Wide Fund for Nature)",
            "framing_col":      "wwf_framing",
            "valence_col":      "speaker_valence_toward_wwf",
            "summary_col":      "what_happened",
            "framing_order":    FRAMING_ORDER,
            "framing_colours":  FRAMING_COLOURS,
            "valence_order":    VALENCE_ORDER,
            "valence_colours":  VALENCE_COLOURS,
            "primary_colour":   WWF_PRIMARY,
            "accent_colour":    WWF_ACCENT,
            "negative_framings": ["activist_adversary", "hypocrite_self_serving"],
            "fp_pattern":       r"Waterside Workers|false positive|WWF wrestler|formatting artefact|stage direction",
            "has_nameform":     True,
            "secondary_col":    "wwf_name_form",
        }

    print(f"Loading {args.enriched}...", flush=True)
    df = pd.read_csv(args.enriched)
    print(f"  {len(df)} rows loaded.", flush=True)

    # ── 1. Data preparation ───────────────────────────────────────────────────
    df["date"]  = pd.to_datetime(df["date"], errors="coerce")
    df["year"]  = df["date"].dt.year.astype("Int64")
    df["parliament"] = df["year"].apply(
        lambda y: year_to_parliament(int(y)) if pd.notna(y) else None
    )
    df["party_group"] = df["party"].apply(map_party_group)

    # Identify false positives
    _summary_col = CFG.get("summary_col", "what_happened")
    fp_mask = (
        df[CFG["framing_col"]].isna() &
        df[_summary_col].fillna("").str.contains(
            CFG["fp_pattern"],
            case=False, regex=True
        )
    )
    fp_count = fp_mask.sum()
    print(f"  False positives identified: {fp_count}", flush=True)

    df_valid = df[~fp_mask].copy()
    n_valid  = len(df_valid)
    print(f"  Valid rows for analysis: {n_valid}", flush=True)

    # ── 2. Key statistics ─────────────────────────────────────────────────────
    total   = len(df)
    senate  = (df["chamber"] == "senate").sum()
    house   = (df["chamber"] == "house").sum()

    dates = df["date"].dropna()
    date_range = f"{dates.min().strftime('%Y')}–{dates.max().strftime('%Y')}"

    # Valence %
    val_counts = df_valid[CFG["valence_col"]].value_counts()
    val_total  = val_counts.sum()
    pct_pos = val_counts.get("positive", 0) / val_total * 100 if val_total else 0
    pct_neg = val_counts.get("negative", 0) / val_total * 100 if val_total else 0

    top_framing = df_valid[CFG["framing_col"]].value_counts().idxmax() \
        if not df_valid[CFG["framing_col"]].dropna().empty else "N/A"
    top_domain  = df_valid["policy_domain"].value_counts().idxmax() \
        if not df_valid["policy_domain"].dropna().empty else "N/A"

    _stage_pat = r"(?i)^stage direction$|^the (president|speaker|chair|deputy|acting|clerk)"
    speaker_counts = df_valid[~df_valid["speaker_name"].fillna("").str.match(_stage_pat, na=False)]["speaker_name"].value_counts()
    top_speaker_name  = speaker_counts.idxmax() if len(speaker_counts) else "N/A"
    top_speaker_count = int(speaker_counts.max()) if len(speaker_counts) else 0

    # Peak year
    year_counts = df_valid["year"].value_counts()
    top_year_val  = int(year_counts.idxmax()) if len(year_counts) else 0
    top_year_cnt  = int(year_counts.max())    if len(year_counts) else 0

    # Coalition negative framing %
    neg_framings = CFG["negative_framings"]
    df_neg = df_valid[df_valid[CFG["framing_col"]].isin(neg_framings)]
    coal_neg_pct = (
        (df_neg["party_group"] == "Coalition").sum() / len(df_neg) * 100
        if len(df_neg) else 0
    )

    top_rf = df_valid["rhetorical_function"].value_counts().idxmax() \
        if not df_valid["rhetorical_function"].dropna().empty else "N/A"

    stats = {
        "total":         total,
        "senate":        senate,
        "house":         house,
        "date_range":    date_range,
        "fp_count":      fp_count,
        "pct_positive":  pct_pos,
        "pct_negative":  pct_neg,
        "top_framing":   top_framing,
        "top_policy_domain": top_domain,
        "top_speaker_name":  top_speaker_name,
        "top_speaker_count": top_speaker_count,
        "top_year":          {"year": top_year_val, "count": top_year_cnt},
        "coalition_neg_pct": coal_neg_pct,
        "top_rhetorical_function": top_rf,
    }

    print("Key statistics:")
    print(f"  Total={total}, Senate={senate}, House={house}")
    print(f"  FP excluded={fp_count}, Valid={n_valid}")
    print(f"  Positive valence={pct_pos:.1f}%, Negative={pct_neg:.1f}%")
    print(f"  Top framing: {top_framing}")
    print(f"  Top domain: {top_domain}")
    print(f"  Top speaker: {top_speaker_name} ({top_speaker_count} mentions)")
    print(f"  Coalition % of negative framings: {coal_neg_pct:.1f}%")
    print(f"  Framing breakdown:", df_valid[CFG["framing_col"]].value_counts().to_dict())
    print(f"  Party group breakdown:", df_valid["party_group"].value_counts().to_dict())

    # ── 3. Charts ─────────────────────────────────────────────────────────────
    print("Generating charts...", flush=True)
    charts = {}
    charts["year_framing"]       = chart_year_framing(df_valid)
    print("  chart A done", flush=True)
    charts["framing_party"]      = chart_framing_by_party(df_valid)
    print("  chart B done", flush=True)
    charts["ingov_framing"]      = chart_ingov_framing(df_valid)
    print("  chart C done", flush=True)
    charts["rhetorical_function"] = chart_rhetorical_function(df_valid)
    print("  chart D done", flush=True)
    charts["policy_domain"]      = chart_policy_domain(df_valid)
    print("  chart E done", flush=True)
    charts["valence_time"]       = chart_valence_over_time(df_valid)
    print("  chart F done", flush=True)
    if CFG["has_nameform"]:
        charts["secondary"] = chart_nameform_evolution(df)
    else:
        charts["secondary"] = chart_secondary_evolution(df)
    print("  chart H done", flush=True)

    speakers_table = build_top_speakers_table(df_valid)
    print("  speakers table done", flush=True)

    # ── 4. LLM narrative ──────────────────────────────────────────────────────
    topic_label = f"  [topic: {args.narrative_topic}]" if args.narrative_topic else ""
    print(f"Requesting LLM narrative from {args.model}{'  [grounded mode]' if args.grounded else ''}{topic_label}...", flush=True)
    narrative_html, in_toks, out_toks, cost = get_llm_narrative(
        df_valid, args.model, grounded=args.grounded, topic=args.narrative_topic)

    # ── 5. Assemble HTML ──────────────────────────────────────────────────────
    print("Assembling HTML report...", flush=True)
    html = build_html(stats, charts, speakers_table, narrative_html, fp_count, n_valid)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")

    size_kb = out_path.stat().st_size / 1024
    print(f"\nReport written to: {out_path}  ({size_kb:.0f} KB)", flush=True)
    if in_toks:
        print(f"API usage: {in_toks:,} input + {out_toks:,} output tokens "
              f"≈ ${cost:.4f} USD", flush=True)


if __name__ == "__main__":
    main()
