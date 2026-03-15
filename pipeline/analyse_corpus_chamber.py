#!/usr/bin/env python3
"""
analyse_corpus_chamber.py — Structural analysis of a Senate or House Hansard corpus.
No taxonomy enrichment required. Uses claude-sonnet-4-6 for a brief stats-based overview.

Usage:
    python analyse_corpus_chamber.py --chamber senate --out ../data/output/senate/corpus/corpus_report.html
    python analyse_corpus_chamber.py --chamber house  --out ../data/output/house/corpus/corpus_report.html
"""
import argparse
import base64
import datetime
import io
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

SENATE_CORPUS = ROOT / "data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.parquet"
HOUSE_CORPUS  = ROOT / "data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet"

PARTY_COLOURS = {
    "Coalition":  "#003087",
    "Labor":      "#E3231F",
    "Greens":     "#009B55",
    "Nationals":  "#006633",
    "Other":      "#888888",
}
SPEECH_TYPE_COLOURS = {
    "speech":          "#2196F3",
    "answer":          "#4CAF50",
    "question":        "#FF9800",
    "written_question":"#FF5722",
    "interjection":    "#9C27B0",
}
SPEECH_TYPE_ORDER = ["speech", "answer", "question", "written_question", "interjection"]

# Parliamentary election years → parliament label
PARLIAMENT_YEARS = {
    1998: "39th", 2001: "40th", 2004: "41st", 2007: "42nd",
    2010: "43rd", 2013: "44th", 2016: "45th", 2019: "46th",
    2022: "47th",
}

PROCEDURAL_NAMES = {
    "The PRESIDENT", "The SPEAKER", "The DEPUTY PRESIDENT",
    "The CHAIR", "The CHAIRMAN", "Stage direction", "Business start",
    "The Deputy Speaker", "The DEPUTY SPEAKER", "The Acting Deputy President",
    "The Acting President", "Procedural text",
}


def fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    buf.seek(0)
    data = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return data


def apply_style():
    plt.rcParams.update({
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "axes.grid":         True,
        "grid.alpha":        0.3,
        "font.family":       "sans-serif",
        "font.size":         11,
    })


def map_party_group(party) -> str:
    if pd.isna(party) or str(party).strip() == "":
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


def classify_turn(row) -> str:
    if int(row.get("q_in_writing", 0) or 0):
        return "written_question"
    if int(row.get("question", 0) or 0):
        return "question"
    if int(row.get("answer", 0) or 0):
        return "answer"
    if int(row.get("interject", 0) or 0):
        return "interjection"
    return "speech"


def year_to_parliament(year: int) -> str:
    for y in sorted(PARLIAMENT_YEARS.keys(), reverse=True):
        if year >= y:
            return PARLIAMENT_YEARS[y]
    return "pre-39th"


# ── Charts ────────────────────────────────────────────────────────────────────

def chart_volume_by_type(df: pd.DataFrame) -> str:
    """Stacked bar: speech turns per year broken down by speech type."""
    apply_style()
    yearly = df.groupby(["year", "speech_type"]).size().unstack(fill_value=0)
    for t in SPEECH_TYPE_ORDER:
        if t not in yearly.columns:
            yearly[t] = 0
    yearly = yearly[SPEECH_TYPE_ORDER]

    fig, ax = plt.subplots(figsize=(14, 5))
    bottom = np.zeros(len(yearly))
    for t in SPEECH_TYPE_ORDER:
        vals = yearly[t].values
        ax.bar(yearly.index, vals, bottom=bottom,
               color=SPEECH_TYPE_COLOURS[t], label=t.replace("_", " ").title(),
               alpha=0.9, width=0.75)
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Speech turns")
    ax.set_title("Speech Volume by Year and Turn Type", fontsize=13, fontweight="bold")
    ax.legend(ncol=5, fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_party_activity(df: pd.DataFrame) -> str:
    """Stacked bar: substantive speech turns per year by party group (excl. procedural)."""
    apply_style()
    sub = df[~df["name"].isin(PROCEDURAL_NAMES)].copy()
    sub["party_group"] = sub["party"].apply(map_party_group)
    yearly = sub.groupby(["year", "party_group"]).size().unstack(fill_value=0)
    groups = [g for g in ["Coalition", "Labor", "Greens", "Nationals", "Other"]
              if g in yearly.columns]
    yearly = yearly[groups]

    fig, ax = plt.subplots(figsize=(14, 5))
    bottom = np.zeros(len(yearly))
    for g in groups:
        vals = yearly[g].values
        ax.bar(yearly.index, vals, bottom=bottom,
               color=PARTY_COLOURS[g], label=g, alpha=0.9, width=0.75)
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Speech turns")
    ax.set_title("Speech Turns by Party Group Over Time", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_ingov_balance(df: pd.DataFrame) -> str:
    """Line: % of substantive turns from government members per year."""
    apply_style()
    sub = df[~df["name"].isin(PROCEDURAL_NAMES) & df["in_gov"].notna()].copy()
    sub["in_gov"] = pd.to_numeric(sub["in_gov"], errors="coerce")
    yr = sub.groupby("year")["in_gov"].agg(["sum", "count"])
    yr["pct_gov"] = yr["sum"] / yr["count"] * 100

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.fill_between(yr.index, yr["pct_gov"], 50,
                    where=yr["pct_gov"] >= 50, alpha=0.15, color="#003087")
    ax.fill_between(yr.index, yr["pct_gov"], 50,
                    where=yr["pct_gov"] < 50, alpha=0.15, color="#E3231F")
    ax.plot(yr.index, yr["pct_gov"], color="#1a1a1a", linewidth=2)
    ax.axhline(50, color="#888", linestyle="--", linewidth=1)

    # Mark election years
    for ey in PARLIAMENT_YEARS:
        if yr.index.min() <= ey <= yr.index.max():
            ax.axvline(ey, color="#ccc", linestyle=":", linewidth=1)

    ax.set_ylim(0, 100)
    ax.set_xlabel("Year")
    ax.set_ylabel("% of turns")
    ax.set_title("Government vs Opposition Share of Speech Turns", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_question_time(df: pd.DataFrame) -> str:
    """Line: oral questions, written questions, and answers per year."""
    apply_style()
    yr_q  = df[df["speech_type"] == "question"].groupby("year").size()
    yr_wq = df[df["speech_type"] == "written_question"].groupby("year").size()
    yr_a  = df[df["speech_type"] == "answer"].groupby("year").size()

    all_years = sorted(set(yr_q.index) | set(yr_wq.index) | set(yr_a.index))
    yr_q  = yr_q.reindex(all_years, fill_value=0)
    yr_wq = yr_wq.reindex(all_years, fill_value=0)
    yr_a  = yr_a.reindex(all_years, fill_value=0)

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(all_years, yr_q,  color=SPEECH_TYPE_COLOURS["question"],
            linewidth=2, label="Oral questions")
    ax.plot(all_years, yr_wq, color=SPEECH_TYPE_COLOURS["written_question"],
            linewidth=2, label="Written questions", linestyle="--")
    ax.plot(all_years, yr_a,  color=SPEECH_TYPE_COLOURS["answer"],
            linewidth=2, label="Answers")
    ax.set_xlabel("Year")
    ax.set_ylabel("Speech turns")
    ax.set_title("Question Time: Oral Questions, Written Questions, and Answers", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_gender_balance(df: pd.DataFrame) -> str:
    """Stacked area: % female and male speech turns per year."""
    apply_style()
    sub = df[df["gender"].isin(["male", "female"])].copy()
    yr = sub.groupby(["year", "gender"]).size().unstack(fill_value=0)
    if "female" not in yr.columns:
        yr["female"] = 0
    if "male" not in yr.columns:
        yr["male"] = 0
    yr["total"] = yr["female"] + yr["male"]
    yr["pct_female"] = yr["female"] / yr["total"] * 100

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.fill_between(yr.index, yr["pct_female"], alpha=0.3, color="#E91E63")
    ax.plot(yr.index, yr["pct_female"], color="#E91E63", linewidth=2, label="% female")
    # Rolling 3-year mean
    rolled = yr["pct_female"].rolling(3, center=True, min_periods=1).mean()
    ax.plot(yr.index, rolled, color="#880E4F", linewidth=2, linestyle="--", label="3-yr rolling mean")
    ax.set_ylim(0, 60)
    ax.set_xlabel("Year")
    ax.set_ylabel("% of turns")
    ax.set_title("Female Share of Speech Turns Over Time", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_interjection_rate(df: pd.DataFrame) -> str:
    """Line: interjections as % of all turns per year."""
    apply_style()
    yr = df.groupby("year")["speech_type"].apply(
        lambda x: (x == "interjection").sum() / len(x) * 100
    )

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.fill_between(yr.index, yr.values, alpha=0.2, color="#9C27B0")
    ax.plot(yr.index, yr.values, color="#9C27B0", linewidth=2)
    ax.axhline(yr.mean(), color="#888", linestyle="--", linewidth=1,
               label=f"Mean {yr.mean():.1f}%")
    ax.set_xlabel("Year")
    ax.set_ylabel("% of turns")
    ax.set_title("Interjection Rate Over Time", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_first_speeches(df: pd.DataFrame) -> str:
    """Bar: first speeches per parliament."""
    apply_style()
    fs = df[df["first_speech"] == 1].copy()
    fs["parliament"] = fs["year"].apply(year_to_parliament)
    parl_order = [v for v in PARLIAMENT_YEARS.values() if v in fs["parliament"].values]
    counts = fs.groupby("parliament").size().reindex(parl_order, fill_value=0)

    fig, ax = plt.subplots(figsize=(11, 4))
    ax.bar(counts.index, counts.values, color="#2196F3", alpha=0.85)
    ax.set_xlabel("Parliament")
    ax.set_ylabel("First speeches")
    ax.set_title("First Speeches per Parliament", fontsize=13, fontweight="bold")
    for i, (lab, val) in enumerate(zip(counts.index, counts.values)):
        ax.text(i, val + 0.3, str(val), ha="center", fontsize=9)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_geo_distribution(df: pd.DataFrame, chamber: str) -> str:
    """Horizontal bar: speech turns by state (Senate) or top 30 electorates (House)."""
    apply_style()
    if chamber == "senate":
        col = "state"
        title = "Speech Turns by State"
        n = 10
    else:
        col = "electorate"
        title = "Top 30 Electorates by Speech Volume"
        n = 30

    sub = df[df[col].notna() & (df[col] != "")].copy()
    counts = sub.groupby(col).size().sort_values(ascending=False).head(n)
    labels = [str(v)[:50] for v in counts.index]

    fig, ax = plt.subplots(figsize=(11, max(4, n * 0.32)))
    ax.barh(range(len(counts)), counts.values, color="#2196F3", alpha=0.85)
    ax.set_yticks(range(len(counts)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Speech turns")
    ax.set_title(title, fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_words_by_party(df: pd.DataFrame) -> str:
    """Stacked bar: total words per year by party group (excl. procedural & interjections)."""
    apply_style()
    sub = df[
        ~df["name"].isin(PROCEDURAL_NAMES) &
        (df["speech_type"] != "interjection")
    ].copy()
    sub["party_group"] = sub["party"].apply(map_party_group)
    yearly = sub.groupby(["year", "party_group"])["word_count"].sum().unstack(fill_value=0)
    groups = [g for g in ["Coalition", "Labor", "Greens", "Nationals", "Other"]
              if g in yearly.columns]
    yearly = yearly[groups]

    fig, ax = plt.subplots(figsize=(14, 5))
    bottom = np.zeros(len(yearly))
    for g in groups:
        vals = yearly[g].values / 1_000_000
        ax.bar(yearly.index, vals, bottom=bottom,
               color=PARTY_COLOURS[g], label=g, alpha=0.9, width=0.75)
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Words (millions)")
    ax.set_title("Total Words Spoken by Party Group per Year", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_turn_length_distribution(df: pd.DataFrame) -> str:
    """Box plot: distribution of speech turn word counts by party group (capped at 99th pct)."""
    apply_style()
    sub = df[
        ~df["name"].isin(PROCEDURAL_NAMES) &
        (df["speech_type"].isin(["speech", "answer"])) &
        (df["word_count"] > 0)
    ].copy()
    sub["party_group"] = sub["party"].apply(map_party_group)

    cap = int(sub["word_count"].quantile(0.99))
    sub["word_count_capped"] = sub["word_count"].clip(upper=cap)

    groups = [g for g in ["Coalition", "Labor", "Greens", "Nationals", "Other"]
              if g in sub["party_group"].values]
    data   = [sub[sub["party_group"] == g]["word_count_capped"].values for g in groups]
    colours = [PARTY_COLOURS[g] for g in groups]

    fig, ax = plt.subplots(figsize=(10, 5))
    bp = ax.boxplot(data, patch_artist=True, notch=False,
                    medianprops=dict(color="white", linewidth=2))
    for patch, col in zip(bp["boxes"], colours):
        patch.set_facecolor(col)
        patch.set_alpha(0.75)
    ax.set_xticks(range(1, len(groups) + 1))
    ax.set_xticklabels(groups)
    ax.set_ylabel("Words per turn (capped at 99th percentile)")
    ax.set_title("Speech Turn Length by Party Group\n(substantive speeches and answers only)",
                 fontsize=13, fontweight="bold")
    ax.text(0.99, 0.97, f"Cap: {cap:,} words", transform=ax.transAxes,
            ha="right", va="top", fontsize=8, color="#666")
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_top_speaker_words(df: pd.DataFrame) -> str:
    """Horizontal bar: top 30 speakers by total words (excl. procedural & interjections)."""
    apply_style()
    sub = df[
        ~df["name"].isin(PROCEDURAL_NAMES) &
        (df["speech_type"] != "interjection") &
        df["name"].notna()
    ].copy()
    sub["party_group"] = sub["party"].apply(map_party_group)

    agg = sub.groupby("name").agg(
        total_words=("word_count", "sum"),
        party_group=("party_group", lambda x: x.mode().iloc[0]),
    ).sort_values("total_words", ascending=False).head(30)

    colours = [PARTY_COLOURS.get(agg.loc[n, "party_group"], "#888") for n in agg.index]
    labels  = [str(n)[:45] for n in agg.index]

    fig, ax = plt.subplots(figsize=(11, 10))
    bars = ax.barh(range(len(agg)), agg["total_words"].values / 1_000_000,
                   color=colours, alpha=0.85)
    ax.set_yticks(range(len(agg)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Total words (millions)")
    ax.set_title("Top 30 Speakers by Total Words", fontsize=13, fontweight="bold")
    for bar, val in zip(bars, agg["total_words"].values):
        ax.text(bar.get_width() + agg["total_words"].max() / 1_000_000 * 0.005,
                bar.get_y() + bar.get_height() / 2,
                f"{val/1_000_000:.2f}M", va="center", fontsize=8)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_division_flags(df: pd.DataFrame) -> str:
    """Bar: division-flagged turns per year."""
    apply_style()
    df["div_flag_n"] = pd.to_numeric(df["div_flag"], errors="coerce").fillna(0)
    yr = df.groupby("year")["div_flag_n"].sum()

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.bar(yr.index, yr.values, color="#795548", alpha=0.85, width=0.75)
    ax.set_xlabel("Year")
    ax.set_ylabel("Division-flagged turns")
    ax.set_title("Division Votes per Year", fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Speakers table ─────────────────────────────────────────────────────────────

def build_speakers_table(df: pd.DataFrame) -> str:
    sub = df[~df["name"].isin(PROCEDURAL_NAMES) & df["name"].notna()].copy()

    agg = sub.groupby("name").agg(
        turns=("name", "count"),
        party=("party", lambda x: x.mode().iloc[0] if not x.mode().empty else ""),
        gender=("gender", lambda x: x.mode().iloc[0] if not x.mode().empty else ""),
        first_speech=("first_speech", "max"),
        questions=("speech_type", lambda x: (x == "question").sum()),
        interjections=("speech_type", lambda x: (x == "interjection").sum()),
        sittings=("date", "nunique"),
        total_words=("word_count", "sum"),
        avg_words=("word_count", "mean"),
    ).sort_values("turns", ascending=False).head(40)

    rows = ""
    for name, r in agg.iterrows():
        pg  = map_party_group(r["party"])
        col = PARTY_COLOURS.get(pg, "#888")
        fs  = "★" if r["first_speech"] == 1 else ""
        rows += (f"<tr><td>{name}{fs}</td>"
                 f"<td style='color:{col};font-weight:600'>{r['party']}</td>"
                 f"<td>{r['gender']}</td>"
                 f"<td style='text-align:right'>{r['turns']:,}</td>"
                 f"<td style='text-align:right'>{int(r['total_words']):,}</td>"
                 f"<td style='text-align:right'>{r['avg_words']:.0f}</td>"
                 f"<td style='text-align:right'>{r['questions']:,}</td>"
                 f"<td style='text-align:right'>{r['interjections']:,}</td>"
                 f"<td style='text-align:right'>{r['sittings']:,}</td></tr>\n")

    return f"""
    <table class="speakers-table">
      <thead><tr>
        <th>Speaker</th><th>Party</th><th>Gender</th>
        <th>Turns</th><th>Total words</th><th>Avg words/turn</th>
        <th>Questions</th><th>Interjections</th><th>Sitting days</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
    <p style="font-size:0.8rem;color:#888;margin-top:0.4rem">★ = gave a first speech in this corpus. Words exclude interjections.</p>"""


# ── LLM overview ──────────────────────────────────────────────────────────────

def get_llm_overview(stats: dict, chamber: str) -> str:
    try:
        import anthropic
        client = anthropic.Anthropic()

        chamber_label = "Senate" if chamber == "senate" else "House of Representatives"
        stats_text = "\n".join(f"  {k}: {v}" for k, v in stats.items())

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=(
                f"You are a data analyst writing a concise overview of the Australian "
                f"parliamentary {chamber_label} Hansard corpus (1998–2025). "
                f"Write in clean HTML (no markdown, no code fences). "
                f"Do not include <html>, <head>, or <body> tags. "
                f"Be analytical and precise. Ground every claim in the statistics provided. "
                f"Do not use external political knowledge — only what the numbers show."
            ),
            messages=[{"role": "user", "content": f"""
Here are summary statistics for the Australian {chamber_label} Hansard corpus 1998–2025:

{stats_text}

Write three short paragraphs as HTML <p> tags covering:
1. The overall scale and shape of the corpus (volume, date range, sitting days, turn types).
2. Participation patterns: party balance, gender trajectory, government vs opposition share.
3. One or two notable structural features visible in the data (e.g. interjection rate, question time volume, division frequency, first speech count).

Keep each paragraph to 3–4 sentences. Do not use headings — just three <p> elements.
"""}],
        )
        return response.content[0].text
    except Exception as e:
        return f"<p><em>Overview generation failed: {e}</em></p>"


# ── HTML assembly ──────────────────────────────────────────────────────────────

def build_html(stats: dict, charts: dict, speakers_table: str,
               overview: str, chamber: str) -> str:
    today = datetime.date.today().strftime("%d %B %Y")
    chamber_label = "Senate" if chamber == "senate" else "House of Representatives"
    geo_label     = "State Distribution" if chamber == "senate" else "Top Electorates"

    def stat_box(label, value):
        return f"""
        <div class="stat-box">
          <div class="stat-value">{value}</div>
          <div class="stat-label">{label}</div>
        </div>"""

    stat_boxes = "".join([
        stat_box("Total speech turns",   f"{stats['total']:,}"),
        stat_box("Sitting days",          f"{stats['n_days']:,}"),
        stat_box("Date range",            stats["date_range"]),
        stat_box("Oral questions",        f"{stats['n_questions']:,}"),
        stat_box("Written questions",     f"{stats['n_written_q']:,}"),
        stat_box("Interjections",         f"{stats['n_interjections']:,}  ({stats['pct_interject']:.1f}%)"),
        stat_box("% female turns",        f"{stats['pct_female']:.1f}%"),
        stat_box("First speeches",        f"{stats['n_first_speeches']}"),
        stat_box("Division votes",        f"{stats['n_divisions']:,}"),
        stat_box("Total words",           f"{stats['total_words']/1_000_000:.1f}M"),
        stat_box("Median words/turn",     f"{stats['median_turn_words']:,}"),
    ])

    def section(title, chart_b64, caption, chart_id=""):
        return f"""
        <section class="chart-section" id="{chart_id}">
          <h3>{title}</h3>
          <div class="chart-wrap">
            <img src="data:image/png;base64,{chart_b64}"
                 alt="{title}" style="max-width:100%;height:auto;">
          </div>
          <p class="caption">{caption}</p>
        </section>"""

    sections = "".join([
        section("Speech Volume by Turn Type",
                charts["volume"],
                "Each bar shows total speech turns for the year, broken down by turn type. "
                f"Peak year: {stats['peak_year']} ({stats['peak_count']:,} turns).",
                "volume"),
        section("Speech Turns by Party Group",
                charts["party"],
                "Procedural turns (presiding officers, stage directions) excluded. "
                "Party colour reflects grouped affiliation.",
                "party"),
        section("Government vs Opposition Share",
                charts["ingov"],
                "Proportion of substantive turns from government vs opposition members. "
                "Vertical dotted lines mark election years. 50% baseline shown.",
                "ingov"),
        section("Question Time: Oral and Written Questions",
                charts["questions"],
                "Oral questions (question time), written questions on notice, and ministerial answers per year.",
                "questions"),
        section("Female Share of Speech Turns",
                charts["gender"],
                "% of turns by members with gender coded as female. "
                "Dashed line shows 3-year rolling mean.",
                "gender"),
        section("Interjection Rate",
                charts["interjections"],
                "Interjections as a percentage of all speech turns per year. "
                "Dashed line shows corpus mean.",
                "interjections"),
        section("First Speeches per Parliament",
                charts["first_speeches"],
                "Count of maiden speeches by parliament. Reflects the scale of parliamentary turnover.",
                "first_speeches"),
        section(geo_label,
                charts["geo"],
                ("Speech turns by state of senator." if chamber == "senate"
                 else "Top 30 electorates by total speech turns from their member."),
                "geo"),
        section("Division Votes per Year",
                charts["divisions"],
                "Turns flagged as part of a division vote. "
                "Higher counts reflect contested legislative periods.",
                "divisions"),
        section("Total Words by Party Group per Year",
                charts["words_party"],
                "Total words spoken in substantive turns (speeches and answers; interjections excluded), "
                "stacked by party group. Reflects both participation share and verbosity.",
                "words_party"),
        section("Speech Turn Length Distribution by Party",
                charts["turn_length"],
                f"Box plot of words per turn for substantive speeches and answers only. "
                f"Capped at 99th percentile. Median across corpus: {stats['median_turn_words']:,} words; "
                f"mean: {stats['mean_turn_words']:,} words.",
                "turn_length"),
        section("Top 30 Speakers by Total Words",
                charts["top_words"],
                "Total words spoken in substantive turns (interjections excluded). "
                "Colour indicates party group.",
                "top_words"),
    ])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Australian {chamber_label} Hansard — Corpus Analysis 1998–2025</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
          background: #f5f5f5; color: #1a1a1a; line-height: 1.6; }}
  .header {{ background: #1a1a1a; color: white; padding: 2.5rem 3rem; }}
  .header h1 {{ font-size: 1.8rem; font-weight: 700; margin-bottom: 0.4rem; }}
  .header .subtitle {{ color: #aaa; font-size: 1rem; }}
  .stats-banner {{ background: white; border-bottom: 2px solid #e0e0e0;
                   padding: 1.5rem 3rem; display: flex; gap: 1.2rem; flex-wrap: wrap; }}
  .stat-box {{ background: #f9f9f9; border: 1px solid #e0e0e0; border-radius: 8px;
               padding: 0.9rem 1.2rem; min-width: 130px; }}
  .stat-value {{ font-size: 1.4rem; font-weight: 700; color: #1a1a1a; }}
  .stat-label {{ font-size: 0.75rem; color: #666; margin-top: 2px; text-transform: uppercase;
                 letter-spacing: 0.05em; }}
  .content {{ max-width: 1200px; margin: 0 auto; padding: 2rem 3rem; }}
  .overview {{ background: white; border-radius: 10px; padding: 1.8rem 2rem;
               margin-bottom: 2rem; box-shadow: 0 1px 4px rgba(0,0,0,0.08);
               border-left: 4px solid #2196F3; line-height: 1.7; }}
  .overview p {{ margin-bottom: 0.8rem; }}
  .overview p:last-child {{ margin-bottom: 0; }}
  .chart-section {{ background: white; border-radius: 10px; padding: 1.8rem;
                    margin-bottom: 2rem; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  .chart-section h3 {{ font-size: 1.1rem; font-weight: 600; margin-bottom: 1rem; }}
  .chart-wrap {{ text-align: center; margin-bottom: 0.8rem; }}
  .caption {{ font-size: 0.85rem; color: #666; font-style: italic; }}
  .speakers-table {{ width: 100%; border-collapse: collapse; font-size: 0.87rem; margin-top: 1rem; }}
  .speakers-table th {{ background: #f0f0f0; padding: 0.5rem 0.8rem; text-align: left;
                        font-weight: 600; border-bottom: 2px solid #ddd; }}
  .speakers-table td {{ padding: 0.42rem 0.8rem; border-bottom: 1px solid #eee; }}
  .speakers-table tr:hover td {{ background: #fafafa; }}
  h2 {{ font-size: 1.3rem; margin: 2.5rem 0 1rem; color: #1a1a1a;
        border-bottom: 2px solid #e0e0e0; padding-bottom: 0.4rem; }}
  .footer {{ text-align: center; color: #999; font-size: 0.8rem; padding: 2rem; }}
</style>
</head>
<body>
<div class="header">
  <h1>Australian {chamber_label} Hansard</h1>
  <div class="subtitle">Corpus Analysis 1998–2025 &nbsp;·&nbsp; Generated {today}</div>
</div>
<div class="stats-banner">{stat_boxes}</div>
<div class="content">
  <h2>Corpus Overview</h2>
  <div class="overview">{overview}</div>
  <h2>Volume &amp; Composition</h2>
  {sections}
  <h2>Word Volume &amp; Turn Length</h2>
  <h2>Top 40 Speakers</h2>
  <div class="chart-section">
    <h3>Most Active Speakers (Top 40 by speech turns)</h3>
    {speakers_table}
  </div>
</div>
<div class="footer">
  Australian {chamber_label} Hansard 1998–2025 · Corpus analysis generated {today}
</div>
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chamber", choices=["senate", "house"], required=True)
    ap.add_argument("--out",     type=Path, required=True)
    ap.add_argument("--corpus",  type=Path, default=None,
                    help="Override default corpus path.")
    ap.add_argument("--no-llm",  action="store_true",
                    help="Skip the LLM overview paragraph.")
    args = ap.parse_args()

    corpus_path = args.corpus or (SENATE_CORPUS if args.chamber == "senate" else HOUSE_CORPUS)
    print(f"Loading {args.chamber} corpus: {corpus_path}", flush=True)
    df = pd.read_parquet(corpus_path)
    print(f"  {len(df):,} rows loaded.", flush=True)

    # ── Prepare ───────────────────────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date"].dt.year.astype("Int64")
    for col in ("question", "answer", "q_in_writing", "div_flag", "interject", "first_speech", "in_gov"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df["speech_type"] = df.apply(classify_turn, axis=1)

    print("Computing word counts...", flush=True)
    df["word_count"] = df["body"].fillna("").str.split().str.len()

    # ── Stats banner ──────────────────────────────────────────────────────────
    yr_counts  = df.groupby("year").size()
    peak_year  = int(yr_counts.idxmax())
    gender_sub = df[df["gender"].isin(["male", "female"])]
    pct_female = (gender_sub["gender"] == "female").mean() * 100

    stats = {
        "total":            len(df),
        "n_days":           df["date"].nunique(),
        "date_range":       f"{df['date'].min().year}–{df['date'].max().year}",
        "peak_year":        peak_year,
        "peak_count":       int(yr_counts[peak_year]),
        "n_questions":      int((df["speech_type"] == "question").sum()),
        "n_written_q":      int((df["speech_type"] == "written_question").sum()),
        "n_answers":        int((df["speech_type"] == "answer").sum()),
        "n_interjections":  int((df["speech_type"] == "interjection").sum()),
        "pct_interject":    (df["speech_type"] == "interjection").mean() * 100,
        "n_first_speeches": int(df["first_speech"].sum()),
        "n_divisions":      int(df["div_flag"].sum()),
        "pct_female":       pct_female,
        "pct_gov":           df[~df["name"].isin(PROCEDURAL_NAMES)]["in_gov"].mean() * 100,
        "n_unique_speakers": df[~df["name"].isin(PROCEDURAL_NAMES)]["name"].nunique(),
        "total_words":       int(df["word_count"].sum()),
        "median_turn_words": int(df[df["speech_type"].isin(["speech","answer"])]["word_count"].median()),
        "mean_turn_words":   int(df[df["speech_type"].isin(["speech","answer"])]["word_count"].mean()),
    }

    # ── Charts ────────────────────────────────────────────────────────────────
    print("Generating charts...", flush=True)
    charts = {}
    charts["volume"]       = chart_volume_by_type(df);               print("  volume done")
    charts["party"]        = chart_party_activity(df);               print("  party done")
    charts["ingov"]        = chart_ingov_balance(df);                print("  ingov done")
    charts["questions"]    = chart_question_time(df);                print("  questions done")
    charts["gender"]       = chart_gender_balance(df);               print("  gender done")
    charts["interjections"]= chart_interjection_rate(df);            print("  interjections done")
    charts["first_speeches"]= chart_first_speeches(df);             print("  first_speeches done")
    charts["geo"]          = chart_geo_distribution(df, args.chamber); print("  geo done")
    charts["divisions"]    = chart_division_flags(df);               print("  divisions done")
    charts["words_party"]  = chart_words_by_party(df);              print("  words_party done")
    charts["turn_length"]  = chart_turn_length_distribution(df);    print("  turn_length done")
    charts["top_words"]    = chart_top_speaker_words(df);           print("  top_words done")

    speakers_table = build_speakers_table(df)
    print("  speakers table done")

    # ── LLM overview ──────────────────────────────────────────────────────────
    if args.no_llm:
        overview = "<p><em>LLM overview skipped (--no-llm).</em></p>"
    else:
        print("Requesting Sonnet overview...", flush=True)
        overview = get_llm_overview(stats, args.chamber)
        print("  done")

    # ── Assemble ──────────────────────────────────────────────────────────────
    html = build_html(stats, charts, speakers_table, overview, args.chamber)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")
    print(f"\nReport written to: {args.out}  ({args.out.stat().st_size // 1024:,} KB)")


if __name__ == "__main__":
    main()
