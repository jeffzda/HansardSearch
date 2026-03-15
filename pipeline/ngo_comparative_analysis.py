#!/usr/bin/env python3
"""
ngo_comparative_analysis.py — Comparative parliamentary salience analysis
for Australian environmental and conservation NGOs, 1998–2025.

Reads matches.csv files produced by batch_search_ngos.py (plus existing
WWF case study).  Produces a self-contained HTML report with embedded charts.

Analyses performed (all without LLM enrichment):
  1. Landscape overview — speech turns + mentions ranked
  2. Temporal salience (rate-normalised per 10k speech turns by year)
  3. Party alignment — % breakdown per org + over/under-representation
  4. Government vs opposition split
  5. Chamber preference (Senate vs House)
  6. Speech mode (question / answer / interjection / prepared speech)
  7. Speaker concentration — unique speakers, Gini coefficient
  8. Mentions-per-turn (focus density) vs total turns scatter
  9. Peak parliamentary activity per org
 10. Debate terrain — top debate heading keywords per org
"""

from __future__ import annotations

import argparse
import base64
import io
import re
import sys
from collections import Counter
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
CASE = ROOT / "case_studies"

# Default ORG_META — overridden at runtime via set_org_meta() or --type CLI arg
ORG_META: dict[str, str] = {
    "ACF":                       "ACF",
    "Wilderness_Society":        "Wilderness Society",
    "Greenpeace":                "Greenpeace",
    "Climate_Council":           "Climate Council",
    "EDO":                       "EDO",
    "Sea_Shepherd":              "Sea Shepherd",
    "Friends_of_the_Earth":      "Friends of the Earth",
    "Lock_the_Gate":             "Lock the Gate",
    "Landcare":                  "Landcare",
    "AMCS":                      "AMCS",
    "BirdLife":                  "BirdLife",
    "Humane_Society_International": "Humane Soc. Intl",
    "Bob_Brown_Foundation":      "Bob Brown Fdn",
    "WWF":                       "WWF",
    "AWC":                       "AWC",
}

_TYPE_LABEL: str = "Environmental & Conservation NGOs"


def set_org_meta(org_meta: dict[str, str], type_label: str) -> None:
    """Override ORG_META and type label — called before generating each report."""
    global ORG_META, _TYPE_LABEL
    ORG_META    = org_meta
    _TYPE_LABEL = type_label

# Total corpus speech turns per year (Senate + House combined)
# Used as denominator for rate normalisation
CORPUS_TURNS_BY_YEAR: dict[int, int] = {
    1998: 23286 + 21446,
    1999: 29496 + 21009,
    2000: 24600 + 23168,
    2001: 17528 + 18934,
    2002: 17822 + 22691,
    2003: 18033 + 23171,
    2004: 13863 + 17610,
    2005: 16859 + 21995,
    2006: 16178 + 24108,
    2007: 12763 + 17154,
    2008: 15675 + 21406,
    2009: 19375 + 21131,
    2010: 14027 + 16503,
    2011: 25227 + 26029,
    2012: 22538 + 25761,
    2013: 13486 + 18639,
    2014: 22456 + 29461,
    2015: 22571 + 29438,
    2016: 17153 + 19735,
    2017: 22699 + 25146,
    2018: 23682 + 24660,
    2019: 14787 + 17810,
    2020: 17449 + 21368,
    2021: 19031 + 24553,
    2022: 15021 + 17511,
    2023: 32053 + 29437,
    2024: 24121 + 27672,
    2025: 18085 + 16360,
}

# Parliament → year range mapping (Australian federal parliaments)
PARLIAMENT_YEARS: dict[int, tuple[int, int]] = {
    36: (1996, 1998),
    37: (1998, 2001),
    38: (2001, 2004),
    39: (2004, 2007),
    40: (2007, 2010),
    41: (2010, 2013),
    42: (2013, 2016),
    43: (2016, 2019),
    44: (2019, 2022),
    45: (2022, 2025),
    46: (2025, 2028),
}

def year_to_parliament(year: int) -> int:
    for parl, (start, end) in PARLIAMENT_YEARS.items():
        if start <= year < end:
            return parl
    return 46

# ── Party grouping and colours ─────────────────────────────────────────────────
PARTY_GROUP: dict[str, str] = {
    # Coalition
    "LP": "Coalition", "LNP": "Coalition", "NP": "Coalition",
    "NATS": "Coalition", "CLP": "Coalition", "LCP": "Coalition",
    # Labor
    "ALP": "Labor",
    # Greens
    "AG": "Greens", "GRN": "Greens",
    # Cross-bench / minor
    "IND": "Crossbench", "AD": "Crossbench", "NXT": "Crossbench",
    "CA": "Crossbench", "XEN": "Crossbench", "KAP": "Crossbench",
    "PUP": "Crossbench", "UAP": "Crossbench", "PHON": "Crossbench",
    "TG": "Crossbench", "DLP": "Crossbench", "APA": "Crossbench",
    "FF": "Crossbench", "OTH": "Crossbench",
}
GROUP_COLOUR: dict[str, str] = {
    "Coalition":  "#003087",
    "Labor":      "#CC0000",
    "Greens":     "#009900",
    "Crossbench": "#888888",
    "Unknown":    "#cccccc",
}

def map_group(party: str) -> str:
    return PARTY_GROUP.get(str(party).strip().upper(), "Unknown")


# ── Gini coefficient ──────────────────────────────────────────────────────────
def gini(values: list[int]) -> float:
    if not values or sum(values) == 0:
        return 0.0
    arr = sorted(values)
    n   = len(arr)
    cum = sum((2 * (i + 1) - n - 1) * v for i, v in enumerate(arr))
    return cum / (n * sum(arr))


# ── Load data ─────────────────────────────────────────────────────────────────
def _org_path(type_key: str, folder: str) -> Path:
    """Resolve matches.csv path under the LLM_free type subfolder."""
    return CASE / "LLM_free" / type_key / folder / "matches.csv"


def load_all() -> dict[str, pd.DataFrame]:
    from org_types_config import ORG_TYPES
    # Build folder → type_key lookup from the config
    folder_to_type: dict[str, str] = {
        org.folder: type_key
        for type_key, ot in ORG_TYPES.items()
        for org in ot.orgs
    }

    data: dict[str, pd.DataFrame] = {}
    for folder, label in ORG_META.items():
        type_key = folder_to_type.get(folder, "")
        p = _org_path(type_key, folder) if type_key else CASE / folder / "matches.csv"
        if not p.exists():
            print(f"  MISSING: {p}")
            continue
        df = pd.read_csv(p, low_memory=False)
        df["date"]  = pd.to_datetime(df["date"], errors="coerce")
        df["year"]  = df["date"].dt.year
        df["group"] = df["party"].fillna("").apply(map_group)
        # Ensure mention_count exists (older files may not have it)
        if "mention_count" not in df.columns:
            df["mention_count"] = 1
        else:
            df["mention_count"] = pd.to_numeric(df["mention_count"], errors="coerce").fillna(1)
        data[folder] = df
    return data


# ── Chart helpers ─────────────────────────────────────────────────────────────
def fig_to_b64(fig: plt.Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    return b64


def img_tag(b64: str, alt: str = "") -> str:
    return f'<img src="data:image/png;base64,{b64}" alt="{alt}" style="max-width:100%;margin:1em 0;">'


# ── Analysis 1: Landscape overview ───────────────────────────────────────────
def chart_landscape(data: dict[str, pd.DataFrame]) -> str:
    rows = []
    for folder, label in ORG_META.items():
        if folder not in data:
            continue
        df = data[folder]
        rows.append({
            "label":        label,
            "turns":        len(df),
            "mentions":     int(df["mention_count"].sum()),
            "avg":          round(float(df["mention_count"].mean()), 2),
            "speakers":     df["unique_id"].nunique(),
        })
    summary = pd.DataFrame(rows).sort_values("turns", ascending=True)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Australian Environmental NGOs — Parliamentary Salience Overview\n1998–2025",
                 fontsize=13, fontweight="bold")

    summary_m = summary.sort_values("mentions", ascending=True).reset_index(drop=True)
    shared_max = max(summary["turns"].max(), summary_m["mentions"].max()) * 1.18

    def _barh(ax, df, val_col, colors, xlabel, title):
        """Draw a horizontal bar chart with explicit y-positions to avoid
        matplotlib re-ordering the categorical axis."""
        vals   = df[val_col].tolist()
        labels = df["label"].tolist()
        ypos   = list(range(len(vals)))
        bars   = ax.barh(ypos, vals, color=colors, edgecolor="white")
        ax.set_yticks(ypos)
        ax.set_yticklabels(labels, fontsize=9)
        ax.bar_label(bars, labels=[f"{v:,.0f}" for v in vals], padding=3, fontsize=8)
        ax.set_xlim(0, shared_max)
        ax.set_xlabel(xlabel, fontsize=9)
        ax.set_title(title, fontsize=11)
        ax.spines[["top", "right"]].set_visible(False)

    # Left: speech turns
    colors_t = ["#2ecc71" if t > 500 else "#3498db" if t > 100 else "#bdc3c7"
                for t in summary["turns"]]
    _barh(axes[0], summary, "turns", colors_t,
          "Speech turns (rows with at least one alias match)", "Total Speech Turns")

    # Right: total alias mentions
    colors_m = ["#e74c3c" if m > 1000 else "#e67e22" if m > 200 else "#bdc3c7"
                for m in summary_m["mentions"]]
    _barh(axes[1], summary_m, "mentions", colors_m,
          "Total alias mentions (sum of occurrences in body text)", "Total Alias Mentions")

    plt.tight_layout()
    return fig_to_b64(fig)


# ── Analysis 2: Temporal salience (rate-normalised) ───────────────────────────
def chart_temporal(data: dict[str, pd.DataFrame]) -> str:
    denom = pd.Series(CORPUS_TURNS_BY_YEAR, name="total")

    # Select orgs with enough data for a line (>= 30 turns total)
    orgs_plot = [(f, ORG_META[f]) for f in ORG_META if f in data and len(data[f]) >= 30]

    # Tier: large (>= 200 turns) vs small (30–199)
    large = [(f, l) for f, l in orgs_plot if len(data[f]) >= 200]
    small = [(f, l) for f, l in orgs_plot if len(data[f]) < 200]

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    fig.suptitle("Parliamentary Salience Over Time — Mentions per 10,000 Speech Turns",
                 fontsize=12, fontweight="bold")

    cmap_large = plt.cm.tab10
    cmap_small = plt.cm.Set2

    for ax, tier, cmap, title in [
        (axes[0], large, cmap_large, "Major-footprint NGOs (≥200 speech turns)"),
        (axes[1], small, cmap_small, "Smaller-footprint NGOs (30–199 speech turns)"),
    ]:
        ax.set_title(title, fontsize=10)
        for i, (folder, label) in enumerate(tier):
            df = data[folder]
            by_year = df.groupby("year").size()
            rate    = (by_year / denom * 10000).dropna()
            ax.plot(rate.index, rate.values, marker="o", markersize=3,
                    linewidth=1.8, label=label, color=cmap(i / max(len(tier), 1)))
        ax.set_ylabel("Turns per 10k corpus rows", fontsize=9)
        ax.legend(fontsize=8, loc="upper left", framealpha=0.7)
        ax.spines[["top","right"]].set_visible(False)
        ax.grid(axis="y", alpha=0.3)

        # Parliament boundaries
        for parl, (yr_start, _) in PARLIAMENT_YEARS.items():
            if 1998 <= yr_start <= 2025:
                ax.axvline(yr_start, color="grey", linestyle=":", alpha=0.4, linewidth=0.8)
                ax.text(yr_start + 0.1, ax.get_ylim()[1] * 0.95, f"P{parl}",
                        fontsize=6, color="grey", va="top")

    axes[1].set_xlabel("Year", fontsize=10)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Analysis 3: Party alignment ───────────────────────────────────────────────
def chart_party(data: dict[str, pd.DataFrame]) -> tuple[str, pd.DataFrame]:
    groups   = ["Coalition", "Labor", "Greens", "Crossbench", "Unknown"]
    labels   = [ORG_META[f] for f in ORG_META if f in data]
    folders  = [f for f in ORG_META if f in data]

    # % breakdown per org
    matrix = []
    for folder in folders:
        df     = data[folder]
        counts = df["group"].value_counts()
        total  = len(df)
        row    = {g: counts.get(g, 0) / total * 100 for g in groups}
        row["org"] = ORG_META[folder]
        matrix.append(row)
    matrix_df = pd.DataFrame(matrix).set_index("org")

    # Overall corpus baseline (approx, from earlier analysis)
    baseline = {"Coalition": 37, "Labor": 35, "Greens": 10, "Crossbench": 8, "Unknown": 10}

    # Sort by Greens + Labor share (most left-leaning first)
    matrix_df["left_pct"] = matrix_df["Greens"] + matrix_df["Labor"]
    matrix_df = matrix_df.sort_values("left_pct", ascending=True)

    fig, ax = plt.subplots(figsize=(12, 7))
    fig.suptitle("Party Alignment — % of Speech Turns by Party Group\n"
                 "(sorted by Greens+Labor share)", fontsize=12, fontweight="bold")

    bottoms  = np.zeros(len(matrix_df))
    for group in groups:
        vals = matrix_df[group].values
        ax.barh(matrix_df.index, vals, left=bottoms,
                color=GROUP_COLOUR[group], label=group, edgecolor="white", linewidth=0.5)
        # Label if large enough
        for i, (v, b) in enumerate(zip(vals, bottoms)):
            if v >= 8:
                ax.text(b + v / 2, i, f"{v:.0f}%", ha="center", va="center",
                        fontsize=7.5, color="white", fontweight="bold")
        bottoms += vals

    # Baseline markers
    base_coal = baseline["Coalition"]
    base_lab  = baseline["Labor"]
    ax.axvline(base_coal, color="navy", linestyle="--", alpha=0.5, linewidth=1,
               label=f"Corpus avg Coalition ({base_coal}%)")
    ax.axvline(base_coal + base_lab, color="darkred", linestyle="--", alpha=0.5, linewidth=1,
               label=f"Corpus avg Coalition+Labor ({base_coal+base_lab}%)")

    ax.set_xlabel("% of speech turns", fontsize=10)
    ax.set_xlim(0, 105)
    ax.legend(fontsize=8, loc="lower right")
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    return fig_to_b64(fig), matrix_df.drop(columns=["left_pct"])


# ── Analysis 4: Government vs Opposition ─────────────────────────────────────
def chart_gov_opp(data: dict[str, pd.DataFrame]) -> str:
    rows = []
    for folder, label in ORG_META.items():
        if folder not in data:
            continue
        df = data[folder]
        df2 = df[df["in_gov"].isin(["0", "1", 0, 1])].copy()
        df2["in_gov"] = df2["in_gov"].astype(str)
        gov  = (df2["in_gov"] == "1").sum()
        opp  = (df2["in_gov"] == "0").sum()
        total = gov + opp
        if total == 0:
            continue
        rows.append({"org": label, "gov_pct": gov / total * 100, "opp_pct": opp / total * 100,
                     "total": total})
    df_plot = pd.DataFrame(rows).sort_values("gov_pct", ascending=True)

    fig, ax = plt.subplots(figsize=(11, 6))
    fig.suptitle("Government vs Opposition — Share of Speech Turns with Flagged In-Gov Status",
                 fontsize=11, fontweight="bold")

    ax.barh(df_plot["org"], df_plot["gov_pct"], color="#1a6e3c", label="Government speaker")
    ax.barh(df_plot["org"], df_plot["opp_pct"], left=df_plot["gov_pct"],
            color="#c0392b", label="Opposition speaker")
    ax.axvline(50, color="grey", linestyle="--", alpha=0.6, linewidth=1)

    for _, r in df_plot.iterrows():
        ax.text(r["gov_pct"] / 2, df_plot.index[df_plot["org"] == r["org"]].tolist()[0],
                f"{r['gov_pct']:.0f}%", ha="center", va="center",
                fontsize=7.5, color="white", fontweight="bold")

    ax.set_xlabel("% of speech turns (excl. stage directions, unknowns)", fontsize=9)
    ax.set_xlim(0, 100)
    ax.legend(fontsize=9)
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Analysis 5: Chamber preference ───────────────────────────────────────────
def chart_chamber(data: dict[str, pd.DataFrame]) -> str:
    rows = []
    for folder, label in ORG_META.items():
        if folder not in data:
            continue
        df    = data[folder]
        s_pct = (df["chamber"] == "senate").mean() * 100
        h_pct = (df["chamber"] == "house").mean()   * 100
        rows.append({"org": label, "senate": s_pct, "house": h_pct, "total": len(df)})
    df_plot = pd.DataFrame(rows).sort_values("senate", ascending=True)

    fig, ax = plt.subplots(figsize=(11, 6))
    fig.suptitle("Chamber Preference — Senate vs House of Representatives",
                 fontsize=12, fontweight="bold")

    ax.barh(df_plot["org"], df_plot["senate"], color="#2980b9", label="Senate")
    ax.barh(df_plot["org"], df_plot["house"],  left=df_plot["senate"],
            color="#e67e22", label="House of Representatives")
    ax.axvline(50, color="grey", linestyle="--", alpha=0.5, linewidth=1)
    ax.set_xlabel("% of speech turns", fontsize=10)
    ax.set_xlim(0, 105)
    ax.legend(fontsize=9)
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Analysis 6: Speech mode ───────────────────────────────────────────────────
def chart_speech_mode(data: dict[str, pd.DataFrame]) -> str:
    rows = []
    for folder, label in ORG_META.items():
        if folder not in data:
            continue
        df = data[folder]
        n  = len(df)
        # Parse flags — stored as "0"/"1" strings or int
        def flag_pct(col: str) -> float:
            col_s = df[col].astype(str)
            return (col_s.isin(["1", "1.0", "True", "true"])).sum() / n * 100

        q   = flag_pct("is_question")
        a   = flag_pct("is_answer")
        inj = flag_pct("is_interjection")
        qw  = flag_pct("q_in_writing")
        pre = max(0, 100 - q - a - inj - qw)
        rows.append({"org": label, "prepared": pre, "question": q, "answer": a,
                     "interjection": inj, "q_in_writing": qw})

    df_plot = pd.DataFrame(rows).sort_values("prepared", ascending=True)
    modes   = ["prepared", "answer", "question", "q_in_writing", "interjection"]
    mode_colours = {
        "prepared":    "#2980b9",
        "answer":      "#27ae60",
        "question":    "#e67e22",
        "q_in_writing":"#9b59b6",
        "interjection":"#95a5a6",
    }
    mode_labels = {
        "prepared":    "Prepared speech",
        "answer":      "Minister answer",
        "question":    "Question Time",
        "q_in_writing":"Question on notice",
        "interjection":"Interjection",
    }

    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle("Speech Mode — How MPs Engage With Each Organisation",
                 fontsize=12, fontweight="bold")

    bottoms = np.zeros(len(df_plot))
    for mode in modes:
        vals = df_plot[mode].values
        ax.barh(df_plot["org"], vals, left=bottoms,
                color=mode_colours[mode], label=mode_labels[mode],
                edgecolor="white", linewidth=0.5)
        for i, (v, b) in enumerate(zip(vals, bottoms)):
            if v >= 7:
                ax.text(b + v / 2, i, f"{v:.0f}%", ha="center", va="center",
                        fontsize=7, color="white", fontweight="bold")
        bottoms += vals

    ax.set_xlabel("% of speech turns", fontsize=10)
    ax.set_xlim(0, 105)
    ax.legend(fontsize=8, loc="lower right")
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Analysis 7: Speaker concentration ────────────────────────────────────────
def chart_concentration(data: dict[str, pd.DataFrame]) -> str:
    rows = []
    for folder, label in ORG_META.items():
        if folder not in data:
            continue
        df = data[folder]
        spk_counts = df.groupby("unique_id").size().values
        g = gini(spk_counts.tolist())
        rows.append({
            "org":      label,
            "turns":    len(df),
            "unique_speakers": df["unique_id"].nunique(),
            "gini":     round(g, 3),
            "top5_pct": sum(sorted(spk_counts, reverse=True)[:5]) / len(df) * 100,
        })
    df_plot = pd.DataFrame(rows)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Speaker Concentration — How Broadly Is Each Org Discussed?",
                 fontsize=12, fontweight="bold")

    # Left: scatter — unique speakers vs total turns, bubble = Gini
    from adjustText import adjust_text

    ax = axes[0]
    for _, r in df_plot.iterrows():
        size = 200 + r["gini"] * 800
        ax.scatter(r["turns"], r["unique_speakers"], s=size, alpha=0.7,
                   color="#3498db", edgecolors="#1a5276", linewidths=0.8)
    texts = [
        ax.text(r["turns"], r["unique_speakers"], r["org"], fontsize=7.5)
        for _, r in df_plot.iterrows()
    ]
    adjust_text(texts, ax=ax,
                arrowprops=dict(arrowstyle="-", color="grey", lw=0.5),
                expand=(2.0, 2.5), force_text=(1.5, 2.0),
                force_points=(0.5, 1.0), lim=500)
    ax.set_xlabel("Total speech turns", fontsize=9)
    ax.set_ylabel("Unique speakers", fontsize=9)
    ax.set_title("Breadth of engagement\n(bubble size ∝ Gini — higher = more concentrated)",
                 fontsize=9)
    ax.spines[["top","right"]].set_visible(False)

    # Right: Gini bar chart
    ax2 = axes[1]
    df_g = df_plot.sort_values("gini", ascending=True)
    cmap = plt.cm.RdYlGn_r
    colours = [cmap(g) for g in df_g["gini"]]
    bars = ax2.barh(df_g["org"], df_g["gini"], color=colours, edgecolor="white")
    ax2.bar_label(bars, fmt="%.2f", padding=3, fontsize=8)
    ax2.set_xlabel("Gini coefficient (0 = equal spread, 1 = one speaker)", fontsize=9)
    ax2.set_title("Speaker Concentration (Gini)", fontsize=9)
    ax2.set_xlim(0, 1.0)
    ax2.spines[["top","right"]].set_visible(False)

    plt.tight_layout()
    return fig_to_b64(fig)


# ── Analysis 8: Mentions-per-turn scatter ────────────────────────────────────
def chart_focus(data: dict[str, pd.DataFrame]) -> str:
    rows = []
    for folder, label in ORG_META.items():
        if folder not in data:
            continue
        df = data[folder]
        rows.append({
            "org":     label,
            "turns":   len(df),
            "avg_mentions": float(df["mention_count"].mean()),
        })
    df_plot = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle("Parliamentary Focus — Speech Turns vs. Average Alias Mentions per Turn\n"
                 "(top-right = high volume AND high focus)", fontsize=11, fontweight="bold")

    from adjustText import adjust_text

    ax.scatter(df_plot["turns"], df_plot["avg_mentions"], s=120, alpha=0.8,
               color="#8e44ad", edgecolors="#5b2c6f")
    texts = [
        ax.text(r["turns"], r["avg_mentions"], r["org"], fontsize=8)
        for _, r in df_plot.iterrows()
    ]
    adjust_text(texts, ax=ax,
                arrowprops=dict(arrowstyle="-", color="grey", lw=0.5),
                expand=(1.4, 1.6), force_text=(0.5, 0.8))

    ax.axhline(df_plot["avg_mentions"].median(), color="grey", linestyle="--",
               alpha=0.5, linewidth=1, label="Median avg mentions/turn")
    ax.axvline(df_plot["turns"].median(), color="grey", linestyle=":",
               alpha=0.5, linewidth=1, label="Median turns")
    ax.set_xlabel("Total speech turns (volume)", fontsize=10)
    ax.set_ylabel("Average alias mentions per speech turn (focus density)", fontsize=10)
    ax.legend(fontsize=9)
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Analysis 9: Peak parliament ───────────────────────────────────────────────
def chart_parliament(data: dict[str, pd.DataFrame]) -> str:
    rows = []
    for folder, label in ORG_META.items():
        if folder not in data:
            continue
        df = data[folder]
        df2 = df.dropna(subset=["year"])
        df2 = df2.copy()
        df2["parliament"] = df2["year"].astype(int).apply(year_to_parliament)
        by_parl = df2.groupby("parliament").size()
        if by_parl.empty:
            continue
        rows.append({"org": label, "parliament": int(by_parl.idxmax()), "turns": int(by_parl.max())})
    df_plot = pd.DataFrame(rows).sort_values(["parliament", "turns"])

    # Heatmap: orgs × parliaments
    all_parls = sorted({p for p, (s, e) in PARLIAMENT_YEARS.items() if s >= 1998})
    orgs_sorted = sorted(ORG_META.values())
    mat = np.zeros((len(orgs_sorted), len(all_parls)))
    for folder, label in ORG_META.items():
        if folder not in data:
            continue
        df = data[folder]
        df2 = df.dropna(subset=["year"]).copy()
        df2["parliament"] = df2["year"].astype(int).apply(year_to_parliament)
        by_parl = df2.groupby("parliament").size()
        i = orgs_sorted.index(label)
        for j, p in enumerate(all_parls):
            mat[i, j] = by_parl.get(p, 0)

    # Row-normalise (% of org's total)
    row_sums = mat.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    mat_norm = mat / row_sums * 100

    fig, ax = plt.subplots(figsize=(13, 7))
    fig.suptitle("Parliamentary Activity Heatmap — % of Total Turns by Parliament\n"
                 "(each row normalised to 100%)", fontsize=11, fontweight="bold")

    im = ax.imshow(mat_norm, aspect="auto", cmap="YlOrRd", vmin=0, vmax=60)
    ax.set_xticks(range(len(all_parls)))
    ax.set_xticklabels([f"P{p}" for p in all_parls], fontsize=8)
    ax.set_yticks(range(len(orgs_sorted)))
    ax.set_yticklabels(orgs_sorted, fontsize=8)

    # Annotate with raw counts
    for i in range(len(orgs_sorted)):
        for j in range(len(all_parls)):
            raw = int(mat[i, j])
            if raw > 0:
                ax.text(j, i, str(raw), ha="center", va="center",
                        fontsize=6.5, color="black" if mat_norm[i, j] < 40 else "white")

    plt.colorbar(im, ax=ax, label="% of org's total speech turns", shrink=0.6)
    # Parliament year labels on x-axis
    ax2 = ax.twiny()
    ax2.set_xlim(ax.get_xlim())
    ax2.set_xticks(range(len(all_parls)))
    parl_labels = [str(PARLIAMENT_YEARS[p][0]) for p in all_parls]
    ax2.set_xticklabels(parl_labels, fontsize=7, color="grey")

    plt.tight_layout()
    return fig_to_b64(fig)


# ── Analysis 10: Debate terrain ───────────────────────────────────────────────
_STOPWORDS = {
    "the","a","an","of","in","and","to","for","is","are","was","were","that",
    "this","it","on","at","by","with","as","be","been","has","have","from",
    "or","not","but","we","i","he","she","they","his","her","their","our",
    "will","would","should","may","can","do","did","had","all","its","which",
    "about","into","up","out","more","also","there","than","then",
}

def top_heading_words(df: pd.DataFrame, n: int = 8) -> list[tuple[str, int]]:
    words = Counter()
    for h in df["debate_heading"].dropna():
        for w in re.findall(r"[a-zA-Z]{4,}", h.lower()):
            if w not in _STOPWORDS:
                words[w] += 1
    return words.most_common(n)


def chart_debate_terrain(data: dict[str, pd.DataFrame]) -> str:
    orgs = [(f, ORG_META[f]) for f in ORG_META if f in data and len(data[f]) >= 30]
    ncols = 3
    nrows = int(np.ceil(len(orgs) / ncols))

    fig, axes = plt.subplots(nrows, ncols, figsize=(14, nrows * 3.5))
    fig.suptitle("Debate Terrain — Top Heading Keywords per Organisation",
                 fontsize=12, fontweight="bold", y=1.01)
    axes_flat = axes.flatten() if hasattr(axes, "flatten") else [axes]

    for idx, (folder, label) in enumerate(orgs):
        ax = axes_flat[idx]
        df = data[folder]
        words = top_heading_words(df)
        if not words:
            ax.set_visible(False)
            continue
        terms, counts = zip(*words)
        ax.barh(terms[::-1], counts[::-1], color="#16a085", edgecolor="white")
        ax.set_title(label, fontsize=9, fontweight="bold")
        ax.spines[["top","right","bottom"]].set_visible(False)
        ax.tick_params(axis="y", labelsize=7.5)
        ax.tick_params(axis="x", labelsize=7)

    for idx in range(len(orgs), len(axes_flat)):
        axes_flat[idx].set_visible(False)

    plt.tight_layout()
    return fig_to_b64(fig)


# ── Numeric summary table ─────────────────────────────────────────────────────
_CORPUS_START_YEAR = 1998
_CORPUS_END_YEAR   = 2025


def _fmt_date_range(d_min: pd.Timestamp, d_max: pd.Timestamp) -> str:
    """Return an HTML date-range string, bolding whichever end touches the corpus boundary."""
    start = str(d_min.date())
    end   = str(d_max.date())
    start_html = start if d_min.year == _CORPUS_START_YEAR else f"<b>{start}</b>"
    end_html   = end   if d_max.year == _CORPUS_END_YEAR   else f"<b>{end}</b>"
    return f"{start_html} – {end_html}"


def build_summary_table(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for folder, label in ORG_META.items():
        if folder not in data:
            continue
        df = data[folder]
        df2 = df[df["in_gov"].isin(["0","1",0,1])].copy()
        df2["in_gov"] = df2["in_gov"].astype(str)
        gov_pct = (df2["in_gov"] == "1").mean() * 100 if len(df2) else 0.0
        spk_counts = df.groupby("unique_id").size().values

        def flag_pct(col: str) -> float:
            return (df[col].astype(str).isin(["1","1.0","True","true"])).mean() * 100

        rows.append({
            "Organisation":       label,
            "Speech turns":       len(df),
            "Total mentions":     int(df["mention_count"].sum()),
            "Avg mentions/turn":  round(float(df["mention_count"].mean()), 2),
            "Unique speakers":    df["unique_id"].nunique(),
            "Gini":               round(gini(spk_counts.tolist()), 3),
            "Senate %":           round((df["chamber"] == "senate").mean() * 100, 1),
            "Gov speaker %":      round(gov_pct, 1),
            "Greens %":           round((df["group"] == "Greens").mean() * 100, 1),
            "Labor %":            round((df["group"] == "Labor").mean() * 100, 1),
            "Coalition %":        round((df["group"] == "Coalition").mean() * 100, 1),
            "Q or Answer %":      round(flag_pct("is_question") + flag_pct("is_answer"), 1),
            "Date range":         _fmt_date_range(df['date'].min(), df['date'].max()),
        })
    return pd.DataFrame(rows).sort_values("Speech turns", ascending=False).reset_index(drop=True)


def df_to_html_table(df: pd.DataFrame) -> str:
    def colour_cell(val, col):
        if col in ("Gini",):
            v = float(val)
            # High Gini = red (concentrated)
            r = int(255 * min(v / 0.9, 1))
            g = int(255 * (1 - min(v / 0.9, 1)))
            return f"background-color: rgb({r},{g},80); color: white;" if v > 0.5 else ""
        if col == "Greens %":
            v = float(val)
            intensity = min(int(v * 3), 200)
            return f"background-color: rgba(0,{100+intensity},0,0.3);" if v > 15 else ""
        if col == "Coalition %":
            v = float(val)
            intensity = min(int(v / 40 * 200), 200)
            return f"background-color: rgba(0,0,{100+intensity},0.25);" if v > 25 else ""
        if col == "Labor %":
            v = float(val)
            return f"background-color: rgba(200,0,0,0.2);" if v > 30 else ""
        if col == "Q or Answer %":
            v = float(val)
            return f"background-color: rgba(150,80,0,0.25);" if v > 20 else ""
        return ""

    thead = "<thead><tr>" + "".join(f"<th>{c}</th>" for c in df.columns) + "</tr></thead>"
    tbody_rows = []
    for _, row in df.iterrows():
        cells = []
        for col, val in row.items():
            style = colour_cell(val, col)
            cells.append(f'<td style="{style}">{val}</td>')
        tbody_rows.append("<tr>" + "".join(cells) + "</tr>")
    tbody = "<tbody>" + "".join(tbody_rows) + "</tbody>"
    return f'<table class="summary">{thead}{tbody}</table>'


# ── HTML assembly ─────────────────────────────────────────────────────────────
_HTML_STYLE = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       max-width: 1400px; margin: 0 auto; padding: 2em; background: #fafafa; color: #222; }
h1 { font-size: 1.8em; color: #1a3a5c; border-bottom: 3px solid #2980b9; padding-bottom: 0.3em; }
h2 { font-size: 1.25em; color: #2c3e50; margin-top: 2.5em; border-left: 4px solid #2980b9;
     padding-left: 0.5em; }
p, li { line-height: 1.6; font-size: 0.95em; }
.summary { border-collapse: collapse; width: 100%; font-size: 0.82em; margin: 1em 0; }
.summary th { background: #2c3e50; color: white; padding: 6px 10px; text-align: left;
              font-weight: 600; white-space: nowrap; }
.summary td { padding: 5px 10px; border-bottom: 1px solid #e0e0e0; white-space: nowrap; }
.summary tr:hover { background: #f0f4f8; }
.note { font-size: 0.8em; color: #666; font-style: italic; margin-top: 0.3em; }
.section { background: white; border-radius: 6px; padding: 1.5em 2em;
           box-shadow: 0 1px 4px rgba(0,0,0,0.08); margin-bottom: 2em; }
"""

def build_html(charts: dict, summary_table: str, summary_df: pd.DataFrame) -> str:
    now      = pd.Timestamp.now().strftime("%d %B %Y")
    idx      = summary_df.set_index("Organisation")
    org_list = "\n".join(
        f"<li><b>{v}</b> — {idx.at[v,'Speech turns']:,} speech turns"
        f" ({idx.at[v,'Date range']})</li>"
        for v in summary_df["Organisation"]
        if v in idx.index
    )
    type_label = _TYPE_LABEL

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{type_label} — Parliamentary Salience 1998–2025</title>
  <style>{_HTML_STYLE}</style>
</head>
<body>
<h1>{type_label} — Parliamentary Salience Analysis<br>
<small style="font-size:0.55em;color:#666;">Hansard corpus 1998–2025 · Senate + House of Representatives · {now}</small></h1>

<div class="section">
<h2>About this analysis</h2>
<p>This report analyses how 15 major Australian environmental and conservation NGOs are mentioned
in parliamentary speech across the Senate and House of Representatives from 1998 to 2025.
All analyses are derived directly from the Hansard corpus without LLM annotation — using only
speaker metadata (party, in_gov, gender, chamber), speech-act flags (question, answer, interjection,
question-on-notice), and the text itself.</p>
<p><b>Key metrics:</b></p>
<ul>
  <li><b>Speech turns</b>: rows in the corpus where the org is mentioned at least once</li>
  <li><b>Total mentions</b>: sum of all alias occurrences across matched rows</li>
  <li><b>Avg mentions/turn</b>: focus density — how central is the org to the speech it appears in?</li>
  <li><b>Gini coefficient</b>: speaker concentration (0 = equal spread, 1 = one speaker dominates)</li>
</ul>
<p><b>Organisations covered:</b></p>
<ul>{org_list}</ul>
<p class="note">Note: 'ACF' alias may include rare non-ACF uses; 'Landcare' includes government Landcare program references.
'Climate Council' includes the Gillard government's Climate Commission (2011–2013).</p>
</div>

<div class="section">
<h2>1. Summary table</h2>
{summary_table}
</div>

<div class="section">
<h2>2. Landscape overview — speech turns and total mentions</h2>
<p>ACF and Landcare dominate in raw speech turns; Landcare's very high total-mention count
and avg mentions/turn reflects sustained, dedicated debate (agricultural/land policy).
EDO's high avg mentions/turn (2.6) indicates focused legal/regulatory debate.</p>
{img_tag(charts['landscape'], "Landscape overview")}
</div>

<div class="section">
<h2>3. Parliamentary salience over time (rate-normalised)</h2>
<p>Mentions per 10,000 corpus speech turns — controls for variation in parliamentary sitting time
across years. Parliament boundaries marked with vertical dashed lines.</p>
{img_tag(charts['temporal'], "Temporal salience")}
</div>

<div class="section">
<h2>4. Party alignment</h2>
<p>Sorted by Greens+Labor share (most progressive-aligned at top).
Dashed lines show approximate corpus averages for Coalition and Coalition+Labor.
Orgs appearing above their corpus-average coalition line are disproportionately
championed/scrutinised by the Coalition.</p>
{img_tag(charts['party'], "Party alignment")}
</div>

<div class="section">
<h2>5. Government vs opposition engagement</h2>
<p>Share of speech turns where the speaker was in government vs opposition.
Orgs with high government-speaker share are being discussed from the despatch box —
either defended or challenged by ministers.</p>
{img_tag(charts['gov_opp'], "Government vs opposition")}
</div>

<div class="section">
<h2>6. Chamber preference</h2>
<p>The Senate (with its committee system and crossbench presence) tends to engage more
with conservation/legal advocacy orgs; the House reflects government-side energy/program
orgs more.</p>
{img_tag(charts['chamber'], "Chamber preference")}
</div>

<div class="section">
<h2>7. Speech mode — how MPs engage</h2>
<p>Prepared speeches = MPs choosing to cite the org. Questions = scrutiny. Answers = government
position. Q-on-notice = formal inquiry. Interjections = rhetorical use in the heat of debate.</p>
{img_tag(charts['speech_mode'], "Speech mode")}
</div>

<div class="section">
<h2>8. Speaker concentration</h2>
<p>High Gini = a small group of MPs drive most mentions (fragile coalition).
Low Gini = broad cross-chamber interest (more durable). Bubble size in the scatter
is proportional to Gini.</p>
{img_tag(charts['concentration'], "Speaker concentration")}
</div>

<div class="section">
<h2>9. Focus density — mentions per speech turn vs total volume</h2>
<p>Top-right quadrant: high parliamentary volume AND high focus (org is the subject of debate).
Bottom-right: high volume but low focus (org mentioned in passing). Top-left: low volume but
very focused when mentioned (specialist/niche orgs).</p>
{img_tag(charts['focus'], "Focus density")}
</div>

<div class="section">
<h2>10. Parliamentary activity heatmap</h2>
<p>Each cell shows the raw speech turn count; rows are normalised to 100% so peak
parliamentary periods are visible regardless of org size. Australian federal parliaments
37–46 shown (1998–2025).</p>
{img_tag(charts['parliament'], "Parliamentary heatmap")}
</div>

<div class="section">
<h2>11. Debate terrain — top heading keywords</h2>
<p>Most frequent words in the <em>debate headings</em> under which each org appears.
Reveals the policy domains each org is attached to without any text classification.</p>
{img_tag(charts['terrain'], "Debate terrain")}
</div>

</body>
</html>"""


# ── Core report generation (callable from parallel runner) ────────────────────
def generate_report(type_key: str, output_path: Path) -> None:
    """Generate a comparative report for one org type."""
    from org_types_config import ORG_TYPES

    if type_key not in ORG_TYPES:
        print(f"Unknown org type: {type_key!r}", file=sys.stderr)
        return

    org_type = ORG_TYPES[type_key]
    org_meta = {org.folder: org.display for org in org_type.orgs}
    set_org_meta(org_meta, org_type.label)

    print(f"[{type_key}] Loading data …")
    data = load_all()
    if not data:
        print(f"[{type_key}] No data found — skipping.", file=sys.stderr)
        return
    print(f"[{type_key}] {len(data)} orgs loaded")

    charts: dict[str, str] = {}
    charts["landscape"]    = chart_landscape(data)
    charts["temporal"]     = chart_temporal(data)
    charts["party"], _     = chart_party(data)
    charts["gov_opp"]      = chart_gov_opp(data)
    charts["chamber"]      = chart_chamber(data)
    charts["speech_mode"]  = chart_speech_mode(data)
    charts["concentration"]= chart_concentration(data)
    charts["focus"]        = chart_focus(data)
    charts["parliament"]   = chart_parliament(data)
    charts["terrain"]      = chart_debate_terrain(data)

    summary_df    = build_summary_table(data)
    summary_table = df_to_html_table(summary_df)

    html = build_html(charts, summary_table, summary_df)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"[{type_key}] Report → {output_path}")


# ── CLI entry point ───────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Generate org-type comparative report.")
    ap.add_argument("--type", default="environmental_ngos", metavar="TYPE",
                    help="Org type key from org_types_config (default: environmental_ngos)")
    ap.add_argument("--output", type=Path, default=None, metavar="PATH",
                    help="Output HTML path (default: case_studies/reports/<type>_report.html)")
    args = ap.parse_args()

    out = args.output or (CASE / "reports" / f"{args.type}_report.html")
    generate_report(args.type, out)


if __name__ == "__main__":
    main()
