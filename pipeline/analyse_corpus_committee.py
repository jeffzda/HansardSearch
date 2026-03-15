#!/usr/bin/env python3
"""
analyse_corpus_committee.py — No-LLM structural analysis of the full committee Hansard corpus.

Produces a self-contained HTML report with charts covering hearing volume, committee
activity, witness/member balance, party dynamics, estimates portfolios, and top inquiries.

Usage:
    python analyse_corpus_committee.py \
        --corpus ../data/output/committee/corpus/committee_hansard_corpus_2010_to_2025.parquet \
        --out    ../data/output/committee/corpus/corpus_report.html
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
DEFAULT_CORPUS = ROOT / "data/output/committee/corpus/committee_hansard_corpus_2010_to_2025.parquet"

PARTY_COLOURS = {
    "Coalition":  "#003087",
    "Labor":      "#E3231F",
    "Greens":     "#009B55",
    "Nationals":  "#006633",
    "Other":      "#888888",
    "Witness":    "#BDBDBD",
}
CHAMBER_COLOURS = {
    "Senate":              "#003087",
    "Joint":               "#9C27B0",
    "House of Reps":       "#E3231F",
    "House of Representatives": "#E3231F",
}
TYPE_COLOURS = {
    "committee": "#2196F3",
    "estimates":  "#FF9800",
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
        "axes.spines.top":    False,
        "axes.spines.right":  False,
        "axes.grid":          True,
        "grid.alpha":         0.3,
        "font.family":        "sans-serif",
        "font.size":          11,
    })


def map_party_group(party) -> str:
    if pd.isna(party) or str(party).strip() == "":
        return "Witness"
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


def normalise_chamber(c) -> str:
    s = str(c).strip()
    if "Joint" in s:
        return "Joint"
    if "Senate" in s:
        return "Senate"
    return "House of Reps"


# ── Charts ────────────────────────────────────────────────────────────────────

def chart_volume_by_year(df: pd.DataFrame) -> str:
    """Stacked bar: speech turns per year, committee vs estimates."""
    apply_style()
    yearly = df.groupby(["year", "hearing_type"]).size().unstack(fill_value=0)
    for t in ("committee", "estimates"):
        if t not in yearly.columns:
            yearly[t] = 0
    yearly = yearly[["committee", "estimates"]]

    fig, ax = plt.subplots(figsize=(13, 5))
    bottom = np.zeros(len(yearly))
    for htype, col in TYPE_COLOURS.items():
        vals = yearly[htype].values
        ax.bar(yearly.index, vals, bottom=bottom, color=col,
               label=htype.title(), alpha=0.9, width=0.7)
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Speech turns")
    ax.set_title("Committee Hansard Volume by Year", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_chamber_split(df: pd.DataFrame) -> str:
    """Stacked bar: Senate / Joint / House split per year."""
    apply_style()
    yearly = df.groupby(["year", "chamber_clean"]).size().unstack(fill_value=0)
    chambers = ["Senate", "Joint", "House of Reps"]
    for c in chambers:
        if c not in yearly.columns:
            yearly[c] = 0
    yearly = yearly[chambers]

    fig, ax = plt.subplots(figsize=(13, 5))
    bottom = np.zeros(len(yearly))
    for ch in chambers:
        col = CHAMBER_COLOURS.get(ch, "#888")
        vals = yearly[ch].values
        ax.bar(yearly.index, vals, bottom=bottom, color=col,
               label=ch, alpha=0.9, width=0.7)
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Speech turns")
    ax.set_title("Chamber Composition Over Time", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_witness_balance(df: pd.DataFrame) -> str:
    """Line chart: % witness speech turns per year."""
    apply_style()
    yr = df.groupby("year")["witness_flag"].agg(["sum", "count"])
    yr["pct_witness"] = yr["sum"] / yr["count"] * 100

    fig, ax = plt.subplots(figsize=(13, 4))
    ax.fill_between(yr.index, yr["pct_witness"], alpha=0.25, color="#2196F3")
    ax.plot(yr.index, yr["pct_witness"], color="#2196F3", linewidth=2)
    ax.axhline(yr["pct_witness"].mean(), color="#888", linestyle="--",
               linewidth=1, label=f"Mean {yr['pct_witness'].mean():.1f}%")
    ax.set_ylim(0, 100)
    ax.set_xlabel("Year")
    ax.set_ylabel("% of speech turns")
    ax.set_title("Witness Speech Turns as % of Total per Year", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_party_activity(df: pd.DataFrame) -> str:
    """Stacked bar: member speech turns by party group per year (witnesses excluded)."""
    apply_style()
    members = df[df["witness_flag"] == 0].copy()
    members["party_group"] = members["party"].apply(map_party_group)
    members = members[members["party_group"] != "Witness"]

    yearly = members.groupby(["year", "party_group"]).size().unstack(fill_value=0)
    groups = [g for g in ["Coalition", "Labor", "Greens", "Nationals", "Other"]
              if g in yearly.columns]
    yearly = yearly[groups]

    fig, ax = plt.subplots(figsize=(13, 5))
    bottom = np.zeros(len(yearly))
    for g in groups:
        col = PARTY_COLOURS.get(g, "#888")
        vals = yearly[g].values
        ax.bar(yearly.index, vals, bottom=bottom, color=col,
               label=g, alpha=0.9, width=0.7)
        bottom += vals

    ax.set_xlabel("Year")
    ax.set_ylabel("Member speech turns")
    ax.set_title("Member Participation by Party Group Over Time", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_top_committees(df: pd.DataFrame) -> str:
    """Horizontal bar: top 25 committees by speech turns."""
    apply_style()
    counts = (
        df.groupby("committee_name").size()
        .sort_values(ascending=False)
        .head(25)
    )
    # Truncate long names
    labels = [str(n)[:60] + ("…" if len(str(n)) > 60 else "") for n in counts.index]

    fig, ax = plt.subplots(figsize=(11, 9))
    bars = ax.barh(range(len(counts)), counts.values, color="#2196F3", alpha=0.85)
    ax.set_yticks(range(len(counts)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Speech turns")
    ax.set_title("Top 25 Committees by Speech Volume", fontsize=13, fontweight="bold")
    for bar, val in zip(bars, counts.values):
        ax.text(bar.get_width() + counts.values.max() * 0.005, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=8)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_top_portfolios(df: pd.DataFrame) -> str:
    """Horizontal bar: top 20 estimates portfolios by speech turns."""
    apply_style()
    est = df[(df["hearing_type"] == "estimates") & df["portfolio"].notna() & (df["portfolio"] != "")]
    counts = (
        est.groupby("portfolio").size()
        .sort_values(ascending=False)
        .head(20)
    )
    labels = [str(n)[:60] + ("…" if len(str(n)) > 60 else "") for n in counts.index]

    fig, ax = plt.subplots(figsize=(11, 7))
    bars = ax.barh(range(len(counts)), counts.values, color="#FF9800", alpha=0.85)
    ax.set_yticks(range(len(counts)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Speech turns")
    ax.set_title("Top 20 Estimates Portfolios by Speech Volume", fontsize=13, fontweight="bold")
    for bar, val in zip(bars, counts.values):
        ax.text(bar.get_width() + counts.values.max() * 0.005, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=8)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_top_inquiries(df: pd.DataFrame) -> str:
    """Horizontal bar: top 25 inquiry references by speech turns (committee hearings only)."""
    apply_style()
    comm = df[(df["hearing_type"] == "committee") & df["reference"].notna() & (df["reference"] != "")]
    counts = (
        comm.groupby("reference").size()
        .sort_values(ascending=False)
        .head(25)
    )
    labels = [str(n)[:70] + ("…" if len(str(n)) > 70 else "") for n in counts.index]

    fig, ax = plt.subplots(figsize=(12, 9))
    bars = ax.barh(range(len(counts)), counts.values, color="#4CAF50", alpha=0.85)
    ax.set_yticks(range(len(counts)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("Speech turns")
    ax.set_title("Top 25 Inquiries by Speech Volume", fontsize=13, fontweight="bold")
    for bar, val in zip(bars, counts.values):
        ax.text(bar.get_width() + counts.values.max() * 0.005, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=8)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_top_member_speakers(df: pd.DataFrame) -> str:
    """Horizontal bar: top 30 member speakers (non-witness) by speech turns."""
    apply_style()
    members = df[(df["witness_flag"] == 0) & df["name"].notna() & (df["name"] != "")]
    # Exclude procedural names
    procedural = {"The Chair", "CHAIR", "Chair", "The CHAIR", "Deputy Chair",
                  "The Deputy Chair", "Acting Chair"}
    members = members[~members["name"].isin(procedural)]
    counts = members.groupby("name").size().sort_values(ascending=False).head(30)

    # Annotate with party
    party_map = (
        members.groupby("name")["party"]
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else "")
    )

    fig, ax = plt.subplots(figsize=(11, 10))
    colours = [PARTY_COLOURS.get(map_party_group(party_map.get(n, "")), "#888")
               for n in counts.index]
    bars = ax.barh(range(len(counts)), counts.values, color=colours, alpha=0.85)
    labels = [f"{n} ({party_map.get(n, '?')})" for n in counts.index]
    ax.set_yticks(range(len(counts)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Speech turns")
    ax.set_title("Top 30 Member Speakers in Committee Hearings", fontsize=13, fontweight="bold")
    for bar, val in zip(bars, counts.values):
        ax.text(bar.get_width() + counts.values.max() * 0.005, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=8)
    plt.tight_layout()
    return fig_to_b64(fig)


def chart_ingov_balance(df: pd.DataFrame) -> str:
    """Line chart: % of member speech turns from government vs opposition per year."""
    apply_style()
    members = df[df["witness_flag"] == 0].copy()
    yr = members.groupby(["year", "in_gov"]).size().unstack(fill_value=0)
    for v in (0, 1):
        if v not in yr.columns:
            yr[v] = 0
    yr["total"] = yr[0] + yr[1]
    yr["pct_gov"] = yr[1] / yr["total"] * 100

    fig, ax = plt.subplots(figsize=(13, 4))
    ax.fill_between(yr.index, yr["pct_gov"], 50, where=yr["pct_gov"] >= 50,
                    alpha=0.2, color="#003087", label="Government majority")
    ax.fill_between(yr.index, yr["pct_gov"], 50, where=yr["pct_gov"] < 50,
                    alpha=0.2, color="#E3231F", label="Opposition majority")
    ax.plot(yr.index, yr["pct_gov"], color="#1a1a1a", linewidth=2)
    ax.axhline(50, color="#888", linestyle="--", linewidth=1)
    ax.set_ylim(0, 100)
    ax.set_xlabel("Year")
    ax.set_ylabel("% of member turns")
    ax.set_title("Government vs Opposition Share of Member Participation", fontsize=13, fontweight="bold")
    ax.legend()
    plt.tight_layout()
    return fig_to_b64(fig)


# ── Speakers table ─────────────────────────────────────────────────────────────

def build_speakers_table(df: pd.DataFrame) -> str:
    members = df[(df["witness_flag"] == 0)].copy()
    procedural = {"The Chair", "CHAIR", "Chair", "The CHAIR", "Deputy Chair",
                  "The Deputy Chair", "Acting Chair"}
    members = members[~members["name"].isin(procedural)]

    top = members.groupby("name").agg(
        turns=("name", "count"),
        party=("party", lambda x: x.mode().iloc[0] if not x.mode().empty else ""),
        committees=("committee_name", "nunique"),
        hearings=("date", "nunique"),
    ).sort_values("turns", ascending=False).head(40)

    rows = ""
    for name, r in top.iterrows():
        pg = map_party_group(r["party"])
        col = PARTY_COLOURS.get(pg, "#888")
        rows += (f"<tr><td>{name}</td>"
                 f"<td style='color:{col};font-weight:600'>{r['party']}</td>"
                 f"<td style='text-align:right'>{r['turns']:,}</td>"
                 f"<td style='text-align:right'>{r['committees']}</td>"
                 f"<td style='text-align:right'>{r['hearings']}</td></tr>\n")

    return f"""
    <table class="speakers-table">
      <thead><tr>
        <th>Speaker</th><th>Party</th><th>Turns</th>
        <th>Committees</th><th>Sitting days</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


# ── HTML assembly ──────────────────────────────────────────────────────────────

def build_html(stats: dict, charts: dict, speakers_table: str) -> str:
    today = datetime.date.today().strftime("%d %B %Y")

    def stat_box(label, value):
        return f"""
        <div class="stat-box">
          <div class="stat-value">{value}</div>
          <div class="stat-label">{label}</div>
        </div>"""

    stat_boxes = "".join([
        stat_box("Total speech turns", f"{stats['total']:,}"),
        stat_box("Committee / Estimates", f"{stats['n_committee']:,} / {stats['n_estimates']:,}"),
        stat_box("Sitting days", f"{stats['n_days']:,}"),
        stat_box("Date range", stats["date_range"]),
        stat_box("Unique committees", f"{stats['n_committees']:,}"),
        stat_box("Member / Witness turns", f"{stats['n_members']:,} / {stats['n_witnesses']:,}"),
    ])

    def section(title, chart_b64, caption, chart_id=""):
        img_tag = (f'<img src="data:image/png;base64,{chart_b64}" '
                   f'alt="{title}" style="max-width:100%;height:auto;">')
        return f"""
        <section class="chart-section" id="{chart_id}">
          <h3>{title}</h3>
          <div class="chart-wrap">{img_tag}</div>
          <p class="caption">{caption}</p>
        </section>"""

    chart_sections = "".join([
        section("Hearing Volume by Year",
                charts["volume"],
                f"Total speech turns per year, split by hearing type. "
                f"Peak year: {stats['peak_year']} ({stats['peak_count']:,} turns).",
                "volume"),
        section("Chamber Composition Over Time",
                charts["chamber"],
                "Senate committees have consistently dominated committee Hansard volume, "
                "with Joint committees making up most of the remainder.",
                "chamber"),
        section("Witness vs Member Balance",
                charts["witness"],
                f"Witness testimony accounts for {stats['pct_witness']:.1f}% of all speech turns overall. "
                f"Variation by year reflects the mix of inquiry hearings vs estimates.",
                "witness"),
        section("Member Participation by Party Group",
                charts["party"],
                "Member speech turns only (witnesses excluded). "
                "Party share reflects committee membership, not vote counts.",
                "party"),
        section("Government vs Opposition Participation Share",
                charts["ingov"],
                "Proportion of member speech turns from government vs opposition members per year. "
                "Majority committees produce government-dominant years; minority periods flip this.",
                "ingov"),
        section("Top 25 Committees by Volume",
                charts["committees"],
                "Committees ranked by total speech turns across all hearings in the corpus.",
                "committees"),
        section("Top 25 Inquiries by Volume",
                charts["inquiries"],
                "Committee hearing inquiries ranked by total speech turns. "
                "Long-running or high-profile inquiries generate disproportionate volume.",
                "inquiries"),
        section("Top 20 Estimates Portfolios",
                charts["portfolios"],
                "Senate Estimates portfolio blocks ranked by speech volume. "
                "Larger portfolios and contested policy areas generate more testimony.",
                "portfolios"),
        section("Top 30 Member Speakers",
                charts["speakers"],
                "Most active committee members by total speech turns (chair/procedural names excluded). "
                "Colour indicates party affiliation.",
                "members"),
    ])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Australian Parliamentary Committee Hansard — Corpus Analysis</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
          background: #f5f5f5; color: #1a1a1a; line-height: 1.6; }}
  .header {{ background: #1a1a1a; color: white; padding: 2.5rem 3rem; }}
  .header h1 {{ font-size: 1.8rem; font-weight: 700; margin-bottom: 0.4rem; }}
  .header .subtitle {{ color: #aaa; font-size: 1rem; }}
  .stats-banner {{ background: white; border-bottom: 2px solid #e0e0e0;
                   padding: 1.5rem 3rem; display: flex; gap: 1.5rem; flex-wrap: wrap; }}
  .stat-box {{ background: #f9f9f9; border: 1px solid #e0e0e0; border-radius: 8px;
               padding: 1rem 1.4rem; min-width: 150px; }}
  .stat-value {{ font-size: 1.5rem; font-weight: 700; color: #1a1a1a; }}
  .stat-label {{ font-size: 0.78rem; color: #666; margin-top: 2px; text-transform: uppercase;
                 letter-spacing: 0.05em; }}
  .content {{ max-width: 1200px; margin: 0 auto; padding: 2rem 3rem; }}
  .chart-section {{ background: white; border-radius: 10px; padding: 1.8rem;
                    margin-bottom: 2rem; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  .chart-section h3 {{ font-size: 1.1rem; font-weight: 600; margin-bottom: 1rem; color: #1a1a1a; }}
  .chart-wrap {{ text-align: center; margin-bottom: 0.8rem; }}
  .caption {{ font-size: 0.85rem; color: #666; font-style: italic; }}
  .speakers-table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; margin-top: 1rem; }}
  .speakers-table th {{ background: #f0f0f0; padding: 0.5rem 0.8rem; text-align: left;
                        font-weight: 600; border-bottom: 2px solid #ddd; }}
  .speakers-table td {{ padding: 0.45rem 0.8rem; border-bottom: 1px solid #eee; }}
  .speakers-table tr:hover td {{ background: #fafafa; }}
  .footer {{ text-align: center; color: #999; font-size: 0.8rem; padding: 2rem; }}
  h2 {{ font-size: 1.35rem; margin: 2.5rem 0 1rem; color: #1a1a1a; border-bottom: 2px solid #e0e0e0;
        padding-bottom: 0.4rem; }}
</style>
</head>
<body>
<div class="header">
  <h1>Australian Parliamentary Committee Hansard</h1>
  <div class="subtitle">Corpus Analysis — {stats['date_range']} &nbsp;·&nbsp; Generated {today}</div>
</div>
<div class="stats-banner">{stat_boxes}</div>
<div class="content">
  <h2>Volume &amp; Composition</h2>
  {chart_sections}
  <h2>Top Member Speakers</h2>
  <div class="chart-section">
    <h3>Most Active Committee Members (Top 40)</h3>
    {speakers_table}
    <p class="caption" style="margin-top:0.8rem">
      Ranked by total speech turns across all committee hearings and estimates.
      Party colours: <span style="color:#003087">■ Coalition</span>
      <span style="color:#E3231F"> ■ Labor</span>
      <span style="color:#009B55"> ■ Greens</span>
      <span style="color:#006633"> ■ Nationals</span>.
    </p>
  </div>
</div>
<div class="footer">
  Australian Parliamentary Committee Hansard corpus analysis · {today} ·
  Data: 2010–2025 ({stats['n_days']:,} sitting days)
</div>
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="No-LLM structural analysis of committee Hansard corpus.")
    ap.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    ap.add_argument("--out",    type=Path, required=True)
    args = ap.parse_args()

    print(f"Loading corpus: {args.corpus}", flush=True)
    df = pd.read_parquet(args.corpus)
    print(f"  {len(df):,} rows loaded.", flush=True)

    # ── Prepare ───────────────────────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = df["date"].dt.year.astype("Int64")
    df["chamber_clean"] = df["committee_chamber"].apply(normalise_chamber)
    df["witness_flag"]  = pd.to_numeric(df["witness_flag"], errors="coerce").fillna(0).astype(int)

    # ── Stats banner ──────────────────────────────────────────────────────────
    yr_counts = df.groupby("year").size()
    peak_year = int(yr_counts.idxmax())
    stats = {
        "total":         len(df),
        "n_committee":   int((df["hearing_type"] == "committee").sum()),
        "n_estimates":   int((df["hearing_type"] == "estimates").sum()),
        "n_days":        df["date"].nunique(),
        "n_committees":  df["committee_name"].nunique(),
        "n_members":     int((df["witness_flag"] == 0).sum()),
        "n_witnesses":   int((df["witness_flag"] == 1).sum()),
        "pct_witness":   df["witness_flag"].mean() * 100,
        "date_range":    f"{df['date'].min().year}–{df['date'].max().year}",
        "peak_year":     peak_year,
        "peak_count":    int(yr_counts[peak_year]),
    }

    # ── Charts ────────────────────────────────────────────────────────────────
    print("Generating charts...", flush=True)
    charts = {}
    charts["volume"]     = chart_volume_by_year(df);     print("  volume done")
    charts["chamber"]    = chart_chamber_split(df);      print("  chamber done")
    charts["witness"]    = chart_witness_balance(df);    print("  witness done")
    charts["party"]      = chart_party_activity(df);     print("  party done")
    charts["ingov"]      = chart_ingov_balance(df);      print("  ingov done")
    charts["committees"] = chart_top_committees(df);     print("  committees done")
    charts["inquiries"]  = chart_top_inquiries(df);      print("  inquiries done")
    charts["portfolios"] = chart_top_portfolios(df);     print("  portfolios done")
    charts["speakers"]   = chart_top_member_speakers(df);print("  speakers done")

    speakers_table = build_speakers_table(df)
    print("  speakers table done")

    # ── Assemble ──────────────────────────────────────────────────────────────
    html = build_html(stats, charts, speakers_table)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")
    print(f"\nReport written to: {args.out}  ({args.out.stat().st_size // 1024:,} KB)")


if __name__ == "__main__":
    main()
