"""
10_compare_ka.py — Quantitative comparison of our House corpus vs Katz & Alexander (2023).

Outputs key metrics covering:
  - Row counts and temporal coverage
  - Metadata completeness (null rates per field)
  - Word count parity by year
  - Speaker coverage
  - Stage direction / procedural row analysis

Usage:
    python 10_compare_ka.py \
        --our-corpus  ../data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet \
        --ka-corpus   ../data/ka_corpus/hansard-corpus/hansard_corpus_1998_to_2022.parquet \
        [--out-csv    ../data/output/house/ka_comparison.csv]
        [--year-csv   ../data/output/house/ka_comparison_by_year.csv]
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


# ── Column name normalisation ──────────────────────────────────────────────────

# K&A uses dots; we use underscores.  Map K&A → canonical.
_KA_RENAME = {
    "page.no":      "page_no",
    "time.stamp":   "time_stamp",
    "name.id":      "name_id",
    "in.gov":       "in_gov",
    "first.speech": "first_speech",
    "uniqueID":     "unique_id",
}

# Fields to check for null-rate comparison (canonical names)
_METADATA_FIELDS = [
    "name_id", "unique_id", "gender", "party", "electorate",
    "partyfacts_id", "page_no", "time_stamp",
]

# ── K&A known benchmark values (from their paper / Zenodo readme) ──────────────
_KA_PAPER_ROWS = 586_830
_KA_PAPER_DAYS = 1_245   # approximate (1998-2022 House sitting days)


# ── Helpers ───────────────────────────────────────────────────────────────────

def load(path: Path, label: str) -> pd.DataFrame:
    print(f"Loading {label} … ", end="", flush=True)
    df = pd.read_parquet(path) if str(path).endswith(".parquet") else pd.read_csv(path, dtype=str)
    print(f"{len(df):,} rows")
    return df


def normalise_ka(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=_KA_RENAME)


def word_count(series: pd.Series) -> int:
    """Approximate word count for a body column."""
    return series.dropna().str.split().str.len().sum()


def pct_nonnull(series: pd.Series) -> float:
    return 100.0 * series.notna().mean()


def _print_section(title: str) -> None:
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


# ── Metric blocks ─────────────────────────────────────────────────────────────

def section_overview(our: pd.DataFrame, ka: pd.DataFrame) -> dict:
    _print_section("1. Overview")

    our_dates = pd.to_datetime(our["date"], errors="coerce")
    ka_dates  = pd.to_datetime(ka["date"],  errors="coerce")

    our_days = our_dates.dt.date.nunique()
    ka_days  = ka_dates.dt.date.nunique()

    metrics = {
        "our_rows":        len(our),
        "ka_rows":         len(ka),
        "our_sitting_days": our_days,
        "ka_sitting_days":  ka_days,
        "our_date_min":    str(our_dates.min().date()),
        "our_date_max":    str(our_dates.max().date()),
        "ka_date_min":     str(ka_dates.min().date()),
        "ka_date_max":     str(ka_dates.max().date()),
    }

    print(f"  {'':30s}  {'Ours':>12s}  {'K&A':>12s}")
    print(f"  {'Rows':30s}  {metrics['our_rows']:>12,}  {metrics['ka_rows']:>12,}")
    print(f"  {'Sitting days':30s}  {metrics['our_sitting_days']:>12,}  {metrics['ka_sitting_days']:>12,}")
    print(f"  {'Date min':30s}  {metrics['our_date_min']:>12s}  {metrics['ka_date_min']:>12s}")
    print(f"  {'Date max':30s}  {metrics['our_date_max']:>12s}  {metrics['ka_date_max']:>12s}")
    return metrics


def section_metadata(our: pd.DataFrame, ka: pd.DataFrame) -> dict:
    _print_section("2. Metadata Completeness (% non-null)")

    metrics = {}
    print(f"  {'Field':25s}  {'Ours':>8s}  {'K&A':>8s}  {'Delta':>8s}")
    print(f"  {'-'*25}  {'-'*8}  {'-'*8}  {'-'*8}")

    for col in _METADATA_FIELDS:
        our_pct = pct_nonnull(our[col]) if col in our.columns else float("nan")
        ka_pct  = pct_nonnull(ka[col])  if col in ka.columns  else float("nan")
        delta   = our_pct - ka_pct if not (np.isnan(our_pct) or np.isnan(ka_pct)) else float("nan")

        our_s   = f"{our_pct:7.1f}%" if not np.isnan(our_pct) else "     N/A"
        ka_s    = f"{ka_pct:7.1f}%"  if not np.isnan(ka_pct)  else "     N/A"
        delta_s = f"{delta:+7.1f}%"  if not np.isnan(delta)   else "     N/A"

        print(f"  {col:25s}  {our_s:>8s}  {ka_s:>8s}  {delta_s:>8s}")
        metrics[f"pct_nonnull_our_{col}"]   = round(our_pct, 2)
        metrics[f"pct_nonnull_ka_{col}"]    = round(ka_pct, 2)
        metrics[f"pct_nonnull_delta_{col}"] = round(delta, 2)

    return metrics


def section_overlap(our: pd.DataFrame, ka: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Restrict both corpora to their common date range for fair comparisons."""
    _print_section("3. Overlap Period (shared date range)")

    our_dates = pd.to_datetime(our["date"], errors="coerce").dt.date
    ka_dates  = pd.to_datetime(ka["date"],  errors="coerce").dt.date

    overlap_min = max(our_dates.min(), ka_dates.min())
    overlap_max = min(our_dates.max(), ka_dates.max())

    our_ov = our[our_dates.between(overlap_min, overlap_max)].copy()
    ka_ov  = ka[ka_dates.between(overlap_min, overlap_max)].copy()

    metrics = {
        "overlap_start":    str(overlap_min),
        "overlap_end":      str(overlap_max),
        "our_rows_overlap": len(our_ov),
        "ka_rows_overlap":  len(ka_ov),
    }

    print(f"  Overlap: {overlap_min} → {overlap_max}")
    print(f"  Our rows in overlap:  {metrics['our_rows_overlap']:>10,}")
    print(f"  K&A rows in overlap:  {metrics['ka_rows_overlap']:>10,}")
    print(f"  Row ratio (ours/K&A): {metrics['our_rows_overlap'] / metrics['ka_rows_overlap']:.3f}")

    metrics["row_ratio_overlap"] = round(
        metrics["our_rows_overlap"] / metrics["ka_rows_overlap"], 4
    )
    return our_ov, ka_ov, metrics


def section_word_counts(our_ov: pd.DataFrame, ka_ov: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    _print_section("4. Word Count by Year (overlap period)")

    our_ov = our_ov.copy()
    ka_ov  = ka_ov.copy()

    our_ov["year"] = pd.to_datetime(our_ov["date"], errors="coerce").dt.year
    ka_ov["year"]  = pd.to_datetime(ka_ov["date"],  errors="coerce").dt.year

    our_wc = our_ov.groupby("year")["body"].apply(word_count).rename("our_words")
    ka_wc  = ka_ov.groupby("year")["body"].apply(word_count).rename("ka_words")

    by_year = pd.concat([our_wc, ka_wc], axis=1).dropna()
    by_year["ratio"] = (by_year["our_words"] / by_year["ka_words"]).round(4)
    by_year["our_words"] = by_year["our_words"].astype(int)
    by_year["ka_words"]  = by_year["ka_words"].astype(int)

    print(f"  {'Year':6s}  {'Our words':>12s}  {'K&A words':>12s}  {'Ratio':>7s}")
    print(f"  {'-'*6}  {'-'*12}  {'-'*12}  {'-'*7}")
    for yr, row in by_year.iterrows():
        flag = "  ← pre-v2.2" if yr < 2011 else ""
        print(f"  {yr:<6d}  {int(row.our_words):>12,}  {int(row.ka_words):>12,}  {row.ratio:>7.3f}{flag}")

    pre2011  = by_year[by_year.index < 2011]
    post2011 = by_year[(by_year.index >= 2011) & (by_year.index <= 2021)]
    overall  = by_year[by_year.index <= 2021]

    metrics = {
        "wc_ratio_pre2011":   round(pre2011["ratio"].mean(),  4),
        "wc_ratio_post2011":  round(post2011["ratio"].mean(), 4),
        "wc_ratio_overall":   round(overall["ratio"].mean(),  4),
        "total_our_words":    int(by_year["our_words"].sum()),
        "total_ka_words":     int(by_year["ka_words"].sum()),
    }

    print()
    print(f"  Mean ratio 1998–2010: {metrics['wc_ratio_pre2011']:.3f}")
    print(f"  Mean ratio 2011–2021: {metrics['wc_ratio_post2011']:.3f}")
    print(f"  Mean ratio overall:   {metrics['wc_ratio_overall']:.3f}")

    return by_year, metrics


def section_speaker_coverage(our_ov: pd.DataFrame, ka_ov: pd.DataFrame) -> dict:
    _print_section("5. Speaker Coverage (distinct speakers per day)")

    our_ov = our_ov.copy()
    ka_ov  = ka_ov.copy()

    our_ov["date_d"] = pd.to_datetime(our_ov["date"], errors="coerce").dt.date
    ka_ov["date_d"]  = pd.to_datetime(ka_ov["date"],  errors="coerce").dt.date

    our_spk = our_ov.groupby("date_d")["name"].nunique().rename("our_speakers")
    ka_spk  = ka_ov.groupby("date_d")["name"].nunique().rename("ka_speakers")

    joined = pd.concat([our_spk, ka_spk], axis=1).dropna()
    joined["ratio"] = joined["our_speakers"] / joined["ka_speakers"]

    metrics = {
        "speaker_ratio_mean":   round(joined["ratio"].mean(), 4),
        "speaker_ratio_median": round(joined["ratio"].median(), 4),
        "speaker_ratio_p5":     round(joined["ratio"].quantile(0.05), 4),
        "speaker_ratio_p95":    round(joined["ratio"].quantile(0.95), 4),
    }

    print(f"  Distinct-speakers-per-day ratio (ours / K&A):")
    print(f"    Mean:   {metrics['speaker_ratio_mean']:.3f}")
    print(f"    Median: {metrics['speaker_ratio_median']:.3f}")
    print(f"    P5:     {metrics['speaker_ratio_p5']:.3f}")
    print(f"    P95:    {metrics['speaker_ratio_p95']:.3f}")
    return metrics


def section_interject_procedural(our_ov: pd.DataFrame, ka_ov: pd.DataFrame) -> dict:
    _print_section("6. Interjection & Procedural Row Analysis")

    our_int = our_ov["interject"].astype(float).eq(1).sum()
    ka_int  = ka_ov["interject"].astype(float).eq(1).sum()

    our_total = len(our_ov)
    ka_total  = len(ka_ov)

    row_gap = ka_total - our_total

    metrics = {
        "our_interject_rows":  int(our_int),
        "ka_interject_rows":   int(ka_int),
        "our_pct_interject":   round(100.0 * our_int / our_total, 2),
        "ka_pct_interject":    round(100.0 * ka_int  / ka_total,  2),
        "row_gap":             int(row_gap),
    }

    print(f"  {'':35s}  {'Ours':>10s}  {'K&A':>10s}")
    print(f"  {'Interjection rows':35s}  {our_int:>10,}  {ka_int:>10,}")
    print(f"  {'% interjection':35s}  {metrics['our_pct_interject']:>9.1f}%  {metrics['ka_pct_interject']:>9.1f}%")
    print()
    print(f"  Row gap (K&A − ours): {row_gap:,}")
    print(f"  Approx. K&A stage direction surplus: ~58,000 (from prior audit)")
    return metrics


def section_temporal_coverage(our: pd.DataFrame, ka: pd.DataFrame) -> dict:
    _print_section("7. Temporal Coverage Advantage")

    our_max = pd.to_datetime(our["date"], errors="coerce").max().date()
    ka_max  = pd.to_datetime(ka["date"],  errors="coerce").max().date()
    extra_days = (our_max - ka_max).days

    our_excl = our[pd.to_datetime(our["date"], errors="coerce").dt.date > ka_max]
    excl_sitting = pd.to_datetime(our_excl["date"], errors="coerce").dt.date.nunique()
    excl_rows = len(our_excl)

    metrics = {
        "our_coverage_end":    str(our_max),
        "ka_coverage_end":     str(ka_max),
        "extra_calendar_days": extra_days,
        "extra_sitting_days":  excl_sitting,
        "extra_rows":          excl_rows,
    }

    print(f"  Our coverage ends:    {our_max}")
    print(f"  K&A coverage ends:    {ka_max}")
    print(f"  Extra calendar days:  {extra_days:,}")
    print(f"  Extra sitting days:   {excl_sitting:,}")
    print(f"  Extra rows:           {excl_rows:,}")
    return metrics


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compare our House Hansard corpus against Katz & Alexander (2023)."
    )
    parser.add_argument(
        "--our-corpus",
        default="../data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet",
        help="Path to our House corpus parquet",
    )
    parser.add_argument(
        "--ka-corpus",
        default="../data/ka_corpus/hansard-corpus/hansard_corpus_1998_to_2022.parquet",
        help="Path to K&A corpus parquet",
    )
    parser.add_argument(
        "--out-csv",
        default="../data/output/house/ka_comparison.csv",
        help="CSV for flat summary metrics",
    )
    parser.add_argument(
        "--year-csv",
        default="../data/output/house/ka_comparison_by_year.csv",
        help="CSV for per-year word count metrics",
    )
    args = parser.parse_args()

    our = load(Path(args.our_corpus), "our corpus")
    ka  = normalise_ka(load(Path(args.ka_corpus), "K&A corpus"))

    all_metrics: dict = {}

    all_metrics.update(section_overview(our, ka))
    all_metrics.update(section_metadata(our, ka))

    our_ov, ka_ov, overlap_metrics = section_overlap(our, ka)
    all_metrics.update(overlap_metrics)

    by_year, wc_metrics = section_word_counts(our_ov, ka_ov)
    all_metrics.update(wc_metrics)

    all_metrics.update(section_speaker_coverage(our_ov, ka_ov))
    all_metrics.update(section_interject_procedural(our_ov, ka_ov))
    all_metrics.update(section_temporal_coverage(our, ka))

    # ── Save outputs ──────────────────────────────────────────────────────────
    out_csv  = Path(args.out_csv)
    year_csv = Path(args.year_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    summary_df = pd.DataFrame([
        {"metric": k, "value": v} for k, v in all_metrics.items()
    ])
    summary_df.to_csv(out_csv, index=False)
    print(f"\nSummary metrics  → {out_csv}")

    by_year.reset_index().rename(columns={"index": "year"}).to_csv(year_csv, index=False)
    print(f"Per-year metrics → {year_csv}")

    # ── Quick verdict ─────────────────────────────────────────────────────────
    _print_section("Summary verdict")
    print(f"  Word count ratio (pre-2011):   {all_metrics['wc_ratio_pre2011']:.3f}  (≈ parity)")
    print(f"  Word count ratio (2011–2021):  {all_metrics['wc_ratio_post2011']:.3f}  (≈ 15% gap)")
    print(f"  Speaker ratio (mean):          {all_metrics['speaker_ratio_mean']:.3f}  (≈ identical)")
    print(f"  Gender completeness delta:     {all_metrics.get('pct_nonnull_delta_gender', float('nan')):+.1f}%")
    print(f"  Party completeness delta:      {all_metrics.get('pct_nonnull_delta_party', float('nan')):+.1f}%")
    print(f"  Electorate completeness delta: {all_metrics.get('pct_nonnull_delta_electorate', float('nan')):+.1f}%")
    print(f"  Extra sitting days (post-K&A): {all_metrics['extra_sitting_days']:,}")
    print()


if __name__ == "__main__":
    main()
