"""
09d_interpolate_timestamps.py
─────────────────────────────
Post-processing script that linearly interpolates missing timestamps within each
sitting day, then writes back the corpus parquets with:
  - time_stamp  : existing nulls filled; original non-null values unchanged
  - time_est    : new bool column — True for interpolated/extended rows
"""

import numpy as np
import pandas as pd
import pathlib

CORPORA = [
    "data/output/senate/corpus/senate_hansard_corpus_1981_to_2026.parquet",
    "data/output/house/corpus/house_hansard_corpus_1981_to_2026.parquet",
    "data/output/senate/corpus/senate_hansard_corpus_1998_to_2026.parquet",
    "data/output/house/corpus/house_hansard_corpus_1998_to_2026.parquet",
]


# ── helpers ────────────────────────────────────────────────────────────────────

def ts_to_min(ts):
    """'HH:MM:SS' or 'HH:MM' → float minutes since midnight; anything else → NaN."""
    if not isinstance(ts, str):
        return float("nan")
    parts = ts.strip().split(":")
    try:
        h, m = int(parts[0]), int(parts[1])
        s = int(parts[2]) if len(parts) >= 3 else 0
        return h * 60.0 + m + s / 60.0
    except (ValueError, IndexError):
        return float("nan")


def min_to_hhmm(m):
    """Float minutes (possibly >1440 for overnight) → 'HH:MM' string."""
    m = m % 1440  # normalise to 0-1439
    h = int(m) // 60
    mi = int(m) % 60
    return f"{h:02d}:{mi:02d}"


def fix_overnight(mins: np.ndarray) -> np.ndarray:
    """
    Given a 1-D float array of anchor minutes (with NaN for non-anchors),
    scan consecutive valid pairs: if the next anchor is >30 min earlier than
    the previous, add 1440 to it and all subsequent values.
    Returns a new array with the same shape (NaN slots untouched).
    """
    result = mins.copy()
    valid_idx = np.where(~np.isnan(result))[0]
    if len(valid_idx) < 2:
        return result
    for j in range(1, len(valid_idx)):
        prev_i = valid_idx[j - 1]
        curr_i = valid_idx[j]
        if result[curr_i] < result[prev_i] - 30:
            result[valid_idx[j:]] += 1440
    return result


def interpolate_day(group: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a single (date, chamber) group, sorted by order.
    Fills time_stamp nulls via linear interpolation between anchor timestamps.
    Sets time_est=True for any filled row.
    Returns modified copy.
    """
    g = group.copy()
    n = len(g)

    # Convert existing timestamps to minutes
    raw_mins = np.array([ts_to_min(v) for v in g["time_stamp"]], dtype=float)

    # Find anchor positions (rows that already have a valid timestamp)
    anchor_mask = ~np.isnan(raw_mins)
    anchor_positions = np.where(anchor_mask)[0]

    if len(anchor_positions) == 0:
        # No anchors — leave everything as-is
        return g

    # Fix midnight crossings in the anchor values
    corrected = fix_overnight(raw_mins)

    # Build output arrays
    out_mins = np.full(n, float("nan"))
    time_est = np.zeros(n, dtype=bool)

    # Copy anchor values unchanged
    out_mins[anchor_mask] = corrected[anchor_mask]

    # Interpolate between consecutive anchors
    for k in range(len(anchor_positions) - 1):
        p0 = anchor_positions[k]
        p1 = anchor_positions[k + 1]
        t0 = out_mins[p0]
        t1 = out_mins[p1]
        for i in range(p0 + 1, p1):
            frac = (i - p0) / (p1 - p0)
            out_mins[i] = t0 + frac * (t1 - t0)
            time_est[i] = True

    # Extend before first anchor
    first_t = out_mins[anchor_positions[0]]
    for i in range(0, anchor_positions[0]):
        out_mins[i] = first_t
        time_est[i] = True

    # Extend after last anchor
    last_t = out_mins[anchor_positions[-1]]
    for i in range(anchor_positions[-1] + 1, n):
        out_mins[i] = last_t
        time_est[i] = True

    # Write back: estimated → HH:MM; originals kept as-is
    new_ts = list(g["time_stamp"])
    for i in range(n):
        if time_est[i] and not np.isnan(out_mins[i]):
            new_ts[i] = min_to_hhmm(out_mins[i])

    g["time_stamp"] = new_ts
    g["time_est"] = time_est
    return g


# ── main ───────────────────────────────────────────────────────────────────────

def process(path_str: str) -> None:
    path = pathlib.Path(path_str)
    if not path.exists():
        print(f"  [SKIP] {path} not found")
        return

    print(f"\nProcessing {path.name} …")
    df = pd.read_parquet(path)

    # Ensure time_est column exists
    df["time_est"] = False

    total_rows = len(df)
    before_filled = df["time_stamp"].notna().sum()

    # Group by date + chamber, interpolate each day
    group_keys = ["date", "chamber"] if "chamber" in df.columns else ["date"]
    groups = []
    for keys, g in df.groupby(group_keys, sort=False):
        groups.append(interpolate_day(g.sort_values("order")))

    out = pd.concat(groups).sort_index()

    after_filled = out["time_stamp"].notna().sum()
    est_count    = out["time_est"].sum()

    print(f"  Rows            : {total_rows:,}")
    print(f"  Timestamps before: {before_filled:,}  ({100*before_filled/total_rows:.1f}%)")
    print(f"  Timestamps after : {after_filled:,}  ({100*after_filled/total_rows:.1f}%)")
    print(f"  Interpolated     : {est_count:,}")

    out.to_parquet(path, index=False)
    print(f"  Saved → {path}")


if __name__ == "__main__":
    for corpus in CORPORA:
        process(corpus)
    print("\nDone.")
