"""
09_divisions.py — Extract division (vote) records from daily XML files.

Produces a supplementary dataset with one row per senator per division,
capturing the full vote roll for each procedural division.

Schema (division-level):
  date, division_no, debate_topic, question, ayes_count, noes_count, result

Schema (vote-level, long format):
  date, division_no, unique_id, name_id, name, state, party, vote

Usage:
    python 09_divisions.py --xml-dir ../data/raw/senate
                           --daily-dir ../data/output/senate/daily
                           --out-dir ../data/output/senate/divisions
"""

import argparse
import re
from pathlib import Path

import pandas as pd
from lxml import etree
from tqdm import tqdm

from parallel_utils import eager_map


def _t(element, tag: str) -> str | None:
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip() or None
    return None


def _all_text(element) -> str:
    return "".join(element.itertext()).strip()


def _parse_name_id(name_el) -> str | None:
    """Extract PHID from a <name> element."""
    if name_el is None:
        return None
    nid = name_el.get("id") or name_el.get("name.id") or name_el.get("nameId")
    return nid.upper() if nid else None


# ── v2.1 division parsing ──────────────────────────────────────────────────────

def _extract_divisions_v21(root, date_str: str) -> tuple[list[dict], list[dict]]:
    """
    Extract divisions from v2.0/v2.1 XML.
    Returns (division_summary_rows, vote_rows).
    """
    division_rows = []
    vote_rows = []
    div_no = 0

    for div in root.iter("division"):
        div_no += 1

        # Division header
        divinfo = div.find("division.header") or div.find("divisioninfo")
        question = None
        if divinfo is not None:
            question = _all_text(divinfo).strip() or None

        # Count ayes/noes
        ayes_el = div.find(".//ayes")
        noes_el = div.find(".//noes")

        def _count_names(container):
            if container is None:
                return 0, []
            names = container.findall(".//name")
            return len(names), names

        ayes_count, aye_names = _count_names(ayes_el)
        noes_count, noe_names = _count_names(noes_el)

        result = "aye" if ayes_count > noes_count else "noe" if noes_count > ayes_count else "tie"

        division_rows.append({
            "date": date_str,
            "division_no": div_no,
            "question": question,
            "ayes_count": ayes_count,
            "noes_count": noes_count,
            "result": result,
            "senate_flag": 1,
        })

        for name_el in aye_names:
            nid = _parse_name_id(name_el)
            name_text = _all_text(name_el).strip()
            vote_rows.append({
                "date": date_str,
                "division_no": div_no,
                "name_id": nid,
                "name": name_text,
                "vote": "aye",
            })

        for name_el in noe_names:
            nid = _parse_name_id(name_el)
            name_text = _all_text(name_el).strip()
            vote_rows.append({
                "date": date_str,
                "division_no": div_no,
                "name_id": nid,
                "name": name_text,
                "vote": "noe",
            })

        # Pairs
        pairs_el = div.find(".//pairs")
        if pairs_el is not None:
            for pair in pairs_el.findall(".//pair"):
                for name_el in pair.findall(".//name"):
                    nid = _parse_name_id(name_el)
                    name_text = _all_text(name_el).strip()
                    vote_rows.append({
                        "date": date_str,
                        "division_no": div_no,
                        "name_id": nid,
                        "name": name_text,
                        "vote": "pair",
                    })

    return division_rows, vote_rows


# ── v2.2 division parsing ──────────────────────────────────────────────────────

_AYES_RE = re.compile(r"AYES?[:\s]+(\d+)", re.I)
_NOES_RE = re.compile(r"NOES?[:\s]+(\d+)", re.I)


def _extract_divisions_v22(root, date_str: str) -> tuple[list[dict], list[dict]]:
    """
    Extract divisions from v2.2 XML.

    In v2.2, divisions use the same <division> element but vote lists
    appear inside HPS-DivisionList paragraphs with name.id attributes on
    <a> elements. We fall back to the same structural approach as v2.1
    but also scan paragraph text for aye/noe headings.
    """
    division_rows = []
    vote_rows = []
    div_no = 0

    for div in root.iter("division"):
        div_no += 1

        # Gather all text to find question and counts
        full_text = _all_text(div)
        question_match = re.search(
            r"(?:The question was|Question)[—:\s]+(.+?)(?:\n|AYES|$)", full_text, re.I
        )
        question = question_match.group(1).strip() if question_match else None

        ayes_m = _AYES_RE.search(full_text)
        noes_m = _NOES_RE.search(full_text)
        ayes_count = int(ayes_m.group(1)) if ayes_m else 0
        noes_count = int(noes_m.group(1)) if noes_m else 0
        result = "aye" if ayes_count > noes_count else "noe" if noes_count > ayes_count else "tie"

        division_rows.append({
            "date": date_str,
            "division_no": div_no,
            "question": question,
            "ayes_count": ayes_count,
            "noes_count": noes_count,
            "result": result,
            "senate_flag": 1,
        })

        # Vote rolls: look for <name> elements inside ayes/noes containers
        current_side = None
        for el in div.iter():
            tag = el.tag
            if tag in ("ayes", "aye"):
                current_side = "aye"
            elif tag in ("noes", "noe"):
                current_side = "noe"
            elif tag == "pairs":
                current_side = "pair"
            elif tag == "name" and current_side:
                nid = _parse_name_id(el)
                name_text = _all_text(el).strip()
                if name_text:
                    vote_rows.append({
                        "date": date_str,
                        "division_no": div_no,
                        "name_id": nid,
                        "name": name_text,
                        "vote": current_side,
                    })

    return division_rows, vote_rows


# ── Enrichment: join senator details from daily parquet ───────────────────────

def _enrich_votes(vote_df: pd.DataFrame, daily_dir: Path,
                  date_str: str) -> pd.DataFrame:
    """Add unique_id, state, party from the daily parquet for this date."""
    daily_pq = daily_dir / f"{date_str}.parquet"
    if not daily_pq.exists() or vote_df.empty:
        for col in ("unique_id", "state", "party"):
            if col not in vote_df.columns:
                vote_df[col] = None
        return vote_df

    daily = pd.read_parquet(daily_pq, columns=["name_id", "unique_id", "state", "party"])
    daily = (
        daily[daily["name_id"].notna()]
        .drop_duplicates("name_id")
        [["name_id", "unique_id", "state", "party"]]
    )

    vote_df = vote_df.merge(daily, on="name_id", how="left")
    return vote_df


# ── Top-level entry ────────────────────────────────────────────────────────────

def extract_divisions(xml_path: Path,
                      daily_dir: Path | None = None
                      ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse one XML and return (division_summary_df, vote_df)."""
    date_str = xml_path.stem
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(str(xml_path), parser)
    root = tree.getroot()

    version = root.get("version", "2.1")

    if version == "2.2":
        div_rows, vote_rows = _extract_divisions_v22(root, date_str)
    else:
        div_rows, vote_rows = _extract_divisions_v21(root, date_str)

    _empty_div = pd.DataFrame(columns=[
        "date", "division_no", "question",
        "ayes_count", "noes_count", "result", "senate_flag",
    ])
    _empty_vote = pd.DataFrame(columns=[
        "date", "division_no", "name_id", "name",
        "vote", "unique_id", "state", "party",
    ])

    if not div_rows:
        return _empty_div, _empty_vote

    div_df = pd.DataFrame(div_rows)
    div_df["date"] = pd.to_datetime(date_str).date()

    vote_df = pd.DataFrame(vote_rows) if vote_rows else _empty_vote.copy()
    if not vote_df.empty:
        vote_df["date"] = pd.to_datetime(date_str).date()
        if daily_dir is not None:
            vote_df = _enrich_votes(vote_df, daily_dir, date_str)

    return div_df, vote_df


# Module-level globals used by the parallel worker
_OUT_DIR: Path = None
_DAILY_DIR: Path | None = None
_SKIP_EXISTING: bool = True


def _init_worker(out_dir: Path, daily_dir: Path | None, skip_existing: bool) -> None:
    """Initializer run once per worker process to set module globals."""
    global _OUT_DIR, _DAILY_DIR, _SKIP_EXISTING
    _OUT_DIR = out_dir
    _DAILY_DIR = daily_dir
    _SKIP_EXISTING = skip_existing


def _process_xml(xml_path: Path) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Worker: parse one XML file, write per-day parquets, return (div_df, vote_df)."""
    date_str = xml_path.stem
    out_div = _OUT_DIR / f"{date_str}_divisions.parquet"
    out_vote = _OUT_DIR / f"{date_str}_votes.parquet"

    if _SKIP_EXISTING and out_div.exists() and out_vote.exists():
        return pd.read_parquet(out_div), pd.read_parquet(out_vote)

    try:
        div_df, vote_df = extract_divisions(xml_path, _DAILY_DIR)
    except Exception as exc:
        print(f"  ERROR {xml_path.name}: {exc}")
        return None, None

    if not div_df.empty:
        div_df.to_parquet(out_div, index=False)
    if not vote_df.empty:
        vote_df.to_parquet(out_vote, index=False)

    return (div_df if not div_df.empty else None,
            vote_df if not vote_df.empty else None)


def extract_all(xml_dir: Path, daily_dir: Path | None,
                out_dir: Path, skip_existing: bool = True) -> None:
    xml_files = sorted(xml_dir.glob("*.xml"))
    if not xml_files:
        print(f"No XML files found in {xml_dir}")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Extracting divisions from {len(xml_files)} XML files → {out_dir}")

    results = eager_map(
        _process_xml,
        xml_files,
        initializer=_init_worker,
        initargs=(out_dir, daily_dir, skip_existing),
        desc="Divisions",
        unit="file",
    )
    all_divs = [div_df for div_df, _ in results if div_df is not None]
    all_votes = [vote_df for _, vote_df in results if vote_df is not None]

    if all_divs:
        combined_divs = pd.concat(all_divs, ignore_index=True)
        combined_divs.to_parquet(out_dir / "senate_divisions.parquet", index=False)
        combined_divs.to_csv(out_dir / "senate_divisions.csv", index=False)
        print(f"\nDivision summary: {len(combined_divs):,} divisions")

    if all_votes:
        combined_votes = pd.concat(all_votes, ignore_index=True)
        combined_votes.to_parquet(out_dir / "senate_division_votes.parquet", index=False)
        combined_votes.to_csv(out_dir / "senate_division_votes.csv", index=False)
        print(f"Vote records:     {len(combined_votes):,} rows")

    if not all_divs and not all_votes:
        print("No divisions found.")


def main():
    parser = argparse.ArgumentParser(
        description="Extract Senate Hansard division vote records."
    )
    parser.add_argument("--xml-dir", default="../data/raw/senate")
    parser.add_argument("--daily-dir", default="../data/output/senate/daily",
                        help="Daily parquet dir for senator detail enrichment")
    parser.add_argument("--out-dir", default="../data/output/senate/divisions")
    parser.add_argument("--no-skip", action="store_true")
    args = parser.parse_args()

    daily_dir = Path(args.daily_dir) if args.daily_dir else None
    extract_all(Path(args.xml_dir), daily_dir,
                Path(args.out_dir), skip_existing=not args.no_skip)


if __name__ == "__main__":
    main()
