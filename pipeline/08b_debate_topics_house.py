"""
08b_debate_topics_house.py — Extract debate topic hierarchy from House of
Representatives daily XML files.

Produces a supplementary dataset with one row per debate/subdebate, capturing
the topic tree structure for each sitting day.

Schema:
  date, order, level, debate_id, parent_id, topic, cognate, gvt_business,
  fedchamb_flag

  fedchamb_flag=0  →  main chamber (chamber.xscript)
  fedchamb_flag=1  →  Federation Chamber / Main Committee
                       (fedchamb.xscript in v2.2, maincomm.xscript in v2.1)

Usage:
    python 08b_debate_topics_house.py --xml-dir ../data/raw/reps
                                      --out-dir ../data/output/house/topics
"""

import argparse
import re
from pathlib import Path

import pandas as pd
from lxml import etree
from tqdm import tqdm

from parallel_utils import eager_map


CORPUS_COLUMNS = [
    "date", "order", "level", "debate_id", "parent_id",
    "topic", "cognate", "gvt_business", "fedchamb_flag",
]


def _t(element, tag: str) -> str | None:
    """Return text of first matching child tag, stripped."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip() or None
    return None


def _all_text(element) -> str:
    """Concatenate all descendant text nodes."""
    return "".join(element.itertext()).strip()


def _extract_topics_from_section_v21(section, date_str: str,
                                     fedchamb_flag: int) -> list[dict]:
    """Extract debate topics from a single xscript section (v2.0/v2.1 XML).

    subdebate.1 and subdebate.2 in House XML use <subdebateinfo> not
    <debateinfo>, so we look for both tags.
    """
    rows = []
    order_counter = [0]   # mutable so nested helper can increment

    def next_order():
        order_counter[0] += 1
        return order_counter[0]

    for debate in section.findall("debate"):
        debateinfo = debate.find("debateinfo")
        if debateinfo is None:
            continue

        topic = _t(debateinfo, "title") or _t(debateinfo, "cognate.title")
        cognate = _t(debateinfo, "cognate.title")
        gvt_business_raw = _t(debateinfo, "type") or ""
        gvt_business = 1 if re.search(
            r"government|ministerial|executive|budget|appropriation",
            gvt_business_raw, re.I
        ) else 0

        order = next_order()
        debate_id = f"{date_str}_{order:04d}"

        rows.append({
            "date": date_str,
            "order": order,
            "level": 0,
            "debate_id": debate_id,
            "parent_id": None,
            "topic": topic,
            "cognate": cognate,
            "gvt_business": gvt_business,
            "fedchamb_flag": fedchamb_flag,
        })

        # Sub-debates (level 1): House uses <subdebateinfo>
        for sub1 in debate.findall("subdebate.1"):
            sub1info = sub1.find("subdebateinfo")
            if sub1info is None:
                sub1info = sub1.find("debateinfo")
            if sub1info is None:
                continue
            sub1_topic = _t(sub1info, "title") or _t(sub1info, "cognate.title")
            order1 = next_order()
            sub1_id = f"{date_str}_{order1:04d}"
            rows.append({
                "date": date_str,
                "order": order1,
                "level": 1,
                "debate_id": sub1_id,
                "parent_id": debate_id,
                "topic": sub1_topic,
                "cognate": _t(sub1info, "cognate.title"),
                "gvt_business": gvt_business,
                "fedchamb_flag": fedchamb_flag,
            })

            # Sub-sub-debates (level 2)
            for sub2 in sub1.findall("subdebate.2"):
                sub2info = sub2.find("subdebateinfo")
                if sub2info is None:
                    sub2info = sub2.find("debateinfo")
                if sub2info is None:
                    continue
                sub2_topic = _t(sub2info, "title") or _t(sub2info, "cognate.title")
                order2 = next_order()
                sub2_id = f"{date_str}_{order2:04d}"
                rows.append({
                    "date": date_str,
                    "order": order2,
                    "level": 2,
                    "debate_id": sub2_id,
                    "parent_id": sub1_id,
                    "topic": sub2_topic,
                    "cognate": _t(sub2info, "cognate.title"),
                    "gvt_business": gvt_business,
                    "fedchamb_flag": fedchamb_flag,
                })

    return rows


def _extract_topics_from_section_v22(section, date_str: str,
                                     fedchamb_flag: int) -> list[dict]:
    """Extract debate topics from a single xscript section (v2.2 XML).

    subdebate.1 and subdebate.2 in House XML use <subdebateinfo> not
    <debateinfo>.  Title text may be wrapped in inline elements so we use
    _all_text() rather than .text directly.
    """
    rows = []
    order_counter = [0]

    def next_order():
        order_counter[0] += 1
        return order_counter[0]

    for debate in section.findall("debate"):
        debateinfo = debate.find("debateinfo")
        if debateinfo is None:
            continue

        title_el = debateinfo.find("title")
        topic = _all_text(title_el) if title_el is not None else None
        cognate_el = debateinfo.find("cognate.title")
        cognate = _all_text(cognate_el) if cognate_el is not None else None
        gvt_raw = _t(debateinfo, "type") or ""
        gvt_business = 1 if re.search(
            r"government|ministerial|executive|budget|appropriation",
            gvt_raw, re.I
        ) else 0

        order = next_order()
        debate_id = f"{date_str}_{order:04d}"

        rows.append({
            "date": date_str,
            "order": order,
            "level": 0,
            "debate_id": debate_id,
            "parent_id": None,
            "topic": topic,
            "cognate": cognate,
            "gvt_business": gvt_business,
            "fedchamb_flag": fedchamb_flag,
        })

        # Level 1 subdebates: House uses <subdebateinfo>
        for sub1 in debate.findall("subdebate.1"):
            sub1info = sub1.find("subdebateinfo")
            if sub1info is None:
                sub1info = sub1.find("debateinfo")
            if sub1info is None:
                continue
            t1 = sub1info.find("title")
            sub1_topic = _all_text(t1) if t1 is not None else None
            order1 = next_order()
            sub1_id = f"{date_str}_{order1:04d}"
            rows.append({
                "date": date_str,
                "order": order1,
                "level": 1,
                "debate_id": sub1_id,
                "parent_id": debate_id,
                "topic": sub1_topic,
                "cognate": None,
                "gvt_business": gvt_business,
                "fedchamb_flag": fedchamb_flag,
            })

            # Level 2 subdebates
            for sub2 in sub1.findall("subdebate.2"):
                sub2info = sub2.find("subdebateinfo")
                if sub2info is None:
                    sub2info = sub2.find("debateinfo")
                if sub2info is None:
                    continue
                t2 = sub2info.find("title")
                sub2_topic = _all_text(t2) if t2 is not None else None
                order2 = next_order()
                sub2_id = f"{date_str}_{order2:04d}"
                rows.append({
                    "date": date_str,
                    "order": order2,
                    "level": 2,
                    "debate_id": sub2_id,
                    "parent_id": sub1_id,
                    "topic": sub2_topic,
                    "cognate": None,
                    "gvt_business": gvt_business,
                    "fedchamb_flag": fedchamb_flag,
                })

    return rows


# Module-level globals used by the parallel worker
_OUT_DIR: Path = None
_SKIP_EXISTING: bool = True


def _process_xml(xml_path: Path) -> pd.DataFrame | None:
    """Worker: parse one XML file, write per-day parquet, return DataFrame."""
    out_pq = _OUT_DIR / xml_path.name.replace(".xml", ".parquet")
    if _SKIP_EXISTING and out_pq.exists():
        return pd.read_parquet(out_pq)
    try:
        df = extract_topics(xml_path)
    except Exception as exc:
        print(f"  ERROR {xml_path.name}: {exc}")
        return None
    df.to_parquet(out_pq, index=False)
    return df


def _init_worker(out_dir: Path, skip_existing: bool) -> None:
    """Initializer run once per worker process to set module globals."""
    global _OUT_DIR, _SKIP_EXISTING
    _OUT_DIR = out_dir
    _SKIP_EXISTING = skip_existing


def extract_topics(xml_path: Path) -> pd.DataFrame:
    """Parse one House XML file and return debate topics DataFrame.

    Handles v2.0/v2.1 (maincomm.xscript for Federation Chamber) and
    v2.2 (fedchamb.xscript for Federation Chamber).  Each xscript section
    is processed separately with the appropriate fedchamb_flag value.
    """
    date_str = xml_path.stem
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(str(xml_path), parser)
    root = tree.getroot()

    version = root.get("version", "2.1")
    is_v22 = version in ("2.2",)

    extract_fn = (
        _extract_topics_from_section_v22 if is_v22
        else _extract_topics_from_section_v21
    )

    # Determine the federation chamber tag name by version
    fedchamb_tag = "fedchamb.xscript" if is_v22 else "maincomm.xscript"

    rows = []

    # Main chamber section (fedchamb_flag=0)
    chamber_section = root.find("chamber.xscript")
    if chamber_section is not None:
        rows.extend(extract_fn(chamber_section, date_str, fedchamb_flag=0))

    # Federation Chamber / Main Committee section (fedchamb_flag=1)
    fedchamb_section = root.find(fedchamb_tag)
    if fedchamb_section is not None:
        rows.extend(extract_fn(fedchamb_section, date_str, fedchamb_flag=1))

    # Fallback: no xscript wrappers (very old XML) — iterate all debates
    if chamber_section is None and fedchamb_section is None:
        rows.extend(extract_fn(root, date_str, fedchamb_flag=0))

    if not rows:
        return pd.DataFrame(columns=CORPUS_COLUMNS)

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(date_str).date()
    return df


def extract_all(xml_dir: Path, out_dir: Path,
                skip_existing: bool = True) -> None:
    xml_files = sorted(xml_dir.glob("*.xml"))
    if not xml_files:
        print(f"No XML files found in {xml_dir}")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Extracting debate topics from {len(xml_files)} XML files → {out_dir}")

    results = eager_map(
        _process_xml,
        xml_files,
        initializer=_init_worker,
        initargs=(out_dir, skip_existing),
        desc="Topics",
        unit="file",
    )
    all_dfs = [df for df in results if df is not None]

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined.to_parquet(out_dir / "house_debate_topics.parquet", index=False)
        combined.to_csv(out_dir / "house_debate_topics.csv", index=False)
        print(f"\nDone. {len(combined):,} topic rows across {len(all_dfs)} days.")
        # Breakdown by fedchamb_flag
        breakdown = combined["fedchamb_flag"].value_counts().sort_index()
        for flag, count in breakdown.items():
            label = "main chamber" if flag == 0 else "Federation Chamber"
            print(f"  fedchamb_flag={flag} ({label}): {count:,} rows")
    else:
        print("No output produced.")


def main():
    parser = argparse.ArgumentParser(
        description="Extract House of Representatives Hansard debate topic hierarchy."
    )
    parser.add_argument("--xml-dir", default="../data/raw/reps")
    parser.add_argument("--out-dir", default="../data/output/house/topics")
    parser.add_argument("--no-skip", action="store_true")
    args = parser.parse_args()
    extract_all(Path(args.xml_dir), Path(args.out_dir),
                skip_existing=not args.no_skip)


if __name__ == "__main__":
    main()
