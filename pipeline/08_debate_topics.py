"""
08_debate_topics.py — Extract debate topic hierarchy from daily XML files.

Produces a supplementary dataset with one row per debate/subdebate, capturing
the topic tree structure for each sitting day.

Schema:
  date, order, level, debate_id, parent_id, topic, cognate, gvt_business,
  senate_flag

Usage:
    python 08_debate_topics.py --xml-dir ../data/raw/senate
                               --out-dir ../data/output/senate/topics
"""

import argparse
import re
from pathlib import Path

import pandas as pd
from lxml import etree
from tqdm import tqdm

from parallel_utils import eager_map


def _t(element, tag: str) -> str | None:
    """Return text of first matching child tag, stripped."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip() or None
    return None


def _all_text(element) -> str:
    """Concatenate all descendant text nodes."""
    return "".join(element.itertext()).strip()


def _extract_topics_v21(root, date_str: str) -> list[dict]:
    """Extract debate topics from v2.0/v2.1 XML."""
    rows = []
    order = 0

    for debate in root.iter("debate"):
        parent = debate.getparent()
        is_top = parent is not None and parent.tag in ("hansard", "session.header", "chamber.xscript")

        # Debate title from <debateinfo><title>
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

        order += 1
        debate_id = f"{date_str}_{order:04d}"

        # Parent debate id (if nested)
        parent_id = None
        if parent is not None and parent.tag == "subdebate.1":
            grandparent = parent.getparent()
            if grandparent is not None and grandparent.tag == "debate":
                # Find parent debate's order — not directly available here; skip
                pass

        rows.append({
            "date": date_str,
            "order": order,
            "level": 0 if is_top else 1,
            "debate_id": debate_id,
            "parent_id": parent_id,
            "topic": topic,
            "cognate": cognate,
            "gvt_business": gvt_business,
            "senate_flag": 1,
        })

        # Sub-debates (level 1 and 2)
        for sub1 in debate.findall("subdebate.1"):
            sub1info = sub1.find("debateinfo")
            if sub1info is None:
                continue
            sub1_topic = _t(sub1info, "title") or _t(sub1info, "cognate.title")
            order += 1
            sub1_id = f"{date_str}_{order:04d}"
            rows.append({
                "date": date_str,
                "order": order,
                "level": 1,
                "debate_id": sub1_id,
                "parent_id": debate_id,
                "topic": sub1_topic,
                "cognate": _t(sub1info, "cognate.title"),
                "gvt_business": gvt_business,
                "senate_flag": 1,
            })

            for sub2 in sub1.findall("subdebate.2"):
                sub2info = sub2.find("debateinfo")
                if sub2info is None:
                    continue
                sub2_topic = _t(sub2info, "title") or _t(sub2info, "cognate.title")
                order += 1
                sub2_id = f"{date_str}_{order:04d}"
                rows.append({
                    "date": date_str,
                    "order": order,
                    "level": 2,
                    "debate_id": sub2_id,
                    "parent_id": sub1_id,
                    "topic": sub2_topic,
                    "cognate": _t(sub2info, "cognate.title"),
                    "gvt_business": gvt_business,
                    "senate_flag": 1,
                })

    return rows


def _extract_topics_v22(root, date_str: str) -> list[dict]:
    """Extract debate topics from v2.2 XML (OOXML-derived)."""
    rows = []
    order = 0

    for debate in root.iter("debate"):
        debateinfo = debate.find("debateinfo")
        if debateinfo is None:
            continue

        # v2.2 title may be in <p> children or <title> child
        title_el = debateinfo.find("title")
        if title_el is not None:
            topic = _all_text(title_el) or None
        else:
            topic = None

        cognate_el = debateinfo.find("cognate.title")
        cognate = _all_text(cognate_el) if cognate_el is not None else None

        gvt_raw = _t(debateinfo, "type") or ""
        gvt_business = 1 if re.search(
            r"government|ministerial|executive|budget|appropriation",
            gvt_raw, re.I
        ) else 0

        parent = debate.getparent()
        is_top = parent is None or parent.tag not in ("subdebate.1", "subdebate.2")

        order += 1
        debate_id = f"{date_str}_{order:04d}"

        rows.append({
            "date": date_str,
            "order": order,
            "level": 0 if is_top else 1,
            "debate_id": debate_id,
            "parent_id": None,
            "topic": topic,
            "cognate": cognate,
            "gvt_business": gvt_business,
            "senate_flag": 1,
        })

        for sub1 in debate.findall("subdebate.1"):
            sub1info = sub1.find("debateinfo")
            if sub1info is None:
                continue
            t1 = sub1info.find("title")
            sub1_topic = _all_text(t1) if t1 is not None else None
            order += 1
            sub1_id = f"{date_str}_{order:04d}"
            rows.append({
                "date": date_str,
                "order": order,
                "level": 1,
                "debate_id": sub1_id,
                "parent_id": debate_id,
                "topic": sub1_topic,
                "cognate": None,
                "gvt_business": gvt_business,
                "senate_flag": 1,
            })

            for sub2 in sub1.findall("subdebate.2"):
                sub2info = sub2.find("debateinfo")
                if sub2info is None:
                    continue
                t2 = sub2info.find("title")
                sub2_topic = _all_text(t2) if t2 is not None else None
                order += 1
                sub2_id = f"{date_str}_{order:04d}"
                rows.append({
                    "date": date_str,
                    "order": order,
                    "level": 2,
                    "debate_id": sub2_id,
                    "parent_id": sub1_id,
                    "topic": sub2_topic,
                    "cognate": None,
                    "gvt_business": gvt_business,
                    "senate_flag": 1,
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
    """Parse one XML file and return debate topics DataFrame."""
    date_str = xml_path.stem
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(str(xml_path), parser)
    root = tree.getroot()

    version = root.get("version", "2.1")

    if version in ("2.2",):
        rows = _extract_topics_v22(root, date_str)
    else:
        rows = _extract_topics_v21(root, date_str)

    if not rows:
        return pd.DataFrame(columns=[
            "date", "order", "level", "debate_id", "parent_id",
            "topic", "cognate", "gvt_business", "senate_flag",
        ])

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
        combined.to_parquet(out_dir / "senate_debate_topics.parquet", index=False)
        combined.to_csv(out_dir / "senate_debate_topics.csv", index=False)
        print(f"\nDone. {len(combined):,} topic rows across {len(all_dfs)} days.")
    else:
        print("No output produced.")


def main():
    parser = argparse.ArgumentParser(
        description="Extract Senate Hansard debate topic hierarchy."
    )
    parser.add_argument("--xml-dir", default="../data/raw/senate")
    parser.add_argument("--out-dir", default="../data/output/senate/topics")
    parser.add_argument("--no-skip", action="store_true")
    args = parser.parse_args()
    extract_all(Path(args.xml_dir), Path(args.out_dir),
                skip_existing=not args.no_skip)


if __name__ == "__main__":
    main()
