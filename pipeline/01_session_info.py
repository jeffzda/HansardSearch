"""
01_session_info.py — Extract session.header metadata from Hansard XML files.

Accepts one or more --xml-dir arguments so Senate and House can be combined
in a single run, producing data/lookup/session_info_all.csv with one row per
sitting day across both chambers.

Usage (both chambers):
    python 01_session_info.py --xml-dir ../data/raw/senate \\
                               --xml-dir ../data/raw/reps \\
                               --out ../data/lookup/session_info_all.csv

Usage (single chamber):
    python 01_session_info.py --xml-dir ../data/raw/senate \\
                               --out ../data/lookup/session_info_all.csv
"""

import argparse
from pathlib import Path

import pandas as pd
from lxml import etree
from tqdm import tqdm

from parallel_utils import eager_map


def extract_session_header(xml_path: Path) -> dict:
    """Parse session.header from one XML file. Returns a flat dict."""
    try:
        tree = etree.parse(str(xml_path))
        root = tree.getroot()
    except etree.XMLSyntaxError as e:
        return {"filename": xml_path.stem, "parse_error": str(e)}

    header = root.find(".//session.header")
    if header is None:
        return {"filename": xml_path.stem, "parse_error": "no session.header found"}

    def txt(tag: str) -> str | None:
        el = header.find(tag)
        return el.text.strip() if el is not None and el.text else None

    return {
        "filename": xml_path.stem,
        "schema_version": root.get("version", "unknown"),
        "date": txt("date"),
        "parliament_no": txt("parliament.no"),
        "session_no": txt("session.no"),
        "period_no": txt("period.no"),
        "chamber": txt("chamber"),
        "page_no": txt("page.no"),
        "proof": txt("proof"),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Extract session.header metadata from Hansard XML files."
    )
    parser.add_argument(
        "--xml-dir",
        action="append",
        dest="xml_dirs",
        default=None,
        help="Directory containing YYYY-MM-DD.xml files (repeatable for multiple chambers)",
    )
    parser.add_argument(
        "--out",
        default="../data/lookup/session_info_all.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    xml_dirs = args.xml_dirs or ["../data/raw/senate"]

    all_rows = []
    for xml_dir_str in xml_dirs:
        xml_dir = Path(xml_dir_str)
        xml_files = sorted(xml_dir.glob("*.xml"))
        print(f"Processing {len(xml_files)} XML files from {xml_dir}")
        rows = eager_map(
            extract_session_header,
            xml_files,
            desc=xml_dir.name,
            unit="file",
        )
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    df = df.sort_values("filename").reset_index(drop=True)

    # Coerce numeric columns
    for col in ("parliament_no", "session_no", "period_no", "page_no", "proof"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"\nSaved {len(df)} rows → {out_path}")

    # Quick summary
    if "schema_version" in df.columns:
        print("\nSchema versions found:")
        print(df["schema_version"].value_counts().to_string())

    if "chamber" in df.columns:
        print("\nChambers found:")
        print(df["chamber"].value_counts().to_string())

    errors = df[df.get("parse_error", pd.Series(dtype=str)).notna()] if "parse_error" in df.columns else df.iloc[0:0]
    if len(errors):
        print(f"\n[WARN] {len(errors)} files had parse errors:")
        print(errors[["filename", "parse_error"]].to_string(index=False))


if __name__ == "__main__":
    main()
