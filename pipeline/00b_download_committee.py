"""
00b_download_committee.py — Download Australian Committee Hansard XML from APH ParlInfo.

Committee Hansard differs from chamber Hansard in two key ways:
  1. Multiple documents can exist for the same date (one per committee hearing).
  2. The ParlInfo summary page lists all hearings for a given date and dataset.

Supported dataset codes:
  commsen   — Senate committees (standing, references, select)
  commrep   — House of Representatives committees
  commjnt   — Joint committees
  estimate  — Senate Estimates (budget estimates hearings)
  commbill  — Bills committees

Output files are named: {YYYY-MM-DD}_{dataset}_{doc_id}.xml
e.g. 2024-03-01_commsen_27718.xml

The XML URL is derived from the PDF download URL:
  .../toc_pdf/{name}.pdf → .../toc_unixml/{name}.xml

Usage:
    python 00b_download_committee.py --datasets commsen,commrep,commjnt,estimate,commbill \\
        --start 1998-03-02 --end 2025-12-31 --out ../data/raw/committee

    # Single dataset:
    python 00b_download_committee.py --datasets estimate \\
        --start 2024-01-01 --end 2024-12-31 --out ../data/raw/committee

    # Single date:
    python 00b_download_committee.py --datasets commsen --date 2024-03-01 \\
        --out ../data/raw/committee
"""

import argparse
import re
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import unquote

import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm

from parallel_utils import eager_threaded_map

# ── Constants ──────────────────────────────────────────────────────────────────

ALL_DATASETS = ["commsen", "commrep", "commjnt", "estimate", "commbill"]

PARLINFO_BASE = "https://parlinfo.aph.gov.au"

PARLINFO_SUMMARY = (
    "{base}/parlInfo/search/summary/summary.w3p"
    ";adv=yes;orderBy=_fragment_number,doc_date-rev;page=0"
    ";query=Dataset%3A{dataset}%20Date%3A{day}%2F{month}%2F{year}"
    ";resCount=Default"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}

# Delay between requests (seconds) — ParlInfo rate limit is sensitive.
REQUEST_DELAY = 1.2

# ── Core helpers ───────────────────────────────────────────────────────────────


def _summary_url(d: date, dataset: str) -> str:
    return PARLINFO_SUMMARY.format(
        base=PARLINFO_BASE,
        dataset=dataset,
        day=d.day,
        month=d.month,
        year=d.year,
    )


def _find_committee_docs(html: str, dataset: str) -> list[tuple[str, str]]:
    """
    Parse the ParlInfo summary HTML page and return a list of
    (doc_id, xml_url) tuples found on the page.

    Strategy: find all PDF download links (.../toc_pdf/...) and derive
    the XML URL by swapping toc_pdf → toc_unixml and .pdf → .xml.
    """
    soup = BeautifulSoup(html, "html.parser")
    seen_ids = set()
    results = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if f"/{dataset}/" not in href or "toc_pdf" not in href:
            continue

        # Extract doc_id from path: .../committees/{dataset}/{doc_id}/toc_pdf/...
        m = re.search(rf"/{re.escape(dataset)}/(\w+)/toc_pdf/(.+?)(?:;|$)", href)
        if not m:
            continue

        doc_id = m.group(1)
        pdf_filename = m.group(2)  # may contain URL encoding

        if doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)

        # Derive XML URL: replace toc_pdf with toc_unixml, .pdf with .xml
        xml_filename = pdf_filename.replace(".pdf", ".xml").split(";")[0]
        xml_url = (
            f"{PARLINFO_BASE}/parlInfo/download/committees"
            f"/{dataset}/{doc_id}/toc_unixml/{xml_filename}"
        )
        results.append((doc_id, xml_url))

    return results


def _fetch_and_save(
    client: httpx.Client,
    xml_url: str,
    out_path: Path,
) -> bool:
    """Download a single XML file. Returns True on success, False otherwise."""
    try:
        r = client.get(xml_url, headers=HEADERS, follow_redirects=True, timeout=60)
    except httpx.RequestError as e:
        print(f"  [ERROR] Network error: {e}")
        return False

    if r.status_code != 200:
        return False

    # Validate content
    start = r.content[:300].decode("utf-8", errors="replace").lstrip("\ufeff \r\n\t")
    if not (start.startswith("<?xml") or start.startswith("<committee")):
        return False

    out_path.write_bytes(r.content)
    return True


def _fetch_one_dataset_date(args: tuple) -> dict:
    """
    Worker: fetch all committee XML files for one (date, dataset) combination.

    args = (d, dataset, out_dir, skip_existing)
    Returns a summary dict: {downloaded, skipped, not_found, errors}.
    """
    d, dataset, out_dir, skip_existing = args
    local = {"downloaded": 0, "skipped": 0, "not_found": 0, "errors": 0}

    time.sleep(REQUEST_DELAY)
    url = _summary_url(d, dataset)

    with httpx.Client() as client:
        try:
            r = client.get(url, headers=HEADERS, follow_redirects=True, timeout=30)
        except httpx.RequestError as e:
            print(f"  [ERROR] {d}/{dataset}: {e}")
            local["errors"] += 1
            return local

        if r.status_code != 200:
            local["not_found"] += 1
            return local

        docs = _find_committee_docs(r.text, dataset)
        if not docs:
            local["not_found"] += 1
            return local

        for doc_id, xml_url in docs:
            out_path = out_dir / f"{d.isoformat()}_{dataset}_{doc_id}.xml"

            if skip_existing and out_path.exists():
                local["skipped"] += 1
                continue

            time.sleep(REQUEST_DELAY)
            ok = _fetch_and_save(client, xml_url, out_path)
            if ok:
                local["downloaded"] += 1
            else:
                local["errors"] += 1

    return local


def download_date_range(
    datasets: list[str],
    start_date: date,
    end_date: date,
    out_dir: Path,
    skip_existing: bool = True,
) -> dict:
    """
    Download committee XML files for all dates in [start_date, end_date]
    and all specified dataset codes.

    Returns summary dict: {downloaded, skipped, not_found, errors}.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {"downloaded": 0, "skipped": 0, "not_found": 0, "errors": 0}

    # Generate candidate dates (weekdays only)
    candidate_dates = []
    d = start_date
    while d <= end_date:
        if d.weekday() < 5:
            candidate_dates.append(d)
        d += timedelta(days=1)

    total = len(candidate_dates) * len(datasets)
    print(f"Querying {len(candidate_dates)} dates × {len(datasets)} datasets "
          f"= {total} summary requests")
    print(f"Output directory: {out_dir}")

    items = [
        (d, dataset, out_dir, skip_existing)
        for d in candidate_dates
        for dataset in datasets
    ]
    results = eager_threaded_map(
        _fetch_one_dataset_date, items, desc="Fetching", unit="query"
    )
    for local in results:
        for key in summary:
            summary[key] += local[key]

    return summary


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Download committee Hansard XML files from APH ParlInfo."
    )
    parser.add_argument(
        "--datasets",
        default=",".join(ALL_DATASETS),
        help=(
            f"Comma-separated dataset codes (default: all five). "
            f"Options: {', '.join(ALL_DATASETS)}"
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", help="Start date YYYY-MM-DD (use with --end)")
    group.add_argument("--date", help="Single date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD (required with --start)")
    parser.add_argument(
        "--out",
        default="../data/raw/committee",
        help="Output directory (default: ../data/raw/committee)",
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Re-download files that already exist",
    )
    args = parser.parse_args()

    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    invalid = [d for d in datasets if d not in ALL_DATASETS]
    if invalid:
        parser.error(f"Unknown dataset codes: {invalid}. Valid: {ALL_DATASETS}")

    out_dir = Path(args.out)

    if args.date:
        start = end = date.fromisoformat(args.date)
    else:
        if not args.end:
            parser.error("--end is required when using --start")
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)

    summary = download_date_range(
        datasets=datasets,
        start_date=start,
        end_date=end,
        out_dir=out_dir,
        skip_existing=not args.no_skip,
    )
    print(
        f"\nDone. Downloaded: {summary['downloaded']}, "
        f"Skipped (existing): {summary['skipped']}, "
        f"No results: {summary['not_found']}, "
        f"Errors: {summary['errors']}"
    )


if __name__ == "__main__":
    main()
