"""
00_download.py — Download Australian Hansard XML files from APH ParlInfo.

Supports Senate (hansardS/hansardS80) and House of Representatives
(hansardr/hansardr80).  House XML also contains Federation Chamber debates
(fedchamb.xscript / maincomm.xscript nodes) — no separate download needed.

The download requires a two-step process:
  1. Query ParlInfo with the sitting date to get the document page HTML.
  2. Parse the HTML to find the XML download link, then fetch the XML.

Three URL eras exist for the XML file itself:
  - 1998–~Mar 2011:  .../hansards/YYYY-MM-DD/toc_unixml/filename.xml
  - ~May 2011–2021:  .../hansards/{uuid}/toc_unixml/filename.xml
  - 2021–present:    .../hansards/{integer}/toc_unixml/filename.xml

Usage:
    python 00_download.py --chamber senate --start 1998-03-02 --end 2025-12-31 --out ../data/raw/senate
    python 00_download.py --chamber reps   --start 1998-03-02 --end 2025-12-31 --out ../data/raw/house
    python 00_download.py --chamber senate --date 2020-02-04 --out ../data/raw/senate
"""

import argparse
import re
import time
from datetime import date, timedelta
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm

from parallel_utils import eager_threaded_map

# ── Constants ──────────────────────────────────────────────────────────────────

# ParlInfo dataset codes per chamber
CHAMBER_DATASETS = {
    "senate": "hansardS,hansardS80",
    "reps":   "hansardr,hansardr80",
}

PARLINFO_SEARCH = (
    "https://parlinfo.aph.gov.au/parlInfo/search/display/display.w3p"
    ";adv=yes;orderBy=_fragment_number,doc_date-rev;page=0"
    ";query=Dataset%3A{datasets}%20Date%3A{day}%2F{month}%2F{year}"
    ";rec=0;resCount=Default"
)

PARLINFO_BASE = "https://parlinfo.aph.gov.au"

# A user-agent that APH ParlInfo allows (without one, returns HTTP 403).
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}

# Delay between requests (seconds) to avoid hammering the server.
REQUEST_DELAY = 1.0

# ── Helpers ───────────────────────────────────────────────────────────────────


def search_url(sitting_date: date, datasets: str) -> str:
    """Build the ParlInfo search URL for a given sitting date and dataset codes."""
    return PARLINFO_SEARCH.format(
        datasets=datasets,
        day=sitting_date.day,
        month=sitting_date.month,
        year=sitting_date.year,
    )


def find_xml_link(html: str, sitting_date: date) -> str | None:
    """
    Parse the ParlInfo HTML page and return the absolute URL of the XML file.

    APH ParlInfo renders a 'View/Save XML' link on the document page.
    The href ends in '.xml;fileType=text%2Fxml'.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Strategy 1: look for links containing 'toc_unixml' or ending with
    # ';fileType=text%2Fxml'
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "toc_unixml" in href or "fileType=text%2Fxml" in href:
            if not href.startswith("http"):
                href = PARLINFO_BASE + href
            return href

    # Strategy 2: look for any link to an XML file in the hansards path
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/hansards/" in href and href.endswith(".xml"):
            if not href.startswith("http"):
                href = PARLINFO_BASE + href
            return href

    return None


def fetch_xml(client: httpx.Client, sitting_date: date,
              datasets: str = CHAMBER_DATASETS["senate"]) -> bytes | None:
    """
    Fetch the XML for a sitting date. Returns raw bytes or None if not found.
    """
    url = search_url(sitting_date, datasets)
    try:
        resp = client.get(url, headers=HEADERS, follow_redirects=True, timeout=30)
    except httpx.RequestError as e:
        print(f"  [ERROR] Network error fetching {sitting_date}: {e}")
        return None

    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        print(f"  [WARN] HTTP {resp.status_code} for {sitting_date}")
        return None

    xml_url = find_xml_link(resp.text, sitting_date)
    if xml_url is None:
        # No XML available for this date (may be a non-sitting day, or
        # the document is only available as PDF).
        return None

    time.sleep(REQUEST_DELAY)
    try:
        xml_resp = client.get(xml_url, headers=HEADERS, follow_redirects=True, timeout=60)
    except httpx.RequestError as e:
        print(f"  [ERROR] Network error fetching XML {xml_url}: {e}")
        return None

    if xml_resp.status_code != 200:
        print(f"  [WARN] HTTP {xml_resp.status_code} for XML at {xml_url}")
        return None

    return xml_resp.content


def sitting_dates(start: date, end: date):
    """
    Yield all dates in [start, end] that are weekdays (Mon–Fri).
    Parliament only sits on weekdays; this filters the universe of candidate
    dates without needing a sitting calendar.

    Note: Not every weekday is a sitting day. Files that do not exist on
    ParlInfo will simply return None in fetch_xml() and be skipped.
    """
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon=0, Fri=4
            yield current
        current += timedelta(days=1)


def _fetch_one(args: tuple) -> str:
    """
    Worker: fetch XML for one date and save to disk.

    args = (d, out_dir, datasets, skip_existing)
    Returns one of: 'skipped', 'not_found', 'error', 'downloaded'.
    """
    d, out_dir, datasets, skip_existing = args
    out_path = out_dir / f"{d.isoformat()}.xml"

    if skip_existing and out_path.exists():
        return "skipped"

    time.sleep(REQUEST_DELAY)
    with httpx.Client() as client:
        xml_bytes = fetch_xml(client, d, datasets)

    if xml_bytes is None:
        return "not_found"

    start = xml_bytes[:300].decode("utf-8", errors="replace").lstrip("\ufeff \r\n\t")
    if not (start.startswith("<?xml") or start.startswith("<hansard")):
        print(f"  [WARN] {d}: response does not look like XML — skipping")
        return "error"

    out_path.write_bytes(xml_bytes)
    return "downloaded"


def download_date_list(dates: list[date], out_dir: Path,
                       datasets: str = CHAMBER_DATASETS["senate"],
                       skip_existing: bool = True) -> dict:
    """
    Download XML files for a list of sitting dates.

    Returns a summary dict: {'downloaded': N, 'skipped': N, 'not_found': N, 'errors': N}
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {"downloaded": 0, "skipped": 0, "not_found": 0, "errors": 0}

    items = [(d, out_dir, datasets, skip_existing) for d in dates]
    results = eager_threaded_map(_fetch_one, items, desc="Downloading", unit="date")

    for outcome in results:
        if outcome == "downloaded":
            summary["downloaded"] += 1
        elif outcome == "skipped":
            summary["skipped"] += 1
        elif outcome == "not_found":
            summary["not_found"] += 1
        else:
            summary["errors"] += 1

    return summary


# ── Date-range helpers ────────────────────────────────────────────────────────


def date_range_dates(start_str: str, end_str: str) -> list[date]:
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    return list(sitting_dates(start, end))


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Download Hansard XML files from APH ParlInfo."
    )
    parser.add_argument(
        "--chamber",
        choices=list(CHAMBER_DATASETS.keys()),
        default="senate",
        help="Which chamber to download: senate or reps (default: senate)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", help="Start date YYYY-MM-DD (use with --end)")
    group.add_argument("--date", help="Single date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD (use with --start)")
    parser.add_argument(
        "--out",
        default=None,
        help="Output directory for XML files (default: ../data/raw/<chamber>)",
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Re-download files that already exist",
    )
    args = parser.parse_args()

    datasets = CHAMBER_DATASETS[args.chamber]
    out_dir = Path(args.out) if args.out else Path(f"../data/raw/{args.chamber}")

    if args.date:
        dates = [date.fromisoformat(args.date)]
    else:
        if not args.end:
            parser.error("--end is required when using --start")
        dates = date_range_dates(args.start, args.end)

    print(f"Chamber: {args.chamber}  ({datasets})")
    print(f"Downloading {len(dates)} candidate dates → {out_dir}")
    summary = download_date_list(dates, out_dir, datasets=datasets,
                                  skip_existing=not args.no_skip)
    print(
        f"\nDone. Downloaded: {summary['downloaded']}, "
        f"Skipped (existing): {summary['skipped']}, "
        f"Not found: {summary['not_found']}, "
        f"Errors: {summary['errors']}"
    )


if __name__ == "__main__":
    main()
