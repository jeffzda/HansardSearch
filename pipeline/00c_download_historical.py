"""
00c_download_historical.py — Download pre-1998 Hansard from wragge/hansard-xml.

Source: https://github.com/wragge/hansard-xml
  - House: hofreps/{year}/{filename}.xml
  - Senate: senate/{year}/{filename}.xml

Two file naming eras:
  1901-1980: YYYYMMDD_reps_NN_horNN_vN.xml   (lowercase v2.1 XML)
  1981-1997: reps_YYYY-MM-DD.xml             (uppercase tag XML)
  1998-2005: standard (overlaps with main pipeline — skip by default)

Output directory structure:
  --house-out: one dir per era (auto-sub-dirs: v21/ and uppercase/)
  --senate-out: same

Usage:
    # Clone then organize (default: 1901-1997 only)
    python 00c_download_historical.py --house-out ../data/raw/historical/house \
                                      --senate-out ../data/raw/historical/senate

    # Also include 1998-2005 overlap years
    python 00c_download_historical.py --include-overlap

    # Skip clone (repo already present)
    python 00c_download_historical.py --repo-dir /tmp/hansard-xml --no-clone
"""

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO_URL    = "https://github.com/wragge/hansard-xml.git"
DEFAULT_REPO = Path("/tmp/hansard-xml")

# Files named YYYYMMDD_... use lowercase v2.1 XML
RE_V21  = re.compile(r"^(\d{8})_")
# Files named reps/senate_YYYY-MM-DD use uppercase XML
RE_UPPER = re.compile(r"^(?:reps|senate)_(\d{4}-\d{2}-\d{2})\.xml$")


def _clone_or_update(repo_dir: Path) -> None:
    if (repo_dir / ".git").exists():
        print(f"Updating existing clone at {repo_dir} …")
        subprocess.run(["git", "-C", str(repo_dir), "pull", "--ff-only"],
                       check=True)
    else:
        print(f"Cloning {REPO_URL} → {repo_dir} …")
        subprocess.run(["git", "clone", "--depth=1", REPO_URL, str(repo_dir)],
                       check=True)


def _year_from_filename(filename: str) -> int | None:
    m = RE_V21.match(filename)
    if m:
        return int(m.group(1)[:4])
    m = RE_UPPER.match(filename)
    if m:
        return int(m.group(1)[:4])
    return None


def _era(filename: str) -> str:
    """Return 'v21' (1901-1980), 'uppercase' (1981-1997), or 'overlap' (1998+)."""
    m = RE_V21.match(filename)
    if m:
        year = int(m.group(1)[:4])
        return "v21" if year < 1981 else "overlap"
    m = RE_UPPER.match(filename)
    if m:
        year = int(m.group(1)[:4])
        if year < 1998:
            return "uppercase"
        return "overlap"
    return "unknown"


def _canonical_stem(filename: str) -> str:
    """Return a YYYY-MM-DD stem suitable for output files."""
    m = RE_V21.match(filename)
    if m:
        d = m.group(1)        # YYYYMMDD
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    m = RE_UPPER.match(filename)
    if m:
        return m.group(1)     # already YYYY-MM-DD
    # Fallback: strip .xml
    return filename.removesuffix(".xml")


def _organize(repo_dir: Path, house_out: Path, senate_out: Path,
              include_overlap: bool) -> None:
    """
    Copy XML files from the cloned repo into the output directories.
    Sub-directories: v21/ (1901-1980) and uppercase/ (1981-1997).
    Files are renamed to YYYY-MM-DD.xml.
    """
    stats = {"house_v21": 0, "house_up": 0, "senate_v21": 0, "senate_up": 0,
             "skipped_overlap": 0, "unknown": 0}

    for chamber, src_dir, out_dir in [
        ("house",  repo_dir / "hofreps", house_out),
        ("senate", repo_dir / "senate",  senate_out),
    ]:
        if not src_dir.exists():
            print(f"  [WARN] {src_dir} not found — skipping {chamber}")
            continue

        for year_dir in sorted(src_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            for xml_file in sorted(year_dir.glob("*.xml")):
                fname = xml_file.name
                era = _era(fname)

                if era == "overlap" and not include_overlap:
                    stats["skipped_overlap"] += 1
                    continue
                if era == "unknown":
                    stats["unknown"] += 1
                    continue  # post-1998 alt-format files; silently skip

                sub = "v21" if era == "v21" else "uppercase"
                dest_dir = out_dir / sub
                dest_dir.mkdir(parents=True, exist_ok=True)

                stem = _canonical_stem(fname)
                dest = dest_dir / f"{stem}.xml"
                if not dest.exists():
                    shutil.copy2(xml_file, dest)

                key = f"{chamber}_{sub[:2] if era == 'v21' else 'up'}"
                stats[f"{chamber}_{'v21' if era == 'v21' else 'up'}"] += 1

    print("\n── Summary ──────────────────────────")
    print(f"  House   v21 (1901-1980): {stats['house_v21']:>5}")
    print(f"  House   uppercase (1981-1997): {stats['house_up']:>5}")
    print(f"  Senate  v21 (1901-1980): {stats['senate_v21']:>5}")
    print(f"  Senate  uppercase (1981-1997): {stats['senate_up']:>5}")
    if stats["skipped_overlap"]:
        print(f"  Skipped (1998+ overlap): {stats['skipped_overlap']:>5}")
    if stats["unknown"]:
        print(f"  Unknown pattern: {stats['unknown']:>5}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--house-out", default="../data/raw/historical/house",
                    help="Output dir for House XML files")
    ap.add_argument("--senate-out", default="../data/raw/historical/senate",
                    help="Output dir for Senate XML files")
    ap.add_argument("--repo-dir", default=str(DEFAULT_REPO),
                    help="Where to clone/find the wragge/hansard-xml repo")
    ap.add_argument("--no-clone", action="store_true",
                    help="Skip git clone/pull (repo already present)")
    ap.add_argument("--include-overlap", action="store_true",
                    help="Also copy 1998-2005 files (overlap with main pipeline)")
    args = ap.parse_args()

    repo_dir  = Path(args.repo_dir)
    house_out = Path(args.house_out)
    senate_out = Path(args.senate_out)

    if not args.no_clone:
        _clone_or_update(repo_dir)

    if not (repo_dir / "hofreps").exists():
        print(f"ERROR: {repo_dir}/hofreps not found. "
              f"Clone may have failed or --repo-dir is wrong.", file=sys.stderr)
        sys.exit(1)

    print(f"\nOrganizing files → house: {house_out}, senate: {senate_out}")
    _organize(repo_dir, house_out, senate_out, args.include_overlap)
    print("\nDone.")


if __name__ == "__main__":
    main()
