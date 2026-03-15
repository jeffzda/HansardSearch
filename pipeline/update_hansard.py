"""
update_hansard.py — Check for new Hansard and update the corpus.

Every run, attempts to download Senate and House Hansard for today ± 3 days.
If any new files are downloaded, runs parse → fill → rebuild corpus → reload gunicorn.

Usage:
    python update_hansard.py [--dry-run]
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE       = Path("/opt/hansard")
PIPELINE   = BASE / "pipeline"
DATA       = BASE / "data"
RAW_SEN    = DATA / "raw" / "senate"
RAW_REP    = DATA / "raw" / "reps"
DAILY_SEN  = DATA / "output" / "senate" / "daily"
DAILY_REP  = DATA / "output" / "house"  / "daily"
CORPUS_SEN = DATA / "output" / "senate" / "corpus"
CORPUS_REP = DATA / "output" / "house"  / "corpus"
LOOKUP_DIR = DATA / "lookup"

PYTHON = str(BASE / "venv" / "bin" / "python3")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/var/log/hansard_update.log"),
    ],
)
log = logging.getLogger(__name__)


def _run(cmd: list[str]) -> bool:
    result = subprocess.run(cmd, cwd=PIPELINE, capture_output=True, text=True)
    if result.returncode != 0:
        log.error("FAILED (exit %d):\n%s", result.returncode, result.stderr[-2000:])
        return False
    if result.stdout.strip():
        log.info(result.stdout.strip()[-1000:])
    return True


def _download(chamber: str, d: date) -> bool:
    out_dir = str(RAW_SEN if chamber == "senate" else RAW_REP)
    return _run([PYTHON, "00_download.py", "--chamber", chamber,
                 "--date", str(d), "--out", out_dir])


def _parse(chamber: str) -> bool:
    if chamber == "senate":
        return _run([PYTHON, "03_parse.py",
                     "--in-dir",  str(RAW_SEN),
                     "--out-dir", str(DATA / "output" / "senate" / "daily_raw")])
    else:
        return _run([PYTHON, "03b_parse_house.py",
                     "--in-dir",  str(RAW_REP),
                     "--out-dir", str(DATA / "output" / "house" / "daily_raw")])


def _fill(chamber: str) -> bool:
    if chamber == "senate":
        return _run([PYTHON, "04_fill_details.py",
                     "--daily-raw-dir", str(DATA / "output" / "senate" / "daily_raw"),
                     "--out-dir",       str(DAILY_SEN),
                     "--lookup-dir",    str(LOOKUP_DIR)])
    else:
        return _run([PYTHON, "04b_fill_details_house.py",
                     "--daily-raw-dir", str(DATA / "output" / "house" / "daily_raw"),
                     "--out-dir",       str(DAILY_REP),
                     "--lookup-dir",    str(LOOKUP_DIR)])


def _build_corpus(chamber: str) -> bool:
    if chamber == "senate":
        return _run([PYTHON, "07_corpus.py",
                     "--daily-dir", str(DAILY_SEN),
                     "--out-dir",   str(CORPUS_SEN),
                     "--prefix",    "senate_hansard_corpus"])
    else:
        return _run([PYTHON, "07_corpus.py",
                     "--daily-dir", str(DAILY_REP),
                     "--out-dir",   str(CORPUS_REP),
                     "--prefix",    "house_hansard_corpus"])


def _reload_gunicorn() -> None:
    result = subprocess.run(["pgrep", "-f", "gunicorn.*app:app"],
                            capture_output=True, text=True)
    pids = result.stdout.strip().splitlines()
    if pids:
        pid = int(pids[0])
        log.info("Reloading gunicorn (PID %d)", pid)
        os.kill(pid, signal.SIGHUP)
    else:
        log.warning("Could not find gunicorn PID — skipping reload")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    today = date.today()
    dates = [today + timedelta(days=i) for i in range(-3, 4)]

    log.info("=== Hansard update check — %s (checking %s to %s) ===",
             today, dates[0], dates[-1])

    if args.dry_run:
        log.info("Dry run — dates to check: %s", [str(d) for d in dates])
        return

    updated_chambers = set()

    for chamber, daily_dir in [("senate", DAILY_SEN), ("reps", DAILY_REP)]:
        for d in dates:
            if (daily_dir / f"{d}.parquet").exists():
                continue
            log.info("Trying %s %s …", chamber, d)
            if _download(chamber, d):
                # Only counts as updated if a new parquet appeared
                if (daily_dir / f"{d}.parquet").exists():
                    updated_chambers.add(chamber)

    if not updated_chambers:
        log.info("No new files — nothing to do.")
        return

    for chamber in updated_chambers:
        log.info("Parsing %s …", chamber)
        if not _parse(chamber):
            log.error("Parse failed for %s", chamber)
            continue
        log.info("Filling %s …", chamber)
        if not _fill(chamber):
            log.error("Fill failed for %s", chamber)
            continue
        log.info("Building corpus %s …", chamber)
        if not _build_corpus(chamber):
            log.error("Corpus build failed for %s", chamber)
            continue

    _reload_gunicorn()
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
