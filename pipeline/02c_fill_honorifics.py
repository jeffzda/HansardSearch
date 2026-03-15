"""
02c_fill_honorifics.py
──────────────────────
Fetches official honorifics (Dr, Prof, etc.) for MPs and Senators from the
APH website and writes them back into the lookup CSVs.

For House members  : updates member_lookup.csv  `title` column + form1–form5
For Senators       : adds/updates senator_lookup.csv `title` column
                     (form fields stay as "Senator …" — that's how Hansard
                     refers to senators regardless of academic titles)

Usage:
    python 02c_fill_honorifics.py [--lookup-dir data/lookup]
"""

import argparse
import csv
import re
import time
import urllib.request
from pathlib import Path

APH_SEARCH = "https://www.aph.gov.au/Senators_and_Members/Parliamentarian_Search_Results?q={}"
# Titles we care about extracting (add more if needed)
TITLE_RE = re.compile(r'\b(Dr|Prof\.?|Professor|Rev\.?|Sir|Dame)\b')
NAME_RE   = re.compile(r'chorFullName_\d+">(.*?)</a>', re.DOTALL)


# ── helpers ──────────────────────────────────────────────────────────────────

def fetch_aph_names(surname: str) -> list[str]:
    """Return list of full APH display names matching surname."""
    url = APH_SEARCH.format(urllib.parse.quote(surname))
    req = urllib.request.Request(url, headers={"User-Agent": "HansardResearch/1.0 (academic)"})
    try:
        html = urllib.request.urlopen(req, timeout=12).read().decode("utf-8", errors="ignore")
        return NAME_RE.findall(html)
    except Exception as e:
        print(f"  WARNING: APH fetch failed for '{surname}': {e}")
        return []


def extract_title(full_name: str) -> str:
    """Pull the first academic-style title from an APH full name string."""
    m = TITLE_RE.search(full_name)
    if not m:
        return ""
    t = m.group(1)
    # Normalise
    if t.lower().startswith("prof"):
        return "Prof"
    if t.lower().startswith("rev"):
        return "Rev"
    return t  # Dr, Sir, Dame


def build_aph_map(surnames: list[str], delay: float = 0.1) -> dict[str, str]:
    """
    Returns dict mapping normalised full name → title.
    Queries the APH once per unique surname.
    """
    unique = sorted(set(s.lower() for s in surnames))
    result: dict[str, str] = {}
    print(f"  Querying APH for {len(unique)} unique surnames …")
    for i, surname in enumerate(unique, 1):
        names = fetch_aph_names(surname)
        for full in names:
            title = extract_title(full)
            if title:
                # Key: strip all noise (titles, Hon, MP, Senator), lowercase for matching
                clean = re.sub(r'\b(Hon|the Hon|MP|Senator|Dr|Prof\.?|Professor|Rev\.?|Sir|Dame)\b', '', full)
                clean = re.sub(r'\.', '', clean).strip()
                clean = re.sub(r'\s+', ' ', clean).lower()
                result[clean] = title
        if i % 50 == 0:
            print(f"    … {i}/{len(unique)} surnames done")
        time.sleep(delay)
    titled = sum(1 for v in result.values() if v)
    print(f"  APH map built: {len(result)} titled entries")
    return result


def match_title(first: str, surname: str, common: str, aph_map: dict) -> str:
    """Try to match a lookup row against the APH map."""
    display = common.strip() if common.strip() else first.strip()
    candidates = [
        f"{display} {surname}".lower(),
        f"{first} {surname}".lower(),
    ]
    for c in candidates:
        # Remove stray titles already in c
        c_clean = re.sub(r'\b(dr|prof|sir|dame|rev)\b\.?\s*', '', c).strip()
        if c_clean in aph_map:
            return aph_map[c_clean]
    return ""


# ── form field regeneration (House members only) ─────────────────────────────

def make_forms(title: str, first: str, surname: str, common: str) -> dict:
    """Regenerate form1–form5 for a House member."""
    prefix = title if title else "Mr"   # fallback; gender-aware Mr/Ms handled below
    name   = common.strip() if common.strip() else first.strip()
    return {
        "form1": f"{prefix} {name} {surname}".strip(),
        "form2": f"{prefix} {surname}".strip(),
        "form3": f"{prefix} {name.upper()} {surname.upper()}".strip(),
        "form4": f"{prefix} {surname.upper()}".strip(),
        "form5": surname,
    }


# ── main ─────────────────────────────────────────────────────────────────────

import urllib.parse   # needed by fetch_aph_names

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookup-dir", default="data/lookup")
    args = ap.parse_args()
    ldir = Path(args.lookup_dir)

    # ── House members ────────────────────────────────────────────────────────
    member_path = ldir / "member_lookup.csv"
    print(f"\n[House] Loading {member_path}")
    with open(member_path, newline="", encoding="utf-8") as f:
        members = list(csv.DictReader(f))
    surnames = [r["surname"] for r in members]
    aph_map = build_aph_map(surnames)

    updated_house = 0
    for row in members:
        new_title = match_title(row["first_name"], row["surname"],
                                row.get("common_name", ""), aph_map)
        if not new_title:
            continue
        old_title = row.get("title", "").strip()
        if new_title == old_title:
            continue
        print(f"  House: {row['display_name']}  {old_title!r} → {new_title!r}")
        row["title"] = new_title
        # Only regenerate forms if we're upgrading from Mr/Mrs/Ms/blank to Dr/Prof/etc.
        if new_title in ("Dr", "Prof", "Rev"):
            forms = make_forms(new_title, row["first_name"],
                               row["surname"], row.get("common_name", ""))
            row.update(forms)
        updated_house += 1

    with open(member_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=members[0].keys())
        writer.writeheader()
        writer.writerows(members)
    print(f"[House] Updated {updated_house} rows → {member_path}")

    # ── Senators ─────────────────────────────────────────────────────────────
    senator_path = ldir / "senator_lookup.csv"
    print(f"\n[Senate] Loading {senator_path}")
    with open(senator_path, newline="", encoding="utf-8") as f:
        senators = list(csv.DictReader(f))
    fieldnames = list(senators[0].keys())

    # Add title column if missing
    if "title" not in fieldnames:
        fieldnames.insert(fieldnames.index("gender"), "title")
        for row in senators:
            row["title"] = ""
        print("  Added 'title' column to senator_lookup")

    surnames_s = [r["surname"] for r in senators]
    aph_map_s = build_aph_map(surnames_s)

    updated_senate = 0
    for row in senators:
        new_title = match_title(row["first_name"], row["surname"],
                                row.get("common_name", ""), aph_map_s)
        if not new_title:
            continue
        old_title = row.get("title", "").strip()
        if new_title == old_title:
            continue
        print(f"  Senate: {row['display_name']}  {old_title!r} → {new_title!r}")
        row["title"] = new_title
        updated_senate += 1
        # Note: senator form fields deliberately NOT updated — Hansard always
        # uses "Senator [Name]", not "Dr Senator [Name]"

    with open(senator_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(senators)
    print(f"[Senate] Updated {updated_senate} rows → {senator_path}")

    print("\nDone. Re-run 04_fill_details.py and 07_corpus.py to propagate changes.")


if __name__ == "__main__":
    main()
