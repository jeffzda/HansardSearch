# Committee Hansard Pipeline

> **Status:** Pipeline built and tested (2026-03-11). Three new scripts handle download, parsing, and enrichment. Full historical download not yet run — see commands below.

---

## Overview

Committee Hansard covers parliamentary committee hearings: standing committees, references committees, select committees, joint committees, and Senate Estimates. These represent a qualitatively different data type from chamber Hansard: each hearing has multiple witness panels, and witnesses (public servants, experts, lobbyists) appear alongside senators and MPs.

### APH ParlInfo dataset codes

| Code | Coverage |
|---|---|
| `commsen` | Senate committees (standing, references, select) |
| `commrep` | House of Representatives committees |
| `commjnt` | Joint committees (Senate + House members) |
| `estimate` | Senate Estimates (budget estimates hearings) |
| `commbill` | Bills committees |

### File naming convention

Downloaded files: `{YYYY-MM-DD}_{dataset}_{doc_id}.xml`
e.g. `2024-03-01_commsen_27718.xml`

Multiple files per date are normal — on a typical sitting week, 3–6 committee hearings may occur on a single day.

---

## XML structure

Two structural variants exist within the same `<committee version="2.2">` root:

### Hearing format (commsen, commrep, commjnt, commbill)

```
committee
├── committeeinfo
│   ├── comm.name     — full committee name
│   ├── chamber       — "Senate" / "Reps" / "Joint"
│   ├── date          — YYYY-MM-DD
│   └── reference     — inquiry title
└── discussion  [one per witness panel]
    ├── witness.group
    │   └── witness  [list of appearing witnesses]
    │       ├── name
    │       └── name.id  (always "unknown" — PHID not in header)
    └── talk.text
        └── body  [HPS v2.2 markup]
```

### Estimates format

```
committee
├── committeeinfo
│   ├── comm.name     — committee name
│   ├── chamber       — "Senate"
│   ├── date
│   └── reference     — "Estimates"
└── chamber.xscript
    └── debate  [one per portfolio]
        ├── debateinfo
        │   └── title   — "PORTFOLIO NAME"
        ├── debate.text
        │   └── body    [HPS v2.2 — brief intro]
        ├── discussion  [witness panel, optional]
        │   └── talk.text/body
        └── subdebate.2  [agency level]
            ├── subdebateinfo/title   — agency name
            ├── subdebate.text/body   [intro]
            └── discussion
                └── talk.text/body
```

### HPS body markup (both variants)

All transcript content is in `<p class="HPS-Normal">` elements. Speaker attribution appears at the start of each paragraph:

| Speaker type | Markup |
|---|---|
| Senator/MP (with PHID) | `<a href="PHID" type="MemberContinuation"><span class="HPS-MemberContinuation">Name:</span></a>` |
| Committee role (CHAIR etc.) | `<span class="HPS-OfficeCommittee">CHAIR:</span>` — no PHID anchor |
| External witness | `<span class="HPS-WitnessName">Mr Surname</span><span class="HPS-GeneralBold">:</span>` |
| Witness intro line | `<p class="HPS-StartWitness">SURNAME, Title, Organisation</p>` — skip |

The PHID in `<a href>` uses the same integer-format IDs as chamber v2.2 XML (e.g. `252157`) and also old-style alphanumeric IDs (e.g. `DZY`). Both are in `senator_lookup.csv` / `member_lookup.csv`.

---

## Output schema (19 columns)

| Column | Type | Notes |
|---|---|---|
| `date` | str | YYYY-MM-DD from committeeinfo |
| `name` | str | Speaker display name |
| `order` | int | Sequential row number within file |
| `speech_no` | int | Sequential utterance within panel |
| `panel_no` | int | 1-indexed panel (discussion block or debate body) |
| `page_no` | float | Often 0 in committee XML |
| `name_id` | str | PHID (for senators/MPs); NULL for witnesses |
| `party` | str | Filled by enrichment; NULL for witnesses |
| `in_gov` | float | 1.0/0.0; NULL for witnesses |
| `body` | str | Speech text |
| `witness_flag` | int | 1 = external witness; 0 = senator/MP/CHAIR |
| `gender` | str | Filled by enrichment; NULL for witnesses |
| `unique_id` | str | AustralianPoliticians ID; NULL for witnesses |
| `partyfacts_id` | float | PartyFacts cross-national ID; NULL for witnesses |
| `committee_name` | str | Full committee name |
| `committee_chamber` | str | "Senate" / "Reps" / "Joint" |
| `hearing_type` | str | "committee" or "estimates" |
| `reference` | str | Inquiry title or "Estimates" |
| `portfolio` | str | Portfolio/agency (estimates only); NULL for hearings |

### Known fill-rate limitations

| Column | Approx. fill rate | Reason |
|---|---|---|
| `name_id` | ~55% member rows | CHAIR role spans without PHID anchor; forward-fill resolves within-panel cases |
| `unique_id` | ~55% member rows | PHIDs from post-2021 senators not in lookup; CHAIR fill gaps |
| `party` / `in_gov` | ~40% member rows | Depends on unique_id fill |
| `partyfacts_id` | ~40% member rows | Depends on party fill |
| Witnesses | 0% | By design — witnesses have no PHID |

---

## Pipeline scripts

### Step 1: Download

```bash
# All datasets, full range (slow — ~10-15 hours for 1998-2025)
python 00b_download_committee.py \
    --datasets commsen,commrep,commjnt,estimate,commbill \
    --start 1998-03-02 --end 2025-12-31 \
    --out ../data/raw/committee

# Recent years only (recommended starting point)
python 00b_download_committee.py \
    --datasets commsen,commrep,commjnt,estimate,commbill \
    --start 2010-01-01 --end 2025-12-31 \
    --out ../data/raw/committee

# Single dataset (estimates only — highest research value)
python 00b_download_committee.py \
    --datasets estimate \
    --start 1998-03-02 --end 2025-12-31 \
    --out ../data/raw/committee

# Single date (testing)
python 00b_download_committee.py \
    --datasets commsen,commrep,commjnt,estimate \
    --date 2024-03-01 \
    --out ../data/raw/committee
```

**Rate:** ~1.2s per query + ~1.2s per download. On days with 4 datasets × 4 hearings each: ~20s per day. For 5,000 weekdays: ~28 hours. Run with `nohup` or `tmux`.

### Step 2: Parse

```bash
python 03c_parse_committee.py \
    --xml-dir  ../data/raw/committee \
    --out-dir  ../data/output/committee/daily_raw

# Single file (testing)
python 03c_parse_committee.py \
    --xml-dir  ../data/raw/committee \
    --out-dir  ../data/output/committee/daily_raw \
    --file 2024-03-01_commsen_27718.xml
```

### Step 3: Enrich

```bash
python 04c_fill_committee.py \
    --daily-dir  ../data/output/committee/daily_raw \
    --lookup-dir ../data/lookup \
    --out-dir    ../data/output/committee/daily
```

### Step 4: Corpus assembly (not yet implemented)

Assemble daily enriched parquets into a single corpus file. Use `07_corpus.py` with adjusted column list, or write `07c_corpus_committee.py`.

---

## Tested on

Single date: 2024-03-01 (8 files, 3,500 rows)

| File | Rows | Witnesses | Members w/ PHID | Committee |
|---|---|---|---|---|
| commjnt_27678 | 807 | 388 | 118 | Parliamentary Joint Committee on Corporations and Financial Services |
| commjnt_27739 | 568 | 296 | 118 | Joint Standing Committee on Foreign Affairs, Defence and Trade |
| commjnt_27842 | 72 | 38 | 17 | Joint Standing Committee on Trade and Investment Growth |
| commrep_27795 | 427 | 213 | 109 | Standing Committee on Health, Aged Care and Sport |
| commsen_27698 | 411 | 219 | 119 | Community Affairs References Committee |
| commsen_27718 | 430 | 186 | 133 | Economics Legislation Committee |
| commsen_27768 | 210 | 98 | 90 | Select Committee on Australia's Disaster Resilience |
| commsen_27833 | 575 | 292 | 219 | Foreign Affairs, Defence and Trade Legislation Committee |
