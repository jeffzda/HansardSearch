# Australian Parliamentary Hansard Pipeline

A Python pipeline for building a dual-chamber Hansard corpus covering the Australian Senate and House of Representatives from 1998 to 2025, plus committee Hansard. Outputs are CSV and Parquet, one file per sitting day and one full corpus per chamber.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Prerequisites](#prerequisites)
3. [Directory Layout](#directory-layout)
4. [Pipeline Stages](#pipeline-stages)
5. [Running the Full Pipeline](#running-the-full-pipeline)
6. [Output Schema](#output-schema)
7. [Corpus Counts](#corpus-counts)
8. [Known Data Quality Issues](#known-data-quality-issues)
9. [Parallelism](#parallelism)
10. [Analysis Scripts](#analysis-scripts)

---

## Project Overview

This pipeline downloads, parses, enriches, validates, and assembles the complete Hansard record for both chambers of the Australian Parliament. Source data is XML published by the Australian Parliament House (APH) ParlInfo service.

**Chambers covered:**

- Senate (1998–2025): 1,482 sitting days
- House of Representatives (1998–2025): 1,722 sitting days
- Committee Hansard (Senate, House, Joint, Estimates, Bills): ongoing

**Output formats:** Apache Parquet and CSV, at both the daily and full-corpus level.

The pipeline is numbered 00–09b, with `b` and `c` variants for the House and committee streams respectively. Supporting utilities live in `parallel_utils.py`.

---

## Prerequisites

Python 3.12 or later is required. Install dependencies with:

```bash
pip install pandas pyarrow lxml requests tqdm psutil matplotlib jinja2 anthropic
```

| Package | Purpose |
|---------|---------|
| `pandas` | Data manipulation throughout |
| `pyarrow` | Parquet read/write |
| `lxml` | XML parsing |
| `requests` | HTTP downloads from APH |
| `tqdm` | Progress bars |
| `psutil` | Resource-aware parallelism (legacy dynamic scaling) |
| `matplotlib` | Charts in analysis reports |
| `jinja2` | HTML report templates |
| `anthropic` | LLM-assisted steps (optional) |

---

## Directory Layout

```
Hansard/
├── pipeline/                        ← all scripts live here
│   ├── parallel_utils.py
│   ├── 00_download.py
│   ├── 00b_download_committee.py
│   ├── 01_session_info.py
│   ├── 02_member_lookup.py
│   ├── 02b_member_lookup_house.py
│   ├── 03_parse.py
│   ├── 03b_parse_house.py
│   ├── 03c_parse_committee.py
│   ├── 04_fill_details.py
│   ├── 04b_fill_details_house.py
│   ├── 04c_fill_committee.py
│   ├── 05_validate.py
│   ├── 05b_validate_house.py
│   ├── 06_add_partyfacts.py
│   ├── 06b_fill_first_speech.py
│   ├── 06c_normalize_names.py
│   ├── 07_corpus.py
│   ├── 07c_corpus_committee.py
│   ├── 08_debate_topics.py
│   ├── 08b_debate_topics_house.py
│   ├── 09_divisions.py
│   └── 09b_divisions_house.py
│
├── data/
│   ├── raw/
│   │   ├── senate/                  ← Senate XML files
│   │   ├── reps/                    ← House XML files
│   │   └── committee/               ← Committee XML files
│   │
│   ├── lookup/
│   │   ├── senator_lookup.csv       ← 666 senators with name_id, gender, dates
│   │   ├── member_lookup.csv        ← 1,280 House members
│   │   ├── state_lookup.csv         ← 697 rows; maps name_id → state by date range
│   │   ├── electorate_lookup.csv    ← maps name_id → electorate by date range
│   │   ├── party_lookup.csv         ← 789 rows; date-range entries for party changers
│   │   ├── partyfacts_map.csv       ← 27 rows; party abbreviation → PartyFacts ID
│   │   └── session_info_all.csv     ← 3,204 rows (1,482 Senate + 1,722 House dates)
│   │
│   └── output/
│       ├── senate/
│       │   ├── daily_raw/           ← parsed but unenriched daily parquets
│       │   ├── daily/               ← 1,482 enriched daily parquets
│       │   └── corpus/              ← senate_hansard_corpus_1998_to_2025.parquet/.csv
│       ├── house/
│       │   ├── daily_raw/
│       │   ├── daily/               ← 1,722 enriched daily parquets
│       │   └── corpus/              ← house_hansard_corpus_1998_to_2025.parquet/.csv
│       └── committee/
│           ├── daily_raw/
│           ├── daily/
│           └── corpus/
│
└── case_studies/                    ← ARENA, WWF, GRDC, climate_change, _archive
```

---

## Pipeline Stages

Scripts are run from the `pipeline/` directory. The table below lists every script in execution order.

| Script | Purpose | Parallelism |
|--------|---------|------------|
| `00_download.py` | Download Senate/House XML from APH ParlInfo | threaded (I/O) |
| `00b_download_committee.py` | Download committee XML (commsen, commrep, commjnt, estimate, commbill) | threaded (I/O) |
| `01_session_info.py` | Extract session metadata from XML headers → `session_info_all.csv` | process pool |
| `02_member_lookup.py` | Build senator_lookup, party_lookup, state_lookup; fills gender via Wikidata SPARQL | single-threaded |
| `02b_member_lookup_house.py` | Build member_lookup, electorate_lookup, party_lookup_house, speaker_lookup | single-threaded |
| `03_parse.py` | Parse Senate XML → daily_raw/ parquets; handles HPS v2.0 and v2.2 | process pool |
| `03b_parse_house.py` | Parse House XML → daily_raw/ parquets | process pool |
| `03c_parse_committee.py` | Parse committee XML → daily_raw/ parquets; two XML variants (discussion blocks, estimates) | process pool |
| `04_fill_details.py` | Enrich Senate daily parquets: name_id, party, state, page_no forward-fill, in_gov | process pool |
| `04b_fill_details_house.py` | Enrich House daily parquets: name_id, party, electorate, page_no, in_gov | process pool |
| `04c_fill_committee.py` | Enrich committee daily parquets: committee metadata, witness_flag, portfolio | process pool |
| `05_validate.py` | 7 automated data-quality tests — Senate | process pool |
| `05b_validate_house.py` | 7 automated data-quality tests — House (requires `--lookup-dir`) | process pool |
| `06_add_partyfacts.py` | Join PartyFacts numeric IDs to daily parquets via `partyfacts_map.csv` | process pool |
| `06b_fill_first_speech.py` | Backfill `first_speech` flag for post-2009 XML where attribute is absent | process pool |
| `06c_normalize_names.py` | Normalize `name` field: strip trailing colons, fix casing, expand first names | process pool |
| `07_corpus.py` | Assemble full Senate/House corpus from daily parquets | threaded (I/O) |
| `07c_corpus_committee.py` | Assemble committee corpus from daily parquets | threaded (I/O) |
| `08_debate_topics.py` | Extract debate topic hierarchy — Senate | process pool |
| `08b_debate_topics_house.py` | Extract debate topic hierarchy — House | process pool |
| `09_divisions.py` | Extract division metadata and vote records — Senate | process pool |
| `09b_divisions_house.py` | Extract division metadata and vote records — House | process pool |

### Validation tests (T1–T7)

`05_validate.py` and `05b_validate_house.py` run seven tests on every daily parquet:

| Test | Description |
|------|-------------|
| T1 | Date in file matches session_info date |
| T2 | No consecutive duplicate speech turns |
| T3 | No time-expired procedural artefacts in body text |
| T4 | No speaker with multiple parties or states on the same day |
| T5 | All name_ids resolve to a known lookup entry |
| T6 | Speaker birth/death dates consistent with sitting date |
| T7 | Speaker was marked active on the sitting date |

---

## Running the Full Pipeline

Run all scripts from the `pipeline/` directory.

```bash
# 1. Download XML
python 00_download.py --chamber senate --start 1998-01-01 --end 2025-12-31
python 00_download.py --chamber reps   --start 1998-01-01 --end 2025-12-31
python 00b_download_committee.py

# 2. Session metadata (run once, or after new sittings)
python 01_session_info.py

# 3. Build lookup tables (run once, or after membership changes)
python 02_member_lookup.py
python 02b_member_lookup_house.py

# 4. Parse XML to daily parquets
python 03_parse.py
python 03b_parse_house.py
python 03c_parse_committee.py

# 5. Enrich daily parquets
python 04_fill_details.py
python 04b_fill_details_house.py
python 04c_fill_committee.py

# 6. Validate
python 05_validate.py
python 05b_validate_house.py --lookup-dir ../data/lookup

# 7. PartyFacts, first speech, name normalisation
python 06_add_partyfacts.py --chamber senate
python 06_add_partyfacts.py --chamber house
python 06b_fill_first_speech.py --chamber senate
python 06b_fill_first_speech.py --chamber house
python 06c_normalize_names.py --chamber senate
python 06c_normalize_names.py --chamber house

# 8. Assemble full corpora
python 07_corpus.py --chamber senate
python 07_corpus.py --chamber house
python 07c_corpus_committee.py

# 9. Debate topics
python 08_debate_topics.py
python 08b_debate_topics_house.py

# 10. Division votes
python 09_divisions.py
python 09b_divisions_house.py
```

Most scripts are idempotent: re-running them overwrites existing output files. Steps 3 and 4 (parse and enrich) are the most time-consuming and benefit most from parallelism.

---

## Output Schema

All chamber corpus files share the following columns. Columns marked with a slash differ by chamber.

| Column | Type | Notes |
|--------|------|-------|
| `date` | date | Sitting day |
| `name` | str | Canonical form: `Senator ABETZ`, `Mr ABBOTT` |
| `name_id` | str | Stable person identifier linking to lookup tables |
| `order` | int | Turn order within the sitting day |
| `speech_no` | int | Speech number within the day |
| `page_no` | int | Hansard page number (forward-filled; 99.4% coverage) |
| `time_stamp` | str | Time from Hansard where present |
| `party` | str | Party abbreviation at the time of speaking |
| `in_gov` | int | 1 if the speaker was in government on that date |
| `state` / `electorate` | str | State (Senate) or electorate (House) |
| `senate_flag` / `fedchamb_flag` | int | 1 = main chamber; 0 = committee of the whole / Federation Chamber |
| `first_speech` | int | 1 for maiden speeches and re-entry speeches |
| `body` | str | Full speech text |
| `question` | int | 1 if this turn is a question |
| `answer` | int | 1 if this turn is an answer |
| `q_in_writing` | int | 1 if this is a question on notice (in writing) |
| `interject` | int | 1 if this turn is an interjection |
| `div_flag` | int | 1 if a division was called during this turn |
| `gender` | str | `M` or `F` |
| `partyfacts_id` | float | PartyFacts numeric party identifier |
| `unique_id` | str | Composite key: `{name_id}_{date}_{order}` |

### Committee corpus additional columns

| Column | Type | Notes |
|--------|------|-------|
| `witness_flag` | int | 1 if speaker is a witness (not a member) |
| `committee_name` | str | Full committee name |
| `committee_chamber` | str | `senate`, `reps`, or `joint` |
| `hearing_type` | str | e.g. `estimates`, `inquiry`, `bills` |
| `reference` | str | Inquiry reference or portfolio under examination |
| `portfolio` | str | Portfolio (estimates hearings) |
| `panel_no` | int | Panel number within a hearing day |

---

## Corpus Counts

| Dataset | Rows | Files |
|---------|------|-------|
| Senate corpus | 549,864 | 1,482 sitting days |
| House corpus | 623,906 | 1,722 sitting days |
| Senate debate topics | 43,066 | `senate_debate_topics.parquet/.csv` |
| Senate divisions | 8,001 divisions; 466,557 vote records | `senate_divisions.parquet/.csv`, `senate_division_votes.parquet/.csv` |
| House debate topics | 388,154 | `house_debate_topics_1998_to_2025.parquet/.csv` |
| House divisions | 4,616 divisions; 614,057 vote records | `house_divisions.parquet/.csv`, `house_division_votes.parquet/.csv` |

---

## Known Data Quality Issues

| Test | Senate | House | Nature |
|------|--------|-------|--------|
| T1 — date mismatch | 1 day | 2 days | Header date differs from file date in source XML |
| T2 — consecutive duplicates | 24 days | 25 days | Genuine speech repetition in source XML; not fixable |
| T3 — time expired | 87 days | 130 days | Residual procedural edge cases not matched by exclusion patterns |
| T4 — party/state multiplicity | 18 days | 33 days | Real mid-term party or state changes |
| T5 — unknown name_ids | ~78 Senate | 0 House | Senate: garbled XML artefacts (e.g. `/7E4`, `XH4`) |
| T6 — birth/death date | 144 days | 451 days | Pass 2 false matches; date-range guard applied in fill step |
| T7 — not active on date | 229 days | 762 days | Pass 2 false matches; date-range guard applied in fill step |

Overall validation pass rate: Senate 76.3% clean (1,131/1,482 days); House 89.7% clean (1,545/1,722 days).

The T6 and T7 figures for the House reflect the state before daily files were regenerated with the updated date-range guard in `04b_fill_details_house.py`. Re-running the fill and validation steps will reduce these counts.

`first_speech` backfill (script `06b`) infers maiden speeches from member entry dates. Re-election speeches are flagged for members who were absent from the chamber for more than 180 days.

---

## Parallelism

All file-processing scripts use `eager_map()` and `eager_threaded_map()` from `parallel_utils.py`.

- `eager_map(func, items)` — CPU-bound work; uses `ProcessPoolExecutor` with all available cores.
- `eager_threaded_map(func, items)` — I/O-bound work (downloads, parquet reads); uses `ThreadPoolExecutor`.

Legacy variants `dynamic_map()` and `threaded_map()` implement load-aware scaling via `psutil`: workers start at 4, scale up when CPU usage is below 50%, and scale down when CPU is above 80% or memory above 85%. These are retained for shared-machine environments but are not the default for new scripts.

The `--workers N` flag and `--sequential` flag are available on both validator scripts for manual control.

---

## Analysis Scripts

After the corpus is assembled, the following scripts produce reports and search output:

| Script | Description |
|--------|-------------|
| `analyse_corpus_chamber.py --chamber {senate\|house}` | Generates an HTML structural report: speaker counts, party breakdowns, time-series charts |
| `analyse_corpus_committee.py` | Generates an HTML report for the committee corpus |
| `search_corpus.py` | Boolean keyword search across one or both chamber corpora |
| `analyse_case_study.py --subject {WWF\|ARENA\|GRDC}` | Case-study report for a named subject area |

Case study outputs are written to `Hansard/case_studies/<subject>/`, not to `data/output/`.

---

## Reference

- Katz & Alexander Hansard project: https://github.com/lindsaykatz/hansard-proj
- Their published dataset (Zenodo): https://zenodo.org/records/8121950
- PartyFacts: https://partyfacts.herokuapp.com/
- APH ParlInfo XML source: https://parlinfo.aph.gov.au/
