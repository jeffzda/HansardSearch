# Australian Hansard Database (1998–2025)

## Overview

This repository contains a structured database of Australian parliamentary proceedings for both the Senate and the House of Representatives, covering every sitting day from 2 March 1998 to 27 November 2025 (the end of the 47th Parliament). Each row in the main corpus represents a single speech, question, answer, interjection, or continuation utterance, with the speaker identified by name, party, state or electorate, and gender where available. The corpus is suitable for computational text analysis, legislative studies, and political science research.

The project translates and extends the Katz and Alexander (2023) House of Representatives Hansard database, which was originally implemented in R and covered only the lower chamber. This implementation re-engineers that pipeline in Python using direct node-iteration over the APH ParlInfo XML source files (rather than the text-blob splitting approach used in the original R code), adds full Senate coverage, and extends both chambers to November 2025. The output schema is kept deliberately consistent with Katz and Alexander to allow the two datasets to be used together or compared directly.

In addition to the main speech corpora, supplementary datasets are provided for Senate debate topics and Senate divisions (recorded votes). House supplementary datasets exist as per-day parquet files and are pending final corpus assembly.

## Coverage

| Dataset | Rows | Sitting Days | Date Range | Files |
|---|---|---|---|---|
| Senate corpus | 549,864 | 1,482 | 1998-03-02 to 2025-11-27 | `senate_hansard_corpus_1998_to_2025.parquet/.csv` |
| House corpus | 623,906 | 1,722 | 1998-03-02 to 2025-11-27 | `house_hansard_corpus_1998_to_2025.parquet/.csv` |
| Senate debate topics | 43,066 | 1,482 | 1998-03-02 to 2025-11-27 | `senate_debate_topics.parquet/.csv` + daily parquets |
| House debate topics | 388,154 | 1,722 | 1998-03-02 to 2025-11-27 | `house_debate_topics_1998_to_2025.parquet/.csv` + daily parquets |
| Senate divisions | 8,001 divisions | — | 1998-03-02 to 2025-11-27 | `senate_divisions.parquet/.csv` |
| Senate division votes | 466,557 vote records | — | 1998-03-02 to 2025-11-27 | `senate_division_votes.parquet/.csv` |
| House divisions | 4,616 divisions | — | 1998-03-02 to 2025-11-27 | `house_divisions.parquet/.csv` |
| House division votes | 614,057 vote records | — | 1998-03-02 to 2025-11-27 | `house_division_votes.parquet/.csv` |

The House has more sitting days than the Senate over the same date range because the House sits on some days when the Senate does not (and vice versa, but the net effect favours the House).

### Validation status

Automated validation (T1–T7 for both chambers) is run against all enriched daily files.

| Chamber | Clean files | Total files | % clean |
|---|---|---|---|
| Senate | 1,131 | 1,482 | 76.3% |
| House | 1,545 | 1,722 | 89.7% |

A file is "clean" if it passes all validation tests (T1–T7 for both chambers). The main sources of remaining failures are T3 edge cases (87 Senate, 130 House), T4 genuine party/electorate changers (18 Senate, 33 House), and T6/T7 Pass 2 false name-form matches (~144/229 Senate; ~451/762 House before date-range guard re-fill). T2 (consecutive duplicate procedural phrases) has been reduced from 331/567 to 24/25 after adding procedural-phrase exclusions to the validator. See `docs/10_pipeline_status.md` for a full breakdown by test.

## Repository Structure

```
Hansard/
├── pipeline/                   Python pipeline scripts (steps 00–09)
│   ├── 00_download.py          Download XML from APH ParlInfo
│   ├── 01_session_info.py      Extract session header metadata
│   ├── 02_member_lookup.py     Build senator lookup tables
│   ├── 02b_member_lookup_house.py  Build House member lookup tables
│   ├── 03_parse.py             Senate XML parser
│   ├── 03b_parse_house.py      House XML parser
│   ├── 04_fill_details.py      Senate member enrichment (7 passes)
│   ├── 04b_fill_details_house.py   House member enrichment (5 passes)
│   ├── 05_validate.py          Senate validation (T1–T7)
│   ├── 05b_validate_house.py   House validation (T1–T7)
│   ├── 06_add_partyfacts.py    Add PartyFacts IDs (both chambers)
│   ├── 07_corpus.py            Assemble daily files into corpus
│   ├── 08_debate_topics.py     Extract Senate debate topics
│   ├── 08b_debate_topics_house.py  Extract House debate topics
│   ├── 09_divisions.py         Extract Senate divisions
│   └── 09b_divisions_house.py  Extract House divisions
├── data/
│   ├── raw/
│   │   ├── senate/             1,482 Senate XML files (YYYY-MM-DD.xml)
│   │   └── reps/               1,722 House XML files (YYYY-MM-DD.xml) [stored as reps/ to match ParlInfo dataset code hansardr]
│   ├── lookup/                 Lookup tables (member, party, president, PartyFacts)
│   └── output/
│       ├── senate/
│       │   ├── daily/          Per-day enriched parquet + CSV files
│       │   ├── corpus/         senate_hansard_corpus_1998_to_2025.parquet/.csv
│       │   ├── topics/         senate_debate_topics.parquet/.csv + daily parquets
│       │   └── divisions/      senate_divisions.parquet/.csv + senate_division_votes.parquet/.csv
│       └── house/
│           ├── daily/          Per-day enriched parquet + CSV files
│           ├── corpus/         house_hansard_corpus_1998_to_2025.parquet/.csv
│           ├── topics/         house_debate_topics_1998_to_2025.parquet/.csv + daily parquets
│           └── divisions/      house_divisions.parquet/.csv + house_division_votes.parquet/.csv + daily parquets
└── docs/                       Technical documentation (pipeline design, schemas, execution record)
```

## Quick Start

```python
import pandas as pd

# Load the Senate corpus
senate = pd.read_parquet(
    "data/output/senate/corpus/senate_hansard_corpus/senate_hansard_corpus_1998_to_2025.parquet"
)

# Load the House corpus
house = pd.read_parquet(
    "data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet"
)

# Most active senators by total speeches (excluding procedural rows)
top_senators = (
    senate[senate["name"] != "business start"]
    .groupby(["name", "party", "state"])
    .size()
    .reset_index(name="n_speeches")
    .sort_values("n_speeches", ascending=False)
    .head(10)
)
print(top_senators)

# All speeches by a single senator
dorinda = senate[senate["unique_id"] == "Cox1970"]

# Combine both chambers for a unified analysis
senate["chamber"] = "Senate"
house["chamber"] = "House"
# Note: Senate uses 'state', House uses 'state' column in the assembled corpus
# (the per-day House files use 'electorate'; see Data Schema for details)
combined = pd.concat([senate, house], ignore_index=True)
```

## Data Schema

### Main Corpus — Senate

File: `data/output/senate/corpus/senate_hansard_corpus/senate_hansard_corpus_1998_to_2025.parquet`

21 columns. One row per utterance (speech, question, answer, interjection, continuation, or procedural row).

| Column | Type | Description | Example |
|---|---|---|---|
| `date` | object (date string) | Sitting day date | `1998-03-02` |
| `name` | str | Speaker display name; `"business start"` for the opening row of each sitting day; `"stage direction"` for procedural notices | `"Senator O'BRIEN"` |
| `order` | int64 | Sequential row number within the sitting day, 1-indexed | `42` |
| `speech_no` | float64 (nullable) | Speech group identifier; all rows belonging to the same speech share a number; NULL for standalone procedural rows | `7.0` |
| `page_no` | float64 (nullable) | Hansard printed page number | `1234.0` |
| `time_stamp` | str (nullable) | 24-hour timestamp `HH:MM:SS`; present for most speeches in v2.1+ XML; NULL for older files or where absent in the XML | `"14:32:00"` |
| `name_id` | str (nullable) | APH Parliamentary PHID (uppercase); `"10000"` for the President/presiding officer; `"UNKNOWN"` for unidentified speakers | `"8O6"` |
| `state` | str | Senator's state or territory, full name | `"Western Australia"` |
| `party` | str | Party abbreviation at time of sitting | `"ALP"` |
| `in_gov` | int64 | 1 = government party; 0 = opposition or crossbench | `0` |
| `first_speech` | int64 | 1 = this is the senator's maiden (first) speech; 0 otherwise | `0` |
| `body` | str | Full text of the utterance | `"Mr President, I move..."` |
| `question` | int64 | 1 = question asked during Senate question time; 0 otherwise | `0` |
| `answer` | int64 | 1 = answer given during Senate question time; 0 otherwise | `0` |
| `q_in_writing` | int64 | 1 = question on notice / written answer detected in the daily XML; 4.8% of Senate rows, 6.2% of House rows | `0` |
| `div_flag` | int64 | 1 = this row is associated with a division (recorded vote); 0 otherwise | `0` |
| `gender` | str (nullable) | `"male"` or `"female"`; NULL for `"business start"` and `"stage direction"` rows only; all senators now filled | `"male"` |
| `unique_id` | str (nullable) | AustralianPoliticians unique identifier | `"OBrien1959"` |
| `interject` | int64 | 1 = this row is an interjection within another senator's speech; 0 otherwise | `0` |
| `senate_flag` | int64 | Always `1` for Senate corpus rows; provided for chamber identification when combining with the House corpus | `1` |
| `partyfacts_id` | float64 (nullable) | PartyFacts cross-national party identifier; NULL for parties not in the PartyFacts database | `1385.0` |

### Main Corpus — House of Representatives

File: `data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet`

21 columns. Same schema as the Senate corpus with two substitutions and one note about the assembled corpus column naming:

| Column | Type | Description | Example |
|---|---|---|---|
| `date` | object (date string) | Sitting day date | `1998-03-02` |
| `name` | str | Speaker display name | `"Mr ALBANESE"` |
| `order` | int64 | Sequential row number within the sitting day | `15` |
| `speech_no` | float64 (nullable) | Speech group identifier | `3.0` |
| `page_no` | float64 (nullable) | Hansard printed page number | `567.0` |
| `time_stamp` | str (nullable) | 24-hour timestamp; NULL where absent in XML | `"10:15:00"` |
| `name_id` | str (nullable) | APH Parliamentary PHID; `"10000"` for Speaker/presiding officer | `"A56"` |
| `state` | str / object | In the per-day files, this column is named `electorate` (the member's electoral division, e.g. `"Brand"`). In the assembled corpus parquet the column retains the `state` slot from `CORPUS_COLUMNS` but holds electorate values. | `"Grayndler"` |
| `party` | str | Party abbreviation | `"ALP"` |
| `in_gov` | int64 | 1 = government; 0 = opposition/crossbench | `1` |
| `first_speech` | int64 | 1 = maiden speech | `0` |
| `body` | str | Full text of the utterance | `"I thank the member..."` |
| `question` | int64 | 1 = question during question time | `0` |
| `answer` | int64 | 1 = answer during question time | `0` |
| `q_in_writing` | int64 | 1 = question on notice / written answer detected in the daily XML; 6.2% of House rows | `0` |
| `div_flag` | int64 | 1 = division-related row | `0` |
| `gender` | str (nullable) | `"male"` or `"female"` | `"male"` |
| `unique_id` | str (nullable) | AustralianPoliticians unique identifier | `"Albanese1963"` |
| `interject` | int64 | 1 = interjection | `0` |
| `fedchamb_flag` | int64 | 0 = main chamber; 1 = Federation Chamber | `0` |
| `partyfacts_id` | float64 (nullable) | PartyFacts cross-national party identifier | `1385.0` |

### Debate Topics Schema

File (Senate): `data/output/senate/topics/senate_debate_topics.parquet/.csv`
File (House): daily parquets only (`data/output/house/topics/YYYY-MM-DD.parquet`)

9 columns. One row per debate or subdebate entry within a sitting day.

| Column | Type | Description | Example |
|---|---|---|---|
| `date` | str | Sitting day date | `"1998-03-02"` |
| `order` | int | Sequential order of this topic entry within the sitting day | `3` |
| `level` | int | Hierarchy level: `0` = top-level debate, `1` = subdebate | `0` |
| `debate_id` | str | Unique identifier for this topic entry, format `{date}_{order:04d}` | `"1998-03-02_0003"` |
| `parent_id` | str (nullable) | `debate_id` of the parent debate for subdebate rows; NULL for top-level rows | `"1998-03-02_0002"` |
| `topic` | str (nullable) | Title of the debate or subdebate as it appears in the XML | `"QUESTIONS WITHOUT NOTICE"` |
| `cognate` | str (nullable) | Cognate bill or topic title where a debate covers multiple bills; NULL for most rows | `"Income Tax Assessment Bill 1998"` |
| `gvt_business` | int | 1 = the debate type field contains a government/ministerial/budget keyword; 0 otherwise | `0` |
| `senate_flag` | int | Always `1` for Senate topic rows; `fedchamb_flag` (0 or 1) for House topic rows | `1` |

### Divisions Schema

#### Division Summary Table

File (Senate): `data/output/senate/divisions/senate_divisions.parquet/.csv`
House: daily parquets (`data/output/house/divisions/YYYY-MM-DD_divisions.parquet`)

| Column | Type | Description | Example |
|---|---|---|---|
| `date` | str | Sitting day date | `"2005-06-16"` |
| `division_no` | int | Sequential division number within the sitting day | `1` |
| `debate_topic` | str (nullable) | Title of the debate in which the division occurred | `"Appropriation Bill (No. 1) 2005-2006"` |
| `question` | str (nullable) | Text of the question put to the vote | `"That the amendment be agreed to."` |
| `ayes_count` | int | Number of senators/members voting aye | `36` |
| `noes_count` | int | Number of senators/members voting no | `30` |
| `result` | str | Outcome: `"aye"`, `"noe"`, or `"tie"` | `"aye"` |
| `fedchamb_flag` | int | House only: 0 = main chamber, 1 = Federation Chamber. All 4,616 recorded House divisions are main-chamber divisions. | `0` |

#### Division Vote-Level Table (Long Format)

File (Senate): `data/output/senate/divisions/senate_division_votes.parquet/.csv`
House: daily parquets (`data/output/house/divisions/YYYY-MM-DD_votes.parquet`)

One row per senator/member per division.

| Column | Type | Description | Example |
|---|---|---|---|
| `date` | str | Sitting day date | `"2005-06-16"` |
| `division_no` | int | Division number (joins to summary table) | `1` |
| `unique_id` | str (nullable) | AustralianPoliticians unique identifier | `"OBrien1959"` |
| `name_id` | str (nullable) | APH Parliamentary PHID | `"8O6"` |
| `name` | str | Senator's or member's name as it appears in the division list | `"O'Brien"` |
| `state` | str | Senator's state (Senate); `electorate` (member's electoral division) for House | `"Tasmania"` |
| `party` | str (nullable) | Party abbreviation back-filled from the daily corpus file | `"ALP"` |
| `vote` | str | `"aye"` or `"noe"` | `"aye"` |

## Reproducing the Pipeline

All pipeline scripts are run from the `pipeline/` directory. Full step-by-step execution commands, including all flags and directory arguments, are documented in `docs/11_pipeline_execution.md`.

A brief summary of the pipeline stages:

```bash
cd /home/jeffzda/Hansard/pipeline

# Step 0: Download raw XML from APH ParlInfo
python 00_download.py --chamber senate --start 1998-03-02 --end 2025-11-27 --out ../data/raw/senate
python 00_download.py --chamber reps   --start 1998-03-02 --end 2025-11-27 --out ../data/raw/reps

# Step 1: Session metadata
python 01_session_info.py

# Step 2: Build member lookup tables (downloads from GitHub at runtime)
python 02_member_lookup.py --out ../data/lookup
python 02b_member_lookup_house.py --out ../data/lookup

# Step 3: Parse XML (output to daily_raw/ first; fill_details reads from daily_raw/ and writes to daily/)
python 03_parse.py  --xml-dir ../data/raw/senate --out-dir ../data/output/senate/daily_raw
python 03b_parse_house.py --xml-dir ../data/raw/reps --out-dir ../data/output/house/daily_raw

# Step 4: Enrich with member metadata
python 04_fill_details.py  --daily-dir ../data/output/senate/daily --lookup-dir ../data/lookup --out-dir ../data/output/senate/daily
python 04b_fill_details_house.py --daily-dir ../data/output/house/daily --lookup-dir ../data/lookup --out-dir ../data/output/house/daily

# Step 5: Validate
python 05_validate.py  --daily-dir ../data/output/senate/daily --lookup-dir ../data/lookup --session-info ../data/lookup/session_info_all.csv
python 05b_validate_house.py --daily-dir ../data/output/house/daily --session-info ../data/lookup/session_info_all.csv --lookup-dir ../data/lookup --error-log ../data/output/house/validation_errors.csv

# Step 6: Add PartyFacts IDs
python 06_add_partyfacts.py --daily-dir ../data/output/senate/daily --lookup-dir ../data/lookup
python 06_add_partyfacts.py --daily-dir ../data/output/house/daily  --lookup-dir ../data/lookup

# Step 7: Assemble corpus
python 07_corpus.py --daily-dir ../data/output/senate/daily --out-dir ../data/output/senate/corpus --prefix senate_hansard_corpus
python 07_corpus.py --daily-dir ../data/output/house/daily  --out-dir ../data/output/house/corpus  --prefix house_hansard_corpus

# Steps 8–9: Supplementary datasets
python 08_debate_topics.py --xml-dir ../data/raw/senate --out-dir ../data/output/senate/topics
python 09_divisions.py     --xml-dir ../data/raw/senate --daily-dir ../data/output/senate/daily --out-dir ../data/output/senate/divisions
```

**Required packages** (install via `pip install -r requirements.txt`): `lxml`, `pandas`, `pyarrow`, `httpx`, `tqdm`, `requests`, `openpyxl`, `regex`, `beautifulsoup4`.

## Data Sources

| Resource | Description | URL |
|---|---|---|
| APH ParlInfo | Primary source — Senate and House XML Hansard files | https://parlinfo.aph.gov.au |
| AustralianPoliticians | Member biographical data, PHIDs, party spells, term dates (to ~Nov 2021) | https://github.com/RohanAlexander/australian_politicians |
| OpenAustralia parser | `senators.csv` and `people.csv` — gap fill for post-2021 senators | https://github.com/openaustralia/openaustralia-parser |
| Wikidata | SPARQL batch query used to fill gender for 35 post-2021 senators not covered by AustralianPoliticians | https://query.wikidata.org |
| PartyFacts | Cross-national party identifier database | https://partyfacts.herokuapp.com |
| GLAM Workbench | Senate XML harvest (reference) | https://github.com/wragge/hansard-xml |

## Relationship to Katz and Alexander (2023)

Katz and Alexander (2023) constructed a corpus of Australian House of Representatives Hansard proceedings covering 1998 to 2022, implemented in R. That dataset is available at https://zenodo.org/records/8121950 and the pipeline code is at https://github.com/lindsaykatz/hansard-proj.

This project extends Katz and Alexander in two respects. First, it adds full Senate coverage using the same APH ParlInfo XML source files and the same output schema conventions, making it straightforward to merge or compare the two chambers. Second, it extends both chambers forward to November 2025 (end of the 47th Parliament), whereas the original Katz and Alexander corpus ends in 2022.

The output schema is intentionally identical to Katz and Alexander where possible. The only structural differences are those forced by Senate-specific XML content: `state` replaces `electorate` in the Senate corpus (senators represent states, not electorates), and `senate_flag` (always 1 for Senate rows) replaces `fedchamb_flag`. Both flags serve the same purpose of identifying chamber membership when a combined corpus is assembled. Researchers who have used the Katz and Alexander dataset will find the column layout directly familiar.

## To Do

Pending work is tracked in [`docs/17_todo.md`](docs/17_todo.md). Items range from immediate pipeline fixes (re-running the fill step to apply the Pass 2 date-range guard) through data quality improvements, pre-1998 corpus expansion, committee Hansard ingestion, and active case studies.

## Known Gaps and Limitations

- **`partyfacts_id` is NULL for a small number of parties.** The following parties are not catalogued in the PartyFacts database and retain NULL `partyfacts_id` values: JLN (Jacqui Lambie Network), NatsWA (Nationals WA), AV (Australia's Voice), and IND (Independent). PRES and DPRES are non-party presiding-officer role labels and are not mapped. All other Australian parties appearing in the corpus now have verified PartyFacts IDs.

- **T6/T7 validation has ~144/229 Senate and ~451/762 House known errors (before re-fill).** These are false positives from Pass 2 name-form matching assigning the wrong `unique_id` when a modern member shares a surname with a historical one. A date-range guard has been added to Pass 2 in both `04_fill_details.py` and `04b_fill_details_house.py`; re-running the fill step (→ `06_add_partyfacts.py` → `07_corpus.py`) will resolve most of these. The fix is in place but daily files have not yet been regenerated.

- **Committee Hansard is collected separately, not in the main corpus.** A committee Hansard pipeline was built in March 2026 (`00b_download_committee.py`, `03c_parse_committee.py`, `04c_fill_committee.py`). Committee data lives in `data/raw/committee/` and `data/output/committee/`. See `docs/18_committee_pipeline.md` for schema and usage. The full historical download has not yet been run.

- **`in_gov` was recomputed from the known government timeline on 2026-03-11.** The XML `<in.gov>` flag was unreliable from ~2012 onward (APH changed to a self-closing empty form). `fix_in_gov.py` recomputes `in_gov` from hardcoded Coalition/Labor periods (1996–present) and rewrites both corpus files. If the corpus is regenerated, re-run `fix_in_gov.py` before case-study searches.

- **Raw House XML is stored in `data/raw/reps/` (not `data/raw/house/`).** This naming reflects the APH ParlInfo dataset code (`hansardr`) and was not corrected retroactively.

## Citation

A formal citation and DOI (Zenodo record) are pending. In the meantime, if you use this dataset please cite:

> [Authors]. (2025). *Australian Hansard Database, 1998–2025* [Data set]. Retrieved from [repository URL].

This dataset extends: Katz, L., & Alexander, R. (2023). *Australian House of Representatives Hansard Corpus, 1998–2022*. Zenodo. https://doi.org/10.5281/zenodo.8121950

## License

The pipeline code in this repository is available for reuse. The underlying data is derived from the Commonwealth of Australia Hansard transcripts, which are Crown copyright. Users are responsible for complying with the Australian Parliament House terms of use. Hansard is generally available for non-commercial and research use; see https://www.aph.gov.au/Help/Disclaimer_Privacy_Copyright for current terms.
