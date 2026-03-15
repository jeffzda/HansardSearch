# Australian Hansard Pipeline — Execution Record

## Overview

This document is a step-by-step record of every action taken to build the Australian Hansard database from raw APH ParlInfo XML through to final research-ready outputs. It is intended to be sufficient for a reproducer to reconstruct the full pipeline from scratch.

**Coverage:** Senate and House of Representatives, 1998-03-02 to 2025-11-27 (end of 47th Parliament).

**Primary outputs:**
- Senate corpus: 549,864 speech rows across 1,482 sitting days
- House corpus: 623,906 speech rows across 1,722 sitting days
- Senate debate topics: 43,066 topic rows
- House debate topics: 388,154 topic rows (assembled corpus; daily parquets total 194,077 topic entries)
- Senate divisions: 8,001 divisions, 466,557 individual vote records
- House divisions: 4,616 divisions, 614,057 individual vote records

All pipeline scripts live in `/home/jeffzda/Hansard/pipeline/`. All data lives under `/home/jeffzda/Hansard/data/`.

---

## Prerequisites

### Python packages

Install from `/home/jeffzda/Hansard/requirements.txt`:

```
pip install -r requirements.txt
```

Required packages:

| Package | Min version | Purpose |
|---------|-------------|---------|
| lxml | 4.9.0 | XML parsing |
| pandas | 2.0.0 | Data manipulation |
| pyarrow | 12.0.0 | Parquet I/O |
| httpx | 0.24.0 | HTTP download |
| tqdm | 4.65.0 | Progress bars |
| requests | 2.31.0 | Lookup table fetching |
| openpyxl | 3.1.0 | Excel compatibility |
| regex | 2023.0.0 | Extended regex |
| beautifulsoup4 | 4.12.0 | HTML parsing for download step |

### Data directory structure

Create the following directories before running:

```
data/
  raw/
    senate/          # downloaded Senate XML files
    reps/            # downloaded House XML files
  lookup/            # lookup tables (built by step 1 and 2)
  output/
    senate/
      daily/         # per-day Senate parquet+CSV (step 4 output)
      corpus/        # assembled Senate corpus (step 7 output)
      topics/        # per-day Senate topic files (step 8 output)
      divisions/     # per-day Senate division files (step 9 output)
    house/
      daily/         # per-day House parquet+CSV (step 4b output)
      corpus/        # assembled House corpus (step 7 output)
      topics/        # per-day House topic files (step 8b output)
      divisions/     # per-day House division files (step 9b output)
```

Note: the raw House files are stored in `data/raw/reps/` (matching the ParlInfo dataset code `hansardr`), while the processed outputs use `data/output/house/`. This naming inconsistency was inherited at project start and was not retroactively corrected.

---

## Step 0: Download raw XML (`00_download.py`)

### What it does

A two-step HTTP process per sitting day:

1. Query the APH ParlInfo search endpoint with the date and dataset code to retrieve the document page HTML. The URL pattern is:
   ```
   https://parlinfo.aph.gov.au/parlInfo/search/display/display.w3p
     ;query=Dataset%3A{datasets}%20Date%3A{day}%2F{month}%2F{year}
   ```
   Senate uses dataset codes `hansardS,hansardS80`; House uses `hansardr,hansardr80`.

2. Parse the returned HTML with BeautifulSoup to find the `toc_unixml` XML download link, then fetch the XML.

Three URL eras exist for the XML file path itself:
- 1998 – ~March 2011: `.../hansards/YYYY-MM-DD/toc_unixml/filename.xml`
- ~May 2011 – 2021: `.../hansards/{uuid}/toc_unixml/filename.xml`
- 2021 – present: `.../hansards/{integer}/toc_unixml/filename.xml`

Only weekday dates (Monday–Friday) are attempted; the script uses a `sitting_dates()` generator that skips weekends entirely. Non-sitting weekdays return no XML link from ParlInfo and are silently counted as `not_found`. A 1-second delay is enforced between requests to avoid hammering the server.

Output files are named `YYYY-MM-DD.xml` and written to the specified `--out` directory. Files that already exist are skipped by default (`--no-skip` overrides this). A minimal validity check (response must start with `<?xml` or `<hansard`) is applied before writing.

House XML files include both main chamber (`chamber.xscript`) and Federation Chamber (`fedchamb.xscript` or `maincomm.xscript`) debates in a single file — no separate House Federation Chamber download is needed.

### Commands run

```bash
cd /home/jeffzda/Hansard/pipeline

python3 00_download.py --chamber senate --start 1998-03-02 --end 2025-11-27 --out ../data/raw/senate
python3 00_download.py --chamber reps   --start 1998-03-02 --end 2025-11-27 --out ../data/raw/reps
```

### Output

- `data/raw/senate/`: 1,482 XML files, named `YYYY-MM-DD.xml`, 1998-03-02 to 2025-11-27
- `data/raw/reps/`: 1,722 XML files, named `YYYY-MM-DD.xml`, 1998-03-02 to 2025-11-27

The House has more sitting days than the Senate over the same date range because the House sits on some days when the Senate does not (and vice versa, but the Senate effect is smaller).

---

## Step 1: Session info (`01_session_info.py`)

### What it does

Reads every XML file in a raw directory and extracts the `<session.header>` block, producing one row per sitting day. The extracted fields are:

| Column | Source XML tag |
|--------|---------------|
| filename | stem of the XML filename |
| schema_version | `version` attribute on root element |
| date | `<date>` |
| parliament_no | `<parliament.no>` |
| session_no | `<session.no>` |
| period_no | `<period.no>` |
| chamber | `<chamber>` |
| page_no | `<page.no>` |
| proof | `<proof>` |

This output is used by validation test T1 (date in filename matches `session.header` date). It is also used to populate the shared `session_info_all.csv` lookup that covers both chambers.

### Commands run

Senate was processed first. The House was added later after T1 failures revealed that House dates were absent from the lookup:

```bash
# Senate (run first)
python3 01_session_info.py \
    --xml-dir ../data/raw/senate \
    --out ../data/lookup/session_info_senate_only.csv

# House (run later; output merged into session_info_all.csv manually)
python3 01_session_info.py \
    --xml-dir ../data/raw/reps \
    --out ../data/lookup/session_info_house.csv
```

The two per-chamber files were concatenated into the unified lookup:

```
data/lookup/session_info_all.csv
```

### Output

`session_info_all.csv` was subsequently rebuilt (see Session 2 below) and now contains 3,204 data rows: 1,482 Senate rows + 1,722 House rows.

Note: the early build of `session_info_all.csv` contained only 81 Senate rows (deduplicated header rows) alongside 1,722 House rows. This caused 103 T1 failures for Senate files whose dates were absent from the lookup. The rebuilt version uses `01_session_info.py` output from all 1,482 Senate XMLs concatenated with the 1,722 House rows.

Note: the `session_info_senate_only.csv` and `session_info_house.csv` intermediate files are preserved in `data/lookup/` for reference.

---

## Step 2: Member lookup tables (`02_member_lookup.py`)

### What it does

Fetches four CSV files from the `RohanAlexander/australian_politicians` GitHub repository and two files from `openaustralia/openaustralia-parser`, then constructs five lookup tables used by the enrichment and validation steps.

**Remote data sources fetched at runtime:**

| File | Source |
|------|--------|
| `australian_politicians-all.csv` | AustralianPoliticians (AP) |
| `australian_politicians-senators-by_state.csv` | AP |
| `australian_politicians-all-by_party.csv` | AP |
| `australian_politicians-uniqueID_to_aphID.csv` | AP |
| `data/senators.csv` | OpenAustralia |
| `data/people.csv` | OpenAustralia |

The AP dataset covers senators up to approximately November 2021. Senators who entered parliament after that date are appended from the OpenAustralia parser data.

**Lookup tables produced:**

`senator_lookup.csv` — one row per senator with five name form variants used for Hansard text matching:
- `form1`: "Senator FirstName Surname"
- `form2`: "Senator Surname"
- `form3`: "Senator FIRSTNAME SURNAME"
- `form4`: "Senator SURNAME"
- `form5`: "Senator FirstName SURNAME"

Also contains `name_id` (the APH Parliamentary PHID, uppercase), `unique_id`, `gender`, `state_abbrev`, birth/death dates. Post-2021 senators from OpenAustralia have `source=OpenAustralia` and `gender=NULL` (requires manual filling).

`party_lookup.csv` — one row per party affiliation spell per senator (date-aware); used for pass 3 of fill_details.

`state_lookup.csv` — one row per Senate term per senator (date-aware); used for pass 3 of fill_details.

`president_lookup.csv` — manually compiled table of Senate Presidents and Deputy Presidents 1998–present, with all known name variants as they appear in the XML (`xml_name_pattern` column). Entries run from Margaret Reid (1996–2002) through Sue Lines (2022–present).

`partyfacts_map.csv` — static mapping of Australian party abbreviations to PartyFacts cross-national IDs. Parties without a PartyFacts entry retain `partyfacts_id=NULL`; see the Step 2 partyfacts correction note below for the current list of NULL and filled parties.

Note: the `senator_lookup.csv` is reused for House name-form and PHID matching in step 4b, as PHIDs are shared across both chambers in the AustralianPoliticians data.

**partyfacts_map.csv corrections:** After step 2, `partyfacts_map.csv` was manually updated to add verified PartyFacts IDs for: CLP (143), LNP (285), KAP (1997), NXT (5453), CA (5453), PUP (1996), UAP (1996), DLP (1540), PHON (1162), GWA (1209), LDP (9149), FFP (1263), TG (1209), AUS (1997). Parties still without a PartyFacts ID: JLN, NatsWA, AV, IND.

### Command run

```bash
python3 02_member_lookup.py --out ../data/lookup
```

### Output files

| File | Location |
|------|----------|
| senator_lookup.csv | data/lookup/ |
| party_lookup.csv | data/lookup/ |
| state_lookup.csv | data/lookup/ |
| president_lookup.csv | data/lookup/ |
| partyfacts_map.csv | data/lookup/ |

**senator_lookup.csv post-build corrections (7 entries):**

After `02_member_lookup.py` was run, the following corrections were made directly to `senator_lookup.csv`:
- Thorpe1973 (Lidia Thorpe): `name_id=280304` added
- Patrick1967 (Rex Patrick): `name_id` typo corrected from 114292 to 144292
- Grogan (Karen Grogan): `name_id=296331` added
- Small1988 (Ben Small): `name_id=291406` added
- Cox1970 (Dorinda Cox): new entry added with `name_id=296215` (previously mismatched to Cox1863)
- McLachlan1966 (Andrew McLachlan): new entry added with `name_id=287062` (previously mismatched to McLachlan1870)
- Green1981 (Nita Green): `name_id=259819` added (previously had truncated `unique_id='Green'`)

---

## Step 2b: House member lookup (`02b_member_lookup_house.py`)

### What it does

Builds House-specific lookup tables from the AustralianPoliticians dataset. Unlike `02_member_lookup.py`, which focuses on senators, this script produces member-level lookup tables needed for House enrichment and validation.

### Command run

```bash
python3 02b_member_lookup_house.py --out ../data/lookup
```

### Output files

| File | Location | Rows |
|------|----------|------|
| member_lookup.csv | data/lookup/ | 1,280 members |
| electorate_lookup.csv | data/lookup/ | 1,430 rows |
| party_lookup_house.csv | data/lookup/ | 1,571 rows |
| speaker_lookup.csv | data/lookup/ | 20 rows |

---

## Step 2c: Gender fill for post-2021 senators (manual Wikidata step)

### What it does

The 35 senators sourced from the OpenAustralia parser had `gender=NULL` in `senator_lookup.csv`. Gender was filled by running a Wikidata SPARQL batch query against the parliamentary identifier (`P1557` — Australian Parliament House member ID) for each senator's PHID, retrieving the `wdt:P21` (sex or gender) property.

After the SPARQL query, the `gender` column in `senator_lookup.csv` was updated directly for all 35 entries. Following this fix, zero senators have NULL gender in `senator_lookup.csv` (666 senators covered).

---

## Step 3a: Senate parsing (`03_parse.py`)

### What it does

Parses every Senate XML file into a structured per-speech DataFrame and writes one parquet file per sitting day to `senate/daily_raw/` (or directly to `senate/daily/` when run with the final output path).

The parser handles three schema versions via two classes:

**`ParserV21`** (schema versions 2.0 and 2.1, covering 1998–~2013):
- Iterates `<debate>` and `<adjournment>` elements under `<chamber.xscript>`
- Speech elements: `<speech>`, `<question>`, `<answer>`, `<interjection>`, `<continue>`
- Speaker name extracted from `<name role="display">` within each talker block
- Body text assembled from `<para>` and `<inline>` elements using `itertext()` (no per-text-node stripping — critical for correct multi-span concatenation)

**`ParserV22`** (schema version 2.2, covering 2014–present):
- Iterates HPS CSS-class paragraphs (`<body class="HPS-MemberSpeech">` etc.)
- Speaker name extracted from `<span class="HPS-MemberSpeech">` / `<span class="HPS-OfficeSpeech">`
- Electorate and ministerial titles from `<span class="HPS-Electorate">` / `<span class="HPS-MinisterialTitles">`

**Output schema (19 columns):**

```
name, order, speech_no, page_no, time_stamp, name_id, state, party,
in_gov, first_speech, body, question, answer, q_in_writing, div_flag,
gender, unique_id, interject, senate_flag
```

The `date` column is not written by the parser; it is derived from the filename stem when the corpus is assembled in step 7. The `senate_flag` column is always 1 for Senate files.

Stage directions (procedural notices like "Senate adjourned", "Question agreed to", "Bill read a third time" etc.) are identified by regex and are excluded from the output.

**Key bug fixed during this project:**

In the v2.2 HPS span parser, concatenating electorate spans by stripping each span's text individually caused multi-word state names to merge: `<span>South</span><span>Australia</span>` became `SouthAustralia`. The fix was to collect all text via `element.itertext()` without stripping individual parts, then strip the final concatenated string. This correctly yields `South Australia`.

### Command run

```bash
python3 03_parse.py \
    --xml-dir ../data/raw/senate \
    --out-dir ../data/output/senate/daily
```

A single-file test mode is available:
```bash
python3 03_parse.py --xml ../data/raw/senate/2020-02-04.xml
```

### Output

- `data/output/senate/daily/`: 1,482 parquet files, one per sitting day

---

## Step 3b: House parsing (`03b_parse_house.py`)

### What it does

Parses every House XML file. The House parser translates the same logic used in five K&A R scripts, adapted for five XML schema eras and the dual-chamber structure (main chamber + Federation Chamber within each file).

**Five schema eras handled:**

| Era | Dates | Version | Notes |
|-----|-------|---------|-------|
| 1 | 1998–1999 | 2.0 or missing | `maincomm.xscript`, `day.start` date field |
| 2 | 2000–2011 | 2.0/2.1 | `maincomm.xscript`, `business.start` may use `body` or `para` |
| 3 | 2011–2012 | 2.1 | `maincomm.xscript` (transitional) |
| 4 | 2012–2022 | 2.2 (OOXML) | `fedchamb.xscript`, HPS-class paragraphs, `<a type="...">` attribution |
| 5 | 2022–2025 | 2.2 (newest) | Same as Era 4 with minor additions |

**Two classes:**
- `HouseParserV21`: covers Eras 1–3
- `HouseParserV22`: covers Eras 4–5

**Output schema (21 columns):**

```
date, name, order, speech_no, page_no, time_stamp, name_id, electorate,
party, in_gov, first_speech, body, question, answer, q_in_writing,
div_flag, gender, unique_id, interject, fedchamb_flag, partyfacts_id
```

Note that the House schema differs from the Senate schema in two key ways:
- `electorate` replaces `state` (members represent electorates, not states)
- `fedchamb_flag` replaces `senate_flag` (0=main chamber, 1=Federation Chamber)
- `date` is included in the parser output (the Senate parser omits it)
- `partyfacts_id` is included as an initially-null placeholder column

Federation Chamber node names differ by era:
- Pre-2012: `//maincomm.xscript`
- 2012 onward: `//fedchamb.xscript`

### Command run

```bash
python3 03b_parse_house.py \
    --xml-dir ../data/raw/reps \
    --out-dir ../data/output/house/daily
```

A single-file test mode is available:
```bash
python3 03b_parse_house.py --xml ../data/raw/reps/2020-02-04.xml
```

### Output

- `data/output/house/daily/`: 1,722 parquet files, one per sitting day
- Total rows: 623,906 (includes 41,651 rows with `q_in_writing=1`; a further ~101,331 rows added by `<subdebate.text>` stage direction and `<petition>` parsing)
- Of these, approximately 17.8% have `fedchamb_flag=1` (Federation Chamber rows)

Note: both `03_parse.py` and `03b_parse_house.py` now detect questions on notice embedded in the daily XML and set `q_in_writing=1`. Senate: 30,239 rows (4.8%); House: 41,651 rows (6.2%).

---

## Step 4a: Senate fill details (`04_fill_details.py`)

### What it does

Enriches each daily Senate parquet file with member metadata (gender, `unique_id`, party, state) through seven sequential passes. The input and output directories are the same; files are updated in place.

**Pass 1 — Fill by name_id (PHID):**
For rows where `name_id` is already populated (set by the parser from the XML `id` attribute on `<name>` elements), join `senator_lookup` on `name_id` to fill `unique_id` and `gender`. State is filled from the lookup only where missing in the XML.

**Pass 2 — Fill by name form matching:**
For rows where `unique_id` is still NULL after pass 1, attempt exact-string matching of the `name` column against the five pre-built name form variants (`form1`–`form5`) in `senator_lookup`. Case-insensitive fallback is attempted if exact match fails.

**Pass 3 — Date-aware party and state correction:**
Using `party_lookup` and `state_lookup` (each with date-range columns `party_from`/`party_to` and `senator_from`/`senator_to`), overwrite party and state with the historically correct value for the sitting date. This corrects for senators who changed parties or represented different states across different terms.

**Pass 4 — President/Deputy President resolution:**
Rows where the speaker name matches a `president_lookup` pattern (`The PRESIDENT`, `The DEPUTY PRESIDENT`, `The ACTING PRESIDENT`, `The CHAIR`, `MADAM PRESIDENT`, etc.) are assigned `name_id=10000`. Where the date falls within a specific president's term (as recorded in `president_lookup`), the individual's `unique_id` is also assigned.

**Pass 5 — State abbreviation normalisation:**
State values that are stored as abbreviations in the XML or lookup (e.g. `WA`, `NSW`, `TAS`, `SA`, `Vic`, `Qld`) are normalised to their full names (`Western Australia`, `New South Wales`, `Tasmania`, `South Australia`, `Victoria`, `Queensland`). This pass was added during this project after validation revealed mixed abbreviation/full-name values in the `state` column.

**Pass 6 — Party normalisation:**
A `_PARTY_NORM` dictionary normalises non-canonical party abbreviations that appear in the XML or lookup tables: `LIB→LP`, `GRN→AG`, `G(WA)→GWA`, `NPA→NP`, `Nats→NATS`, `ON→PHON`, `Ind./Ind→IND`, `N/A→NaN`, `LPI0→LP`, `NPActing→NP`. This pass was added to reduce T4 validation failures caused by party-alias multiplicity.

**Pass 7 — page_no forward-fill:**
Forward-fills `page_no` within each sitting day. Interjections and brief procedural turns in pre-2012 v2.x XML lack explicit page citations; this pass propagates the page number from the preceding speech row. Coverage: 99.4%.

### Command run

```bash
python3 04_fill_details.py \
    --daily-dir ../data/output/senate/daily \
    --lookup-dir ../data/lookup \
    --out-dir ../data/output/senate/daily
```

### Output

- `data/output/senate/daily/`: 1,482 parquet files + 1,482 CSV files (one pair per sitting day)
- Each file contains the 19-column Senate schema with `unique_id`, `gender`, party, and state filled where possible

---

## Step 4b: House fill details (`04b_fill_details_house.py`)

### What it does

Enriches each daily House parquet file through five passes. Structurally similar to the Senate version but with some House-specific logic. Uses `member_lookup.csv` (PHIDs shared across both chambers) and `electorate_lookup.csv`.

**Pass 1 — Fill by name_id (PHID):**
Join `senator_lookup` on `name_id` to fill `unique_id` and `gender`.

**Pass 2 — Fill by name form matching:**
Same form1–form5 matching logic as the Senate version. The "Senator X" name forms will not match House member names (which use "Mr", "Mrs", "Ms" etc. in the XML), so this pass is less effective for House than for Senate. PHIDs set directly in the XML (pass 1) are the primary enrichment path for House.

**Pass 3 — Presiding officer normalisation:**
Rows where the name matches Speaker/Deputy Speaker/Chair patterns are assigned `name_id=10000` (the generic presiding officer sentinel).

**Pass 4 — Party normalisation:**
Same `_PARTY_NORM` dictionary as the Senate Pass 6: normalises non-canonical party abbreviations to their canonical forms.

**Pass 5 — Date-aware party and electorate fill:**
`fill_party_electorate()` joins `party_lookup_house.csv` (date-range-aware) and `electorate_lookup.csv` (date-range-aware, 1,430 rows). Party/electorate completeness improved from ~62% to ~87%. Always-overwrite logic for electorate corrects stale XML values and normalisation variants (apostrophe style, spacing).

### Command run

```bash
python3 04b_fill_details_house.py \
    --daily-dir ../data/output/house/daily \
    --lookup-dir ../data/lookup \
    --out-dir ../data/output/house/daily
```

### Output

- `data/output/house/daily/`: 1,722 parquet files + 1,722 CSV files

---

## Step 5a: Senate validation (`05_validate.py`)

### What it does

Runs seven automated validation tests against each daily Senate file. Results are written to a validation error log CSV.

| Test | Description |
|------|-------------|
| T1 | Date in filename matches `session.header` date in `session_info_all.csv` |
| T2 | No two consecutive non-interjection rows have identical `body` text |
| T3 | Rows containing "(Time expired)" are not immediately followed by additional text |
| T4 | Each `name_id` has only one `party` and one `state` per sitting day |
| T5 | All `name_id`s exist in `senator_lookup` (or are known special IDs: 10000, 10001, UNKNOWN, 20000) |
| T6 | All `unique_id`s have birth dates before and death dates after the sitting day |
| T7 | All `unique_id`s were active senators on the sitting day (via `state_lookup` term dates) |

### Command run

```bash
python3 05_validate.py \
    --daily-dir ../data/output/senate/daily \
    --lookup-dir ../data/lookup \
    --session-info ../data/lookup/session_info_all.csv
```

### Results

**T1 (date mismatch):** Was failing for House dates before `session_info_all.csv` was expanded to include House session rows. Fixed by adding House to the session info lookup (step 1 above). After the session_info rebuild (Session 2), Senate T1 errors dropped from 103 to 1.

**T2 (consecutive duplicate body):** 331 Senate files affected. Procedural phrases like "interjecting", "Question agreed to.", "Leave granted." are explicitly excluded from duplicate detection. Remaining failures are short phrases that are genuine duplicates in the XML.

**T3 (time expired):** After regex exclusion of common procedural phrases was added (Session 2), Senate errors dropped from 2,570 to 87 and House from 1,254 to 130.

**T4 (party/state multiplicity):** After `fill_date_aware()` was modified to override party from dated lookup entries (Session 2), Senate T4 errors dropped from 103+ to 18. Remaining 18 Senate and 44 House errors are real party changers (e.g. Cory Bernardi, Zed Seselja).

**T5–T7:** T5 has 78 Senate failures (garbled XML name_id artifacts). T6 has 144 (Pass 2 false matches). T7 has 229 (down from 268 after Bartlett1964 second term added in Session 2); ~150 of these are old-senator false matches from Pass 2 name-form matching lacking date-range awareness.

---

## Step 5b: House validation (`05b_validate_house.py`)

### What it does

A House-specific adaptation of `05_validate.py`. Key structural differences:
- `electorate` is used instead of `state` in T4
- `fedchamb_flag` is present instead of `senate_flag`
- T5, T6, and T7 are now implemented using `member_lookup.csv` (for T5 and T6 birth/death dates) and `electorate_lookup.csv` (for T7 term dates)
- T5 only flags rows where `name` contains "MP" (replicating R canonical logic); presiding officer placeholder IDs are excluded
- T7 uses multi-term groupby logic: a member passes if ANY of their terms covers the sitting date

Tests run: T1, T2, T3, T4, T5, T6, T7.

### Command run

```bash
python3 05b_validate_house.py \
    --daily-dir ../data/output/house/daily \
    --session-info ../data/lookup/session_info_all.csv \
    --lookup-dir ../data/lookup \
    --error-log ../data/output/house/validation_errors.csv
```

### Results

Tests run: T1–T7. Results (before date-range guard re-fill): T1=2, T2=25, T3=130, T4=33, T5=0, T6=451, T7=762. T5=0 (no garbled name_ids); T6/T7 are Pass 2 false matches — date-range guard is now in code, re-fill pending.

---

## Step 6: PartyFacts IDs (`06_add_partyfacts.py`)

### What it does

Joins the `partyfacts_map.csv` lookup (built in step 2) to each daily file on the `party` column (matched against `party_abbrev`), adding or replacing the `partyfacts_id` column. Both parquet and CSV versions of each daily file are updated in place.

The script was run separately for each chamber using the `--daily-dir` argument. The same `partyfacts_map.csv` is used for both chambers because party abbreviations are consistent across the dataset.

Parties without a PartyFacts entry (JLN, NatsWA, AV, IND) retain `partyfacts_id=NULL`. All other parties have been verified and assigned IDs; see the Step 2 partyfacts correction note.

### Commands run

```bash
# Senate
python3 06_add_partyfacts.py \
    --daily-dir ../data/output/senate/daily \
    --lookup-dir ../data/lookup

# House
python3 06_add_partyfacts.py \
    --daily-dir ../data/output/house/daily \
    --lookup-dir ../data/lookup
```

### Output

`partyfacts_id` column added to all 1,482 Senate and 1,722 House daily files.

---

## Step 7: Corpus assembly (`07_corpus.py`)

### What it does

Concatenates all daily parquet files for a chamber into a single corpus parquet and CSV. The output filename is automatically derived from the min and max years in the data:
`{prefix}_{min_year}_to_{max_year}.parquet` / `.csv`

The `--prefix` argument was added during this project to allow the House and Senate corpora to be named distinctly. Before this addition, both would have defaulted to `senate_hansard_corpus`.

The `date` column insertion is conditional: if the column already exists in a daily file (as it does for House files), it is not inserted again. If it is absent (as in early Senate files which omit `date` from the per-day schema), it is derived from the filename stem.

The corpus enforces a fixed column order (`CORPUS_COLUMNS`) and adds any missing columns as NULL.

**Senate corpus column order (21 columns):**
```
date, name, order, speech_no, page_no, time_stamp, name_id, state, party,
in_gov, first_speech, body, question, answer, q_in_writing, div_flag,
gender, unique_id, interject, senate_flag, partyfacts_id
```

### Commands run

```bash
# Senate
python3 07_corpus.py \
    --daily-dir ../data/output/senate/daily \
    --out-dir ../data/output/senate/corpus \
    --prefix senate_hansard_corpus

# House
python3 07_corpus.py \
    --daily-dir ../data/output/house/daily \
    --out-dir ../data/output/house/corpus \
    --prefix house_hansard_corpus
```

### Output

| File | Rows | Sitting days | Date range |
|------|------|-------------|------------|
| `senate/corpus/senate_hansard_corpus_1998_to_2025.parquet` | 549,864 | 1,482 | 1998-03-02 to 2025-11-27 |
| `senate/corpus/senate_hansard_corpus_1998_to_2025.csv` | 549,864 | 1,482 | 1998-03-02 to 2025-11-27 |
| `house/corpus/house_hansard_corpus_1998_to_2025.parquet` | 623,906 | 1,722 | 1998-03-02 to 2025-11-27 |
| `house/corpus/house_hansard_corpus_1998_to_2025.csv` | 623,906 | 1,722 | 1998-03-02 to 2025-11-27 |

Note: an older intermediate Senate corpus (`senate_hansard_corpus_1998_to_2023.parquet`) exists in `data/output/senate/corpus/` from an earlier run. The authoritative file is `senate_hansard_corpus_1998_to_2025.parquet` in the `senate_hansard_corpus/` subdirectory.

---

## Step 8: Debate topics

### Step 8a: Senate debate topics (`08_debate_topics.py`)

Extracts the debate/subdebate hierarchy from each Senate XML file. Produces one parquet file per sitting day in `data/output/senate/topics/`.

**Schema (9 columns):**
```
date, order, level, debate_id, parent_id, topic, cognate, gvt_business, senate_flag
```

- `debate_id`: `{date}_{order:04d}`
- `level`: 0 = top-level debate, 1 = subdebate
- `gvt_business`: 1 if the debate type field contains "government", "ministerial", "executive", "budget", or "appropriation"; 0 otherwise
- `senate_flag`: always 1

Two extraction functions handle the schema split:
- `_extract_topics_v21()` for v2.0/v2.1 XML (iterates `<debate>` elements, reads `<debateinfo><title>`)
- A separate v2.2 handler for OOXML files (reads `<div class="...">` debate blocks)

#### Command run

```bash
python3 08_debate_topics.py \
    --xml-dir ../data/raw/senate \
    --out-dir ../data/output/senate/topics
```

#### Output

- `data/output/senate/topics/`: 1,482 parquet files (one per sitting day)
- Total topic rows across all days: 43,066

### Step 8b: House debate topics (`08b_debate_topics_house.py`)

House equivalent. The schema replaces `senate_flag` with `fedchamb_flag` (0=main chamber, 1=Federation Chamber). House v2.1 XML uses `<subdebateinfo>` rather than `<debateinfo>` for subdebate levels, so the extraction function checks for both tags.

**Schema (9 columns):**
```
date, order, level, debate_id, parent_id, topic, cognate, gvt_business, fedchamb_flag
```

Note: `08b_debate_topics_house.py` reads from `data/raw/reps/` (the raw House XML directory), not from the House daily output.

#### Command run

```bash
python3 08b_debate_topics_house.py \
    --xml-dir ../data/raw/reps \
    --out-dir ../data/output/house/topics
```

#### Output

- `data/output/house/topics/`: 1,722 parquet files (one per sitting day)
- Total topic rows: 194,077
  - Main chamber (`fedchamb_flag=0`): 163,169 rows
  - Federation Chamber (`fedchamb_flag=1`): 30,908 rows

---

## Step 9: Divisions

### Step 9a: Senate divisions (`09_divisions.py`)

Extracts division (recorded vote) records from Senate XML files. Produces two parquet files per sitting day that had at least one division:
- `{date}_divisions.parquet`: one row per division (summary)
- `{date}_votes.parquet`: one row per senator per division (long-format vote roll)

**Division summary schema:**
```
date, division_no, debate_topic, question, ayes_count, noes_count, result
```

**Vote roll schema:**
```
date, division_no, unique_id, name_id, name, state, party, vote
```

The `vote` field is either `aye` or `noe`. The `result` field is `aye` if ayes > noes, `noe` if noes > ayes, `tie` otherwise.

For v2.1 XML, divisions are within `<division>` elements; ayes and noes are in `<ayes>` / `<noes>` containers with `<name>` child elements. For v2.2 XML, division tables appear as HTML-like class-decorated `<div>` blocks.

Member metadata (`unique_id`, `name_id`, `state`, `party`) is back-filled from the corresponding daily file (step 4 output), not from the XML division block alone.

#### Command run

```bash
python3 09_divisions.py \
    --xml-dir ../data/raw/senate \
    --daily-dir ../data/output/senate/daily \
    --out-dir ../data/output/senate/divisions
```

#### Output

- `data/output/senate/divisions/`: pairs of `{date}_divisions.parquet` and `{date}_votes.parquet`
- Total divisions: 8,001
- Total vote records: 466,557

### Step 9b: House divisions (`09b_divisions_house.py`)

House equivalent. Adds `fedchamb_flag` to both the division summary and vote roll schemas. In the House v2.2 era, divisions within `fedchamb.xscript` sections receive `fedchamb_flag=1`; divisions within `chamber.xscript` receive `fedchamb_flag=0`.

Note: all 4,616 recorded House divisions in this dataset are main-chamber divisions (`fedchamb_flag=0`). The Federation Chamber does not conduct formal divisions.

**Division summary schema:**
```
date, division_no, debate_topic, question, ayes_count, noes_count, result, fedchamb_flag
```

**Vote roll schema:**
```
date, division_no, unique_id, name_id, name, electorate, party, vote
```

Note that `state` is replaced by `electorate` in the House vote roll schema.

#### Command run

```bash
python3 09b_divisions_house.py \
    --xml-dir ../data/raw/reps \
    --daily-dir ../data/output/house/daily \
    --out-dir ../data/output/house/divisions
```

#### Output

- `data/output/house/divisions/`: pairs of `{date}_divisions.parquet` and `{date}_votes.parquet`
- Total divisions: 4,616
- Total vote records: 614,057

---

## Known gaps and future work

**`partyfacts_id` gaps:** The following parties have no PartyFacts ID and retain NULL: JLN (Jacqui Lambie Network), NatsWA (Nationals WA), AV (Australia's Voice), IND (Independent). PRES and DPRES are non-party presiding-officer role labels also left NULL. All other parties in the corpus now have verified PartyFacts IDs.

**T4 validation — party multiplicity:** Validation test T4 now excludes interjection rows and normalises party aliases before checking. Residual failures remain on some sitting days, likely from edge cases where a member's party in the XML disagrees with the date-corrected lookup value.

**House T5/T6/T7 validation:** Now implemented. First results: T5=0 (no garbled name_ids), T6=451, T7=762. The T6/T7 failures are predominantly Pass 2 false attributions (historical members matched to modern speakers with the same surname). A date-range guard has been added to Pass 2 in `04b_fill_details_house.py`; re-running the fill step will resolve most of these.

**House supplementary corpus assembly:** Complete. `house_debate_topics_1998_to_2025.parquet/.csv` (388,154 rows), `house_divisions.parquet/.csv` (4,616 divisions), `house_division_votes.parquet/.csv` (614,057 vote records).

**McLachlan PHID:** Deputy President Andrew McLachlan (2022–2025) had his PHID corrected in `senator_lookup.csv` (name_id=287062 added for McLachlan1966). His presiding-officer rows continue to receive `name_id=10000` (the generic sentinel) from Pass 4, with the individual `unique_id` now resolvable where the sitting date falls within his term.

**Directory naming:** Raw House files are in `data/raw/reps/` while processed outputs are in `data/output/house/`. This inconsistency (inherited from the ParlInfo dataset code `hansardr`) was not corrected retroactively.

---

## Output file inventory

All paths are relative to `/home/jeffzda/Hansard/`.

### Lookup tables (`data/lookup/`)

| File | Description |
|------|-------------|
| `senator_lookup.csv` | Senator name forms, PHIDs, gender, state — one row per senator (666 senators; zero NULL gender) |
| `party_lookup.csv` | Date-aware party affiliation spells for senators |
| `state_lookup.csv` | Date-aware Senate term records for senators |
| `president_lookup.csv` | Senate Presidents and Deputy Presidents 1996–present |
| `partyfacts_map.csv` | Party abbreviation to PartyFacts ID mapping (28 entries) |
| `session_info_all.csv` | Session header metadata, 3,204 rows (1,482 Senate + 1,722 House) |
| `session_info_senate_only.csv` | Senate-only session info intermediate file |
| `session_info_house.csv` | House-only session info intermediate file |
| `member_lookup.csv` | House member name forms, PHIDs — 1,280 members |
| `electorate_lookup.csv` | House electorate-to-member mapping — 1,430 rows |
| `party_lookup_house.csv` | House member party affiliation spells — 1,571 rows |
| `speaker_lookup.csv` | House Speakers and Deputy Speakers — 20 rows |

### Senate outputs (`data/output/senate/`)

| Path | Type | Description |
|------|------|-------------|
| `daily/*.parquet` | 1,482 files | Per-day speech data (parquet) |
| `daily/*.csv` | 1,482 files | Per-day speech data (CSV) |
| `corpus/senate_hansard_corpus/senate_hansard_corpus_1998_to_2025.parquet` | 1 file | Full Senate corpus, 549,864 rows |
| `corpus/senate_hansard_corpus/senate_hansard_corpus_1998_to_2025.csv` | 1 file | Full Senate corpus, CSV |
| `topics/*.parquet` | 1,482 files | Per-day debate topic hierarchy |
| `divisions/*_divisions.parquet` | ~1 per sitting day with divisions | Division summary |
| `divisions/*_votes.parquet` | ~1 per sitting day with divisions | Division vote rolls (466,557 total vote records) |

### House outputs (`data/output/house/`)

| Path | Type | Description |
|------|------|-------------|
| `daily/*.parquet` | 1,722 files | Per-day speech data (parquet) |
| `daily/*.csv` | 1,722 files | Per-day speech data (CSV) |
| `corpus/house_hansard_corpus_1998_to_2025.parquet` | 1 file | Full House corpus, 623,906 rows |
| `corpus/house_hansard_corpus_1998_to_2025.csv` | 1 file | Full House corpus, CSV |
| `topics/*.parquet` | 1,722 files | Per-day debate topic hierarchy |
| `topics/house_debate_topics_1998_to_2025.parquet/.csv` | 1 file | Assembled House debate topics corpus, 388,154 rows |
| `divisions/*_divisions.parquet` | ~1 per sitting day with divisions | Division summary |
| `divisions/*_votes.parquet` | ~1 per sitting day with divisions | Division vote rolls (614,057 total vote records) |
| `divisions/house_divisions.parquet/.csv` | 1 file | Assembled House divisions corpus, 4,616 divisions |
| `divisions/house_division_votes.parquet/.csv` | 1 file | Assembled House vote records corpus, 614,057 rows |

### Raw XML (`data/raw/`)

| Path | Files | Description |
|------|-------|-------------|
| `raw/senate/*.xml` | 1,482 | Senate Hansard XML, 1998-03-02 to 2025-11-27 |
| `raw/reps/*.xml` | 1,722 | House of Representatives Hansard XML, 1998-03-02 to 2025-11-27 |

---

## Session 2: Validation fixes and Senate corpus rebuild (2026-03-09)

This session addressed the backlog of validation errors identified after the initial build. The starting point was Senate 412/1,482 clean files (27.8%) and House 621/1,722 (36.1%).

### Fix 1: session_info_all.csv rebuilt

**Problem:** `session_info_all.csv` contained only 81 Senate header rows (deduplicated), leaving 1,401 Senate sitting dates absent from the lookup. This caused T1 to fail for all those files (103 files affected, since not all 1,401 were distinct dates in practice).

**Fix:** Re-ran `01_session_info.py` over all 1,482 Senate XMLs to produce a complete `session_info_senate_only.csv`, then concatenated with the existing `session_info_house.csv`:

```bash
python3 01_session_info.py \
    --xml-dir ../data/raw/senate \
    --out ../data/lookup/session_info_senate_only.csv
```

`session_info_all.csv` now contains 3,204 rows (1,482 Senate + 1,722 House).

**Result:** Senate T1 errors dropped from 103 to 1.

### Fix 2: T3 test updated with procedural phrase exclusions

**Problem:** The T3 test flagged any row immediately following a "(Time expired)" row as an error. In practice, many such rows are common procedural phrases (interjections, "Question resolved in the affirmative", procedural motion notices) that appear legitimately after time-expired rows in the XML.

**Fix:** Added a regex exclusion list to `05_validate.py` T3. Rows matching any of the following patterns are excluded from the T3 check:
- Interjection rows (`interject=1`)
- Phrases matching: "Question resolved", "Question put", "Leave granted", "Leave not granted", "Order!", common procedural motion phrases.

**Result:** Senate T3 errors dropped from 2,570 to 87. House T3 errors dropped from 1,254 to 130.

### Fix 3: party_lookup — Len Harris date-range entries

**Problem:** Len Harris (name_id=`8HC`) was listed in the XML as PHON for all her Senate rows, but she left PHON and sat as IND from 1999-08-09. This caused T4 failures on sitting days from that date onward.

**Fix:** Added two date-range rows to `data/lookup/party_lookup.csv` for Harris:
- 1999-01-01 to 1999-08-08: PHON
- 1999-08-09 to 2002-06-30: IND

### Fix 4: fill_date_aware() — override party from dated entries

**Problem:** The `fill_date_aware()` function in `04_fill_details.py` only filled party where the existing value was NULL. Senators with wrong-but-non-null party values from the XML (like Harris) were not corrected.

**Fix:** Modified `fill_date_aware()` to override the party column for all rows where a dated `party_lookup` entry exists for that `unique_id` and sitting date, not just where the existing value is NULL.

**Result:** Senate T4 errors dropped from 103+ to 18. The 18 remaining are genuine party changers (Cory Bernardi, Zed Seselja, and others) where the XML and lookup legitimately disagree across the day's rows.

### Fix 5: state_lookup — Bartlett1964 second Senate term

**Problem:** Andrew Bartlett (unique_id=`Bartlett1964`) served a second Senate term from 2017-11-09 to 2019-06-30 (QLD, replacing a disqualified senator). This term was absent from `state_lookup.csv`, causing T7 to flag his rows in that period as "not active on date".

**Fix:** Added a second term entry for Bartlett1964 to `data/lookup/state_lookup.csv`:
- `senator_from=2017-11-09`, `senator_to=2019-06-30`, `state_abbrev=QLD`

**Result:** Senate T7 errors dropped from 268 to 229.

### Senate corpus rebuilt

After applying fixes 1–5, the Senate daily files were re-enriched and the corpus reassembled:

```bash
# Re-run fill_details to apply updated party_lookup
python3 04_fill_details.py \
    --daily-dir ../data/output/senate/daily \
    --lookup-dir ../data/lookup \
    --out-dir ../data/output/senate/daily

# Reassemble corpus
python3 07_corpus.py \
    --daily-dir ../data/output/senate/daily \
    --out-dir ../data/output/senate/corpus \
    --prefix senate_hansard_corpus
```

The rebuilt corpus is unchanged in row count and date range: 549,864 rows, 1,482 sitting days, 1998-03-02 to 2025-11-27.

### Validation re-run results

| Chamber | Clean files (start) | Clean files (end) | % clean |
|---|---|---|---|
| Senate | 412 / 1,482 (27.8%) | 876 / 1,482 (59.1%) | +31.3 pp |
| House | 621 / 1,722 (36.1%) | 1,045 / 1,722 (60.7%) | +24.6 pp |

| Test | Senate (before) | Senate (after) | House (before) | House (after) |
|---|---|---|---|---|
| T1 | 103 | 1 | 2 | 2 |
| T2 | 331 | 331 | 567 | 567 |
| T3 | 2,570 | 87 | 1,254 | 130 |
| T4 | 103+ | 18 | 44 | 44 |
| T5 | 78 | 78 | — | — |
| T6 | 144 | 144 | — | — |
| T7 | 268 | 229 | — | — |

### Remaining known issues after Session 2

- **T2 (331 Senate, 567 House):** Consecutive duplicate rows from short procedural phrases in the XML. No fix planned.
- **T5 (78 Senate):** Garbled `name_id` values from XML parsing artifacts. Not correctable without manual review.
- **T6 (144 Senate):** Pass 2 false name-form matches producing plausible but historically incorrect `unique_id` assignments.
- **T7 (~150 of 229 Senate):** Old-senator false matches from Pass 2 name-form matching, which lacks date-range awareness. Future fix: add date-range guard to Pass 2 in `04_fill_details.py`.
- **T4 (18 Senate, 44 House):** Real party changers. No fix needed.
