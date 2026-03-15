# Implementation Record: Australian Hansard Database (Python)

This document records what was actually built, replacing the earlier build plan document which contained pseudocode and forward-looking speculation. For the original design rationale, see the other documents in this directory.

---

## Design principles (as implemented)

1. **Node-iteration, not text-blob splitting** — Python's `lxml` walks the XML tree in document order, making the text-blob split approach used in the Katz & Alexander R pipeline unnecessary
2. **Identical output schema to Katz & Alexander** — with the minimal Senate-specific and House-specific changes documented in `05_senate_adaptations.md`
3. **Reproducible** — all data sources are programmatically fetchable; gender for post-2021 senators was filled via a Wikidata SPARQL batch query (documented in `11_pipeline_execution.md`)
4. **Validated** — Senate files run seven automated tests (T1–T7); House files now also run T1–T7 (T5/T6/T7 added using `member_lookup.csv`)
5. **Modular** — each pipeline step is a standalone script that can be re-run independently

---

## Project structure (actual)

```
data/
├── raw/
│   ├── senate/          1,482 XML files (1998-03-02 to 2025-11-27)
│   └── house/           1,722 XML files (1998-03-02 to 2025-11-27)
├── lookup/
│   ├── senator_lookup.csv
│   ├── party_lookup.csv
│   ├── state_lookup.csv
│   ├── president_lookup.csv
│   ├── partyfacts_map.csv
│   └── session_info_all.csv
└── output/
    ├── senate/
    │   ├── daily_raw/   Raw parse output (1,482 parquet files)
    │   ├── daily/       After fill_details + partyfacts (1,482 parquet + csv)
    │   ├── corpus/      senate_hansard_corpus_1998_to_2025.parquet/.csv
    │   ├── topics/      senate_debate_topics.parquet/.csv + daily parquets
    │   └── divisions/   senate_divisions.parquet/.csv + senate_division_votes.parquet/.csv
    └── house/
        ├── daily_raw/   Raw parse output (1,722 parquet files)
        ├── daily/       After fill_details + partyfacts (1,722 parquet + csv)
        ├── corpus/      house_hansard_corpus_1998_to_2025.parquet/.csv
        ├── topics/      house_debate_topics_1998_to_2025.parquet/.csv + daily parquets
        └── divisions/   house_divisions.parquet/.csv + house_division_votes.parquet/.csv + daily parquets

pipeline/
├── 00_download.py
├── 01_session_info.py
├── 02_member_lookup.py
├── 02b_member_lookup_house.py  House member lookup (member, electorate, party, speaker tables)
├── 03_parse.py                 Senate parser (v2.0/v2.1/v2.2)
├── 03b_parse_house.py          House parser (HouseParserV21, HouseParserV22)
├── 04_fill_details.py          Senate enrichment (7 passes)
├── 04b_fill_details_house.py   House enrichment (5 passes)
├── 05_validate.py              Senate validation (T1–T7)
├── 05b_validate_house.py       House validation (T1–T7)
├── 06_add_partyfacts.py        PartyFacts join (both chambers)
├── 07_corpus.py                Corpus assembly (--prefix parameter)
├── 08_debate_topics.py         Senate debate topics
├── 08b_debate_topics_house.py  House debate topics
├── 09_divisions.py             Senate divisions
└── 09b_divisions_house.py      House divisions
```

---

## Stage 0 — Data acquisition (`00_download.py`)

The download script uses a **two-step process**:
1. Query the ParlInfo search URL for the sitting date to retrieve an HTML document page
2. Parse the HTML to find the XML download link, then fetch the XML

Three URL eras exist for the XML file itself:
- **1998–~March 2011:** date-based path (`.../hansards/YYYY-MM-DD/toc_unixml/filename.xml`)
- **~May 2011–2021:** UUID path (`.../hansards/{uuid}/toc_unixml/filename.xml`)
- **2021–present:** numeric integer ID path (`.../hansards/{integer}/toc_unixml/filename.xml`)

The script handles both Senate and House via the `--chamber senate|reps` flag, which selects the appropriate ParlInfo dataset codes (`hansardS,hansardS80` or `hansardr,hansardr80`). It requires a `User-Agent` header — without one, ParlInfo returns HTTP 403.

Outputs: one `YYYY-MM-DD.xml` file per sitting day in `data/raw/senate/` or `data/raw/house/`.

---

## Stage 1 — Session info (`01_session_info.py`)

Loops over all XMLs and extracts `<session.header>` fields into `data/lookup/session_info_all.csv`. Fields: `filename`, `date`, `parliament_no`, `session_no`, `period_no`, `page_no`, `proof`. Used by validation test T1.

---

## Stage 2 — Member lookup (`02_member_lookup.py`)

Builds lookup tables from the AustralianPoliticians GitHub data (primary, covering 1901–November 2021) and the OpenAustralia parser `senators.csv` (gap fill for post-2021 senators).

Lookup files produced:
- `senator_lookup.csv` — one row per senator with PHID, biographical data, and five name-form variants for matching
- `party_lookup.csv` — one row per party spell (supports senators who changed parties)
- `state_lookup.csv` — one row per senate term (supports senators who served multiple terms)
- `president_lookup.csv` — all President/Deputy/Acting President name variants 1998–present, manually compiled
- `partyfacts_map.csv` — party abbreviation to PartyFacts cross-national ID mapping

A House member lookup was subsequently built by `02b_member_lookup_house.py`, producing `member_lookup.csv` (1,280 members), `electorate_lookup.csv`, `party_lookup_house.csv`, and `speaker_lookup.csv`. House enrichment (Stage 4b) still reuses `senator_lookup.csv` for PHID-to-biographic joins (PHIDs are shared across both chambers), but the new House-specific lookup tables are available for validation and future enrichment steps.

---

## Stage 3 — Core XML parsers (`03_parse.py` and `03b_parse_house.py`)

### Senate parser (`03_parse.py`)

Handles three schema versions:
- v2.0 (1998–~2003): `<para>/<inline>` text model
- v2.1 (~2004–2013): same as v2.0 with minor additions
- v2.2 (2014–present): OOXML-derived `<talk.text>/<body>/<p>/<span class="HPS-*">` text model

Uses direct node-iteration over the `lxml.etree` tree (not the text-blob approach from the R pipeline). The parser walks `<debate>/<subdebate.1>/...` in document order, emitting one row per `<speech>`, `<question>`, `<answer>`, `<interjection>`, `<continue>`, or `<motionnospeech>` node. A business-start row is emitted first; stage direction rows are identified by regex pattern matching against body text.

Output schema per sitting day (21 columns):
`name, order, speech_no, page_no, time_stamp, name_id, state, party, in_gov, first_speech, body, question, answer, q_in_writing, div_flag, gender, unique_id, interject, senate_flag`

(`gender`, `unique_id`, and `partyfacts_id` are populated by later stages; the raw parser output goes to `daily_raw/`.)

### House parser (`03b_parse_house.py`)

Handles five schema eras (including the Federation Chamber node rename from `maincomm.xscript` to `fedchamb.xscript` at 2012) via two parser classes: `HouseParserV21` (v2.0/v2.1, 1998–~2013) and `HouseParserV22` (v2.2, 2014–2025).

Output schema per sitting day (21 columns):
`date, name, order, speech_no, page_no, time_stamp, name_id, electorate, party, in_gov, first_speech, body, question, answer, q_in_writing, div_flag, gender, unique_id, interject, fedchamb_flag, partyfacts_id`

Key differences from Senate schema: `electorate` instead of `state`; `fedchamb_flag` instead of `senate_flag`; `date` column is included in the raw parse output (not added later).

---

## Stage 4 — Fill member details (`04_fill_details.py` and `04b_fill_details_house.py`)

### Senate enrichment (`04_fill_details.py`) — 7 passes

1. **Pass 1:** Fill `gender`, `unique_id`, and state abbreviation by joining on `name_id` (PHID) from `senator_lookup.csv`
2. **Pass 2:** For rows with no `name_id`, match the `name` field against all five name-form variants in the lookup (with date-range guard: rejects match if no term covers sitting date)
3. **Pass 3:** Date-aware party and state correction — joins `party_lookup.csv` and `state_lookup.csv` to assign the correct party and state for each PHID on each sitting date
4. **Pass 4:** Presiding officer normalisation — resolves President/Deputy/Acting President display names via `president_lookup.csv`
5. **Pass 5:** State abbreviation normalisation — converts abbreviated state values (e.g. `WA`, `VIC`, `NSW`) to full names (`Western Australia`, `Victoria`, `New South Wales`, etc.)
6. **Pass 6:** Party normalisation — applies `_PARTY_NORM` dict to normalise party aliases (LIB→LP, GRN→AG, G(WA)→GWA, NPA→NP, Nats→NATS, ON→PHON, Ind./Ind→IND, N/A→NaN, LPI0→LP, NPActing→NP)
7. **Pass 7:** page_no forward-fill — propagates page number within each sitting day to rows (interjections, brief procedural turns) that lack explicit page citations in the XML. Coverage: 99.4%.

Each pass asserts that the row count is unchanged after the merge.

### House enrichment (`04b_fill_details_house.py`) — 5 passes

1. **Pass 1:** Fill `gender` and `unique_id` by joining on `name_id` from `member_lookup.csv` (PHIDs are shared across chambers)
2. **Pass 2:** For rows with no `name_id`, match the `name` field against name-form variants (with date-range guard using `electorate_lookup.csv` term dates)
3. **Pass 3:** Presiding officer normalisation (Speaker/Deputy Speaker/Chair)
4. **Pass 4:** Party normalisation — same `_PARTY_NORM` dict as Senate Pass 6
5. **Pass 5:** Date-aware party and electorate fill — joins `party_lookup_house.csv` (fills NULL party) and `electorate_lookup.csv` (always-overwrite to correct stale XML values and normalisation variants); improved party/electorate completeness from ~62% to ~87%

---

## Stage 5 — Validation (`05_validate.py` and `05b_validate_house.py`)

### Senate validation (`05_validate.py`) — T1–T7

| Test | Description |
|---|---|
| T1 | Date in filename matches `session_info_all.csv` |
| T2 | No two consecutive non-interjection rows have identical body text |
| T3 | Body containing `(Time expired)` is not immediately followed by more text |
| T4 | Each `name_id` has only one `party` and `state` per sitting day |
| T5 | All `name_id` values exist in `senator_lookup.csv` (or are known special IDs: 10000, 10001, UNKNOWN) |
| T6 | All `unique_id` values have birth dates before and death dates after the sitting day |
| T7 | All `unique_id` values were senators on the sitting day (via `state_lookup` term dates) |

Errors are written to `data/output/senate/validation_errors.csv`.

### House validation (`05b_validate_house.py`) — T1–T7

All seven tests now run. T5 uses `member_lookup.csv`; T6 uses birth/death dates from `member_lookup.csv`; T7 uses term dates from `electorate_lookup.csv`. Initial results (before date-range guard re-fill): T5=0, T6=451, T7=762.

---

## Stage 6 — PartyFacts IDs (`06_add_partyfacts.py`)

Joins `data/lookup/partyfacts_map.csv` to each daily file on the `party` column, adding `partyfacts_id`. Applied to both Senate and House daily directories by running the script once per chamber.

Parties with no PartyFacts ID (NULL `partyfacts_id`): UAP, PHON, JLN, KAP, and some minor parties.

---

## Stage 7 — Corpus assembly (`07_corpus.py`)

Concatenates all daily parquet files from a `daily/` directory into a single corpus parquet and CSV. The `--prefix` argument controls the output filename (default: `senate_hansard_corpus`; use `house_hansard_corpus` for the House).

The `date` column insertion is conditional — if a daily file already contains a `date` column (as House files do), it is not re-inserted.

### Outputs

| File | Rows | Sitting days | Date range |
|---|---|---|---|
| `senate_hansard_corpus_1998_to_2025.parquet/.csv` | 549,864 | 1,482 | 1998-03-02 to 2025-11-27 |
| `house_hansard_corpus_1998_to_2025.parquet/.csv` | 623,906 | 1,722 | 1998-03-02 to 2025-11-27 |

---

## Stage 8 — Supplementary datasets

### Senate debate topics (`08_debate_topics.py`)

Extracts the full topic hierarchy from `<debateinfo>` and `<subdebateinfo>` elements across all sitting-day XMLs. Schema: `date, order, level, debate_id, parent_id, topic, cognate, gvt_business, senate_flag`.

Output: `senate_debate_topics.parquet/.csv` (43,066 rows) plus one daily parquet per sitting day.

### House debate topics (`08b_debate_topics_house.py`)

Same approach as Senate. Key difference: `fedchamb_flag` instead of `senate_flag`, with `fedchamb_flag=0` for main chamber rows and `fedchamb_flag=1` for Federation Chamber / Main Committee rows. Output: `house_debate_topics_1998_to_2025.parquet/.csv` (388,154 rows) plus daily parquets in `house/topics/`.

### Senate divisions (`09_divisions.py`)

Extracts `<division>` nodes from all sitting-day XMLs, producing:
- **Division-level table** (`senate_divisions.parquet/.csv`): one row per division — date, division number, debate topic, question text, ayes count, noes count, result. 8,001 divisions.
- **Vote-level table** (`senate_division_votes.parquet/.csv`): one row per senator per division — date, division number, unique_id, name_id, name, state, party, vote (Aye/No/Pair). 466,557 vote records.

### House divisions (`09b_divisions_house.py`)

Same structure as Senate, with `electorate` instead of `state` in the vote-level table and `fedchamb_flag` in the division-level table. Output: `house_divisions.parquet/.csv` (4,616 divisions), `house_division_votes.parquet/.csv` (614,057 vote records) plus daily parquets in `house/divisions/`.

---

## Output schema (final)

### Senate corpus columns (21)

| Column | Type | Description |
|---|---|---|
| `date` | `date` | Sitting day date |
| `name` | `str` | Speaker display name or `"business start"` / `"stage direction"` |
| `order` | `int` | Row ordering within sitting day (1-indexed) |
| `speech_no` | `int` (nullable) | Speech group identifier; NULL for stage directions |
| `page_no` | `int` (nullable) | Hansard page number |
| `time_stamp` | `str` (nullable) | 24-hour time `HH:MM:SS`; NULL for most rows |
| `name_id` | `str` (nullable) | PHID; `"10000"` for President; `"UNKNOWN"` for unidentified |
| `state` | `str` | Senator's state or territory (full name, e.g. `"Western Australia"`) |
| `party` | `str` | Party abbreviation |
| `in_gov` | `int` (0/1) | 1 = government, 0 = opposition/crossbench |
| `first_speech` | `int` (0/1) | 1 = maiden speech |
| `body` | `str` | Full text of the utterance |
| `question` | `int` (0/1) | 1 = question during question time |
| `answer` | `int` (0/1) | 1 = answer during question time |
| `q_in_writing` | `int` (0/1) | 1 = question on notice / written answer detected in the daily XML |
| `div_flag` | `int` (0/1) | Division-related flag |
| `gender` | `str` (nullable) | `"male"` or `"female"` |
| `unique_id` | `str` (nullable) | AustralianPoliticians unique ID (e.g. `"Cook1943"`) |
| `interject` | `int` (0/1) | 1 = interjection within another senator's speech |
| `senate_flag` | `int` (always 1) | Chamber identifier for corpus merging |
| `partyfacts_id` | `int` (nullable) | PartyFacts cross-national party identifier |

### House corpus differences

The House corpus uses the same 21 columns with two substitutions:
- `electorate` (electoral division, e.g. `"Brand"`) instead of `state`
- `fedchamb_flag` (0 = main chamber, 1 = Federation Chamber) instead of `senate_flag`

---

## Issues resolved

| Issue | Resolution |
|---|---|
| APH ParlInfo URL pattern for 2006–present XML | Solved: two-step process (query HTML page, extract XML link); three URL eras identified (date-based, UUID, integer ID) |
| Senate XML schema changes 1998–present | Solved: two parser branches (v2.0/v2.1 and v2.2) handle all known variations |
| HPS-Electorate span concatenation bug | `_all_text(s)` stripped trailing space from `"South "` → `"South"`, joining to `"SouthAustralia"`. Fixed by using `"".join(s.itertext())` then `" ".join(raw.split())` |
| State abbreviations in enriched output | Fixed by Pass 5 in `04_fill_details.py` (WA→Western Australia, VIC→Victoria, etc.) |
| Corpus `date` column duplication for House | `07_corpus.py` now inserts `date` only when not already present in the daily file |
| Post-2021 senator gender (~35 senators) | Filled via Wikidata SPARQL batch query; zero senators now have NULL gender |
| `q_in_writing` always 0 | Both parsers now detect questions on notice in the daily XML; 30,239 Senate rows (4.8%) and 41,651 House rows (6.2%) now have `q_in_writing=1` |
| senator_lookup name_id errors (7 entries) | Thorpe1973, Patrick1967, Grogan, Small1988, Cox1970, McLachlan1966, Green1981 corrected or added in senator_lookup.csv |
| partyfacts_id missing for CLP, LNP, KAP, NXT, CA, UAP, PUP, DLP, PHON, GWA, LDP, FFP, TG, AUS | partyfacts_map.csv updated with verified IDs for all these parties |
| Party normalisation inconsistencies (LIB, GRN, ON, etc.) | Pass 6 added to `04_fill_details.py` and Pass 4 added to `04b_fill_details_house.py`; `_PARTY_NORM` dict normalises all known aliases |
| T4 validation failures from interjection rows and party aliases | T4 now excludes interjection rows and normalises party aliases before checking |
| T1 House date failures | House dates added to session_info_all.csv (now 3,204 rows: 1,482 Senate + 1,722 House) |
| No House member lookup | `02b_member_lookup_house.py` now produces member_lookup.csv (1,280 members), electorate_lookup.csv (1,430 rows), party_lookup_house.csv (1,571 rows), speaker_lookup.csv (20 rows) |

---

## Known gaps and future work

| Gap | Notes |
|---|---|
| `partyfacts_id` for JLN, NatsWA, AV, IND | These parties are not in the PartyFacts database; will remain NULL |
| T6/T7 validation false positives | `Cox1863` and `McLachlan1870` are incorrectly matched to modern senators via Pass 2 name-form matching (no date-range awareness); the correct entries Cox1970 and McLachlan1966 have been added to senator_lookup.csv but the old entries remain, causing residual T7 false positives |
| House T5/T6/T7 validation | Now implemented in `05b_validate_house.py`; first run: T5=0, T6=451, T7=762 (bulk are Pass 2 false attributions) |
| House debate topics corpus | Complete — `house_debate_topics_1998_to_2025.parquet/.csv` (388,154 rows) |
| House divisions corpus | Complete — `house_divisions.parquet/.csv` (4,616 divisions), `house_division_votes.parquet/.csv` (614,057 vote records) |
| Estimates sessions | Senate Estimates hearings are in separate XML documents with a committee format; out of scope for this corpus |
