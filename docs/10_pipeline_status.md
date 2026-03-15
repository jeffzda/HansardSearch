# Pipeline Status — Quick Reference

This document provides a quick-reference summary of what has been completed, what is in progress, and how to re-run each pipeline step.

---

## Completed outputs

| Output | Location | Rows | Sitting days | Date range |
|---|---|---|---|---|
| Senate corpus (parquet) | `data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.parquet` | 549,864 | 1,482 | 1998-03-02 to 2025-11-27 |
| Senate corpus (CSV) | `data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.csv` | 549,864 | 1,482 | 1998-03-02 to 2025-11-27 |
| House corpus (parquet) | `data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet` | 623,906 | 1,722 | 1998-03-02 to 2025-11-27 |
| House corpus (CSV) | `data/output/house/corpus/house_hansard_corpus_1998_to_2025.csv` | 623,906 | 1,722 | 1998-03-02 to 2025-11-27 |
| Senate debate topics | `data/output/senate/topics/senate_debate_topics.parquet/.csv` | 43,066 topic rows | 1,482 | 1998-03-02 to 2025-11-27 |
| Senate divisions | `data/output/senate/divisions/senate_divisions.parquet/.csv` | 8,001 divisions | — | 1998-03-02 to 2025-11-27 |
| Senate division votes | `data/output/senate/divisions/senate_division_votes.parquet/.csv` | 466,557 vote records | — | 1998-03-02 to 2025-11-27 |
| Senate daily (enriched) | `data/output/senate/daily/` | 1,482 parquet + csv files | — | — |
| House daily (enriched) | `data/output/house/daily/` | 1,722 parquet + csv files | — | — |
| House debate topics | `data/output/house/topics/house_debate_topics_1998_to_2025.parquet/.csv` | 388,154 topic rows | 1,722 | 1998-03-02 to 2025-11-27 |
| House divisions | `data/output/house/divisions/house_divisions.parquet/.csv` | 4,616 divisions | — | 1998-03-02 to 2025-11-27 |
| House division votes | `data/output/house/divisions/house_division_votes.parquet/.csv` | 614,057 vote records | — | 1998-03-02 to 2025-11-27 |

---

## Known gaps (remaining)

| Gap | Impact | Notes |
|---|---|---|
| `partyfacts_id` for JLN, NatsWA, AV, IND | NULL `partyfacts_id` for these parties | Not in PartyFacts database; PRES/DPRES are non-party roles also left NULL |
| Estimates sessions | Senate Estimates hearings are in separate XML documents with a committee format | Out of scope for this corpus |
| T2 consecutive duplicates (24 Senate, 25 House) | Short procedural phrases emitted as consecutive rows by XML parser | No fix planned; these files are excluded from clean-file count |
| T5 garbled name_ids (~78 Senate) | XML parsing artifacts produce name_id values not in senator_lookup | No fix planned; not correctable without manual review |
| T6/T7 Pass 2 false matches (~144/229 Senate; ~451/762 House before re-fill) | Pass 2 date-range guard added to both fill scripts; daily files must be regenerated for fix to take effect | Re-run `04_fill_details.py` and `04b_fill_details_house.py` then `06_add_partyfacts.py` and `07_corpus.py` |
| T4 real party changers (18 Senate, 33 House) | Senators/members who genuinely changed party mid-term (e.g., Cory Bernardi, Zed Seselja) | Not lookup errors; these are real changes, no fix needed |

## Validation results (current state)

| Chamber | Clean files | Total files | % clean |
|---|---|---|---|
| Senate | 1,131 | 1,482 | 76.3% |
| House | 1,545 | 1,722 | 89.7% |

### Error counts by test

| Test | Senate | House | Notes |
|---|---|---|---|
| T1 (date mismatch) | 1 | 2 | Down from 103 Senate after session_info rebuild |
| T2 (consecutive duplicates) | 24 | 25 | Down from 331/567 after procedural regex exclusions added to validator |
| T3 (time expired) | 87 | 130 | Down from 2,570 Senate / 1,254 House after regex exclusions added |
| T4 (party/state multiplicity) | 18 | 33 | Senate: 18 real party changers; House: 33 residual (real mid-term changers or garbled XML) |
| T5 (name_id not in lookup) | 78 | 0 | Senate: garbled XML artifacts; House: no garbled IDs found |
| T6 (birth/death date) | 144 | 451* | Pass 2 false matches; *House count before date-range guard re-fill |
| T7 (not active on date) | 229 | 762* | Pass 2 false matches; *House count before date-range guard re-fill |

## Recently resolved

| Item | Resolution |
|---|---|
| `q_in_writing` field always 0 | Both parsers now detect questions on notice embedded in the daily XML; 30,239 Senate rows (4.8%) and 41,651 House rows (6.2%) now have `q_in_writing=1` |
| Gender for ~35 post-2021 senators | Filled via Wikidata SPARQL batch query; zero senators now have NULL gender in senator_lookup.csv |
| `partyfacts_id` gaps for CLP, LNP, KAP, NXT, CA, UAP, PUP, DLP, PHON, GWA, LDP, FFP, TG, AUS | partyfacts_map.csv updated with verified IDs for all these parties |
| senator_lookup name_id errors (7 corrections) | Thorpe1973, Patrick1967, Grogan, Small1988, Cox1970, McLachlan1966, Green1981 corrected or added |
| Party normalisation inconsistencies | Pass 6 added to both 04_fill_details.py and 04b_fill_details_house.py; normalises LIB→LP, GRN→AG, G(WA)→GWA, NPA→NP, Nats→NATS, ON→PHON, and others |
| House member lookup not built | 02b_member_lookup_house.py now produces member_lookup.csv (1,280 members), electorate_lookup.csv (1,430 rows), party_lookup_house.csv (1,571 rows), speaker_lookup.csv (20 rows) |
| T4 validation failures from party aliases | T4 now excludes interjection rows and normalises party aliases before checking |
| session_info_all.csv missing Senate rows | Rebuilt from scratch; now contains all 1,482 Senate + 1,722 House = 3,204 rows. T1 Senate errors dropped from 103 to 1. |
| T3 over-flagging procedural phrases | Regex exclusion added for interjections, "Question resolved", and common procedural motions. T3 errors: 2,570→87 Senate, 1,254→130 House. |
| T4 party errors for senators with mid-term party changes | fill_date_aware() now overrides party from dated party_lookup entries (not just fills NULLs). Len Harris (8HC) PHON→IND on 1999-08-09 added as date-range entries. |
| T7 false match for Bartlett1964 second term | Bartlett1964 second Senate term (2017-11-09 to 2019-06-30, QLD) added to state_lookup. T7 errors: 268→229. |
| Body text truncation in v2.2 XML (post-2011) | `_extract_body_text_v22()` in both `03_parse.py` and `03b_parse_house.py` was discarding `el.tail` for attribution span elements (`SPEAKER_ID_CLASSES` and `A_TYPE_MAP`). In v2.2 Hansard XML, speech content lives in the `.tail` of `<span class="HPS-Time">` and `<a type="MemberXxx">` elements. Fix: capture tail text (stripping leading `"):  "` punctuation) before returning. House corpus rows: 465,525 → 522,575 (+57,050, +12.3%); Senate unchanged at 631,175. Post-fix word ratio vs Katz & Alexander: 1.003–1.007 across all tested post-2011 dates. Full pipeline re-run (parse → fill → partyfacts → corpus) completed for both chambers. |
| Missing content categories (petitions, cognates, subdebate.text) | Senate v2.1 parser now emits `petition` rows for `<petition.group>/<petition>` elements (petition text + presenter attribution) and `stage_direction` rows for `<cognate>` bill headers. Both parsers (v2.2) now emit `stage_direction` rows for `<subdebate.text>` procedural blocks (bill reading stage headers between speeches). Senate: 631,175 → 633,047 (+1,872); House: 522,575 → 623,906 (+101,331, large because every bill reading stage has a subdebate.text header). |
| Spurious empty-body rows in Senate v2.2 (21% of rows) | `_extract_utterances_v22()` in `03_parse.py` was flushing the initial `<talk.start>` metadata as an empty row whenever a `<a type="MemberSpeech">` attribution appeared at the start of `<talk.text>`. Fix: only flush a pending utterance if it has body content (`if body_so_far:` not `if body_so_far or current["name_id"]:`). Senate rows: 696,997 → 549,864. Residual 2,076 empty rows (0.38%) are legitimate v2.1 procedural turns with no speech content. |
| `page_no` coverage (pre-2012 gap) | Pass 7 added to `04_fill_details.py` and Pass 5 added to `04b_fill_details_house.py`: forward-fills `page_no` within each sitting day. Interjections and brief procedural turns in pre-2012 v2.x XML lack explicit page citations; they physically occur on the same page as surrounding speech. Coverage: Senate 99.4%, House 99.4%. |

---

## How to re-run each step

All scripts are run from the `pipeline/` directory. Paths shown are relative to `pipeline/`.

### Step 0 — Download XML files

```bash
# Senate
python 00_download.py --chamber senate --start 1998-03-02 --end 2025-12-31 \
    --out ../data/raw/senate

# House
python 00_download.py --chamber reps --start 1998-03-02 --end 2025-12-31 \
    --out ../data/raw/house

# Single date
python 00_download.py --chamber senate --date 2025-11-27 --out ../data/raw/senate
```

### Step 1 — Session info

```bash
python 01_session_info.py
```

Reads all XMLs from `data/raw/senate/` and `data/raw/house/`; writes `data/lookup/session_info_all.csv`.

### Step 2 — Member lookup

```bash
python 02_member_lookup.py
```

Downloads from AustralianPoliticians and OpenAustralia repositories; writes lookup CSVs to `data/lookup/`.

### Step 3 — Parse XML (Senate)

```bash
# One file
python 03_parse.py --xml ../data/raw/senate/2025-11-27.xml

# Full corpus
python 03_parse.py --xml-dir ../data/raw/senate --out-dir ../data/output/senate/daily_raw
```

### Step 3b — Parse XML (House)

```bash
# One file
python 03b_parse_house.py --xml ../data/raw/house/2025-11-27.xml

# Full corpus
python 03b_parse_house.py --xml-dir ../data/raw/house --out-dir ../data/output/house/daily_raw
```

### Step 4 — Fill member details (Senate)

```bash
python 04_fill_details.py \
    --daily-dir ../data/output/senate/daily_raw \
    --lookup-dir ../data/lookup \
    --out-dir ../data/output/senate/daily
```

### Step 4b — Fill member details (House)

```bash
python 04b_fill_details_house.py \
    --daily-dir ../data/output/house/daily_raw \
    --lookup-dir ../data/lookup \
    --out-dir ../data/output/house/daily
```

### Step 5 — Validate (Senate)

```bash
python 05_validate.py \
    --daily-dir ../data/output/senate/daily \
    --lookup-dir ../data/lookup \
    --session-info ../data/lookup/session_info_all.csv \
    [--workers N]      # fix worker count; default auto-scales from 4 up to cpu_count-1
    [--sequential]     # disable parallelism for debugging
```

### Step 5b — Validate (House)

```bash
python 05b_validate_house.py \
    --daily-dir ../data/output/house/daily \
    --session-info ../data/lookup/session_info_all.csv \
    --lookup-dir ../data/lookup \
    --error-log ../data/output/house/validation_errors.csv \
    [--workers N]      # fix worker count; default auto-scales from 4 up to cpu_count-1
    [--sequential]     # disable parallelism for debugging
```

### Step 6 — Add PartyFacts IDs

```bash
# Senate
python 06_add_partyfacts.py \
    --daily-dir ../data/output/senate/daily \
    --lookup-dir ../data/lookup

# House
python 06_add_partyfacts.py \
    --daily-dir ../data/output/house/daily \
    --lookup-dir ../data/lookup
```

### Step 7 — Assemble corpus

```bash
# Senate
python 07_corpus.py \
    --daily-dir ../data/output/senate/daily \
    --out-dir ../data/output/senate/corpus

# House (--prefix sets the output filename)
python 07_corpus.py \
    --daily-dir ../data/output/house/daily \
    --out-dir ../data/output/house/corpus \
    --prefix house_hansard_corpus
```

### Step 8 — Debate topics (Senate)

```bash
python 08_debate_topics.py \
    --xml-dir ../data/raw/senate \
    --out-dir ../data/output/senate/topics
```

### Step 8b — Debate topics (House)

```bash
python 08b_debate_topics_house.py \
    --xml-dir ../data/raw/house \
    --out-dir ../data/output/house/topics
```

To assemble the House topics into a single corpus file, run `07_corpus.py` with the `house/topics` directory once daily files are all written.

### Step 9 — Divisions (Senate)

```bash
python 09_divisions.py \
    --xml-dir ../data/raw/senate \
    --daily-dir ../data/output/senate/daily \
    --out-dir ../data/output/senate/divisions
```

### Step 9b — Divisions (House)

```bash
python 09b_divisions_house.py \
    --xml-dir ../data/raw/house \
    --daily-dir ../data/output/house/daily \
    --out-dir ../data/output/house/divisions
```

---

## File naming conventions

| Pattern | Example | Meaning |
|---|---|---|
| `YYYY-MM-DD.parquet` | `2025-11-27.parquet` | Daily enriched output (Senate or House) |
| `YYYY-MM-DD_divisions.parquet` | `2025-11-27_divisions.parquet` | Division-level records for one sitting day |
| `YYYY-MM-DD_votes.parquet` | `2025-11-27_votes.parquet` | Vote-level records for one sitting day |
| `senate_hansard_corpus_YYYY_to_YYYY.parquet` | `senate_hansard_corpus_1998_to_2025.parquet` | Full Senate corpus |
| `house_hansard_corpus_YYYY_to_YYYY.parquet` | `house_hansard_corpus_1998_to_2025.parquet` | Full House corpus |
| `senate_debate_topics.parquet` | — | Senate topic hierarchy corpus |
| `senate_divisions.parquet` | — | Senate division-level corpus |
| `senate_division_votes.parquet` | — | Senate vote-level corpus |
