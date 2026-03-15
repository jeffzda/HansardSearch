# `search_corpus.py` — Boolean String Search Across Hansard Corpora

## Overview

`pipeline/search_corpus.py` extracts passages from the Senate and House of
Representatives corpora whose body text matches a user-supplied boolean
expression over case-sensitive string literals.  For every match it captures
the full corpus metadata plus inferred debate context and surrounding speech
turns, enabling immediate qualitative and quantitative analysis.

---

## Quick start

```bash
# Single term
python search_corpus.py "'WWF'"

# Union of aliases (default folder name = first term = "WWF")
python search_corpus.py "'WWF' | 'World Wildlife Fund' | 'World Wide Fund for Nature'"

# Explicit folder name
python search_corpus.py --name WWF \
    "'WWF' | 'World Wildlife Fund' | 'World Wide Fund for Nature' | 'WWF Australia' | 'WWF-Australia'"

# Intersection: must contain both terms
python search_corpus.py "'climate change' & 'Paris Agreement'"

# Grouped expression: union first, then intersection
python search_corpus.py "('WWF' | 'World Wildlife Fund') & 'Australia'"

# Senate only, narrow context window
python search_corpus.py --chamber senate --context 2 "'native title'"
```

---

## Boolean expression syntax

Terms must be quoted (single or double quotes).  The five alias forms shown
in the Quick start example work as individual `TERM` nodes; the operators
combine them.

| Operator | Keyword alternatives | Meaning |
|----------|---------------------|---------|
| `\|`     | `OR`                | Union — rows matching either operand |
| `&`      | `AND`               | Intersection — rows matching both operands |
| `(…)`    | —                   | Override evaluation order |

**Evaluation order** — all operators have equal precedence and associate
**strictly left-to-right**.  `A | B & C` is therefore `(A | B) & C`, *not*
`A | (B & C)`.  Use parentheses to override.

**Case sensitivity** — matching is case-sensitive throughout.  `'WWF'` does
not match `'wwf'`.

---

## Command-line options

| Flag | Default | Description |
|------|---------|-------------|
| `expression` | *(required)* | Boolean search expression |
| `--name NAME` | First term | Output folder name under `case_studies/` |
| `--chamber {senate,house,both}` | `both` | Which corpus to search |
| `--context N` | `5` | Speech turns to capture before/after each match |
| `--senate-corpus PATH` | auto | Override Senate corpus parquet path |
| `--house-corpus PATH` | auto | Override House corpus parquet path |

---

## Metadata filters

Any combination of filters can be applied to narrow the corpus **before** the
text search runs.  Filters are applied in a single pass and the row count
printed before matching begins.

### Quick examples

```bash
# ALP speakers only
python search_corpus.py --party ALP "'climate change'"

# Opposition speeches (not in government), House, 2010 onwards
python search_corpus.py --chamber house --not-in-gov --date-from 2010-01-01 "'carbon tax'"

# Question Time exchanges by female senators
python search_corpus.py --chamber senate --gender female --row-type exchange "'water'"

# A specific member's speeches (by electorate)
python search_corpus.py --electorate "Grayndler" "'housing'"

# Coalition government speeches that contain embedded interjections
python search_corpus.py --in-gov --has-embedded-interject "'immigration'"

# First speeches only, both chambers
python search_corpus.py --first-speech "'reconciliation'"
```

### Identity filters

| Flag | Value | Behaviour |
|------|-------|-----------|
| `--party ABBREV [...]` | e.g. `ALP GRN LP` | Keep rows where `party` matches any of the supplied abbreviations (case-insensitive) |
| `--gender male\|female` | `male` or `female` | Keep rows where `gender` matches |
| `--speaker SUBSTRING` | e.g. `Wong` | Keep rows where `name` contains the substring (case-insensitive) |
| `--name-id PHID` | e.g. `10001` | Exact match on `name_id` (ausPH PHID) |
| `--unique-id ID` | e.g. `AustralianPolitics-...` | Exact match on `unique_id` |

### Geography filters

| Flag | Value | Behaviour |
|------|-------|-----------|
| `--electorate SUBSTRING` | e.g. `Sydney` | House only — substring match on `electorate` |
| `--state ABBREV` | e.g. `NSW` | Senate only — substring match on `state` |

### Date range filters

| Flag | Value | Behaviour |
|------|-------|-----------|
| `--date-from YYYY-MM-DD` | e.g. `2010-01-01` | Earliest sitting date (inclusive) |
| `--date-to YYYY-MM-DD` | e.g. `2022-12-31` | Latest sitting date (inclusive) |

### Government status filters

| Flag | Behaviour |
|------|-----------|
| `--in-gov` | Keep only rows where the speaker's party is in government on the sitting date |
| `--not-in-gov` | Keep only rows where the speaker's party is in opposition |

`in_gov` is derived from the government-period lookup (Howard 1996 → Albanese
2022–present) and covers all years correctly, including post-2012 rows where
the XML element was empty.

### Row-type filter

| Flag | Value | Behaviour |
|------|-------|-----------|
| `--row-type` | `speech` | Keep only rows where `question=0` AND `answer=0` — substantive speech turns |
| `--row-type` | `exchange` | Keep only rows where `question=1` OR `answer=1` — Question Time and questions on notice |
| `--row-type` | `both` | No filter (default) |

### Speech-flag filters

| Flag | Behaviour |
|------|-----------|
| `--questions-only` | `question=1` |
| `--answers-only` | `answer=1` |
| `--interjections-only` | `interject=1` (structural interjections, pre-2012) |
| `--no-interjections` | `interject=0` |
| `--first-speech` | `first_speech=1` — maiden speeches only |
| `--written-only` | `q_in_writing=1` — questions on notice |
| `--exclude-written` | `q_in_writing=0` |

### Chamber/venue filters (House only)

| Flag | Behaviour |
|------|-----------|
| `--fedchamb-only` | `fedchamb_flag=1` — Federation Chamber sittings only |
| `--main-chamber-only` | `fedchamb_flag=0` — Main chamber sittings only |

### Interjection-content filter

| Flag | Behaviour |
|------|-----------|
| `--has-embedded-interject` | Keep only rows whose `body` contains inline interjection markers (`"Mr X interjecting—"` etc.) — post-2012 v2.2 XML rows only |

### Notes on combining filters

- All filters are combined with AND — every supplied filter must be satisfied.
- `--questions-only` and `--answers-only` can be combined to keep rows that
  are either a question or an answer (equivalent to `--row-type exchange`).
- `--in-gov` and `--not-in-gov` are mutually exclusive; supplying both will
  keep only `in_gov=0` rows (last one wins).
- `--interjections-only` / `--no-interjections` apply to the `interject`
  column (structural interjections from v2.1 XML, pre-2012).  For inline
  interjection markers in v2.2 body text use `--has-embedded-interject`.

---

## Output files

All outputs are written to `case_studies/<NAME>/`.  If the folder already
exists an integer suffix is appended (`WWF_2`, `WWF_3`, …).

### `matches.csv`

One row per matched corpus row.  Columns:

**Identification**

| Column | Description |
|--------|-------------|
| `match_id` | Unique key: `{chamber}-{date}-{row_order}` |
| `chamber` | `senate` or `house` |
| `date` | Sitting date (YYYY-MM-DD) |
| `time_stamp` | Time within sitting (HH:MM, may be empty for pre-2012) |
| `page_no` | Hansard page number |
| `speech_no` | Speech number within the sitting day |
| `row_order` | Row position within the sitting day |

**Speaker metadata**

| Column | Description |
|--------|-------------|
| `speaker_name` | Name as it appears in the XML (e.g. `Senator WONG`) |
| `name_id` | Parliamentary Handbook ID (PHID) |
| `unique_id` | Unique member ID from member lookup |
| `party` | Canonical party abbreviation (e.g. `LP`, `ALP`, `AG`) |
| `partyfacts_id` | Cross-national party identifier from PartyFacts database |
| `state_or_electorate` | State (Senate) or electorate (House) |
| `gender` | `M` or `F` |
| `in_gov` | `1` if speaker was in government on this date, else `0` |
| `first_speech` | `1` if this is the member's first speech (maiden speech) |

**Speech type flags**

| Column | Description |
|--------|-------------|
| `is_question` | `1` if this row is a question |
| `is_answer` | `1` if this row is an answer |
| `q_in_writing` | `1` if this is a question on notice (written question) |
| `is_interjection` | `1` if this is an interjection |
| `div_flag` | `1` if this row is adjacent to a division (vote) |

**Match content**

| Column | Description |
|--------|-------------|
| `body` | Full text of the matched speech turn |
| `matched_terms` | Which literal search terms triggered this row (pipe-separated) |
| `debate_heading` | Most recent stage-direction text before this row in the sitting day — typically the debate title or procedural label (e.g. `ENVIRONMENT AND COMMUNICATIONS LEGISLATION COMMITTEE`, `QUESTION TIME`, bill title) |

**Context**

| Column | Description |
|--------|-------------|
| `context_before` | Up to N speech turns immediately preceding the match in the sitting day, formatted as `[HH:MM] Speaker (Party, State): body text` |
| `context_after` | Up to N speech turns immediately following the match |

The context window size is controlled by `--context N` (default 5).

### `summary.txt`

Plain-text statistics report including:
- Total match counts (Senate / House)
- Date range and number of unique sitting days
- Per-term match frequency
- Top 15 speakers by match count
- Top 10 parties by match count
- Year-by-year bar chart

### `case_studies/logic.md`

A markdown table maintained across all searches, mapping each folder name to
its search expression and match count.  Allows reproducibility: every case
study can be exactly re-run from this file.

---

## Debate heading inference

The `debate_heading` field is inferred deterministically from the corpus
without any external lookup:

1. For the matched row's sitting day, the full day's rows are sorted by
   `order` (position within the day).
2. The script scans backwards from the matched row looking for rows where
   `name` is `stage direction` or `business start`.
3. The body text of the first such row found becomes the `debate_heading`.

These rows are emitted by the parser for `<subdebate.text>` elements in v2.2
XML (post-2013) and for `<subdebate>` headings in v2.1 XML (pre-2013).  They
contain debate titles, bill names, committee names, and procedural labels
exactly as they appear in the official Hansard.

This approach is robust because:
- It requires no separate lookup file
- It works for both chamber formats and all years
- It is reproducible from the corpus alone

---

## WWF case study

The first case study extracts all references to the World Wildlife Fund /
World Wide Fund for Nature across both chambers (1998–2025).

**Search expression:**
```
'WWF' | 'World Wildlife Fund' | 'World Wide Fund for Nature' | 'WWF Australia' | 'WWF-Australia'
```

**Alias rationale:**

| Alias | Rationale |
|-------|-----------|
| `WWF` | Primary abbreviation used in Hansard; also captures `WWF Australia`, `WWF-Australia` as substrings |
| `World Wildlife Fund` | Original English name (used through to the 2000s in Australian debates) |
| `World Wide Fund for Nature` | Official name after 1986 rebranding; common in environmental committee debates |
| `WWF Australia` | Australian national office name |
| `WWF-Australia` | Hyphenated variant appearing in some Hansard records |

**Results** (run 2026-03-10):

| Metric | Value |
|--------|-------|
| Total matches | 383 |
| Senate matches | 225 |
| House matches | 158 |
| Date range | 1998-03-05 → 2025-11-04 |
| Unique sitting days | 283 |
| Most frequent term | `WWF` (218 rows) |

**Output:** `case_studies/WWF/`

---

## Performance notes

- Loading both corpora (~550K Senate + ~624K House rows) takes ~5–10 seconds
  on a modern machine.
- Only sitting days containing at least one match are loaded into the
  per-day context lookup, so context enrichment scales with the number of
  matching days rather than total corpus size.
- For large result sets (1,000+ matches), context enrichment may take
  30–60 seconds.

---

## Extending the script

**Proximity search** — to require two terms to appear in the same speech turn,
use `&` (intersection).  For paragraph-level proximity, the K&A corpus (with
per-paragraph segmentation) would be needed.

**Regex terms** — the current implementation uses literal `re.escape(term)`
matching.  To support regex, remove the `re.escape()` call in `_build_masks()`
and document that terms are Python regex patterns.
