# Katz & Alexander (2023) Parsing Workflow

> **Status:** Reference document. This describes the original R pipeline used as the design reference for this project. The Python pipeline (`pipeline/03_parse.py` and `pipeline/03b_parse_house.py`) implements the same output schema using a node-iteration approach rather than the text-blob split approach described here. See `08_build_plan.md` for what was actually built.

Source repository: https://github.com/lindsaykatz/hansard-proj

---

## Repository structure

```
scripts/corpus_v1/          1998–2022 corpus
  00-scrape_files.R
  01-session_info.R
  02-auspol_lookup.R
  03-parse-1998_to_1999.R
  04-parse-2000_to_2011.R
  05-parse-2011_to_2012.R
  06-parse-2012_to_2022.R
  07-fill_details.R
  08-data_validation.R
  09-check_time_stamps.R
  10-add_party_facts.R
  11-create_one_corpus.R
  12-get_debate_topics.R
  13-divisions_data.R
  14-corrections.R

scripts/corpus_v2/          2022–2025 extension
  00-session_info.R
  01-parse-2022_to_2025.R
  02-fill_details.R
  03-data_validation.R
  04-check_time_stamps.R
  05-add_party_facts.R
  06-create_one_corpus.R
  07-name_fixes.R
  08-final_corrections.R
  09-fix_interjections.R
  10-fix_q_a_flags.R

additional_data/lookup_tables/
  member_lookup.csv
  speaker_lookup.csv
  party_lookup.csv
  electorate_lookup.csv
  PartyFacts_map.csv
  ausPH_AusPol_mapping.xlsx

urls/                       CSV files with download URLs per sitting day
```

---

## Pipeline overview (14 steps)

| Step | Script | Action |
|---|---|---|
| 0 | `00-scrape_files.R` | Bulk-download all sitting-day XML files from APH |
| 1 | `01-session_info.R` | Extract `<session.header>` metadata from every XML |
| 2 | `02-auspol_lookup.R` | Build member lookup tables (names, PHIDs, electorates, parties) |
| 3–6 | `03-` to `06-parse-*.R` | Parse XMLs — four era-specific scripts for schema changes |
| 7 | `07-fill_details.R` | Post-parse enrichment from lookup tables |
| 8 | `08-data_validation.R` | Seven automated tests |
| 9 | `09-check_time_stamps.R` | Scan for malformed timestamps |
| 10 | `10-add_party_facts.R` | Join PartyFacts cross-national party IDs |
| 11 | `11-create_one_corpus.R` | Combine all daily files into single corpus parquet |
| 12 | `12-get_debate_topics.R` | Supplementary debate topics dataset |
| 13 | `13-divisions_data.R` | Supplementary division/vote dataset |
| 14 | `14-corrections.R` | Manual and semi-automated post-corpus fixes |

---

## Step 0 — Download (`00-scrape_files.R`)

Uses the `heapsofpapers` package (`get_and_save()`) to bulk-download all Hansard XML files from the APH website. Three URL CSVs cover 1998–1999, 2000–2009, and 2010–2022. Files are saved as `YYYY-MM-DD.xml`.

---

## Step 1 — Session info (`01-session_info.R`)

Loops over all XMLs, applies `xmlToDataFrame(node=getNodeSet(hansard_xml, "//session.header"))` to extract the `<session.header>` node. Fields: `date`, `parliament.no`, `session.no`, `period.no`, `page.no`, `proof`. Output: `session_info_all.csv`. Used in validation test 1.

---

## Step 2 — Member lookup (`02-auspol_lookup.R`)

### Data sources

- **`AustralianPoliticians` R package** — biographical data, unique IDs, gender
- **`ausPH` R package** — Parliamentary Handbook IDs (PHIDs), electorates, party affiliations

### Merge strategy

The two datasets are merged by `(surname_cap, allOtherNames, gender)`. Surnames are uppercased for matching. 40+ manual name-form fixes handle mismatches between the two packages. The Jenkins father/son case is resolved by filtering on PHID.

### Name form variants

Five name form variants are generated per member to match all Hansard representations:

| Form | Example |
|---|---|
| `form1` | `"Mr FirstName Surname"` — full name with title |
| `form2` | `"Mr Surname"` — surname only with title |
| `form3` | `"Mr FIRSTNAME SURNAME"` — all-caps with title |
| `form4` | `"Mr SURNAME"` — all-caps surname with title |
| `form5` | `"Mr FirstName SURNAME"` — mixed case with title |

Compound surnames (e.g. `McTiernan`) get special regex for `Mc/Mac` prefix. Members with dual titles (e.g. `"Ms|Mrs"`) keep the pipe in the form strings.

### Lookup tables produced

| File | Contents |
|---|---|
| `member_lookup.csv` | All MPs: uniqueID, phid, surname, firstName, displayName, gender, title, electorate, party, form1–form5 |
| `speaker_lookup.csv` | Speaker and Deputy Speaker variants with `electedDeputy` flag |
| `party_lookup.csv` | uniqueID, phid, partyAbbrev, partyName, partyFrom, partyTo |
| `electorate_lookup.csv` | uniqueID, phid, division, mpFrom, mpTo |

---

## Steps 3–6 — Core XML parse (four era scripts)

### Why four scripts?

The Hansard XML schema changed at three points within 1998–2022. The key change is the renaming of the Federation Chamber node:

- `03`: 2 March 1998 – 9 December 1999
- `04`: 15 February 2000 – 24 March 2011 (`maincomm.xscript`)
- `05`: 10 May 2011 – 28 June 2012 (transition era)
- `06`: 14 August 2012 – 8 September 2022 (`fedchamb.xscript`)

### The text-blob split approach

The parse scripts do **not** iterate over XML speech nodes directly. The core mechanism is:

1. Collapse each `<debate>` node's entire text into one large string per debate
2. Extract every `<talk.start>/<talker>` concatenation string (name.id + full_name + name_short) as a "pattern"
3. Use `separate_rows()` with lookahead regex `(?=pattern)` to split the blob at each speaker boundary
4. Join the rows back to the pattern → metadata lookup table

This approach was necessary because R's XML library loses the positional interleaving of speech and interjection nodes when extracting text.

### Key helper function: `item_df()`

```r
item_df <- function(file, path){
  items <- xml_find_all(file, path)
  nodenames <- xml_name(xml_children(items))
  contents <- trimws(xml_text(xml_children(items)))
  itemindex <- rep(1:length(items),
                   times = sapply(items, function(x) length(xml_children(x))))
  df <- data.frame(itemindex, nodenames, contents)
  df <- pivot_wider(df, id_cols = itemindex,
                    names_from = nodenames, values_from = contents,
                    values_fn = list)
  return(df)
}
```

Handles XML nodes with duplicate child names by building a long data frame and pivoting wide. Used throughout all parse and ancillary scripts.

### A. Business start extraction

Extracts `<business.start>` from `//chamber.xscript/business.start` and `//maincomm.xscript/business.start`. Produces a row with:
- `name = "business start"`
- `name.id = NA`, all flags = 0
- Date extracted from body via regex `\d{4}-\d{2}-\d{2}`
- Start time normalised to 24-hour `HH:MM:SS` via `strptime()`; handles formats `9.30 am`, `9 am`, `9.30 a.m.`

### B. Debate text extraction

Each `<debate>` node is collapsed into a single text string per debate via:
```r
tibble(xmlToDataFrame(node=getNodeSet(hansard_xml, "//chamber.xscript/debate"))) %>%
  unite("body", c(debateinfo:last_col(), -debateinfo), na.rm=T, sep=" ")
```
`page.no` is extracted separately using `item_df()` and joined.

### C. Pattern extraction from `<talk.start>` nodes

Each `<talk.start>` node contains a `<talker>` string that encodes `name.id + full_name + name_short` — e.g. `"HH4Jenkins, Harry, MPMr JENKINS"`. This concatenated string serves as a unique split pattern.

Special handling: when a Deputy Speaker's name appears in brackets at the start of a `<para>` (e.g. `(Mr Jenkins)—`), it is appended to the talker string to create a more specific pattern.

Patterns are:
- Deduplicated
- Arranged by descending string length (so longer patterns match first)
- All special regex characters `(`, `)`, `.` are escaped

### D. Speaker metadata from `<talker>` nodes

Each `<talker>` yields: `name.id` (PHID), `name` (full display name), `name_short`, `electorate`, `party`, `role`, `in.gov`, `first.speech`, `page.no`, `time.stamp`.

### E. Sub-debate and Q/A pattern extraction

- `subdebate.1` titles → used to split debate text at new sub-debate sections (`sub1_flag = 1`)
- `subdebate.2` → `sub2_flag = 1`
- `//question/talk.start` talker strings → `question_patterns` (`question = 1`)
- `//answer/talk.start` talker strings → `answer_patterns` (`answer = 1`)

Patterns containing only alphabetic characters (no page number digits) are filtered to prevent accidental mid-sentence splits. Many one-off date-specific manual fixes are applied.

### F. Row separation — the core split

```r
# Split at sub-debate boundaries
debate_text_all <- separate_rows(debate_text_all, body,
  sep = paste0("(?=", paste0(
    "(?<!the |our |so-called |Minister for )",
    sub1_patterns_info,
    "(?!, | |\\?|\\. |\\.Th)", collapse="|"), ")"))

# Split at question/answer boundaries
main <- main %>%
  separate_rows(body, sep = paste0("(?=", question_patterns, ")", collapse="|")) %>%
  separate_rows(body, sep = paste0("(?=", answer_patterns, ")", collapse="|"))

# Split at speech starts
main <- main %>%
  separate_rows(body, sep = paste0("(?=", speech_start, ")", collapse="|"))
```

All splits use lookahead `(?=...)` so the delimiter is kept at the start of the new row. When the pattern list exceeds 400 entries, splits are done in batches of 300.

### G. Metadata attachment

Each row's body is matched against the pattern list; the matched pattern is joined to its metadata and removed from the body (along with the leading em-dash).

### H. General interjection handling

~18 anonymous collective interjection strings are split out:
- `"Opposition members interjecting—"`
- `"Government members interjecting—"`
- `"An honourable member interjecting—"`
- etc.

Named interjections embedded in body text (e.g. `"Mr Causley interjecting"`) are found via regex lookahead after `.` and split out.

### I. Stage direction extraction

~60 regex patterns capture procedural text:
- `"Bill read a [alpha]{0,10} time\\."`
- `"Question agreed to\\."`
- `"The House divided\\."`
- `"House adjourned at \\d\\d:\\d\\d"`
- `"Sitting suspended from \\d\\d:\\d\\d to \\d\\d:\\d\\d"`
- `"Leave granted\\."`
- Plus dozens more

All become `name = "stage direction"`.

### J. Questions in writing (`answers.to.questions`)

If `//answers.to.questions` exists in the XML:
- Questions extracted from `//answers.to.questions/debate/subdebate.1/question/talk.start/para`
- Answers extracted from `//answers.to.questions/debate/subdebate.1/answer/talk.start/para`
- Rows beginning with `"The answer to the ... question is as follows:"` are reclassified as answers
- All get `q_in_writing = 1`, `fedchamb_flag = 0`, `sub1_flag = 1`, `sub2_flag = 0`

### K. Interjection flagging

```r
main <- main %>%
  group_by(speech_no) %>% arrange(order) %>%
  mutate(interject = case_when(
    order == min(order) ~ 0,         # first row of a speech is never an interjection
    str_detect(name, "SPEAKER|stage direction") ~ 0,
    is.na(speech_no) & q_in_writing == 1 ~ 0
  )) %>%
  ungroup() %>%
  group_by(name, speech_no) %>%
  fill(interject, .direction = "down") %>%
  ungroup() %>%
  mutate(interject = ifelse(is.na(interject), 1, interject))
```

---

## Step 7 — Fill member details (`07-fill_details.R`)

Four sequential enrichment passes within `fill_main()`:

| Pass | Method |
|---|---|
| 1 | Left-join `master_lookup` on `name.id` → fill uniqueID, gender, electorate, party |
| 2 | For rows with `is.na(name.id)`, match `name` against all five form variants |
| 3 | Resolve repeated surnames: find full name in the same file to identify the correct PHID |
| 4 | Join `speaker_lookup` for Speaker/Deputy Speaker variants |

`stopifnot(dim(thisFile) == thisFile_dim)` is run after every major step to verify no rows are added or lost.

---

## Step 8 — Data validation (`08-data_validation.R`)

Seven automated tests across all daily files:

| Test | Description | Action on failure |
|---|---|---|
| 1 | Date in filename matches `session_info_all.csv` | Manual check |
| 2 | No two consecutive rows have identical `body` (excluding "interjecting") | Manual investigation |
| 3 | Body containing `(Time expired)` not immediately followed by more text | Manual fix of unsplit bodies |
| 4 | Each `name.id` has only one `party` and `electorate` per sitting day | Re-fill from date-aware lookups |
| 5 | All `name.id` values exist in the `ausPH` package | Manual PHID corrections |
| 6 | All `uniqueID` values have birth dates before and death dates after sitting day | Flag for review |
| 7 | All `uniqueID` values were MPs on the sitting day (using mpFrom/mpTo) | 5 confirmed source errors |

---

## Corpus v2 additional steps

| Script | Purpose |
|---|---|
| `07-name_fixes.R` | Same correction approach as `14-corrections.R` for 2022–2025 data |
| `08-final_corrections.R` | Combines v1 and v2 corpora; fixes column classes; recomputes `interject` globally; exports unified corpus |
| `09-fix_interjections.R` | Detects unsplit interjections embedded in body text via pattern `(?<=—)\s{0,2}[A-Z][a-z].{1,50} interjecting—`; splits and fills from lookup |
| `10-fix_q_a_flags.R` | Re-derives Q/A flags from fresh XML parses of `//chamber.xscript//question/talk.text` and `//answer/talk.text` for 2011–2025 files |

---

## Final data schema

### Per-sitting-day output (CSV and Parquet)

| Column | R type | Description |
|---|---|---|
| `date` | `Date` | Sitting day date (added at corpus creation) |
| `name` | `character` | Speaker display name (`"Surname, FirstName, MP"`) or `"business start"`, `"stage direction"`, interjection description |
| `order` | `double` | Row ordering within a sitting day (1-indexed) |
| `speech_no` | `double` | Identifies which speech a row belongs to; NA for q_in_writing rows |
| `page.no` | `double` | Hansard page number |
| `time.stamp` | `character` / `hms` | 24-hour time HH:MM:SS; NA for most rows |
| `name.id` | `character` | Parliamentary Handbook ID (PHID); `"10000"` for Speaker; `"UNKNOWN"` for unidentified |
| `electorate` | `character` | Member's electorate at time of speaking |
| `party` | `factor` | Party abbreviation: ALP, LP, NATS, AG, AD, Ind, CLP, LNP, KAP, CA, PUP, UAP, NXT, NatsWA |
| `in.gov` | `double` | 1 = government member, 0 = opposition/crossbench |
| `first.speech` | `double` | 1 = maiden speech |
| `body` | `character` | Full text of the utterance |
| `fedchamb_flag` | `factor` | 0 = main chamber, 1 = Federation Chamber |
| `question` | `factor` | 1 = question during question time |
| `answer` | `factor` | 1 = answer during question time |
| `q_in_writing` | `factor` | 1 = question on notice / written answer |
| `div_flag` | `factor` | Division-related flag |
| `gender` | `factor` | `"male"` or `"female"` |
| `uniqueID` | `character` | AustralianPoliticians package unique ID (e.g. `"Jenkins1952"`) |
| `interject` | `factor` | 1 = interjection within another member's speech |
| `partyfacts_id` | `double` | PartyFacts cross-national party identifier |

### Supplementary datasets

| File | Columns |
|---|---|
| `all_debate_topics.parquet` | date, item_index, title, page.no |
| `division_data.parquet` | date, div_num, time.stamp, num.votes_AYES, names_AYES, num.votes_NOES, names_NOES, num.votes_PAIRS, names_PAIRS, result |

---

## Key R packages

| Package | Role |
|---|---|
| `xml2` | Primary XML parser: `read_xml()`, `xml_find_all()`, `xml_name()`, `xml_children()`, `xml_text()` |
| `XML` | Secondary: `xmlParse()`, `xmlToDataFrame()`, `getNodeSet()` |
| `tidyverse` | Data manipulation throughout (dplyr, tidyr, stringr, purrr) |
| `arrow` | Parquet read/write: `write_parquet()`, `read_parquet()` |
| `hms` | Time-of-day encoding: `as_hms()` |
| `heapsofpapers` | Bulk file download |
| `AustralianPoliticians` | Biographical data: `get_auspol()` |
| `ausPH` | Parliamentary Handbook data — PHIDs, electorates, parties |
| `here` | Relative file paths |
| `fs` | File system: `dir_ls()`, `file_size()` |
| `readxl` / `writexl` | Excel lookup tables |
| `googledrive` | Download correction spreadsheets |
