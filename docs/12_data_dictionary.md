# Data Dictionary — Australian Hansard Database (1998–2025)

This document provides a column-level reference for all output datasets in the Australian Hansard Database. Coverage: Senate corpus, House corpus, Senate debate topics, House debate topics, Senate division summary, Senate division votes, House division summary, and House division votes.

---

## 1. Main Corpus Columns

The Senate and House corpora share 21 columns. Where a column differs between chambers, both variants are described.

---

### `date`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | object (Python `date` stored as string in parquet) |
| Nullable | No |
| Description | The sitting day date. Derived from the XML filename stem at corpus assembly. |
| Valid values | ISO 8601 date strings: `YYYY-MM-DD` |
| Range | `1998-03-02` to `2025-11-27` |
| Example | `1998-03-02` |
| Notes | Stored as a Python `datetime.date` object during processing; serialised as a string in the parquet file via pandas `to_datetime().dt.date`. Load with `pd.to_datetime(df["date"])` if date arithmetic is needed. |

---

### `name`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | str |
| Nullable | No |
| Description | The display name of the speaker as it appears in the XML. For Senate v2.0/v2.1 files, sourced from `<name role="display">` in the `<talker>` block. For v2.2 files, sourced from the `<span class="HPS-MemberSpeech">` or `<span class="HPS-OfficeSpeech">` within the speech body text. |
| Valid values | Free text; typically `"Senator SURNAME"` (Senate) or `"Mr/Mrs/Ms SURNAME"` (House). Special values: `"business start"` (first row of each sitting day), `"stage direction"` (procedural notices). |
| Example (Senate) | `"Senator O'BRIEN"`, `"The PRESIDENT"`, `"business start"` |
| Example (House) | `"Mr ALBANESE"`, `"The SPEAKER"`, `"business start"` |
| Notes | Presiding officers appear as `"The PRESIDENT"`, `"The DEPUTY PRESIDENT"`, `"The ACTING PRESIDENT"` (Senate) or `"The SPEAKER"`, `"The DEPUTY SPEAKER"`, `"The CHAIR"` (House). Stage direction rows contain procedural text (e.g. `"Question agreed to."`) and are identified by regex pattern matching against the body text during parsing. |

---

### `order`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | int64 |
| Nullable | No |
| Description | Sequential row number within the sitting day, 1-indexed. Rows are ordered as they appear in the source XML document. |
| Valid values | Positive integers starting at 1 |
| Range | 1 to ~1,000+ depending on the length of the sitting day |
| Example | `42` |
| Notes | The `order` column can be used to reconstruct the full sequence of utterances within a day by filtering on `date` and sorting by `order`. |

---

### `speech_no`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | float64 (nullable integer) |
| Nullable | Yes |
| Description | Speech group identifier. All rows belonging to the same speech (including the main speech body plus any associated continuation rows) share the same `speech_no` value within a sitting day. |
| Valid values | Positive integers (stored as float due to pandas nullable integer representation) |
| Example | `7.0` |
| Notes | NULL for `"business start"` rows and some standalone procedural rows. Values are not necessarily contiguous across the sitting day. Not globally unique — use `(date, speech_no)` as the composite key if needed. |

---

### `page_no`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | float64 (nullable integer) |
| Nullable | Yes |
| Description | Hansard printed page number. Sourced from the `<page.no>` element within the speech `<talker>` block in the XML. |
| Valid values | Positive integers |
| Example | `1234.0` |
| Notes | Page numbers are assigned per speech and may not increment strictly with row order (multiple speeches can share a page or a speech can span pages). NULL for some procedural and stage direction rows. |

---

### `time_stamp`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | str (nullable) |
| Nullable | Yes |
| Description | 24-hour wall-clock time at which the speech was delivered. |
| Valid values | `HH:MM:SS` format, e.g. `"14:32:00"` |
| Example | `"14:32:00"` |
| Notes | Present for most speeches in v2.1 and v2.2 XML files. NULL for many rows in the 1998–~2004 era where the `<time.stamp>` element was absent or empty. In v2.2 files the timestamp is embedded in the body text as a `<span class="HPS-Time">` element and extracted during parsing. Only the first timestamp encountered per speech is stored. |

---

### `name_id`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | str (nullable) |
| Nullable | Yes |
| Description | APH Parliamentary PHID (Parliamentary Hansard Identifier, also called the APH ID). This is the stable identifier used by the Australian Parliament House to identify individual members across all records. |
| Valid values | Alphanumeric string (typically 2–5 characters, uppercase), e.g. `"8O6"`, `"A56"`. Special values: `"10000"` = generic presiding officer (President/Speaker/Chair); `"10001"` = second presiding officer sentinel; `"20000"` = additional sentinel; `"UNKNOWN"` = speaker identified in the XML but not matched to any known member. |
| Example | `"8O6"` |
| Notes | Sourced primarily from the `<name.id>` element in the XML talker block, or from the `href` attribute of `<a type="MemberSpeech">` elements in v2.2 files (stored in lowercase in the XML; normalised to uppercase). For presiding officers, the raw XML PHID is replaced with `"10000"` during the enrichment pass (step 4). NULL for `"business start"` and some `"stage direction"` rows. |

---

### `state` (Senate) / `electorate` (House per-day files)

| Attribute | Value |
|---|---|
| Datasets | Senate corpus (`state`); House corpus and per-day files (`electorate`) |
| Type | str (nullable) |
| Nullable | Yes (rarely) |
| Description | **Senate:** The Australian state or territory the senator represents, as a full name. Sourced from the XML `<electorate>` element and normalised to full-name form during enrichment (Pass 5). **House:** The electoral division (constituency) the member represents, e.g. `"Grayndler"`. Sourced from the `<electorate>` element in the XML. |
| Valid values (Senate) | `"New South Wales"`, `"Victoria"`, `"Queensland"`, `"South Australia"`, `"Western Australia"`, `"Tasmania"`, `"Australian Capital Territory"`, `"Northern Territory"` |
| Valid values (House) | Australian electoral division names, e.g. `"Brand"`, `"Grayndler"`, `"Warringah"` |
| Example (Senate) | `"Western Australia"` |
| Example (House) | `"Grayndler"` |
| Notes | Presiding officer rows (`name_id=10000`) may have a generic state/electorate value or NULL. The assembled House corpus parquet correctly uses the column name `electorate` (not `state`); no renaming is required. |

---

### `party`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | str (nullable) |
| Nullable | Yes (rarely) |
| Description | Party abbreviation for the speaker at the time of the sitting. Date-aware correction is applied during enrichment (Pass 3 for Senate) to assign the historically correct party for each senator on each sitting date, handling senators who changed parties during their tenure. |
| Common values | `ALP` (Australian Labor Party), `LP` (Liberal Party), `NP` or `Nats` (National Party), `GRN` (Australian Greens), `AD` (Australian Democrats, to 2008), `FF` (Family First), `OPP` (other/independent), `IND` (independent), `PHON` (Pauline Hanson's One Nation), `UAP` (United Australia Party), `JLN` (Jacqui Lambie Network), `KAP` (Katter's Australian Party) |
| Example | `"ALP"` |
| Notes | `"business start"` and `"stage direction"` rows may have an empty string or NULL party. Party values for presiding officers are set to the officer's actual party where known. The date-aware correction in step 4 (Pass 3) joins `party_lookup.csv` on PHID and date to assign the historically correct value; however, T4 validation failures indicate this correction is not fully consistent across all rows in both corpora. |

---

### `in_gov`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | int64 |
| Nullable | No |
| Description | Indicates whether the speaker's party was in government on the sitting day. |
| Valid values | `1` = government party; `0` = opposition or crossbench |
| Example | `0` |
| Notes | Recomputed from the known Australian government timeline by `fix_in_gov.py` (applied 2026-03-11). The XML `<in.gov>` flag was unreliable from ~2012 onward (APH changed to a self-closing empty form). The hardcoded government periods cover 1996-03-11 to present (Howard/Rudd/Gillard/Abbott/Turnbull/Morrison/Albanese). If the corpus is regenerated from raw XML, re-run `fix_in_gov.py` before analysis. |

---

### `first_speech`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | int64 |
| Nullable | No |
| Description | Indicates whether this speech was the speaker's first (maiden) speech to the chamber. |
| Valid values | `1` = maiden speech; `0` otherwise |
| Example | `0` |
| Notes | Sourced from the `<first.speech>` element in the XML talker block. The vast majority of rows have value `0`. |

---

### `body`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | str |
| Nullable | No |
| Description | The full plain text of the utterance. For v2.0/v2.1 XML, assembled by concatenating all `<para>` and `<inline>` element text nodes under the speech element. For v2.2 XML, assembled from the `<p>` elements within `<talk.text>/<body>`, with attribution spans (name, electorate, timestamp, ministerial title) stripped from the output. Paragraphs are separated by `\n`. |
| Valid values | Free text |
| Example | `"Mr President, I move that the bill be read a second time."` |
| Notes | Stage direction rows contain the procedural text as their body value. `"business start"` rows contain the date and opening procedural text for the sitting day. Formatting markup (bold, italic, lists, tables) is stripped; only the plain text is retained. |

---

### `question`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | int64 |
| Nullable | No |
| Description | Indicates whether the utterance was a question asked during question time. |
| Valid values | `1` = question during question time; `0` otherwise |
| Example | `0` |
| Notes | Sourced from the XML element type: rows parsed from `<question>` elements receive `question=1`. Not to be confused with `q_in_writing`. |

---

### `answer`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | int64 |
| Nullable | No |
| Description | Indicates whether the utterance was an answer given during question time. |
| Valid values | `1` = answer during question time; `0` otherwise |
| Example | `0` |
| Notes | Sourced from the XML element type: rows parsed from `<answer>` elements receive `answer=1`. |

---

### `q_in_writing`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | int64 |
| Nullable | No |
| Description | Indicates whether the row is a question on notice or written answer, as detected from question-on-notice sections embedded within the daily Hansard XML. |
| Valid values | `1` = question on notice or written answer; `0` otherwise |
| Example | `0` |
| Notes | Populated for both chambers as of the current release. Both `03_parse.py` and `03b_parse_house.py` detect questions on notice within the daily XML and set `q_in_writing=1`. Senate: 30,239 rows are flagged (4.8% of all rows). House: 41,651 rows are flagged (6.2% of all rows). Note that some written questions/answers that appear in separate APH ParlInfo XML documents (outside the daily Hansard XML) are not captured; this column reflects only what is embedded in the standard daily Hansard XML files. |

---

### `div_flag`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | int64 |
| Nullable | No |
| Description | Indicates whether this row is associated with or immediately follows a division (recorded vote) call. |
| Valid values | `1` = division-related; `0` otherwise |
| Example | `0` |
| Notes | Sourced from the XML structure: set to `1` for rows within or immediately adjacent to `<division>` elements. Most rows have value `0`. The supplementary divisions dataset (see Section 2) provides the full structured vote roll for each division. |

---

### `gender`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | str (nullable) |
| Nullable | Yes |
| Description | The gender of the speaker. |
| Valid values | `"male"`, `"female"`, or NULL |
| Example | `"female"` |
| Notes | Sourced from the AustralianPoliticians dataset for members covered up to approximately November 2021, joined via PHID or name-form matching during enrichment (step 4). The 35 post-2021 senators originally sourced from OpenAustralia (which does not include gender) had gender filled via a Wikidata SPARQL batch query; all senators in `senator_lookup.csv` now have gender populated. `"business start"` and `"stage direction"` rows always have `gender=NULL`. |

---

### `unique_id`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | str (nullable) |
| Nullable | Yes |
| Description | The AustralianPoliticians dataset unique identifier for the speaker. Provides a stable link to biographical records in the AustralianPoliticians dataset (https://github.com/RohanAlexander/australian_politicians). |
| Valid values | String of the form `SurnameYYYY` where `YYYY` is the birth year, e.g. `"OBrien1959"`, `"Albanese1963"` |
| Example | `"OBrien1959"` |
| Notes | Populated during enrichment by joining on `name_id` (PHID) via `senator_lookup.csv`. For members not found by PHID, a secondary pass matches on name-form variants. NULL for `"business start"`, `"stage direction"`, and rows where no match was found in the lookup. Note that two historical identifiers (`Cox1863`, `McLachlan1870`) are known to be incorrectly matched to modern senators via the name-form pass; see Known Gaps in `README.md`. |

---

### `interject`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | int64 |
| Nullable | No |
| Description | Indicates whether this row is an interjection within another member's speech. |
| Valid values | `1` = interjection; `0` = regular speech |
| Example | `0` |
| Notes | For v2.0/v2.1 XML, set to `1` for rows parsed from `<interjection>` elements. For v2.2 XML, set to `1` for rows identified by `<a type="MemberInterjecting">` or `<a type="OfficeInterjecting">` in the speech body. Interjection rows typically have short body text. |

---

### `senate_flag`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus |
| Type | int64 |
| Nullable | No |
| Description | Chamber identifier flag for the Senate corpus. Always `1` for all rows in the Senate corpus. |
| Valid values | `1` always |
| Example | `1` |
| Notes | This column exists to facilitate merging the Senate and House corpora: when both are concatenated, `senate_flag=1` identifies Senate rows. The House corpus uses a separate column named `fedchamb_flag` (not `senate_flag`); the two corpora use distinct column names for their respective chamber flags. |

---

### `fedchamb_flag` (House per-day files)

| Attribute | Value |
|---|---|
| Datasets | House per-day files and assembled House corpus |
| Type | int64 |
| Nullable | No |
| Description | Identifies whether the row comes from the main House of Representatives chamber or from the Federation Chamber (previously called the Main Committee). Both chambers are included within each House sitting-day XML file. |
| Valid values | `0` = main chamber; `1` = Federation Chamber |
| Example | `0` |
| Notes | Approximately 17.8% of House corpus rows have `fedchamb_flag=1`. The Federation Chamber handles non-controversial legislation and certain committee stages. Both per-day files in `data/output/house/daily/` and the assembled `house_hansard_corpus_1998_to_2025.parquet` use the column name `fedchamb_flag`. |

---

### `partyfacts_id`

| Attribute | Value |
|---|---|
| Datasets | Senate corpus, House corpus |
| Type | float64 (nullable integer) |
| Nullable | Yes |
| Description | The PartyFacts cross-national party identifier. PartyFacts (https://partyfacts.herokuapp.com) provides a harmonised numeric ID for political parties across countries, enabling cross-national comparative research. |
| Valid values | Positive integers (stored as float64 due to pandas nullable integer handling); NULL where no PartyFacts entry exists |
| Example | `1385.0` |
| Notes | Joined from `data/lookup/partyfacts_map.csv` on the `party` abbreviation column during step 6. The following parties have no PartyFacts entry and retain NULL: JLN (Jacqui Lambie Network), NatsWA (Nationals WA), AV (Australia's Voice), IND (Independent). PRES and DPRES are non-party presiding-officer role labels also left NULL. All other parties appearing in the corpus (including CLP, LNP, KAP, NXT, CA, UAP, PUP, DLP, PHON, GWA, LDP, FFP, TG, AUS) now have verified PartyFacts IDs. `"business start"` and `"stage direction"` rows have NULL `partyfacts_id`. |

---

## 2. Debate Topics Columns

Files: `data/output/senate/topics/senate_debate_topics.parquet/.csv` (Senate)
`data/output/house/topics/house_debate_topics_1998_to_2025.parquet/.csv` (House, assembled corpus)
`data/output/house/topics/YYYY-MM-DD.parquet` (House, per-day files)

One row per debate or subdebate entry within a sitting day. The topic hierarchy is a two-level structure: top-level debates (level 0) and subdebates (level 1).

---

### `date`

| Attribute | Value |
|---|---|
| Datasets | Senate debate topics, House debate topics |
| Type | str |
| Nullable | No |
| Description | Sitting day date. |
| Valid values | `YYYY-MM-DD` |
| Example | `"1998-03-02"` |

---

### `order`

| Attribute | Value |
|---|---|
| Datasets | Senate debate topics, House debate topics |
| Type | int |
| Nullable | No |
| Description | Sequential position of this topic entry within the sitting day's topic list. Used as the numeric component of `debate_id`. |
| Valid values | Positive integers starting at 1 |
| Example | `3` |

---

### `level`

| Attribute | Value |
|---|---|
| Datasets | Senate debate topics, House debate topics |
| Type | int |
| Nullable | No |
| Description | Hierarchy level of this topic entry. |
| Valid values | `0` = top-level debate (from `<debate>` element); `1` = subdebate (from `<subdebate.1>` element) |
| Example | `0` |
| Notes | The source XML supports deeper nesting (`<subdebate.2>`, `<subdebate.3>`) but in practice the extracted topics represent at most two levels. |

---

### `debate_id`

| Attribute | Value |
|---|---|
| Datasets | Senate debate topics, House debate topics |
| Type | str |
| Nullable | No |
| Description | Unique identifier for this topic entry within the full dataset. Format: `{date}_{order:04d}`. |
| Valid values | String of the form `YYYY-MM-DD_NNNN` |
| Example | `"1998-03-02_0003"` |
| Notes | Globally unique across all sitting days within a chamber. Can be used to join topic rows to corpus rows that fall within the same debate. |

---

### `parent_id`

| Attribute | Value |
|---|---|
| Datasets | Senate debate topics, House debate topics |
| Type | str (nullable) |
| Nullable | Yes |
| Description | The `debate_id` of the parent top-level debate for subdebate rows. NULL for top-level (level 0) rows. |
| Valid values | `debate_id` format string, or NULL |
| Example | `"1998-03-02_0002"` |
| Notes | NULL for all top-level (level 0) rows. For subdebate rows, the parent_id links to the enclosing top-level debate. |

---

### `topic`

| Attribute | Value |
|---|---|
| Datasets | Senate debate topics, House debate topics |
| Type | str (nullable) |
| Nullable | Yes |
| Description | The title of the debate or subdebate as it appears in the XML `<debateinfo><title>` or `<subdebateinfo><title>` element. |
| Valid values | Free text |
| Example | `"QUESTIONS WITHOUT NOTICE"`, `"Appropriation Bill (No. 1) 2005-2006"` |
| Notes | NULL for a small number of rows where no title element was found in the XML. Topics are not normalised for case or punctuation; they appear as printed in the Hansard. |

---

### `cognate`

| Attribute | Value |
|---|---|
| Datasets | Senate debate topics, House debate topics |
| Type | str (nullable) |
| Nullable | Yes |
| Description | A cognate bill or secondary topic title, present when a single debate covers multiple related bills. Sourced from `<cognate.title>` within `<debateinfo>`. |
| Valid values | Free text, or NULL |
| Example | `"Income Tax Assessment (1997 Act) Bill 1998"` |
| Notes | NULL for the vast majority of rows. Only present in v2.0/v2.1 era files; `<cognate>` and `<cognateinfo>` elements were removed in v2.2. |

---

### `gvt_business`

| Attribute | Value |
|---|---|
| Datasets | Senate debate topics, House debate topics |
| Type | int |
| Nullable | No |
| Description | Indicates whether this debate was classified as government business, based on the `<type>` field in the `<debateinfo>` element. |
| Valid values | `1` = type field contains any of: `government`, `ministerial`, `executive`, `budget`, `appropriation` (case-insensitive); `0` otherwise |
| Example | `0` |
| Notes | This is a heuristic flag derived from the debate type text; it is not a manually curated classification. |

---

### `senate_flag` / `fedchamb_flag`

| Attribute | Value |
|---|---|
| Datasets | Senate debate topics (`senate_flag`); House debate topics (`fedchamb_flag`) |
| Type | int |
| Nullable | No |
| Description | Chamber identifier. Always `1` for Senate topic rows. For House topic rows: `0` = main House chamber topic; `1` = Federation Chamber topic. |
| Valid values | `1` (Senate, always); `0` or `1` (House) |
| Example | `1` |
| Notes | Of the 194,077 House topic rows, 163,169 have `fedchamb_flag=0` (main chamber) and 30,908 have `fedchamb_flag=1` (Federation Chamber). |

---

## 3. Division Summary Columns

Files: `data/output/senate/divisions/senate_divisions.parquet/.csv` (Senate)
`data/output/house/divisions/house_divisions.parquet/.csv` (House, assembled corpus)
`data/output/house/divisions/YYYY-MM-DD_divisions.parquet` (House, per-day files)

One row per recorded vote (division) within a sitting day.

---

### `date`

| Attribute | Value |
|---|---|
| Datasets | Senate divisions, House divisions |
| Type | str |
| Nullable | No |
| Description | Sitting day date on which the division occurred. |
| Valid values | `YYYY-MM-DD` |
| Example | `"2005-06-16"` |

---

### `division_no`

| Attribute | Value |
|---|---|
| Datasets | Senate divisions, House divisions |
| Type | int |
| Nullable | No |
| Description | Sequential division number within the sitting day. Together with `date`, forms the composite key joining division summary rows to vote-level rows. |
| Valid values | Positive integers starting at 1 |
| Example | `1` |
| Notes | Not globally unique; use `(date, division_no)` as the composite key. |

---

### `debate_topic`

| Attribute | Value |
|---|---|
| Datasets | Senate divisions, House divisions |
| Type | str (nullable) |
| Nullable | Yes |
| Description | Title of the debate within which the division occurred, as extracted from the nearest enclosing `<debateinfo>` or `<subdebateinfo>` element in the XML. |
| Valid values | Free text, or NULL |
| Example | `"Appropriation Bill (No. 1) 2005-2006"` |
| Notes | NULL for a small number of divisions where no enclosing debate title could be found. |

---

### `question`

| Attribute | Value |
|---|---|
| Datasets | Senate divisions, House divisions |
| Type | str (nullable) |
| Nullable | Yes |
| Description | The text of the question put to the vote, as it appears in the division header within the XML. |
| Valid values | Free text, or NULL |
| Example | `"That the amendment be agreed to."` |
| Notes | Extracted from the `<division.header>` or `<divisioninfo>` element. May include multi-line procedural text for complex questions. NULL for older XML files where the division header was absent. |

---

### `ayes_count`

| Attribute | Value |
|---|---|
| Datasets | Senate divisions, House divisions |
| Type | int |
| Nullable | No |
| Description | Total number of senators or members who voted aye. |
| Valid values | Non-negative integers |
| Example | `36` |
| Notes | Counted from the `<name>` elements within the `<ayes>` container in the XML. For v2.2 files, extracted from the structured division table. |

---

### `noes_count`

| Attribute | Value |
|---|---|
| Datasets | Senate divisions, House divisions |
| Type | int |
| Nullable | No |
| Description | Total number of senators or members who voted no. |
| Valid values | Non-negative integers |
| Example | `30` |
| Notes | Counted from the `<name>` elements within the `<noes>` container. |

---

### `result`

| Attribute | Value |
|---|---|
| Datasets | Senate divisions, House divisions |
| Type | str |
| Nullable | No |
| Description | The outcome of the division. |
| Valid values | `"aye"` (ayes > noes); `"noe"` (noes > ayes); `"tie"` (ayes == noes) |
| Example | `"aye"` |
| Notes | Derived computationally from `ayes_count` and `noes_count`. Note the spelling `"noe"` (not `"no"`) is consistent with Australian parliamentary convention and with the Katz and Alexander schema. |

---

### `fedchamb_flag` (House divisions only)

| Attribute | Value |
|---|---|
| Datasets | House division summary |
| Type | int |
| Nullable | No |
| Description | Indicates whether the division occurred in the main House chamber or the Federation Chamber. |
| Valid values | `0` = main chamber; `1` = Federation Chamber |
| Example | `0` |
| Notes | All 4,616 recorded House divisions in this dataset have `fedchamb_flag=0`. The Federation Chamber does not conduct formal divisions. |

---

## 4. Division Vote-Level Columns (Long Format)

Files: `data/output/senate/divisions/senate_division_votes.parquet/.csv` (Senate)
`data/output/house/divisions/house_division_votes.parquet/.csv` (House, assembled corpus)
`data/output/house/divisions/YYYY-MM-DD_votes.parquet` (House, per-day files)

One row per senator or member per division. 466,557 Senate vote records; 614,057 House vote records.

---

### `date`

| Attribute | Value |
|---|---|
| Datasets | Senate division votes, House division votes |
| Type | str |
| Nullable | No |
| Description | Sitting day date. |
| Valid values | `YYYY-MM-DD` |
| Example | `"2005-06-16"` |

---

### `division_no`

| Attribute | Value |
|---|---|
| Datasets | Senate division votes, House division votes |
| Type | int |
| Nullable | No |
| Description | Division number within the sitting day. Together with `date`, joins to the division summary table. |
| Valid values | Positive integers |
| Example | `1` |

---

### `unique_id`

| Attribute | Value |
|---|---|
| Datasets | Senate division votes, House division votes |
| Type | str (nullable) |
| Nullable | Yes |
| Description | AustralianPoliticians unique identifier for the voting member. Back-filled from the corresponding daily corpus file during step 9. |
| Valid values | `SurnameYYYY` format string, or NULL |
| Example | `"OBrien1959"` |
| Notes | NULL where the member could not be matched in the daily corpus file. |

---

### `name_id`

| Attribute | Value |
|---|---|
| Datasets | Senate division votes, House division votes |
| Type | str (nullable) |
| Nullable | Yes |
| Description | APH Parliamentary PHID. Back-filled from the corresponding daily corpus file during step 9. |
| Valid values | Alphanumeric PHID string, or NULL |
| Example | `"8O6"` |
| Notes | Extracted from the `id` attribute on `<name>` elements within the division XML block, or from the daily file match. NULL where not available. |

---

### `name`

| Attribute | Value |
|---|---|
| Datasets | Senate division votes, House division votes |
| Type | str |
| Nullable | No |
| Description | The senator's or member's name as it appears in the division list within the XML. |
| Valid values | Free text surname or full name |
| Example | `"O'Brien"` |
| Notes | The name format in division lists is typically a surname only or `"Surname, FirstName"` in older XML eras. May differ from the `name` column in the main corpus (which uses the display name with title). |

---

### `state` (Senate) / `electorate` (House)

| Attribute | Value |
|---|---|
| Datasets | Senate division votes (`state`); House division votes (`electorate`) |
| Type | str (nullable) |
| Nullable | Yes |
| Description | **Senate:** The state or territory of the senator. Back-filled from the daily corpus file. **House:** The electoral division of the member. Back-filled from the daily corpus file. |
| Valid values (Senate) | Full state/territory names, e.g. `"Tasmania"`, `"Western Australia"` |
| Valid values (House) | Electoral division names, e.g. `"Brand"` |
| Example (Senate) | `"Tasmania"` |
| Example (House) | `"Brand"` |
| Notes | Sourced from the enriched daily parquet file for the corresponding sitting day, not from the division XML block alone. NULL where no match was found. |

---

### `party`

| Attribute | Value |
|---|---|
| Datasets | Senate division votes, House division votes |
| Type | str (nullable) |
| Nullable | Yes |
| Description | Party abbreviation for the voting member. Back-filled from the daily corpus file. |
| Valid values | Same party abbreviations as the main corpus `party` column |
| Example | `"ALP"` |
| Notes | NULL where the member could not be matched in the daily corpus file. |

---

### `vote`

| Attribute | Value |
|---|---|
| Datasets | Senate division votes, House division votes |
| Type | str |
| Nullable | No |
| Description | The vote cast by this senator or member in this division. |
| Valid values | `"aye"` or `"noe"` |
| Example | `"aye"` |
| Notes | Note the spelling `"noe"` (not `"no"`), consistent with Australian parliamentary Hansard convention and the Katz and Alexander schema. Pair votes (abstentions) are not currently captured in the vote roll; only aye and noe votes are extracted. |

---

## 5. Lookup Table Reference

The lookup tables in `data/lookup/` are used internally by the pipeline but may also be useful to researchers.

| File | Columns | Description |
|---|---|---|
| `senator_lookup.csv` | `name_id`, `unique_id`, `gender`, `state_abbrev`, `form1`–`form5`, birth/death dates, `source` | One row per senator (666 senators); five name-form variants; zero NULL gender entries |
| `party_lookup.csv` | `name_id`, `party`, `party_from`, `party_to` | Date-aware party affiliation spells per senator |
| `state_lookup.csv` | `name_id`, `state`, `senator_from`, `senator_to` | Date-aware Senate term records per senator |
| `president_lookup.csv` | `xml_name_pattern`, `name_id`, `unique_id`, `date_from`, `date_to` | Senate Presidents, Deputy Presidents, Acting Presidents 1996–present |
| `partyfacts_map.csv` | `party_abbrev`, `partyfacts_id` | Party abbreviation to PartyFacts cross-national ID mapping (28 entries; NULL for JLN, NatsWA, AV, IND) |
| `session_info_all.csv` | `filename`, `date`, `parliament_no`, `session_no`, `period_no`, `chamber`, `page_no`, `proof`, `schema_version` | Session header metadata, 3,204 rows (1,482 Senate + 1,722 House) |
| `member_lookup.csv` | `name_id`, `unique_id`, `gender`, `form1`–`form5` | House member name forms and PHIDs (1,280 members) |
| `electorate_lookup.csv` | `name_id`, `electorate`, `member_from`, `member_to` | Date-aware electorate records per House member (1,430 rows) |
| `party_lookup_house.csv` | `name_id`, `party`, `party_from`, `party_to` | Date-aware party affiliation spells per House member (1,571 rows) |
| `speaker_lookup.csv` | `xml_name_pattern`, `name_id`, `unique_id`, `date_from`, `date_to` | House Speakers and Deputy Speakers (20 rows) |
