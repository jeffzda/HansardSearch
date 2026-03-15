# Required Adaptations for Senate Parsing

This document specifies every adaptation needed to the Katz & Alexander House parsing logic to handle the Senate correctly. All adaptations listed here have been implemented in `03_parse.py` and `04_fill_details.py`.

---

## Schema-level adaptations

### 1. Remove `fedchamb_flag` entirely

**Status: Implemented.**

The House parser processes two XML nodes per sitting day (`//chamber.xscript` and `//maincomm.xscript` / `//fedchamb.xscript`) and uses `fedchamb_flag` to distinguish them. The Senate has only `//chamber.xscript`. No equivalent flag is needed.

The `fedchamb_flag` column is absent from the Senate output schema. A `senate_flag` column (always 1) is added instead, to allow corpus merging with the House dataset.

### 2. Rename `electorate` → `state`

**Status: Implemented.**

The `<electorate>` XML element in Senate files contains the senator's state or territory, not an electoral division. The output column is named `state`.

### 3. Add `senate_flag` column

**Status: Implemented.**

`senate_flag` is always 1 in the Senate corpus; the House corpus uses `fedchamb_flag` (0 = main chamber, 1 = Federation Chamber).

---

## Parsing logic adaptations

### 4. Presiding officer name patterns

**Status: Implemented.**

The Senate presiding officer patterns handled:

| Display name | Context |
|---|---|
| `"The PRESIDENT"` | Senate President |
| `"MADAM PRESIDENT"` | Female President (currently Sue Lines) |
| `"The DEPUTY PRESIDENT"` | Deputy President |
| `"The ACTING PRESIDENT"` | Acting President |
| `"The ACTING DEPUTY PRESIDENT"` | Acting Deputy President |
| `"The CHAIR"` | When sitting as Committee of the Whole |
| `"The TEMPORARY CHAIR OF COMMITTEES"` | Temporary chair |

All have `name.id = "10000"` in the XML. A `president_lookup.csv` covers all variants 1998–present.

### 5. `<name role="metadata">` parsing

**Status: Implemented.**

| Chamber | Format | Parse logic |
|---|---|---|
| House | `"Beazley, Kim, MP"` | Strip `", MP"` suffix; split on first `, ` |
| Senate | `"Cook, Sen Peter"` | Split on first `, `; strip `"Sen "` prefix from first name |

### 6. Name form variant generation

**Status: Implemented.**

Five Senate name forms using `"Senator"` prefix universally (no gendered title variants needed):

| Form | Example |
|---|---|
| `form1` | `"Senator FirstName Surname"` |
| `form2` | `"Senator Surname"` |
| `form3` | `"Senator FIRSTNAME SURNAME"` |
| `form4` | `"Senator SURNAME"` |
| `form5` | `"Senator FirstName SURNAME"` |

### 7. Stage direction patterns

**Status: Implemented.**

Senate-specific stage direction patterns are compiled in `STAGE_PATTERNS_21` in `03_parse.py`, covering Senate-specific vocabulary such as `"The Senate divided."`, `"The committee divided."`, `"Senate adjourned at..."`, etc. House-specific patterns (`"The House divided."`, Federation Chamber references) are absent from the Senate parser.

### 8. Written questions (`q_in_writing`)

**Status: Not yet implemented.**

In the House, written Q&A is extracted from `//answers.to.questions` within the same XML file. In the Senate, written answers appear in separate XML documents.

The `q_in_writing` column is present in the output schema but is always 0. Implementation is tracked as a known gap.

### 9. Anonymous collective interjections

**Status: Implemented.**

Senate-specific variants handled alongside the general patterns: `"Senators interjecting—"`, `"Government senators interjecting—"`, `"Opposition senators interjecting—"`.

### 10. Validation test adjustments

**Status: Implemented (Senate T1–T7; House T1–T7).**

| Test | House logic | Senate adaptation |
|---|---|---|
| 1 | Filename date matches `session.header/date` | Same |
| 2 | No consecutive duplicate `body` rows | Same |
| 3 | `(Time expired)` not followed by more text | Same |
| 4 | Each `name.id` has only one `party` and `electorate` per day | `state` instead of `electorate` |
| 5 | All `name.id` values exist in member lookup | Uses `senator_lookup.csv` |
| 6 | All `uniqueID` values have valid birth/death dates | Same; uses `senator_lookup` |
| 7 | All `uniqueID` values were members on sitting day | Uses `state_lookup` `senatorFrom`/`senatorTo` |

Known issue with T6/T7: `Cox1863` and `McLachlan1870` are incorrectly matched to 2023 senators via Pass 2 name-form matching because the lookup has no date-range awareness.

### 11. Date-aware lookups

**Status: Implemented.**

`party_lookup.csv` and `state_lookup.csv` are used in Pass 3 of `04_fill_details.py` to assign the correct party and state for each PHID on each sitting date.

---

## Additional adaptation: HPS-Electorate span concatenation bug fix

**Status: Implemented.**

In v2.2 XML files, the `<span class="HPS-Electorate">` element sometimes spans multiple text nodes. The original `_all_text()` helper stripped trailing whitespace from each text node, causing multi-word state names to concatenate without spaces (e.g. `"South "` + `"Australia"` → `"SouthAustralia"`).

Fix: replaced `"".join(s.itertext()).strip()` (which stripped each part individually) with `"".join(s.itertext())` followed by `" ".join(raw.split())` (which collapses internal whitespace without stripping parts mid-concatenation).

---

## Additional adaptation: Pass 5 state abbreviation normalisation

**Status: Implemented in `04_fill_details.py`.**

State values in the lookup tables use abbreviations (`WA`, `VIC`, `NSW`, etc.) derived from the AustralianPoliticians dataset. When these values are written into the `state` column via Pass 1 or Pass 2, they need to be expanded to the full names used in the XML (e.g. `WA` → `Western Australia`, `VIC` → `Victoria`).

Pass 5 applies a fixed mapping over all rows where `state` matches a known abbreviation.

---

## Architectural decision: node-iteration vs. text-blob

**Status: Node-iteration used throughout.**

Python's `lxml` library provides document-order iteration and text/tail distinction, making the text-blob split approach unnecessary. The parser walks the XML tree directly via `element.iter()`, collecting text from `<para>` (v2.0/v2.1) or `<p class="HPS-*">` (v2.2) elements.

The output schema is identical to Katz & Alexander; only the parsing mechanism differs internally.

---

## Summary of Senate vs. House output schema differences

| Column | Senate | House |
|---|---|---|
| `state` | Present (full state name) | Absent |
| `electorate` | Absent | Present (electoral division) |
| `senate_flag` | Present (always 1) | Absent |
| `fedchamb_flag` | Absent | Present (0 = main chamber, 1 = Fed. Chamber) |
| All other columns | Same names, same types, same semantics | Same |
