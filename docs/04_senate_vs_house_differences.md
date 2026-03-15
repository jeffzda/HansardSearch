# Senate vs. House XML Structural Differences

> **Status:** Reference document — implemented. All differences documented here have been handled in the respective parsers (`03_parse.py` for Senate, `03b_parse_house.py` for House) and the enrichment scripts. The `state`/`electorate` rename, `senate_flag`/`fedchamb_flag` distinction, and presiding officer lookup differences are all in production. Written Q&A (`q_in_writing`) for the Senate remains unimplemented; see `08_build_plan.md` for known gaps.

This document covers every structural difference found between Senate and House of Representatives Hansard XML, within the target format (lowercase v2.0/2.1, covering 1998–present).

---

## Summary table

| Feature | Senate | House |
|---|---|---|
| `<chamber>` value | `SENATE` | `REPS` |
| `<electorate>` field content | Full **state/territory name** (e.g. `"Western Australia"`) | **Electoral division** name (e.g. `"Brand"`) |
| Second chamber XML node | **None** | `//maincomm.xscript` (pre-2012) → `//fedchamb.xscript` (post-2012) |
| `fedchamb_flag` concept | Not applicable | Required (0 = main chamber, 1 = Federation Chamber) |
| Presiding officer NAMEID | `10000` = President; `10001` = Clerk | `10000` = Speaker |
| Presiding officer display names | `"The PRESIDENT"`, `"The ACTING DEPUTY PRESIDENT"`, `"The ACTING PRESIDENT"`, `"MADAM PRESIDENT"` | `"Mr SPEAKER"`, `"MADAM SPEAKER"`, `"The DEPUTY SPEAKER"`, `"ACTING SPEAKER"` |
| Presiding officer bracket pattern in division para | `(The Acting Deputy President—Senator J.J. Hogg)` | `(Mr Speaker—Mr Neil Andrew)` |
| `<name role="metadata">` format | `"Cook, Sen Peter"` — includes `"Sen"` prefix | `"Beazley, Kim, MP"` — includes `"MP"` suffix |
| Senator/Member display form | `"Senator COOK"` | `"Mr BEAZLEY"`, `"Ms BISHOP"`, `"Dr EMERSON"` |
| Written Q&A location | **Separate sitting-day XML file** (not in same file) | Embedded in same file as `//answers.to.questions` subtree |
| `<cognate>/<cognateinfo>` | Present (marks cognate bills) | Present (same) |
| `<amendments>/<amendment>` | Present | Present (same) |
| `<motionnospeech>` | Present; `<electorate>` has state in brackets: `(South Australia)` | Present; `<electorate>` has division in brackets: `(Grayndler)` |
| `<adjournment>/<adjournmentinfo>` | Present with explicit `page.no` and `time.stamp` | Present (same) |
| `<interjection>` element | Present (same structure) | Present (same) |
| `<continue>` element | Present (same structure) | Present (same) |
| Division para text | `"The Senate divided."` or `"The committee divided."` | `"The House divided."` |
| Senate-specific debate types | Matters of Public Interest, Matters of Urgency, Committee of the Whole, Petitions | Motion for the Adjournment, Private Members' Business |
| Estimates sessions | **Separate XML documents**, entirely outside sitting-day XML | N/A |

---

## Detailed differences

### 1. The `<electorate>` field

This is the most significant functional difference. The column has the same XML tag name in both chambers but contains categorically different data:

| Chamber | `<electorate>` value example | Type |
|---|---|---|
| Senate | `"Western Australia"` | State/territory name |
| Senate | `"New South Wales"` | State/territory name |
| Senate | `"Tasmania"` | State/territory name |
| House | `"Brand"` | Electoral division |
| House | `"Bennelong"` | Electoral division |
| House | `"Melbourne"` | Electoral division |

In the output schema, the House uses this column as `electorate`. The Senate equivalent should be renamed to **`state`** for clarity, or kept as `electorate` with documentation noting the different semantics.

State values in Senate XML use **full names**: `"Australian Capital Territory"`, `"New South Wales"`, `"Northern Territory"`, `"Queensland"`, `"South Australia"`, `"Tasmania"`, `"Victoria"`, `"Western Australia"`.

### 2. Federation Chamber (House only)

The House of Representatives has the **Federation Chamber** (formerly the Main Committee), a parallel sitting body that handles non-controversial legislation and allows the main chamber to proceed with other business simultaneously. It is encoded in separate XML nodes:

- Pre-2012: `//maincomm.xscript`
- Post-2012: `//fedchamb.xscript`

The Senate has no equivalent. There is only one `//chamber.xscript` per sitting day. The `fedchamb_flag` column in the House schema therefore **does not apply to the Senate** and should be dropped from the Senate schema.

### 3. Presiding officer identification

Both chambers use `NAMEID="10000"` (or `name.id` = `10000`) for the presiding officer. However:

- **Senate:** The presiding officer is the **President**, assisted by the **Deputy President** and various **Acting Presidents** and **Acting Deputy Presidents**. A female President would be `"MADAM PRESIDENT"`.
- **House:** The presiding officer is the **Speaker**, assisted by the **Deputy Speaker** and various **Acting Speakers**. A female Speaker would be `"MADAM SPEAKER"`.

The parsing logic must build a **separate president lookup** (analogous to the House `speaker_lookup.csv`) covering all President and Deputy/Acting President name variants from 1998 to present.

Presidents of the Senate 1998–present (for lookup table construction):
- Margaret Reid (1996–2002)
- Paul Calvert (2002–2008)
- John Hogg (2008–2014)
- Stephen Parry (2014–2017)
- Scott Ryan (2017–2021)
- Sue Lines (2022–present)

### 4. Metadata name format

The `<name role="metadata">` element encodes the senator's name in sort form, but the convention differs from the House:

| Chamber | `<name role="metadata">` example |
|---|---|
| Senate | `"Cook, Sen Peter"` — `"Sen"` before first name |
| House | `"Beazley, Kim, MP"` — `"MP"` after last name |

Parsing the metadata name to extract `surname` and `firstName` requires different regex for each chamber:
- House: strip `", MP"` suffix, split on first `, `
- Senate: strip `"Sen "` prefix from first name portion, split on first `, `

### 5. Display name format

| Chamber | `<name role="display">` example |
|---|---|
| Senate | `"Senator COOK"` — always `"Senator"` prefix |
| House | `"Mr HOWARD"`, `"Ms BISHOP"`, `"Dr EMERSON"` — gendered titles |

The House uses gendered titles that must be parsed separately. The Senate uses `"Senator"` universally, which simplifies name-form variant generation.

One consequence: Senate name forms are simpler — there are fewer variant forms needed because `"Senator"` is gender-neutral and universal.

### 6. Written questions and answers

In the House, written Q&A (questions on notice and their answers) is embedded in the same sitting-day XML file under a separate `//answers.to.questions` subtree. This is parsed inline and rows receive `q_in_writing = 1`.

In the Senate, written answers appear to be in **separate XML documents** — either separate sitting days specifically for tabling written answers, or a separate document type entirely. This requires:
- A separate download step to identify and fetch written-answer documents
- A separate processing step to join them to the main corpus
- Verification of whether the Senate written-answers XML uses the same schema

This needs empirical verification during Stage 1 of the build.

### 7. Senate-specific procedural elements

#### Matters of Public Interest
A daily debate in the Senate (after QWN) where senators may speak on any matter of public interest. There is no direct House equivalent.

```xml
<debate>
  <debateinfo>
    <title>MATTERS OF PUBLIC INTEREST</title>
    <type>Miscellaneous</type>
    ...
  </debateinfo>
  ...
</debate>
```

#### Matters of Urgency
A senator may move that the Senate consider a matter of urgency, suspending normal business.

#### Committee of the Whole
When the Senate considers bills in detail, it sits as the Committee of the Whole (not a separate chamber like the House Federation Chamber). This is marked in the debate title/type but uses the same XML structure as regular chamber proceedings.

#### Petitions
Petitions are presented in the Senate and take a distinct structural form in older XML. In the target 1998–present range they appear as regular debate items.

### 8. Estimates sessions (Senate only)

Senate Estimates hearings (where senators question government departments about their budget estimates) are:
- A significant part of Senate business
- Held in Senate committee rooms, not the chamber
- **Encoded in entirely separate XML documents**, not in sitting-day Hansard XML
- Structured as a committee hearing format (questions and answers between senators and public servants), which is categorically different from chamber proceedings

**Decision for this project:** Estimates sessions should be **excluded from the initial corpus** and treated as a separate future dataset. Including them would require a different parsing pipeline and a different output schema.

---

## Structural elements unique to each chamber

### Senate only
- `<adjournmentinfo>` with explicit `<time.stamp>` (House has same element but format may differ)
- `<cognate>/<cognateinfo>` — cognate bill linkage (present in both but more common in Senate)
- President/Deputy President name variants

### House only
- `//maincomm.xscript` / `//fedchamb.xscript` — Federation Chamber
- `//answers.to.questions` — written Q&A inline
- `fedchamb_flag` column concept
- Speaker/Deputy Speaker/Acting Speaker name variants

### Present in both (same structure)
- `<motionnospeech>` — procedural motions
- `<amendments>/<amendment>` — bill amendments
- `<interjection>` — interjections
- `<continue>` — speaker resumes after interjection
- `<division>` — votes (identical structure)
- `<adjournment>` — sitting end
- `<cognate>` — cognate bills
