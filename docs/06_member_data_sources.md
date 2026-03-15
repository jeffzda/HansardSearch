# Senator Member Data Sources

> **Status:** Reference document — implemented. The strategy recommended here was followed: AustralianPoliticians data (primary, 1901–November 2021) joined with OpenAustralia parser `senators.csv` (gap fill for post-2021 senators). The resulting lookup tables are in `data/lookup/`. Known remaining gap: gender is NULL for approximately 35 post-2021 senators. No equivalent House member lookup was built; House enrichment (`04b_fill_details_house.py`) reuses `senator_lookup.csv` for PHID-based joins, since PHIDs are shared across chambers.

This document covers the research into data sources for Australian senator biographical and membership details, and recommends a strategy for the 1998–present corpus.

---

## The senator-specific problem

Senators differ from House members in two important ways that affect the member lookup system:

1. **State, not electorate** — senators represent states and territories, not electoral divisions. States are stable; they are not subject to redistributions.
2. **Staggered six-year terms** — senators serve six-year terms, with half the Senate facing election at each general election. Casual vacancies (s.15 appointments) add complexity. A senator may serve multiple non-consecutive terms.

These differences mean the House `electorate_lookup.csv` must be replaced with a `state_lookup.csv` using different date logic, and the `member_lookup.csv` must source from senator-specific data.

---

## Sources investigated

### 1. AustralianPoliticians R package / data repository (Rohan Alexander, 2021)

**URLs:**
- Package: https://github.com/RohanAlexander/AustralianPoliticians
- Raw data: https://github.com/RohanAlexander/australian_politicians

**Status: Best primary source for 1901–November 2021.**

This is the same package used by Katz & Alexander for the House. It contains three directly relevant CSV files:

#### `australian_politicians-all.csv`

One row per unique politician (both houses). Key fields:

| Field | Notes |
|---|---|
| `uniqueID` | Primary key; format e.g. `"Cook1943"` |
| `surname` | Surname |
| `firstName`, `commonName`, `displayName`, `allOtherNames` | Multiple name forms |
| `gender` | `"male"` or `"female"` — complete for all records |
| `birthDate`, `birthYear` | Some are year-only |
| `deathDate` | Where applicable |
| `member` | `1` if House member |
| `senator` | `1` if Senator |
| `wikidataID`, `wikipedia` | External links for supplementation |

#### `australian_politicians-senators-by_state.csv`

One row per senate term (a senator re-elected after leaving parliament gets a new row). Key fields:

| Field | Notes |
|---|---|
| `uniqueID` | Join key to `all.csv` |
| `senatorsState` | State/territory abbreviation (e.g. `TAS`, `WA`, `ACT`) |
| `senatorFrom` | Term start date (ISO format) |
| `senatorTo` | Term end date; `NA` = currently serving |
| `senatorEndReason` | `"Retired"`, `"Defeated"`, `"Resigned"`, `"Died"`, etc. |
| `sec15Sel` | `1` = appointed under s.15 of Constitution (not elected at general election) |
| `senatorComments` | Data quality notes |

#### `australian_politicians-all-by_party.csv`

One row per party affiliation spell (a senator who changed parties gets multiple rows). Key fields:

| Field | Notes |
|---|---|
| `uniqueID` | Join key |
| `partyAbbrev` | Party abbreviation |
| `partyName` | Full party name |
| `partyFrom` | Affiliation start date |
| `partyTo` | Affiliation end date; `NA` if current |
| `partyChangedName` | `1` = party renamed, not actual switch (e.g. Country → National) |
| `partySimplifiedName` | Collapsed categories |

**Coverage:** 1901 – 29 November 2021 (data frozen).

**Published paper:** Alexander, R. (2021). "AustralianPoliticians: Biographical Data about Australian Politicians." *Scientific Data*, 8:275.

**Limitations:**
- Frozen at November 2021 — approximately 10–15 senators elected or appointed since then are absent
- Some `partyTo` dates are missing when a party affiliation was never formally ended
- Some birth dates are year-only, not full date

---

### 2. OpenAustralia Parser — `data/senators.csv`

**URL:** https://github.com/openaustralia/openaustralia-parser/blob/master/data/senators.csv

**Status: Best source for post-2021 gap-filling; actively maintained through 2025.**

This is the live operational file used by OpenAustralia.org to parse Hansard. Updated as senators enter and leave parliament.

**Columns:**

| Field | Notes |
|---|---|
| `member count` | Internal record ID |
| `person count` | Links to `people.csv` |
| `name` | Full name |
| `Division` | Empty for senators |
| `State/Territory` | e.g. `"Tas."`, `"WA"`, `"Vic."` |
| `Date of election` | Format: `DD.MM.YYYY` |
| `Type of election` | `"General election"`, `"Casual vacancy"`, etc. |
| `Date ceased to be a Member` | End date; blank if current |
| `reason` | `"defeated"`, `"retired"`, `"resigned"`, `"died"` |
| `Most recent party` | Party at departure; does not track party changes |

**Coverage:** Spans from 1964 to September 2025 in the current file.

**Companion file `people.csv`:** adds `aph id` (Parliament House ID / PHID) and alternate names, but `gender` and `birthDate` are sparse (most rows blank).

**Limitations:**
- No `gender` column
- Only `Most recent party` — no party-change history
- Date format `DD.MM.YYYY` (requires parsing)
- State abbreviations are inconsistent (`"Tas."` vs `"TAS"`)
- PHIDs only in companion `people.csv`, not in `senators.csv` directly

---

### 3. AEC Tally Room — Senate election CSVs

**URL:** https://results.aec.gov.au/

**Status: Useful for cross-validation; not a substitute for longitudinal senator list.**

One CSV per election, available for 2004 onward from direct tally room URLs. 1998 and 2001 elections require downloading the AEC stats CD-ROM archive.

**Columns (2022 election example):**
`StateAb`, `GivenNm`, `Surname`, `PartyNm`, `PartyAb`, `ElectedOrder`

**Coverage:** Each file covers only senators elected at that election. Does not include mid-term appointments or senators who resigned between elections.

**No gender field. No term-end dates.**

Elections to cover 1998–present: 1998, 2001, 2004, 2007, 2010, 2013, 2016, 2019, 2022 (approximately 7–8 files).

---

### 4. APH website — senators search and biographical pages

**URL:** https://www.aph.gov.au/Senators_and_Members/Senators

- Shows only **currently serving** senators (76 people)
- No bulk download or CSV export
- No historical/former senators
- **APH has no public API** — all API endpoint attempts returned 404

**Not usable as a primary data source.** Useful only for manually verifying individual senator details.

---

### 5. Biographical Dictionary of the Australian Senate

**URL:** http://biography.senate.gov.au/

- Web-browsable biographies of 415 senators who completed service 1901–2002
- Fields per entry: name, state, party, years of service, offices held
- **No downloadable dataset** — browse-only
- Coverage ends 2002
- Four printed volumes published 2000–2017

**Not usable programmatically.** Useful as a manual cross-reference for early-period data quality checks.

---

### 6. EveryPolitician (mySociety)

**URL:** https://data.mysociety.org/datasets/everypolitician-australia-senate/

- Covers 35th Parliament (1987) through 45th Parliament (2016–2019)
- Available in Popolo JSON and per-term CSVs
- Fields include: name, party, **gender** (from gender-balance.org and Wikidata)
- **Last modified May 2019 — coverage stops at 2019**

**Useful as a cross-check for gender (1998–2019 period only), not current.**

---

### 7. OpenAustralia.org — `/senators/?f=csv`

**URL:** https://www.openaustralia.org.au/senators/?f=csv

- Direct CSV download of **current senators only** (76 people)
- Columns: `Person ID`, `Name`, `Party`, `URI`
- No state column, no term dates, no gender
- **Not useful for historical coverage.**

---

## Field coverage matrix

| Field | AustralianPoliticians | OA Parser senators.csv | AEC Tally Room | EveryPolitician |
|---|---|---|---|---|
| Full name | Yes | Yes | Yes (split) | Yes |
| State | Yes (abbrev) | Yes (inconsistent abbrev) | Yes | Yes |
| Party (current) | Yes (via join) | Most recent only | Yes | Yes |
| Party history | Yes (`allbyparty`) | No | No | Partial |
| Term start date | Yes (ISO) | Yes (`DD.MM.YYYY`) | At election only | Yes |
| Term end date | Yes (ISO) | Yes | No | Yes |
| End reason | Yes | Yes | No | No |
| Gender | Yes (complete) | No | No | Yes (to 2019) |
| Birth date | Yes (partial) | No | No | No |
| s.15 appointment flag | Yes | Via "Type of election" | No | No |
| APH unique ID (PHID) | Yes (via ausPH join) | Via companion `people.csv` | No | No |
| Coverage end | Nov 2021 | Sep 2025 | Per election | ~2019 |

---

## Recommended strategy

### Primary source (1998–November 2021)

Join three CSV files from the `RohanAlexander/australian_politicians` repository:

```python
# Load all three files
all_politicians = pd.read_csv(
    "https://raw.githubusercontent.com/RohanAlexander/australian_politicians/"
    "main/data/australian_politicians-all.csv"
)
senators_by_state = pd.read_csv(
    "https://raw.githubusercontent.com/RohanAlexander/australian_politicians/"
    "main/data/australian_politicians-senators-by_state.csv"
)
all_by_party = pd.read_csv(
    "https://raw.githubusercontent.com/RohanAlexander/australian_politicians/"
    "main/data/australian_politicians-all-by_party.csv"
)

# Filter to senators only
senators = all_politicians[all_politicians['senator'] == 1]

# Join term data
senators_with_terms = senators_by_state.merge(senators, on='uniqueID', how='left')
```

This gives: `uniqueID`, `surname`, `firstName`, `displayName`, `gender`, `birthDate`, `deathDate`, `senatorsState`, `senatorFrom`, `senatorTo`, `senatorEndReason`, `sec15Sel`

### Gap fill (November 2021–present)

Use the OpenAustralia Parser `senators.csv` to add senators who entered parliament after November 2021. For gender, source from Wikidata or APH biographical pages for the ~10–15 affected senators.

### PHID mapping

The `name.id` in the XML **is already the PHID** — it is not necessary to separately look up PHIDs to parse the XML. PHIDs are needed only to **join parsed data back to biographical records**. The OpenAustralia `people.csv` contains `aph id` (PHID) for most senators and can bridge the gap between XML `name.id` values and `uniqueID` values from the AustralianPoliticians dataset.

### Lookup tables to build

| File | Source | Key fields |
|---|---|---|
| `senator_lookup.csv` | AustralianPoliticians + OA parser | uniqueID, name_id (PHID), surname, firstName, displayName, gender, state, form1–form5 |
| `party_lookup.csv` | AustralianPoliticians `allbyparty` | uniqueID, name_id, partyAbbrev, partyName, partyFrom, partyTo |
| `state_lookup.csv` | AustralianPoliticians `senators-by_state` | uniqueID, name_id, state, senatorFrom, senatorTo |
| `president_lookup.csv` | Manually compiled | All President/Deputy/Acting President name variants 1998–present |
| `partyfacts_map.csv` | PartyFacts API + manual | partyAbbrev → partyfacts_id |

### Gaps requiring manual filling

| Gap | Scale | Approach |
|---|---|---|
| Senators elected/appointed after Nov 2021 | ~10–15 people | Use OA parser `senators.csv`; add gender from Wikidata/APH |
| Party history for post-2021 entrants | ~10–15 rows | APH biographical pages |
| President/Deputy President lookup table | ~15 individuals | Manual compilation from Senate records and APH biographical pages |
| PartyFacts IDs for Senate parties | ~15–20 party-era combinations | PartyFacts API + cross-match to existing `PartyFacts_map.csv` |

---

## State/territory abbreviation mapping

The AustralianPoliticians dataset uses abbreviations; the XML uses full names. A mapping is needed:

| Full name (XML) | Abbreviation (AustralianPoliticians) |
|---|---|
| `"New South Wales"` | `NSW` |
| `"Victoria"` | `Vic` |
| `"Queensland"` | `Qld` |
| `"South Australia"` | `SA` |
| `"Western Australia"` | `WA` |
| `"Tasmania"` | `Tas` |
| `"Australian Capital Territory"` | `ACT` |
| `"Northern Territory"` | `NT` |
