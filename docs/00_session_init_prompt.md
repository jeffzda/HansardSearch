# Session Initialization Prompt — Hansard Pipeline Project

Use this prompt at the start of a new session to restore full context.

---

## Paste this at the start of a new session:

---

I'm continuing work on the Australian Parliamentary Hansard database pipeline. Here is the full context.

**Project:** Build a dual-chamber (Senate + House of Representatives) Hansard speech corpus, 1998–present, in CSV and Parquet format. Output is one file per sitting day (daily enriched) plus one full corpus file per chamber.

**Working directory:** `/home/jeffzda/Hansard/`
**Pipeline scripts:** `pipeline/` (Python, scripts 00–10)
**Data:** `data/raw/` (XML), `data/lookup/` (CSVs), `data/output/` (parquet + CSV)
**Docs:** `docs/` — read `docs/10_pipeline_status.md` for current state and `docs/14_session_summary.md` for the most recent session's work

---

### Current corpus state

| Chamber | Rows | Sitting days | Date range |
|---|---|---|---|
| Senate | 549,864 | 1,482 | 1998-03-02 to 2025-11-27 |
| House | 623,906 | 1,722 | 1998-03-02 to 2025-11-27 |

### Validation results (current)

| Chamber | Clean | Total | % clean |
|---|---|---|---|
| Senate | 1,131 | 1,482 | 76.3% |
| House | 1,545 | 1,722 | 89.7% |

Error counts: T1 Senate 1 / House 2 | T2 Senate 24 / House 25 | T3 Senate 87 / House 130 | T4 Senate 18 / House 33 | T5 Senate 78 / House 0 | T6 Senate 144 / House 451* | T7 Senate 229 / House 762* (* before date-range guard re-fill)

### Metadata completeness

| Field | Senate | House |
|---|---|---|
| name_id | 98.4% | 98.1% |
| gender | 93.4% | 86.6% |
| party | 98.6% | 86.7% |
| state/electorate | 94.8% | 87.3% |
| partyfacts_id | 95.5% | 84.7% |

---

### Key pipeline scripts

| Script | Purpose |
|---|---|
| `00_download.py` | Download XML from APH website |
| `01_session_info.py` | Extract session headers → `session_info_all.csv` |
| `02_member_lookup.py` | Build Senate lookups (senator_lookup, party_lookup, state_lookup) |
| `02b_member_lookup_house.py` | Build House lookups (member_lookup, party_lookup_house, electorate_lookup, speaker_lookup); includes Wikidata gender fill and electorate corrections |
| `03_parse.py` / `03b_parse_house.py` | Parse XML → daily parquets in `daily_raw/` |
| `04_fill_details.py` / `04b_fill_details_house.py` | Enrich daily files → `daily/`; 7 passes (Senate) / 5 passes (House) including date-range party, electorate fill, Pass 2 date-range guard, page_no forward-fill |
| `05_validate.py` / `05b_validate_house.py` | Run T1–T7 validation tests |
| `06_add_partyfacts.py` | Join PartyFacts IDs |
| `07_corpus.py` | Assemble full corpus (use `--prefix house_hansard_corpus` for House) |
| `08b_debate_topics_house.py` | Extract debate topics |
| `09b_divisions_house.py` | Extract division votes |
| `10_compare_ka.py` | Compare our corpus against Katz & Alexander (2023) |
| `00b_download_committee.py` | Download committee Hansard XML (datasets: commsen, commrep, commjnt, estimate, commbill) |
| `03c_parse_committee.py` | Parse committee XML → per-file parquet (data/output/committee/daily_raw/) |
| `04c_fill_committee.py` | Enrich committee parquets with member metadata |

### Key lookup files

| File | Description |
|---|---|
| `data/lookup/senator_lookup.csv` | 666 senators with name forms, gender, unique_id |
| `data/lookup/member_lookup.csv` | 1,280 House members |
| `data/lookup/party_lookup.csv` | Senate party with date ranges (789 rows) |
| `data/lookup/party_lookup_house.csv` | House party with date ranges (1,571 rows) |
| `data/lookup/electorate_lookup.csv` | House electorates with date ranges (1,431 rows; includes Wooldridge correction) |
| `data/lookup/session_info_all.csv` | 3,204 rows (1,482 Senate + 1,722 House sitting days) |
| `data/lookup/partyfacts_map.csv` | 27-row mapping to cross-national PartyFacts IDs |

### Important implementation details

- **Inline interjection rows** (post-2011 v2.2 XML): rows where `name.endswith(":")` are embedded attributions inheriting the current speaker's party/electorate — excluded from T4 validation and overwritten in fill step
- **Electorate fill**: `04b_fill_details_house.py` always overwrites electorate from the date-range-filtered `electorate_lookup.csv` (not just fills NULLs) to ensure canonical values
- **Party fill**: fills NULLs only; date-range-aware using `party_from`/`party_to` columns
- **T2 exclusion**: both validators use `_T2_PROCEDURAL_RE` regex to exclude legitimate procedural repetitions (interjections, bill reading formulas, etc.)
- **Parallel processing**: all heavy scripts use `parallel_utils.py` (`dynamic_map` for CPU-bound, `threaded_map` for I/O-bound); use `--sequential` flag for debugging
- **After re-running 04b**: always re-run `06_add_partyfacts.py` before rebuilding corpus (partyfacts_id is added post-fill)

### Comparison with Katz & Alexander (2023)

K&A (Zenodo 8121950) is stored at `data/ka_corpus/hansard-corpus/hansard_corpus_1998_to_2022.parquet`. Our House corpus (623,906 rows) exceeds K&A (586,830 rows) due to additional sitting days (1998–2025) and our inclusion of stage direction / petition rows. Pre-2011 word count ratio is 0.986 (parity); post-2011 ratio is 0.840 (K&A likely uses PDF supplements). We lead on 6/8 metadata fields by 15–21pp; K&A leads on page_no. `in_gov` was recomputed via `fix_in_gov.py` (applied 2026-03-11) to correct unreliable post-2012 XML values.

### Pending work

Full task list with status tracking: **`docs/17_todo.md`** — update it as items are completed.

1. **Re-run fill → partyfacts → corpus for both chambers** to apply Pass 2 date-range guard (now in code); expected to substantially reduce T6/T7 errors. Commands:
   ```bash
   python3 04_fill_details.py --daily-dir ../data/output/senate/daily --lookup-dir ../data/lookup --out-dir ../data/output/senate/daily
   python3 06_add_partyfacts.py --daily-dir ../data/output/senate/daily --lookup-dir ../data/lookup
   python3 07_corpus.py --daily-dir ../data/output/senate/daily --out-dir ../data/output/senate/corpus --prefix senate_hansard_corpus
   # Same sequence for house
   ```
2. **Senate gender completeness** (93.4%) — some older senators may still have NULL gender

---

### Known data quality issues (research readiness)

The corpus is suitable for aggregate and exploratory analysis but has the following issues that should be resolved before publication-grade use. They are listed in priority order.

**1. Pass 2 false person attribution — SYSTEMATIC, HIGH PRIORITY**
Name-form matching in both fill scripts (Pass 2) had no date-range guard. This is now fixed: `_build_term_date_index()` and `_term_covers_date()` were added to both `04_fill_details.py` and `04b_fill_details_house.py`. The fix rejects a name-form match if no term record covers the sitting date. **The code is in place but daily files have not yet been regenerated.** T6/T7 counts (Senate 144/229; House 451/762) reflect the pre-fix state.
- **Impact**: individual-level analysis using `unique_id` is at risk until re-fill is run.
- **Fix status**: code in place; re-run `04_fill_details.py` (and `04b`) → `06_add_partyfacts.py` → `07_corpus.py` to apply.

**2. Post-2011 word gap (~16%) — UNEXPLAINED**
Our word count is ~84% of K&A's from 2011 onward. The most likely cause is K&A supplementing with PDF transcripts. The content of `<subdebate.text>` elements was audited and confirmed to be procedural-only (bill titles, stage directions) — not the source of the gap. However, the exact speeches or content that are missing have not been characterised.
- **Impact**: text-based analysis (word counts, topic models, speech length) will underestimate post-2011 output by ~16%. The missingness may not be random — if K&A's PDF source captures interjections or procedural exchanges that the XML omits, certain member types or debate formats could be systematically under-represented.
- **Fix needed**: compare at the speech level between our corpus and K&A to identify what type of content is absent.

**3. T5: 78 Senate files with garbled name_ids**
XML parsing artifacts produce `name_id` values (e.g., `XH4`, `/7E4`) not present in `senator_lookup.csv`. Rows with these IDs have no valid person attribution.
- **Impact**: small (78/1,482 = 5% of files) but the affected rows within those files are silently unattributed.
- **Fix**: likely requires inspection of the raw XML for the affected dates; no automated fix currently planned.

**4. House T5/T6/T7 now run — results pending re-fill**
`05b_validate_house.py` was extended to T1–T7 (March 2026). Initial results: T5=0 (no garbled name_ids), T6=451, T7=762. The T6/T7 failures are Pass 2 false matches — the date-range guard fix is in code; re-fill will resolve most of these.
- **Impact**: current 89.7% House clean rate does not yet reflect the T6/T7 improvement that will come with re-fill.

**5. Gender NULLs skew historical**
13.4% Senate and 13.4% House gender values are NULL. Missingness is concentrated in pre-2000 members with limited Wikidata coverage.
- **Impact**: gender-based analysis must either impute or drop affected rows; the non-random pattern (older, historical members) can bias results.
- **Fix needed**: manual lookup for remaining NULLs, or alternative sources (e.g., AustralianPoliticians.org gender field).

**Summary table**

| Issue | Scope | Research impact | Fix status |
|---|---|---|---|
| Pass 2 false attribution | T6: 144/229 Senate; 451/762 House | HIGH — individual-level analysis | Fix in code; re-fill pending |
| Post-2011 word gap | ~16% of words 2011–2021 | MEDIUM — text/volume analysis | Unexplained; requires speech-level audit |
| Garbled name_ids (T5) | 78 Senate files | LOW — small fraction | No fix planned |
| House T5/T6/T7 | T6=451, T7=762 (pre-fill) | Resolves after re-fill | Fix in code; re-fill pending |
| Gender NULLs | ~13% Senate; ~13% House | LOW–MEDIUM — historical skew | Partially fixed; ~13% remain |

---

Read `docs/10_pipeline_status.md` for the full run-command reference and `docs/14_session_summary.md` for detail on what was changed in the most recent session.
