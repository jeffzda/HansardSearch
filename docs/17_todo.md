# Hansard Database — To Do

Living task list. Check items off as they are completed. Add new items as they arise.
Last updated: 2026-03-11

---

## A. Immediate pipeline fixes (high priority)

- [ ] **Re-run fill → partyfacts → corpus for both chambers** to apply the Pass 2 date-range guard (code already in place in `04_fill_details.py` and `04b_fill_details_house.py`). Expected to resolve most T6/T7 failures (Senate ~144/229; House ~451/762).
  ```bash
  python3 04_fill_details.py --daily-dir ../data/output/senate/daily --lookup-dir ../data/lookup --out-dir ../data/output/senate/daily
  python3 06_add_partyfacts.py --daily-dir ../data/output/senate/daily --lookup-dir ../data/lookup
  python3 07_corpus.py --daily-dir ../data/output/senate/daily --out-dir ../data/output/senate/corpus --prefix senate_hansard_corpus
  # same sequence for house (04b, 06, 07 with --prefix house_hansard_corpus)
  python3 fix_in_gov.py  # re-apply in_gov recompute after corpus rebuild
  ```
- [ ] **Re-run T5/T6/T7 validation after re-fill** to confirm the date-range guard fix has resolved the bulk of failures and update the validation counts in README.md and `docs/00_session_init_prompt.md`.
- [ ] **Characterise the post-2011 ~16% word gap vs. Katz & Alexander.** Audit at the speech level to identify what content is absent (K&A likely use PDF supplement transcripts). Determine whether missingness is random or systematic by member/debate type.

---

## B. Data quality improvements

- [ ] **Fill remaining gender NULLs.** ~6.6% of Senate rows and ~13.4% of House rows have NULL gender. Missingness is concentrated in pre-2000 members. Check AustralianPoliticians dataset for any records missed during the Wikidata fill; manually look up remaining cases.
- [ ] **Investigate T5 garbled name_ids (78 Senate files).** Values like `XH4`, `/7E4` appear to be XML parsing artifacts. Inspect the affected raw XML files to determine the root cause; determine if a targeted fix is feasible.
- [ ] **Assemble House supplementary datasets into full corpus files.** House debate topics and division votes currently exist only as per-day parquets. Run `08b_debate_topics_house.py` and `09b_divisions_house.py` in corpus-assembly mode to produce single-file outputs matching the Senate equivalents.

---

## C. Data expansion — main chamber pre-1998

Analysis documented in `docs/17_todo.md` (this file) preamble and in the session analysis (2026-03-11). The GLAM Workbench (`wragge/hansard-xml`) has harvested both chambers 1901–2005; the 1998–2005 portion overlaps with the existing corpus.

### C1. Pre-1998 lowercase v2.x XML (1901–1980 House; some Senate)
- [ ] **Download GLAM Workbench harvest** for the pre-1998 portion and verify the 1998–2005 overlap files match the APH ParlInfo versions (spot-check ~10 dates).
- [ ] **Run existing parsers** (`03_parse.py` / `03b_parse_house.py`) against the pre-1998 lowercase files. The schema is identical — no parser changes expected.
- [ ] **Extend `senator_lookup.csv` and `member_lookup.csv`** to cover 1901–1997 politicians. AustralianPoliticians already has this data; the lookup-building scripts (`02_member_lookup.py`, `02b_member_lookup_house.py`) need to be run without the 1998-start date filter.
- [ ] **Extend `party_lookup.csv` and `partyfacts_map.csv`** to cover historical party abbreviations (pre-1998 Labor, Country Party, Democratic Labor, etc.).
- [ ] **Rebuild corpora** to include pre-1998 rows; update README coverage table and session init prompt.

### C2. Pre-1998 SGML/uppercase XML (1981–1997, both chambers)
- [ ] **Write SGML parser** — a new parser class (`03_parse_sgml.py` or a new branch in the existing parsers) that handles the uppercase SGML format. Key differences from current:
  - Metadata as element attributes (`DATE`, `NAMEID`, `PARTY`, `GOV`) not child elements
  - Date format `DD/MM/YYYY` → convert to ISO 8601
  - Time stamp as `(10.31)` text → parse to `HH:MM:SS` where possible
  - Division votes in CALS TABLE structure (not flat `<names>/<name>` list)
  - `first.speech` flag absent → NULL
  - Optional: capture `MINISTERIAL` attribute (0/1) as a new `ministerial_flag` column
- [ ] **Test SGML parser** against a sample of 1981–1997 files; verify row counts and field completeness.
- [ ] **Integrate SGML output** into the fill → validate → corpus pipeline.
- [ ] **Assess impact of absent `first.speech`** on research use cases; document limitation.

---

## D. Data expansion — committee Hansard

Pipeline built and tested (2026-03-11). All three scripts are in `pipeline/`. See `docs/18_committee_pipeline.md` for schema and usage.

**Dataset codes confirmed:** `commsen` (Senate), `commrep` (House), `commjnt` (Joint), `estimate` (Senate Estimates), `commbill` (Bills committees)

**XML variants confirmed:**
- Hearings (`commsen`, `commrep`, `commjnt`): `<discussion>` blocks with `<witness.group>` + HPS v2.2 body
- Estimates (`estimate`): `<chamber.xscript>/<debate>` outer wrapper; same HPS v2.2 body internally

**Tested on 2024-03-01:** 8 files / 3,500 rows; ~52% member PHID attribution; ~40% party/in_gov fill (gaps due to CHAIR roles without anchors and post-2021 PHIDs not yet in lookup tables)

- [x] **Identify APH ParlInfo dataset codes** — confirmed by querying ParlInfo (2026-03-11)
- [x] **Design schema extension** — 19 columns; `witness_flag`, `committee_name`, `committee_chamber`, `hearing_type`, `reference`, `portfolio`, `panel_no`
- [x] **Write `00b_download_committee.py`** — queries summary page per date×dataset; derives XML URL from PDF link; handles multiple documents per date
- [x] **Write `03c_parse_committee.py`** — handles both structural variants; HPS v2.2 body parsing; WitnessName / MemberContinuation / OfficeCommittee attribution
- [x] **Write `04c_fill_committee.py`** — PHID forward-fill within name groups; joins to senator_lookup + member_lookup; fills party/in_gov/gender/partyfacts_id for member rows
- [ ] **Run full historical download (1998–2025)** — estimated ~100,000 XML files; ~10–15 hours with 1.2s delay. Start with recent years (2010–2025) for immediate value. Commands in `docs/18_committee_pipeline.md`.
- [ ] **Parse and enrich full download** — run `03c_parse_committee.py` + `04c_fill_committee.py` on all downloaded files
- [ ] **Assemble committee corpus** — adapt `07_corpus.py` or write `07c_corpus_committee.py` to produce a single parquet corpus file
- [ ] **Improve PHID fill rate** — ~48% of member rows lack PHID lookup because CHAIR office roles don't always carry anchors; investigate whether committee XML committee membership lists can supplement this
- [ ] **Witness registry** — parse `HPS-StartWitness` intro lines (currently skipped) to extract name + organisation affiliation (format: `SURNAME, Title, Organisation`). Build a lookup table mapping witness name → organisation for each file. Add `organisation` column to the committee schema; join back to speech rows by `(file, name)`. Enables organisation-level analysis (e.g. frequency of NGO/industry appearances, government vs. non-government witnesses).

---

## E. Case studies and research outputs

- [x] **GRDC case study** — full pipeline (search → taxonomy → LLM enrichment → cited report). Completed 2026-03-11. Output: `case_studies/GRDC/report_final_cited.html` (72 KB, 53 cited sources, 208 enriched rows). Cost: $6.31.
- [x] **Climate change case study (unenriched)** — statistical report from 18,990 matches. Completed 2026-03-11. Output: `case_studies/climate_change/report.html`. Enriched version (~1,500-row stratified sample, ~$35) deferred.
- [x] **2025 word clusters** — bigrams, trigrams, 4-grams from both chambers, procedural tokens filtered. Completed 2026-03-11.
- [ ] **Climate change enriched version** — run stratified sample enrichment (~1,500 rows from 18,990, ~$35). Design taxonomy first.
- [x] **ARENA case study** — 85 exchanges extracted and enriched ($0.57). Output: `case_studies/ARENA_5/report_exchanges.html`. Key finding: 0% hostile/accountability questions answered; "alibi" pattern identified (ministers citing ARENA projects to deflect systemic questions). Completed 2026-03-11.
- [x] **WWF case study** — 60 exchanges extracted and enriched ($0.90). Output: `case_studies/WWF/report_exchanges.html`. Key findings: two-era structure (cooperative 1998–2007, adversarial 2008–2025); WWF attacked from both right and left; 8-year parliamentary gap 2014–2022; alibi pattern confirmed 2025. Completed 2026-03-11.

---

## F. Infrastructure and tooling

- [ ] **Rename `data/raw/reps/` to `data/raw/house/`** for consistency with all other paths. Requires updating `00_download.py` default path and any other hardcoded references; low risk but easy to defer.
- [ ] **Add `08b`/`09b` corpus-assembly mode** so House debate topics and divisions can be assembled into single-file outputs via CLI flag rather than requiring manual script modification.
- [ ] **Citation and DOI** — prepare Zenodo deposit for the corpus; update README citation section once a DOI is assigned.

---

## Reconciliation notes (reference)

Field compatibility for each expansion type — summary from the 2026-03-11 analysis:

| Data type | Schema compat. | New parser | Lookup extension | New columns | Estimated effort |
|---|---|---|---|---|---|
| Pre-1998 lowercase v2.x | ~100% | No | Yes (1901–1997 politicians) | None | Low |
| Pre-1998 SGML (1981–1997) | ~85% | Yes (~300 lines) | Yes (same) | Optional: `ministerial_flag` | Medium |
| Committee Hansard (1998–2025) | ~60% | Yes (significant) | Partial (witnesses unresolvable) | 4 new columns | High |
| Pre-1998 committee | ~50% | Yes | Partial | 4+ new columns | Very high |
