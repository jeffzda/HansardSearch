# Australian Hansard Database — Project Overview

## Goal

Build a rectangular database of Australian parliamentary proceedings for both the Senate and the House of Representatives, covering 1998 to 2025. The project translates and extends the Katz and Alexander (2023) House of Representatives Hansard database into Python, adds Senate coverage, and produces aligned corpora for both chambers.

- **Coverage:** 1998-03-02 to 2025-11-27 (both chambers)
- **Output formats:** CSV and Parquet (one file per sitting day + one full corpus file per chamber)
- **Implementation language:** Python
- **Intended use:** Published as a companion dataset contribution

## Current status

Both chamber corpora are complete. Supplementary datasets for Senate are complete; House supplementary datasets are in progress.

| Dataset | Status | Rows | Sitting days | Output files |
|---|---|---|---|---|
| Senate corpus | Complete | 549,864 | 1,482 | `senate_hansard_corpus_1998_to_2025.parquet/.csv` |
| House corpus | Complete | 623,906 | 1,722 | `house_hansard_corpus_1998_to_2025.parquet/.csv` |
| Senate debate topics | Complete | 43,066 | 1,482 | `senate_debate_topics.parquet/.csv` |
| Senate divisions | Complete | 8,001 divisions; 466,557 vote records | — | `senate_divisions.parquet/.csv`, `senate_division_votes.parquet/.csv` |
| House debate topics | Complete | 388,154 | 1,722 | `house_debate_topics_1998_to_2025.parquet/.csv` |
| House divisions | Complete | 4,616 divisions; 614,057 vote records | — | `house_divisions.parquet/.csv`, `house_division_votes.parquet/.csv` |

## Reference datasets

| Resource | URL |
|---|---|
| Katz & Alexander GitHub repo | https://github.com/lindsaykatz/hansard-proj |
| Katz & Alexander Zenodo dataset | https://zenodo.org/records/8121950 |
| GLAM Workbench Senate XML harvest | https://github.com/wragge/hansard-xml |
| AustralianPoliticians data (Alexander) | https://github.com/RohanAlexander/australian_politicians |
| OpenAustralia parser data | https://github.com/openaustralia/openaustralia-parser |

## Documentation index

| File | Contents |
|---|---|
| `01_katz_alexander_workflow.md` | Full description of the R pipeline that was used as a reference |
| `02_senate_xml_structure.md` | Senate XML format reference (all eras) |
| `03_house_xml_structure.md` | House XML format reference (for comparison) |
| `04_senate_vs_house_differences.md` | Complete structural diff between Senate and House XML |
| `05_senate_adaptations.md` | Adaptations to the parsing logic for the Senate — all implemented |
| `06_member_data_sources.md` | Senator biographical data sources and strategy used |
| `07_r_to_python_translation.md` | R to Python translation decisions (reference) |
| `08_build_plan.md` | Implementation record — what was built and how |
| `09_schema_v22_changes.md` | Schema version 2.2 changes (2014–present), both chambers |
| `10_pipeline_status.md` | Quick-reference: completed outputs, in-progress work, CLI commands |
| `11_pipeline_execution.md` | Step-by-step execution record; full reproduction guide |
| `12_data_dictionary.md` | Column-level reference for all output datasets |
| `13_corpus_quality_assessment.md` | Comparative quality assessment vs Katz & Alexander (2023) |
| `14_session_summary.md` | March 2026 session work — validation fixes, enrichment, supplementary corpora |
| `15_search_corpus.md` | Boolean search tool (`search_corpus.py`) documentation |
| `16_case_study_pipeline.md` | Case study pipeline: search → taxonomy → enrich → analyse |
