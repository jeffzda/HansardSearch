# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview
Full-text search database of Australian Federal Parliament Hansard (Senate + House of Representatives), 1998–present. A numbered Python ETL pipeline ingests APH XML transcripts, enriches them with speaker metadata, and outputs Parquet/CSV corpora. A Flask webapp serves boolean/phrase search over a SQLite FTS5 index.

## Tech stack
- **Language:** Python 3.12
- **Pipeline:** pandas, pyarrow, lxml, httpx, requests, beautifulsoup4, tqdm, regex, openpyxl
- **Webapp:** Flask, pandas, pyarrow (served by gunicorn, 1 worker 4 threads)
- **Search index:** SQLite FTS5
- **Frontend:** vanilla JS + Bootstrap 5.3 + Chart.js 4.4 + D3 v7
- **Server:** Ubuntu, nginx reverse proxy → gunicorn on 127.0.0.1:5000

## Project structure
- `pipeline/` — numbered ETL scripts (00–10) + analysis helpers
- `webapp/` — Flask app + static HTML; `app.py` is the entire backend
- `webapp/static/` — `index.html` (search UI), `help.html`, `about.html`
- `data/lookup/` — authoritative CSVs: senator/member/party/state/session lookups
- `data/raw/` — source XML by chamber (git-ignored, large)
- `data/output/` — parsed Parquet, enriched daily files, FTS DB (git-ignored, large)
- `case_studies/` — per-topic analysis scripts and outputs
- `docs/` — methodology docs

## Common commands
```bash
# Full pipeline — run from repo root in this order
python pipeline/00_download.py              # Senate XML
python pipeline/00b_download_committee.py   # Committee XML
python pipeline/01_session_info.py          # Session metadata
python pipeline/02_member_lookup.py         # Senator lookup
python pipeline/02b_member_lookup_house.py  # Member lookup
python pipeline/03_parse.py                 # Parse Senate XML → daily Parquet
python pipeline/03b_parse_house.py          # Parse House XML → daily Parquet
python pipeline/03c_parse_committee.py      # Parse committee XML
python pipeline/04_fill_details.py          # Enrich Senate daily files
python pipeline/04b_fill_details_house.py   # Enrich House daily files
python pipeline/05_validate.py              # Validate Senate enrichment
python pipeline/05b_validate_house.py       # Validate House enrichment
python pipeline/06_add_partyfacts.py        # Add party facts metadata
python pipeline/07_corpus.py               # Assemble Senate corpus Parquet
python pipeline/07c_corpus_committee.py    # Assemble committee corpus
python pipeline/10_build_fts_index.py      # Build SQLite FTS5 DB (~4-6 GB)

# Incremental update (for production — downloads recent days and rebuilds if new)
python pipeline/update_hansard.py
python pipeline/update_hansard.py --dry-run

# Webapp — local dev
cd webapp && python app.py          # http://localhost:5000

# User management (webapp/users.db)
python webapp/manage_users.py add email@example.com --days 7 --label "Description"
python webapp/manage_users.py list
python webapp/manage_users.py extend email@example.com --days 7

# Case study workflow
python pipeline/search_corpus.py --name TOPIC "'term1' | 'term2'"
python pipeline/enrich_case_study.py --name TOPIC
python pipeline/analyse_case_study.py --name TOPIC

# Newsletter pipeline (auto-detects last sitting week)
python pipeline/newsletter.py                    # auto week, 8 phrases/chamber, citations on
python pipeline/newsletter.py --week 2026-W10   # explicit week
python pipeline/newsletter.py --dry-run          # no API calls, placeholder narratives
python pipeline/newsletter.py --phrases 5 --min-count 3 --no-citations

# Deploy to production server — do NOT deploy unless explicitly asked
rsync -av webapp/app.py root@85.155.188.202:/opt/hansard/webapp/app.py
rsync -av webapp/static/ root@85.155.188.202:/opt/hansard/webapp/static/
ssh root@85.155.188.202 "systemctl restart hansard"
```

## Architecture

### ETL pipeline
- Scripts numbered `NN_` (Senate), `NNb_` (House), `NNc_` (committee) — run in order
- `03_parse.py` handles three Hansard XML schema versions (v2.0 1998–2003, v2.1 2004–2013, v2.2 2014–present)
- Daily Parquet files land in `data/output/{senate,house,committee}/daily/`; `07_corpus.py` concatenates them into a single corpus Parquet
- `10_build_fts_index.py` reads both corpus Parquets and writes `data/output/fts/hansard_fts.db` — a SQLite `speeches` table + `speeches_fts` FTS5 content table

### Webapp / search
- `webapp/app.py` **imports search logic directly from `pipeline/search_corpus.py`** (`parse_expression`, `collect_terms`, `_build_masks`, `_eval_tree`, `_apply_filters`, `_ast_to_fts5`). Edits to `search_corpus.py` affect both the webapp and CLI searches.
- `app.py` keeps a single persistent `sqlite3.Connection` (`_FTS_CONN`) to the FTS DB; `gunicorn.conf.py` re-opens it per worker via a `post_fork` hook (SQLite connections cannot be shared across fork)
- In-memory search result cache: MD5-keyed dict, 5-minute TTL, max 500 entries; pruned by a daemon thread
- `_log()` appends JSON lines to `webapp/activity_log.jsonl` — use for all significant events

### Schema
- Senate: `state` + `senate_flag`; House: `electorate` + `fedchamb_flag`
- Presiding officers stored as `name_id=10000`; resolved to real persons at query time via `president_lookup.csv` / `speaker_lookup.csv` using date-ranged pattern matching

### Newsletter pipeline
- `pipeline/newsletter.py` — weekly digest: finds last sitting week via `data/lookup/session_info_all.csv`, extracts trending bigrams/trigrams by TF-IDF novelty, searches full corpus for history, calls Claude for narrative, produces self-contained HTML
- Output: `newsletters/YYYY-WNN_issue-N/newsletter.html` (+ phrase CSVs, run log)
- `newsletters/manifest.json` tracks used phrases per sitting week — re-running against the same week produces `issue-2` with fresh phrases
- Corpus memory management: three sequential loads per chamber (week slice → body-only for TF-IDF baseline → full for history); `del` between loads prevents OOM on the 7.8GB/no-swap server

### Case studies
- `pipeline/search_corpus.py` produces `case_studies/<NAME>/matches.csv` and `exchanges.csv`
- `enrich_case_study.py` enriches matches; `enrich_exchanges.py` enriches Q&A pairs
- `analyse_case_study.py` / `analyse_combined.py` produce reports; `case_studies/logic.md` maps folder names → search expressions

## Key conventions
- Never use `kill -HUP` on gunicorn — the server has no swap and will OOM; always `systemctl restart hansard`
- Stage files by name when committing — never `git add .`
- `data/raw/reps/` contains House XML (named `reps` for historical reasons, not `house/`)
- **Do not add caps, limits, or truncations to data passed to Claude API calls unless explicitly asked.** The model context windows are large and the user prioritises output quality over token cost. Never silently truncate speech turns, snippets, or any other content.

## Sensitive / excluded files
- `webapp/activity_log.jsonl` — usage log
- `webapp/users.db` — user accounts
- `data/raw/` — all source XML (large)
- `data/output/` — all generated Parquet/CSV/DB files (large)
- `newsletters/` — generated HTML newsletters (not committed; large embedded charts)

## Git
- Branch: `main`
- Remote: https://github.com/jeffzda/HansardSearch.git
- Commit after every change with a descriptive message; stage specific files by name

## Development workflow
- **Work on local server only** (`http://localhost:5000`) unless explicitly asked to deploy
- Test all changes locally before any production deploy

## Production server
- `root@85.155.188.202` — domain hansardsearch.com.au, **7.8GB RAM, NO swap**
- App: `/opt/hansard/webapp/` | Venv: `/opt/hansard/venv/` | FTS DB: `/opt/hansard/data/output/fts/`
- nginx → gunicorn on 127.0.0.1:5000 (1 worker, 4 threads, systemd `hansard.service`)
- Update log: `/var/log/hansard_update.log`