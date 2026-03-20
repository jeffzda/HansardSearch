# Hansard Project — Claude Instructions

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
- `pipeline/` — numbered ETL scripts (00–10); Senate = `NN_name.py`, House = `NNb_`, committee = `NNc_`
- `webapp/` — Flask app + static HTML; `app.py` is the entire backend
- `webapp/static/` — `index.html` (search UI), `help.html`, `about.html`, `login.html`
- `data/lookup/` — authoritative CSVs: senator/member/party/state/session lookups
- `data/raw/` — source XML by chamber (git-ignored, large)
- `data/output/` — parsed Parquet, enriched daily files, FTS DB (git-ignored, large)
- `case_studies/` — per-topic analysis scripts and outputs
- `docs/` — methodology docs
- `memory/` — Claude auto-memory files; `MEMORY.md` is the index

## Common commands
```bash
# Pipeline — run from repo root, in order
python pipeline/00_download.py
python pipeline/10_build_fts_index.py

# Webapp — local dev
cd webapp && python app.py          # http://localhost:5000

# Deploy to production server
rsync -av webapp/app.py webapp/static/ root@85.155.188.202:/opt/hansard/webapp/static/
rsync -av webapp/app.py root@85.155.188.202:/opt/hansard/webapp/app.py
ssh root@85.155.188.202 "systemctl restart hansard"
```

## Key conventions
- Pipeline scripts numbered `NN_` (Senate), `NNb_` (House), `NNc_` (committee) — run in order
- Senate schema uses `state` + `senate_flag`; House uses `electorate` + `fedchamb_flag`
- `user_id` in session/logs is the SQLite integer PK from `users.db`, not an email
- `_log()` in app.py appends JSON lines to `activity_log.jsonl`; always use it for events
- Never use `kill -HUP` on gunicorn — the server has no swap and will OOM; always `systemctl restart hansard`
- Stage files by name when committing — never `git add .`

## Sensitive / excluded files
- `webapp/.secret_key` — Flask secret
- `webapp/users.db` — user accounts
- `webapp/activity_log.jsonl` — usage log
- `data/raw/` — all source XML (large)
- `data/output/` — all generated Parquet/CSV/DB files (large)

## Git
- Branch: `main`
- Remote: https://github.com/jeffzda/HansardSearch.git
- Commit after every change with a descriptive message; stage specific files by name

## Other
- Full project context (corpus counts, validation results, pipeline status, known issues): `memory/MEMORY.md` — read this before starting any pipeline or data work
- Production server: `root@85.155.188.202` — domain hansardsearch.com.au, **7.8GB RAM, NO swap**
  - App: `/opt/hansard/webapp/` | Venv: `/opt/hansard/venv/` | FTS DB: `/opt/hansard/data/output/fts/`
  - nginx → gunicorn on 127.0.0.1:5000 (1 worker, 4 threads, systemd `hansard.service`)
- `data/raw/reps/` contains House XML (named `reps` for historical reasons, not `house/`)
