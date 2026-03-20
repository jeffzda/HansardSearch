# Hansard Project — Claude Instructions

## Git
- Branch: `main` — commit after every change, stage files by name (never `git add .`)
- Remote: https://github.com/jeffzda/HansardSearch.git
- Never commit: `webapp/.secret_key`, `webapp/users.db`, `webapp/activity_log.jsonl`, `data/raw/`, `data/output/`

## Project overview
Full-text search database of Australian Federal Parliament Hansard (Senate + House of Representatives), 1998–present. Python pipeline builds enriched Parquet/CSV corpora from APH XML; Flask webapp serves search over an SQLite FTS5 index.

## Project structure
- `pipeline/` — numbered ETL scripts (00–10); run in order per chamber
- `webapp/` — Flask app (`app.py`) + static HTML; deployed to 85.155.188.202 via rsync + `systemctl restart hansard`
- `data/lookup/` — authoritative CSV lookups (senator, member, party, state, session)
- `data/raw/` — source XML (git-ignored, large)
- `data/output/` — parsed/enriched Parquet + FTS DB (git-ignored, large)
- `case_studies/` — per-topic analysis notebooks/scripts
- `docs/` — methodology docs

## Common commands
```bash
# Pipeline (run from repo root)
python pipeline/00_download.py
python pipeline/10_build_fts_index.py

# Webapp (dev)
cd webapp && python app.py

# Deploy
rsync -av webapp/app.py webapp/static/ root@85.155.188.202:/opt/hansard/webapp/
ssh root@85.155.188.202 "systemctl restart hansard"
```

## Key conventions
- Pipeline scripts are numbered `NN_name.py` (Senate) / `NNb_name.py` (House) / `NNc_name.py` (committee)
- Senate columns: `state`, `senate_flag` — House columns: `electorate`, `fedchamb_flag`
- `user_id` in session/logs is the SQLite integer PK from `users.db`
- Full project context (corpus counts, validation results, known issues): `memory/MEMORY.md`
