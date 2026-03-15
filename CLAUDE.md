# Hansard Project — Claude Instructions

## Git

This project is a git repository (`main` branch). After making any changes to
files, commit them with a descriptive message before ending the session. Stage
specific files by name rather than `git add .` to avoid accidentally including
generated or sensitive files.

- Remote: not yet configured (local only)
- Sensitive files excluded from tracking: `webapp/.secret_key`, `webapp/users.db`, `webapp/activity_log.jsonl`
- Large data directories excluded: `data/raw/`, `data/output/`

## Project overview

Dual-chamber Hansard database (Senate + House of Representatives), 1981–present.
See `memory/MEMORY.md` for full project context.

## Key conventions

- Python pipeline scripts live in `pipeline/`
- Webapp (Flask) lives in `webapp/`
- Case studies live in `case_studies/`
- Lookup CSVs live in `data/lookup/`
- Docs live in `docs/`
