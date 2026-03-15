# Case Study Pipeline — Search → Taxonomy → Enrich → Analyse

## Overview

The case study pipeline extracts all parliamentary mentions of an organisation
or topic from the Hansard corpus, annotates them with a bespoke taxonomy using
the Claude API, and produces a self-contained HTML report with statistical
charts and an LLM-authored analytical narrative.

The pipeline has two parallel tracks from a single search, each a standalone Python script:

```
search_corpus.py
     ↓
case_studies/<NAME>/
  matches.csv       ← non-exchange rows (direct matches not part of a Q&A pair)
  exchanges.csv     ← exchange rows (question + answer rows forming Q&A pairs)
  matches.xlsx      ← all rows (for human inspection)
  summary.txt

Row-level track (matches.csv):
  matches.csv  →  build_taxonomy.py  →  enrich_case_study.py  →  analyse_case_study.py
                   taxonomy_proposal.md   matches_enriched.csv       report.html

Exchange track (exchanges.csv):
  exchanges.csv  →  enrich_exchanges.py  →  analyse_exchanges.py
                     exchanges_enriched.csv   report_exchanges.html
```

All scripts live in `pipeline/`. All outputs live in `case_studies/<NAME>/`.

---

## Prerequisites

```bash
pip install anthropic pandas pyarrow matplotlib numpy openpyxl
export ANTHROPIC_API_KEY="sk-ant-..."
```

The corpus parquet files must exist:
- `data/output/senate/corpus/senate_hansard_corpus_1998_to_2025.parquet`
- `data/output/house/corpus/house_hansard_corpus_1998_to_2025.parquet`

A reference taxonomy for design guidance must exist:
- `ARENA_Taxonomy_v1.1.md` (included in the repo root)

---

## Stage 1 — Search (`search_corpus.py`)

Extracts all corpus rows whose body text matches a boolean expression.
Produces two output CSVs, `matches.xlsx`, and `summary.txt`.

**Full documentation:** `docs/15_search_corpus.md`

```bash
python search_corpus.py --name ARENA \
    "'Australian Renewable Energy Agency' | 'ARENA'"
```

**Alias selection:** Include the full name (zero false positives) and the
common acronym (some false positives tolerated; filtered at enrichment stage).
Avoid overly broad terms that would produce unmanageable false positive rates.

**Output files:**

| File | Contents | Consumed by |
|------|----------|-------------|
| `matches.csv` | Direct-match rows NOT part of any Q&A exchange | `enrich_case_study.py` |
| `exchanges.csv` | All rows that form Q&A pairs (question + answer rows) | `enrich_exchanges.py` |
| `matches.xlsx` | All rows combined (for human inspection) | — |
| `summary.txt` | Aggregate statistics across all rows | — |

Rows that are a direct search hit **and** part of a Q&A pair go into
`exchanges.csv` only (tagged `match_source="search"`). Non-Q&A direct hits go
into `matches.csv`. This prevents double-processing when both pipelines are run.

---

## Stage 2 — Taxonomy (`build_taxonomy.py`)

Sends a stratified sample of matched rows to Claude Opus along with a
structural template (and optionally a reference taxonomy), and asks it to
design a bespoke annotation schema for the case study subject.

```bash
# Recommended: with template + org-function context
python build_taxonomy.py \
    --matches            ../case_studies/GRDC/matches.csv \
    --template           taxonomy_template.md \
    --out                ../case_studies/GRDC/taxonomy_proposal.md \
    --subject            GRDC \
    --subject-full-name  "Grains Research and Development Corporation (GRDC)" \
    --org-description    "a statutory R&D corporation funded by matching grower levies \
        and government contributions, investing in grains research and extension \
        across Australia"

# Legacy: with reference taxonomy only
python build_taxonomy.py \
    --matches   ../case_studies/ARENA/matches.csv \
    --reference ../ARENA_Taxonomy_v1.1.md \
    --out       ../case_studies/ARENA/taxonomy_proposal.md \
    --subject   ARENA \
    --subject-full-name "Australian Renewable Energy Agency"
```

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--template` | *(none)* | Path to `taxonomy_template.md`. When provided, Claude follows its fixed field structure (fields 1–7 universal) and replaces `[ORG-SPECIFIC]` slots with content derived from the sample rows and org function. **Recommended for all new case studies.** Takes structural precedence over `--reference`. |
| `--reference` | *(none)* | Path to a reference taxonomy (legacy mode). Design philosophy is borrowed but all fields are replaced. If `--template` is also supplied, the reference is included as secondary context only. At least one of `--template` or `--reference` must be provided. |
| `--subject` | `WWF` | Short name used in the prompt |
| `--subject-full-name` | `World Wildlife Fund / WWF` | Full name used in the prompt |
| `--org-description` | *(none)* | One or two sentences describing the organisation's institutional function and role. When provided, Claude first derives 4–6 analytical questions appropriate to that function (e.g. levy governance and research priorities for a statutory R&D body; campaign pressure and party alignment for an advocacy NGO) before designing the taxonomy. Omitting this flag falls back to a generic advocacy-NGO question set. |
| `--model` | `claude-opus-4-6` | Model for taxonomy design |
| `--seed` | `42` | Random seed for stratified sampling |

**Sampling:** 6 rows per 5-year band (1998–2002, 2003–2007, 2008–2012,
2013–2017, 2018–2025), spoken rows only (`q_in_writing=0`). Approximately
30 rows total.

**Review the output.** When using `--template`, the generated
`taxonomy_proposal.md` should have consistent universal fields (1–7) and
org-specific fields (8–10) drawn from the template's `[ORG-SPECIFIC]` slots.
Review before proceeding to Stage 3:
- Verify the controlled vocabulary values are appropriate for the subject
- Check that `{subject}_framing` values reflect how *this* org type is discussed
- Confirm the two example records use actual rows from the sample

**Cost:** ~$0.25 (Opus, ~10K input tokens, ~5K output tokens).

---

## Stage 3 — Enrichment (`enrich_case_study.py`)

Annotates every row in `matches.csv` (non-exchange rows only) with taxonomy
fields using the Claude API. Runs 10 parallel workers by default. Writes a JSONL checkpoint after
each row, enabling resume after interruption.

```bash
python enrich_case_study.py \
    --matches   ../case_studies/ARENA/matches.csv \
    --taxonomy  ../case_studies/ARENA/taxonomy_proposal.md \
    --out       ../case_studies/ARENA/matches_enriched.csv \
    --no-cache \
    --workers   10 \
    --fields    "what_happened,policy_domain,arena_framing,rhetorical_function,\
mention_valence,mention_centrality,speech_act_type,target_of_criticism,\
arena_claim_type,debate_topic_normalised"
```

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--fields` | WWF field set | Comma-separated core taxonomy field names (no `enrich_` prefix). Must match the fields defined in `taxonomy_proposal.md`. |
| `--workers` | `10` | Parallel API calls |
| `--no-cache` | off | Disable prompt caching. Use when full body text is needed for `what_happened` quality. |
| `--resume` | off | Skip rows already in the JSONL checkpoint |
| `--dry-run` | off | Process first 5 rows only (no cost estimate prompt) |
| `--body-limit` | `100000` | Max chars of body text per row |
| `--context-limit` | `100000` | Max chars of each context field per row |
| `--model` | `claude-sonnet-4-6` | Model for annotation |

**The `--fields` argument** must list all core taxonomy field names exactly
as they appear in `taxonomy_proposal.md`, excluding `enrich_confidence_note`
and `enrich_error` (those are added automatically). The script builds the
JSON output template from this list, so Claude will return exactly these keys.

**Caching:** With caching enabled the taxonomy system prompt is cached after
the first call, reducing cost for subsequent rows. With `--no-cache` every
call sends the full taxonomy, which is more expensive but ensures the model
attends to the full body text for `what_happened`. For a subject where
narrative quality matters, use `--no-cache`.

**Resume:** If the run is interrupted, re-run with `--resume`. The script
reads the JSONL checkpoint (same path as `--out` but `.jsonl` extension)
and skips already-annotated rows.

**False positives:** The enrichment LLM will correctly flag rows where the
matched term refers to something other than the target subject (e.g. ARENA
as a common noun pre-2012, or WWF as "Waterside Workers Federation"). These
rows will have an empty `arena_framing` / `wwf_framing` field and a
`what_happened` that explains the mismatch. They are filtered out
automatically in Stage 4.

**Cost estimates:**

| Subject | Rows | Workers | Mode | Cost |
|---------|------|---------|------|------|
| WWF | 383 | 10 | no-cache | $18.54 |
| ARENA | 1,075 | 10 | no-cache | $49.35 |

Rule of thumb: ~$0.05 per row with Sonnet 4.6, no caching, full body text.

**Output:**
- `matches_enriched.csv` — original columns + taxonomy columns
- `matches_enriched.jsonl` — per-row checkpoint

---

## Stage 4 — Analysis (`analyse_case_study.py`)

Generates statistical charts and an LLM-authored narrative, assembles them
into a self-contained HTML report.

```bash
python analyse_case_study.py \
    --enriched ../case_studies/ARENA/matches_enriched.csv \
    --out      ../case_studies/ARENA/report.html \
    --subject  ARENA
```

**Key flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--subject` | `WWF` | Selects the subject configuration (column names, colour palette, framing vocabulary, false positive filter) |
| `--grounded` | off | Use a strictly data-grounded narrative prompt. Output to a separate file (e.g. `report_grounded.html`) to preserve the standard report. |
| `--narrative-topic` | none | Focus the narrative on a specific legislative episode (e.g. `"the Future Made in Australia Bill 2024"`). Rewrites all prompt sections for a topic-specific report. Use with a pre-filtered CSV. |
| `--model` | `claude-opus-4-6` | Model for narrative generation |

**Subject configurations** are defined in `main()`. Each configuration specifies:
- `framing_col` — the taxonomy column for organisation framing
- `valence_col` — the taxonomy column for speaker valence
- `framing_order` / `framing_colours` — chart display order and colours
- `negative_framings` — which framing values count as "negative" for stats
- `fp_pattern` — regex to identify false positive rows for exclusion
- `has_nameform` — whether to show the name-form evolution chart (WWF only)

**Charts produced:**

| Chart | Description |
|-------|-------------|
| A — Year × framing stacked bar | Mentions per year broken down by framing type |
| B — Framing × party horizontal bar | Framing distribution across party groups |
| C — Government vs opposition | Framing % for in-government vs in-opposition members |
| D — Rhetorical function bar | Distribution of rhetorical functions |
| E — Policy domain pie | Share of mentions by policy domain |
| F — Valence over time area | 5-year rolling mean of speaker valence |
| H — Secondary evolution | Name-form evolution (WWF) or speech act type over time (ARENA) |
| G — Top 20 speakers table | Speaker × party × gov status × dominant framing × dominant valence |

**False positive exclusion:** Rows with an empty framing field whose
`what_happened` matches the subject's `fp_pattern` are excluded before
analysis. The exclusion count is reported in the stats banner.

**Narrative modes:**

- **Standard** — Claude Opus draws on its training knowledge of Australian
  political history to contextualise the data patterns. Suitable for a
  substantive policy report.
- **Grounded** (`--grounded`) — Claude is instructed to derive all claims
  from the data table only, with no reference to external political context.
  Useful for a data-auditable version of the report.
- **Topic-focused** (`--narrative-topic`) — Rewrites the narrative prompt
  sections for a specific legislative episode. Use with a pre-filtered CSV
  containing only rows relevant to that episode.

**Cost:** ~$0.80–$1.60 (Opus, depending on corpus size).

**Output:** Single self-contained HTML file (~500–900 KB). All charts are
embedded as base64 PNG. No external dependencies.

---

## Topic sub-reports

To analyse a specific legislative episode within a case study:

1. Filter the enriched CSV to relevant rows:

```python
import pandas as pd, csv
csv.field_size_limit(10_000_000)
df = pd.read_csv('case_studies/ARENA/matches_enriched.csv')
mask = (
    df['debate_topic_normalised'].fillna('').str.contains('future made', case=False) |
    df['what_happened'].fillna('').str.contains('Future Made in Australia', case=False) |
    df['body'].fillna('').str.contains('Future Made in Australia', case=False)
)
df[mask].to_csv('case_studies/ARENA/matches_fmia.csv', index=False)
```

2. Run the analysis on the filtered subset with `--narrative-topic`:

```bash
python analyse_case_study.py \
    --enriched ../case_studies/ARENA/matches_fmia.csv \
    --out      ../case_studies/ARENA/report_fmia.html \
    --subject  ARENA \
    --narrative-topic "the Future Made in Australia Bill 2024 and associated legislation"
```

The topic flag rewrites all five narrative sections to focus on the
legislative episode rather than the full arc of the subject's parliamentary
history.

---

## Adding a new case study

1. **Search:** Run `search_corpus.py` with appropriate aliases.

2. **Fix `in_gov`:** The corpus `in_gov` column is derived from the XML
   attribute `in.gov`, which APH stopped populating from ~2012. Run
   `fix_in_gov.py` once to recompute from the known government timeline
   before running the search. (Already applied to both corpus files as of
   2026-03-11.)

3. **Build taxonomy:** Run `build_taxonomy.py` with `--subject` and
   `--subject-full-name`. Review and edit `taxonomy_proposal.md`:
   - Rename `<old_subject>_*` fields to `<new_subject>_*` throughout
   - Update the controlled vocabulary values for the new subject
   - Update the example records at the bottom

4. **Enrich:** Run `enrich_case_study.py` with `--fields` matching the
   taxonomy field names. Add a new subject configuration to
   `analyse_case_study.py` (see step 5).

5. **Add subject config:** In `analyse_case_study.py` `main()`, add an
   `elif args.subject == "NEWSUBJECT":` block with:
   - `framing_col` — name of the framing taxonomy field
   - `valence_col` — name of the valence taxonomy field
   - `framing_order` / `framing_colours` — display order and hex colours
   - `valence_order` / `valence_colours`
   - `negative_framings` — list of framing values that count as negative
   - `fp_pattern` — regex to identify false positive `what_happened` text
   - `has_nameform` — `False` unless the subject has name variants
   - `secondary_col` — column for the secondary evolution chart
   - Update `choices=["WWF", "ARENA", "NEWSUBJECT"]` in the `--subject` arg

6. **Analyse:** Run `analyse_case_study.py --subject NEWSUBJECT`.

---

## Maintenance notes

### `in_gov` corpus fix

The `in_gov` column in both corpus parquet files was broken from 2012 onward
because APH changed the XML format from `<in.gov>1</in.gov>` to the empty
self-closing form `<in.gov />`. The script `pipeline/fix_in_gov.py` recomputes
`in_gov` from the known Australian government timeline (Howard/Rudd/Gillard/
Abbott/Turnbull/Morrison/Albanese) and rewrites both corpus files. This was
applied on 2026-03-11.

If the corpus is regenerated from raw XML, `fix_in_gov.py` must be re-run
before case study searches to ensure correct government membership flags.

### Re-running after corpus updates

If the corpus parquet files are updated (new sitting days added), re-run
`search_corpus.py` to pick up new matches. The enrichment can be resumed from
the existing checkpoint with `--resume`, processing only the new rows.

### Case study registry

`case_studies/logic.md` records each case study folder, its search expression,
match count, and run date. Update this file after each search run.

---

## Complete worked examples

### WWF (World Wildlife Fund)

```bash
# 1. Search
python search_corpus.py --name WWF \
    "'WWF' | 'World Wildlife Fund' | 'World Wide Fund for Nature' | 'WWF Australia' | 'WWF-Australia'"

# 2. Taxonomy (review and edit output before proceeding)
python build_taxonomy.py \
    --matches   ../case_studies/WWF/matches.csv \
    --template  taxonomy_template.md \
    --out       ../case_studies/WWF/taxonomy_proposal.md \
    --subject   WWF \
    --subject-full-name "World Wildlife Fund / WWF (World Wide Fund for Nature)" \
    --org-description "an international conservation NGO that campaigns on biodiversity, \
        endangered species, and environmental threats; in Australia it focuses on marine \
        protection, land clearing, and climate policy advocacy"

# 3. Enrich (~$18, ~15 min at 10 workers)
python enrich_case_study.py \
    --matches   ../case_studies/WWF/matches.csv \
    --taxonomy  ../case_studies/WWF/taxonomy_proposal.md \
    --out       ../case_studies/WWF/matches_enriched.csv \
    --no-cache --workers 10 \
    --fields "what_happened,policy_domain,mention_centrality,wwf_framing,\
speaker_valence_toward_wwf,rhetorical_function,target_of_speech,\
policy_action_urged,wwf_name_form,other_organisations_mentioned"

# 4a. Full report
python analyse_case_study.py \
    --enriched ../case_studies/WWF/matches_enriched.csv \
    --out      ../case_studies/WWF/report.html

# 4b. Data-grounded report
python analyse_case_study.py \
    --enriched ../case_studies/WWF/matches_enriched.csv \
    --out      ../case_studies/WWF/report_grounded.html \
    --grounded
```

### ARENA (Australian Renewable Energy Agency)

```bash
# 1. Search
python search_corpus.py --name ARENA \
    "'Australian Renewable Energy Agency' | 'ARENA'"

# 2. Taxonomy (review and edit output before proceeding)
python build_taxonomy.py \
    --matches            ../case_studies/ARENA/matches.csv \
    --template           taxonomy_template.md \
    --out                ../case_studies/ARENA/taxonomy_proposal.md \
    --subject            ARENA \
    --subject-full-name  "Australian Renewable Energy Agency" \
    --org-description    "a Commonwealth statutory authority that co-funds applied \
        renewable energy R&D and deployment projects through competitive grants; \
        it operates at the intersection of industry, research and government policy"

# 3. Enrich (~$49, ~40 min at 10 workers)
python enrich_case_study.py \
    --matches   ../case_studies/ARENA/matches.csv \
    --taxonomy  ../case_studies/ARENA/taxonomy_proposal.md \
    --out       ../case_studies/ARENA/matches_enriched.csv \
    --no-cache --workers 10 \
    --fields "what_happened,policy_domain,arena_framing,rhetorical_function,\
mention_valence,mention_centrality,speech_act_type,target_of_criticism,\
arena_claim_type,debate_topic_normalised"

# 4. Full report
python analyse_case_study.py \
    --enriched ../case_studies/ARENA/matches_enriched.csv \
    --out      ../case_studies/ARENA/report.html \
    --subject  ARENA

# 5. Topic sub-report (Future Made in Australia)
python analyse_case_study.py \
    --enriched         ../case_studies/ARENA/matches_fmia.csv \
    --out              ../case_studies/ARENA/report_fmia.html \
    --subject          ARENA \
    --narrative-topic  "the Future Made in Australia Bill 2024 and associated legislation"
```
