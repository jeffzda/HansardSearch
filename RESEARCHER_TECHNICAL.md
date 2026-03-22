# Researcher Pipeline: Technical Reference

## Overview

The researcher pipeline is implemented in `pipeline/newsletter.py` and consists of four
functions called in sequence for any phrase corpus exceeding `max_turns` (default:
`MAX_BODIES_FOR_CLAUDE`). It runs before `select_bodies_for_claude()` and operates
entirely on `matches_df` — the full phrase-matched corpus. Charts and spike detection
always use the full corpus regardless of what the researcher does.

```
topic_matching_pass()
        ↓
  matched_topic_titles: list[str]
        ↓
researcher_pass()  ←── matched_topic_titles
        ↓
  ResearcherFilter (priority_filters, rationale, usage, messages)
        ↓
apply_researcher_filter()
        ↓
  bodies_df (filtered matches_df)
        ↓
select_bodies_for_claude(researcher_filtered=True)
        ↓
  bodies: list[dict]  →  writer
```

---

## Functions

### `_load_topic_lookup() → dict[str, list[str]]`

Loads both senate and house debate topic parquets, filters to level-0 (top-level debate
sections only), strips procedural headings via `_PROCEDURAL_TOPIC_RE`, and returns a dict
mapping `UPPERCASE_TITLE → sorted list of ISO date strings`.

Cached at module level in `_TOPIC_LOOKUP`. Called at most once per process regardless of
how many phrases trigger the researcher.

**Source parquets:**
- `data/output/senate/topics/senate_debate_topics.parquet`
- `data/output/house/topics/house_debate_topics.parquet`

**Schema used:** `date`, `level`, `topic` (columns read; `order` not needed here).

**Procedural filter:** `_PROCEDURAL_TOPIC_RE` strips headings that appear on almost every
sitting day and carry no legislative information (COMMITTEES, QUESTIONS WITHOUT NOTICE,
ADJOURNMENT, BUDGET, SENATE ESTIMATES, etc.). Approximately 68% of level-0 rows are
procedural; ~6,000 unique substantive titles remain after filtering.

---

### `topic_matching_pass(phrase, client, model) → list[str]`

Sends the full list of ~6,000 unique substantive debate titles (one per line, sorted) to
Claude and asks it to identify those directly relevant to the phrase — including related
legislation, inquiries, policy frameworks, and international agreements.

**Input tokens:** ~72,000 (the full title list dominates)
**Output tokens:** ~500–3,000 depending on phrase breadth
**Cost (Sonnet):** ~$0.22–0.32 per call

Returns a list of exact title strings (uppercase, verified against the lookup). Returns
`[]` on any failure; `researcher_pass()` falls back to date-filtered calendar when empty.

**Normalisation:** model output is parsed as JSON array, each element stripped and
uppercased, then validated against the lookup keys. Titles not found in the lookup are
discarded (handles hallucinated or slightly variant titles).

---

### `_build_topic_calendar(phrase, matches_df, matched_titles=None, max_rows=60) → str`

Builds the topic calendar string injected into the researcher prompt.

**When `matched_titles` is provided (non-empty):**
Uses `_load_topic_lookup()` to find all sitting days for the matched titles. These dates
are **not** restricted to dates present in `matches_df` — a sitting day where the CPRS
was formally debated is included even if no speech turn on that day contained the search
phrase. The `turns_that_day` column is populated from `matches_df` date counts (0 for
dates with no phrase-matching turns).

Sorted by `turns_that_day` descending then date ascending. Capped at `max_rows=300`
(the targeted set is already filtered to relevant titles; a higher cap is safe).

**When `matched_titles` is None or empty (fallback):**
Original behaviour: loads topic parquets, restricts to dates present in `matches_df`,
shows up to 5 substantive topics per date sorted by sitting-day order, capped at 60 rows.

**Format:** `  YYYY-MM-DD | NNNN turns | TOPIC TITLE`

---

### `_build_speaker_topic_date_matrix(matches_df, topic_matched_dates, n_speakers=15) → str`

For the sitting days identified by the topic matching pass, shows which top speakers had
phrase-matching turns on those specific dates. Restricts `matches_df` to rows whose date
appears in `topic_matched_dates` and whose `name_id` is not `10000` (presiding officers).

Selects the top `n_speakers` by total phrase-matching turns on those dates. For each
speaker, formats nonzero date:count pairs sorted chronologically.

**Format:** `  Speaker Name: YYYY-MM-DD:N YYYY-MM-DD:N ...`
**Typical size:** ~2,000 tokens for 15 speakers × 200 dates.

---

### `researcher_pass(phrase, matches_df, client, model, max_turns, matched_topic_titles=None) → Optional[ResearcherFilter]`

The main researcher call. Assembles a metadata package and calls Claude (Sonnet) with
the `web_search_20260209` tool enabled. Handles the tool-use loop (model may issue one
or more web searches before producing final JSON). Returns `None` on any failure.

**Metadata assembled:**

| Block | Content |
|-------|---------|
| Annual counts | `year → count` dict for all years |
| Spike years | Years with >2× mean annual count |
| Speaker table | Top 40 by total turns: name, party, total, `[stmt:N q:N ans:N int:N]`, per-year counts |
| Party totals | Top 10 parties by total turns |
| Chamber split | Senate vs House counts |
| Gov/opp split | `in_gov` flag counts |
| Turn type totals | Corpus-wide statement/question/answer/interject counts |
| Spike speakers | Top 3 speakers within each spike year |
| Party×year matrix | Top 6 parties, active years only |
| Division turns | Total `div_flag=1` turns |
| State×year matrix | Senate only; top 8 states, active years only |
| Topic calendar | Output of `_build_topic_calendar()` — targeted or fallback |
| Speaker matrix | Output of `_build_speaker_topic_date_matrix()` — when matched titles available |

**Prompt structure:**

1. Context: corpus size, date range, phrase, today's date, training cutoff
2. Corpus statistics blocks (all metadata above)
3. Instructions:
   - Use background knowledge; web-search post-cutoff events
   - Represent actual political landscape proportionally to the story, not mention count
   - Use metadata blocks to enrich analysis (turn types, spike speakers, party matrix)
   - **Filter strategy**: use `debate_date` as primary filter (target significant legislative
     days from the calendar); use `speaker_year` for longitudinal coverage
   - Rank by political significance, not mention count
   - Accumulate elements until estimated total ≈ `max_turns`
4. JSON schema (filter types in priority order)
5. Rules for each filter type

**Tool-use loop:** Capped at 15 iterations. On each `tool_use` stop, appends the
assistant turn and empty `tool_result` placeholders and continues. Token usage is
accumulated across all iterations. The final assistant message is appended to `messages`
before returning, preserving the full conversation for the review pass.

**JSON extraction:** `re.search(r'\{.*\}', final_text, re.DOTALL)` — handles any prose
prefix or markdown wrapping the model may produce.

---

### `apply_researcher_filter(matches_df, rf, max_turns) → pd.DataFrame`

Applies all filter elements from `rf.priority_filters` as a single OR mask against
`matches_df`. All elements are applied simultaneously — the researcher has already
calibrated the set to produce approximately `max_turns` turns.

Safety: if the result contains fewer than `MIN_TURNS_AFTER_FILTER` (50) turns, the full
unfiltered `matches_df` is returned and a warning is printed.

**Supported filter types:**

| Type | Value shape | Logic |
|------|-------------|-------|
| `speaker` | `"surname"` | `name.str.contains(val, case=False)` |
| `year_range` | `[from, to]` | `year.between(from, to)` |
| `party` | `"ALP"` | `party == val` |
| `speaker_date` | `{"speaker": str, "dates": ["YYYY-MM-DD", ...]}` | speaker AND date.isin(dates) |
| `speaker_year` | `{"speaker": str, "years": [int, int]}` | speaker AND year range |
| `speaker_type` | `{"speaker": str, "turn_type": str}` | speaker AND turn type mask |
| `speaker_year_type` | `{"speaker": str, "years": [...], "turn_type": str}` | all three |
| `gov_era` | `"howard"` | year range from `GOV_ERAS` constant |
| `division_turns` | `true` | `div_flag == 1` |
| `state_year` | `{"state": "NSW", "years": [int, int]}` | state AND year range (Senate) |
| `debate_date` | `["YYYY-MM-DD", ...]` | `date.isin(target_dates)` |

Turn type mask logic (`_turn_type_mask()`):
- `statement` → `~question & ~answer & ~interject`
- `question` → `question == True`
- `answer` → `answer == True`
- `interject` → `interject == True`

---

### `select_bodies_for_claude(..., researcher_filtered=False)`

When `researcher_filtered=True`, the Case A threshold (pass everything through without
mechanical selection) is relaxed from `max_turns` to `int(max_turns * 1.2)`. This
prevents the proportional year-allocation selection from discarding turns the researcher
deliberately chose. The token budget check is unchanged — it remains the hard guard
against oversized writer inputs.

---

## Supporting data structures

### `ResearcherFilter` dataclass

```python
@dataclass
class ResearcherFilter:
    priority_filters: list[dict]   # ordered [{type, value, estimated_turns}, ...]
    rationale:        str          # paragraph — logged to run output
    usage:            dict         # {input_tokens, output_tokens} across all iterations
    messages:         list         # full conversation — preserved for review pass
```

### `GOV_ERAS` constant

```python
GOV_ERAS: dict[str, tuple[int, int]] = {
    "howard":   (1996, 2007),
    "rudd":     (2007, 2010),
    "rudd1":    (2007, 2010),
    "gillard":  (2010, 2013),
    "rudd2":    (2013, 2013),
    "abbott":   (2013, 2015),
    "turnbull": (2015, 2018),
    "morrison": (2018, 2022),
    "albanese": (2022, 2100),   # 2100 = open-ended sentinel
}
```

---

## Wiring in `process_phrase_for_newsletter()`

```python
bodies_df: pd.DataFrame = matches_df
rf: Optional[ResearcherFilter] = None
matched_topic_titles: list[str] = []

if not args.dry_run and len(matches_df) > args.max_turns:
    matched_topic_titles = topic_matching_pass(phrase, client, args.narrative_model)
    rf = researcher_pass(
        phrase, matches_df, client, args.narrative_model,
        max_turns=args.max_turns,
        matched_topic_titles=matched_topic_titles,
    )
    if rf:
        bodies_df = apply_researcher_filter(matches_df, rf, args.max_turns)

bodies = select_bodies_for_claude(
    bodies_df, phrase,
    max_turns=args.max_turns,
    researcher_filtered=(rf is not None),
)
```

Charts and spike detection use `matches_df` (full corpus) throughout.

---

## CLI flags

### `--research-only`

Runs the topic matching pass and researcher pass, prints a diagnostic report, then exits
without producing a newsletter. Useful for inspecting filter quality before a full run.

Output includes:
- Corpus size and target
- Topic match results (up to 30 titles shown)
- Researcher filter spec: type, value, estimated turns per element
- Estimated vs actual filtered turn count
- Researcher rationale paragraph
- Total token usage (input + output)

### `--dry-run`

Skips all API calls including both researcher passes. Neither `topic_matching_pass` nor
`researcher_pass` is called. Placeholder narratives are generated.

---

## Trigger condition

Both passes are triggered when:
```python
not args.dry_run and len(matches_df) > args.max_turns
```

`args.max_turns` defaults to `MAX_BODIES_FOR_CLAUDE` at argument-parse time, so
`--max-turns N` adjusts both the trigger threshold and the researcher's calibration target.

Small corpora (≤ `max_turns`) bypass the researcher entirely and pass straight to
`select_bodies_for_claude()` Case A (send everything).

---

## Failure modes and fallbacks

| Failure | Result |
|---------|--------|
| `topic_matching_pass` API error | Returns `[]`; `researcher_pass` uses date-filtered calendar |
| `topic_matching_pass` JSON parse error | Returns `[]`; same fallback |
| `researcher_pass` API error | Returns `None`; full `matches_df` used |
| `researcher_pass` empty response | Returns `None`; full `matches_df` used |
| `researcher_pass` no JSON in response | Returns `None`; full `matches_df` used |
| `apply_researcher_filter` result < 50 turns | Returns full `matches_df` with warning |
| `_load_topic_lookup` parquets missing | Returns `{}`; topic calendar returns unavailable message |

No failure in the researcher pipeline prevents the newsletter from being produced.

---

## Cost profile (Sonnet 4.6 pricing: $3/M input, $15/M output)

| Step | Typical input tokens | Typical output tokens | Cost |
|------|---------------------|----------------------|------|
| `topic_matching_pass` | ~72,000 | ~1,000–3,000 | ~$0.23–0.34 |
| `researcher_pass` (no web search) | ~15,000–25,000 | ~800–1,500 | ~$0.06–0.10 |
| `researcher_pass` (with web search, 3 rounds) | ~40,000–60,000 | ~2,000–4,000 | ~$0.15–0.24 |
| **Total per large-corpus phrase** | | | **~$0.30–0.60** |

The researcher pipeline only triggers for phrases with more than `max_turns` corpus
matches — typically a small subset of the phrases processed in a given newsletter run.
