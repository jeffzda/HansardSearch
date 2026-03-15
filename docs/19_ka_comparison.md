# 19 — Katz & Alexander vs Our Pipeline: Code-Level Comparison

A detailed comparison of the Katz & Alexander (K&A) R-based Hansard pipeline
(`https://github.com/lindsaykatz/hansard-proj`, Zenodo 8121950) against our
Python pipeline, covering parsing strategy, enrichment, validation, and
corrections philosophy.

---

## 1. Parsing Strategy

### K&A — Text-blob splitting (`06-parse-*.R`)

K&A treats each `<speech>` block as a flat text blob:

1. Call `xml2::xml_text()` on the entire speech element to get a single string.
2. Apply ~60+ sequential `tidyr::separate_rows()` calls with regex delimiter
   patterns to split on speaker attributions, interjection markers, procedural
   headings, etc.
3. Dynamically build per-file regex patterns from a `name_forms` table derived
   from the member lookup (up to five name-form variants per member).
4. Post-split, classify each row as speech / interjection / procedural.

The core split function (`split_interjections_fedchamb()`) is ~900 lines for
the House alone; the Senate equivalent is similarly large.

**Implications:**
- Works well when the text blob accurately represents the document structure.
- Fragile to unusual name forms or punctuation variations — triggers hardcoded
  corrections (`14-corrections.R`, ~700 lines; `v2_08-final_corrections.R`,
  ~500 lines).
- Split boundaries are approximate; adjacent rows can silently merge or split
  incorrectly.

### Our pipeline — Structural tree-walk (`03_parse.py`, `03b_parse_house.py`)

We walk the lxml element tree directly:

1. Iterate `<speech>/<p>` elements; detect `<a type="MemberContinuation">` /
   `<a type="MemberInterjecting">` attribution spans to identify speaker
   boundaries.
2. Extract PHID from `href` attribute (`#_Toc...` → PHID integer) to identify
   the speaker without regex name-matching.
3. Accumulate `el.text` and `el.tail` for each paragraph.
4. Flush row on each new attribution span.

**Implications:**
- Zero regex name matching — speaker identity comes from the PHID in the
  element attribute, not from matching name strings.
- No need for a 900-line split function or thousands of hardcoded coordinate
  corrections.
- Structurally correct by construction for all v2.2 XML.

---

## 2. The `el.tail` Bug (and Fix)

During the comparison we discovered a body-text truncation bug in our v2.2
parser.

In lxml, for markup like:
```xml
<p>Some text <a href="...">Mr Smith</a>:  said something.</p>
```
The string `":  said something."` is stored as `el.tail` on the `<a>` element,
**not** as text content of any child. Our original `_extract_body_text_v22()`
returned early after processing the attribution `<a>` tag without capturing
`el.tail`, silently discarding the continuation text on every paragraph that
contained an attribution span.

**Fix** (both `03_parse.py` and `03b_parse_house.py`):
```python
tail = (el.tail or "").lstrip("):  ")
if tail:
    parts.append(tail)
```

After fix, our word-count ratio vs K&A improved from ~0.93 to 1.003–1.007
(slightly higher than K&A because we keep some procedural text they strip).

---

## 3. Interjection Handling

### K&A
K&A splits interjection rows out as separate DataFrame rows during parsing,
using the regex-split approach. Each interjection becomes its own row with
`interject = 1`.

### Our pipeline (v2.2 XML reality)
In v2.2 XML, `<interjection>` elements are direct children of `<speech>` but
their `<talk.text>` is **empty**. The actual interjection text appears inline
in the surrounding speech paragraphs as plain text patterns:

```
Mr SMITH interjecting—
Opposition members interjecting—
```

These are not wrapped in any structural element, so our tree-walk cannot
distinguish them as separate speaker turns without splitting body text.

**Our solution:** flag rows, don't split.
- `has_embedded_interject` column: `True` for rows whose body matches
  `_EMBEDDED_INTERJECT_RE` or `_BARE_CAPS_INTERJECT_RE`.
- `strip_embedded_interjections(body)` utility: removes the inline markers
  on demand (e.g., before LLM enrichment).
- 32,353 rows flagged (~5.2% of House corpus); pattern is consistent across
  all years.

---

## 4. The `in_gov` Column

### K&A
K&A derives `in_gov` from the `<in.gov>` XML element, which was populated in
v1/v2.1 XML but **not in v2.2** (APH stopped filling it after ~2012). K&A's
corpus therefore has `in_gov = 0` or `NA` for all post-2012 rows.

### Our pipeline
We derive `in_gov` from party + a government-period lookup table
(`_GOV_PERIODS` in `04b_fill_details_house.py`), covering Howard through
Albanese governments. This produces correct values across all years.

Coverage: 37–45% of rows in government (consistent with Australian two-party
alternation pattern).

---

## 5. Fill Details

### K&A (`07-fill_details.R`)
- Multi-pass fill using `member_lookup` with five name-form variants
  (`form1`–`form5`).
- No date-range guard — `party` / `state` assigned by name_id alone.
- Party changers (e.g., Len Harris PHON→IND) produce incorrect values without
  the later corrections pass.
- Sequential execution with `Sys.sleep(3)` between files (no parallelisation).

### Our pipeline (`04_fill_details.py`, `04b_fill_details_house.py`)
- Lookup files carry explicit `date_from`/`date_to` columns where relevant.
- `fill_date_aware()` applies date-range guards: party/state assigned based on
  the sitting date, not just name_id.
- Passes run in parallel (threaded map over daily files).
- No hardcoded corrections — lookup files are the source of truth.

---

## 6. Corrections Philosophy

| Dimension | K&A | Our pipeline |
|---|---|---|
| Corrections approach | ~1,200 lines of hardcoded `case_when` mutations keyed to specific `(date, order)` coordinates | Zero hardcoded corrections; fix the lookup files instead |
| Party changers | Handled in `14-corrections.R` with explicit per-person date/value overrides | Date-range entries in `party_lookup.csv`; `fill_date_aware()` applies automatically |
| State/electorate errors | Patched in corrections scripts | Fixed in `state_lookup.csv` / `member_lookup.csv` |
| Maintainability | Each new parliament year requires extending corrections scripts | Add new lookup rows; pipeline unchanged |

---

## 7. Validation Architecture

### K&A (`08-data_validation.R`)
- T1–T7 implemented as sequential `for` loops.
- **T4 (party multiplicity) and T5 (unknown name_ids) mutate and write files
  back to disk** within the validation script — validation and correction are
  entangled.
- No parallelisation.
- Validation is coupled to the corrections pipeline.

### Our pipeline (`05_validate.py`, `05b_validate_house.py`)
- T1–T7 implemented as pure diagnostic checks — no file writes.
- Results written to `validation_results/` as CSV per test per chamber.
- Parallelised over daily files (16 workers, ~3.3× speedup).
- `--workers N` and `--sequential` flags.
- Validation is strictly read-only; fixes go to lookup files, not into the
  validator.

---

## 8. Scale of the Corrections Problem

K&A's corrections files give a concrete measure of how much manual patching
the regex-split approach requires:

| File | Lines | Nature |
|---|---|---|
| `14-corrections.R` | ~700 | Hardcoded `case_when` mutations: date, order, name, party, state fixes |
| `v2_08-final_corrections.R` | ~500 | Same, for 2022–2025 XML |
| Total | ~1,200 | Per-row coordinate corrections |

Our pipeline has zero lines of equivalent corrections code. Fixes go into
lookup CSVs which are version-controlled and human-readable.

---

## 9. Output Comparison

| Metric | K&A corpus | Our corpus |
|---|---|---|
| House rows | ~600,000 (estimated) | 623,906 |
| Senate rows | ~500,000 (estimated) | 549,864 |
| Coverage | 1998–2022 (Zenodo) | 1998–2025 |
| `in_gov` post-2012 | 0 / NA | Correctly derived |
| `has_embedded_interject` | Not present | 32,353 rows flagged |
| Word count ratio (House body) | 1.000 (baseline) | 1.003–1.007 |

The slight word-count surplus in our corpus reflects procedural text that K&A
strips via their correction passes but we retain (and which can be excluded via
the `fedchamb_flag` / `div_flag` columns).

---

## 10. Summary

| Dimension | K&A | Our pipeline |
|---|---|---|
| Parsing | Regex text-blob splitting | Structural lxml tree-walk |
| Speaker ID | Name-form regex matching | PHID from element attribute |
| `in_gov` | XML element (broken post-2012) | Government-period lookup |
| Interjections | Split into separate rows | Flagged; strip utility provided |
| Party changers | Hardcoded corrections | Date-range lookup entries |
| Corrections code | ~1,200 lines | 0 lines |
| Validation | Mutates files in-place | Pure diagnostics |
| Parallelisation | None | Threaded (I/O) + Process (CPU) |
| Year coverage | 1998–2022 | 1998–2025 |
