"""
enrich_case_study.py — Annotate a case-study matches CSV with taxonomy fields
using the Claude API.

Usage:
    python enrich_case_study.py \\
        --matches  ../case_studies/GRDC/matches.csv \\
        --taxonomy ../case_studies/GRDC/taxonomy_grdc.md \\
        --out      ../case_studies/GRDC/matches_enriched.csv

    # dry run (first 5 rows only, no cost):
    python enrich_case_study.py --matches ... --taxonomy ... --out ... --dry-run

    # resume a partial run:
    python enrich_case_study.py --matches ... --taxonomy ... --out ... --resume

Outputs:
    matches_enriched.csv  — original columns + taxonomy columns
    matches_enriched.jsonl — per-row checkpoint (enables --resume)

Default taxonomy columns (universal layer, taxonomy_template.md v2.0):
    summary, policy_domain, mention_centrality, org_framing,
    speaker_valence, rhetorical_function, target_of_speech,
    policy_action_urged, org_name_form, notable_quote,
    enrich_confidence_note, enrich_error

Override with --fields for organisation-specific field names (e.g. GRDC taxonomy
uses grdc_framing, speaker_valence_toward_grdc etc.).
"""

import argparse
import csv
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import anthropic

csv.field_size_limit(10_000_000)

# ── Default taxonomy field names (universal layer, taxonomy_template.md v2.0) ──

_DEFAULT_TAXONOMY_FIELDS = [
    "summary",
    "policy_domain",
    "mention_centrality",
    "org_framing",
    "speaker_valence",
    "rhetorical_function",
    "target_of_speech",
    "policy_action_urged",
    "org_name_form",
    "notable_quote",
]

# Populated in main() from --fields arg (or defaulting to the set above)
TAXONOMY_FIELDS: list[str] = []


def build_instruction(core_fields: list[str]) -> str:
    """Build the JSON-template instruction from the list of core field names."""
    lines = []
    for f in core_fields:
        if f in ("summary", "what_happened"):
            lines.append(f'  "{f}": "one or two sentence neutral summary"')
        elif f == "notable_quote":
            lines.append(f'  "{f}": "<verbatim quote or null>"')
        else:
            lines.append(f'  "{f}": "<allowed value or null>"')
    lines.append('  "confidence_note": "<brief note only if a field was genuinely difficult; otherwise null>"')
    json_template = "{\n" + ",\n".join(lines) + "\n}"
    return f"""
---

## Your task

You will be given one parliamentary speech turn. Annotate it using the taxonomy
and design rules above.

Return ONLY a JSON object with these exact keys:

{json_template}

Rules:
- Apply the design rules above: separate extractable from inferred; only infer
  at medium confidence or better; null beats a low-confidence guess.
- Use ONLY the allowed values specified in the taxonomy for each field.
- Use JSON null (not the string "null") for fields that cannot be determined.
- Never re-state metadata (date, party, speaker) as taxonomy field values.
- Do not add any text outside the JSON object.
- Do not add extra keys.
"""

# ── Row formatting ─────────────────────────────────────────────────────────────

def format_row_for_prompt(row: dict, body_limit: int, context_limit: int) -> str:
    """Serialise a CSV row into a compact prompt string."""

    def truncate(text: str, limit: int) -> str:
        if not text or text in ("", "nan", "None"):
            return ""
        text = text.strip()
        return text[:limit] + "…" if len(text) > limit else text

    body  = truncate(row.get("body", ""), body_limit)
    ctx_b = truncate(row.get("context_before", ""), context_limit)
    ctx_a = truncate(row.get("context_after", ""), context_limit)

    lines = [
        f"match_id: {row['match_id']}",
        f"date: {row['date']}  chamber: {row['chamber']}  party: {row['party']}  "
        f"in_gov: {row['in_gov']}  gender: {row.get('gender','')}",
        f"speaker: {row.get('speaker_name','')}",
        f"is_question: {row['is_question']}  is_answer: {row['is_answer']}  "
        f"q_in_writing: {row['q_in_writing']}",
        f"debate_heading: {row.get('debate_heading','') or '(none)'}",
        f"matched_terms: {row.get('matched_terms','')}",
        "",
        f"BODY:\n{body}",
    ]
    if ctx_b:
        lines += ["", f"CONTEXT BEFORE (preceding turns):\n{ctx_b}"]
    if ctx_a:
        lines += ["", f"CONTEXT AFTER (following turns):\n{ctx_a}"]

    return "\n".join(lines)


# ── JSON extraction ────────────────────────────────────────────────────────────

def extract_json(text: str) -> dict:
    """Extract the first JSON object from the response text."""
    text = re.sub(r"```(?:json)?\s*", "", text).strip()
    start = text.find("{")
    end   = text.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError(f"No JSON object found in response: {text[:200]}")
    return json.loads(text[start:end])


# ── API call ──────────────────────────────────────────────────────────────────

def annotate_row(
    client: anthropic.Anthropic,
    row: dict,
    taxonomy_text: str,
    model: str,
    body_limit: int,
    context_limit: int,
    no_cache: bool = False,
    max_retries: int = 3,
) -> dict:
    """Call the API and return a dict of taxonomy fields."""
    row_text    = format_row_for_prompt(row, body_limit, context_limit)
    instruction = build_instruction([f for f in TAXONOMY_FIELDS
                                     if f not in ("enrich_confidence_note", "enrich_error")])

    for attempt in range(max_retries):
        try:
            system_block: dict = {"type": "text", "text": taxonomy_text + "\n" + instruction}
            if not no_cache:
                system_block["cache_control"] = {"type": "ephemeral"}

            response = client.messages.create(
                model=model,
                max_tokens=1024,
                temperature=0,
                system=[system_block],
                messages=[{"role": "user", "content": row_text}],
            )
            raw    = response.content[0].text
            parsed = extract_json(raw)

            result = {f: parsed.get(f) or "" for f in TAXONOMY_FIELDS
                      if f not in ("enrich_confidence_note", "enrich_error")}
            result["enrich_confidence_note"] = parsed.get("confidence_note") or ""
            result["enrich_error"]           = ""
            result["_usage"] = {
                "input_tokens":                response.usage.input_tokens,
                "output_tokens":               response.usage.output_tokens,
                "cache_creation_input_tokens": getattr(response.usage, "cache_creation_input_tokens", 0),
                "cache_read_input_tokens":     getattr(response.usage, "cache_read_input_tokens", 0),
            }
            return result

        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            print(f"    Rate limit — waiting {wait}s…", flush=True)
            time.sleep(wait)
        except anthropic.APIStatusError as e:
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return {f: "" for f in TAXONOMY_FIELDS} | {"enrich_error": str(e)}
        except (ValueError, json.JSONDecodeError) as e:
            return {f: "" for f in TAXONOMY_FIELDS} | {"enrich_error": f"parse_error: {e}"}

    return {f: "" for f in TAXONOMY_FIELDS} | {"enrich_error": "max_retries_exceeded"}


# ── Cost estimate ─────────────────────────────────────────────────────────────

PRICING = {
    "claude-sonnet-4-6": dict(input=3.00,  output=15.00, cache_write=3.75,  cache_read=0.30),
    "claude-opus-4-6":   dict(input=15.00, output=75.00, cache_write=18.75, cache_read=1.50),
}

def estimate_cost(n_rows: int, model: str, mean_row_tokens: int = 1_500,
                  taxonomy_tokens: int = 5_603, output_tokens: int = 500) -> float:
    p = PRICING.get(model, PRICING["claude-sonnet-4-6"])
    cache_write = taxonomy_tokens / 1e6 * p["cache_write"]
    cache_read  = taxonomy_tokens * (n_rows - 1) / 1e6 * p["cache_read"]
    row_input   = mean_row_tokens * n_rows / 1e6 * p["input"]
    out_cost    = output_tokens * n_rows / 1e6 * p["output"]
    return cache_write + cache_read + row_input + out_cost


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Enrich case-study matches with taxonomy annotations.")
    ap.add_argument("--matches",       type=Path, required=True)
    ap.add_argument("--taxonomy",      type=Path, required=True)
    ap.add_argument("--out",           type=Path, required=True)
    ap.add_argument("--model",         default="claude-sonnet-4-6")
    ap.add_argument("--body-limit",    type=int, default=100_000,
                    help="Max chars of body text sent per row (default 100000 = full text).")
    ap.add_argument("--context-limit", type=int, default=100_000,
                    help="Max chars of each context field sent per row (default 100000 = full text).")
    ap.add_argument("--resume",        action="store_true",
                    help="Skip rows already present in the output JSONL checkpoint.")
    ap.add_argument("--dry-run",       action="store_true",
                    help="Process first 5 rows only (no cost estimate prompt).")
    ap.add_argument("--delay",         type=float, default=0.3,
                    help="Seconds to wait between API calls (default 0.3).")
    ap.add_argument("--no-cache",      action="store_true",
                    help="Disable prompt caching.")
    ap.add_argument("--workers",       type=int, default=10,
                    help="Number of parallel API calls (default 10).")
    ap.add_argument("--fields",        type=str, default=None,
                    help="Comma-separated core taxonomy field names (no enrich_ prefix). "
                         "Defaults to unified 10-field set if omitted.")
    args = ap.parse_args()

    # Resolve taxonomy field list (global, used by annotate_row)
    global TAXONOMY_FIELDS
    if args.fields:
        core = [f.strip() for f in args.fields.split(",") if f.strip()]
    else:
        core = list(_DEFAULT_TAXONOMY_FIELDS)
    TAXONOMY_FIELDS = core + ["enrich_confidence_note", "enrich_error"]

    # Load inputs
    with open(args.matches, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {len(rows)} rows from {args.matches}")

    taxonomy_text = args.taxonomy.read_text(encoding="utf-8")
    print(f"Loaded taxonomy from {args.taxonomy}")

    # Checkpoint file
    checkpoint = args.out.with_suffix(".jsonl")
    already_done: dict[str, dict] = {}
    if args.resume and checkpoint.exists():
        with open(checkpoint, encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                already_done[rec["match_id"]] = rec
        print(f"Resuming: {len(already_done)} rows already annotated")

    if args.dry_run:
        rows = rows[:5]
        print("DRY RUN — processing first 5 rows only")

    # Cost estimate
    n_todo       = len([r for r in rows if r["match_id"] not in already_done])
    mean_row_tok = 1_500 if (args.body_limit <= 2000 and args.context_limit <= 500) else 12_000
    est          = estimate_cost(n_todo, args.model, mean_row_tokens=mean_row_tok)
    cache_note   = "no caching" if args.no_cache else "with caching"
    print(f"\nRows to process: {n_todo}  |  Model: {args.model}  |  {cache_note}")
    print(f"Estimated cost:  ~${est:.2f}  (body≤{args.body_limit} chars, context≤{args.context_limit} chars)")

    if not args.dry_run and n_todo > 5:
        try:
            ans = input("Proceed? [y/N] ").strip().lower()
        except EOFError:
            ans = "y"
        if ans != "y":
            print("Aborted.")
            sys.exit(0)

    # Run enrichment — parallel with ThreadPoolExecutor
    client   = anthropic.Anthropic()
    todo     = [r for r in rows if r["match_id"] not in already_done]
    lock     = threading.Lock()
    ckpt_fh  = open(checkpoint, "a", encoding="utf-8")
    counters = {"done": 0, "errors": 0,
                "input": 0, "output": 0, "cache_write": 0, "cache_read": 0}

    def process(row: dict) -> None:
        result = annotate_row(
            client, row, taxonomy_text,
            model=args.model,
            body_limit=args.body_limit,
            context_limit=args.context_limit,
            no_cache=args.no_cache,
        )
        usage = result.pop("_usage", {})
        mid   = row["match_id"]

        with lock:
            counters["done"]        += 1
            counters["input"]       += usage.get("input_tokens", 0)
            counters["output"]      += usage.get("output_tokens", 0)
            counters["cache_write"] += usage.get("cache_creation_input_tokens", 0)
            counters["cache_read"]  += usage.get("cache_read_input_tokens", 0)
            if result.get("enrich_error"):
                counters["errors"] += 1
                print(f"  [{counters['done']}/{n_todo}] {mid}  ERROR: {result['enrich_error']}", flush=True)
            else:
                framing_field = next((f for f in TAXONOMY_FIELDS
                                      if "framing" in f or f == "org_framing"), None)
                framing_val = result.get(framing_field, "?") if framing_field else "?"
                tag = result.get("mention_centrality", "?") + " / " + framing_val
                print(f"  [{counters['done']}/{n_todo}] {mid}  {tag}", flush=True)
            ckpt_fh.write(json.dumps({"match_id": mid, **result}) + "\n")
            ckpt_fh.flush()
            already_done[mid] = result

    print(f"Running {args.workers} workers in parallel…\n", flush=True)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(process, row) for row in todo]
        for fut in as_completed(futures):
            fut.result()

    ckpt_fh.close()

    # Merge and write final CSV
    fieldnames = list(rows[0].keys()) + TAXONOMY_FIELDS
    args.out.parent.mkdir(parents=True, exist_ok=True)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            enriched = already_done.get(row["match_id"], {f: "" for f in TAXONOMY_FIELDS})
            writer.writerow({**row, **{k: enriched.get(k, "") for k in TAXONOMY_FIELDS}})

    print(f"\nWrote {len(rows)} rows → {args.out}")

    # Cost summary
    p = PRICING.get(args.model, PRICING["claude-sonnet-4-6"])
    actual_cost = (
        counters["input"]       / 1e6 * p["input"] +
        counters["cache_write"] / 1e6 * p["cache_write"] +
        counters["cache_read"]  / 1e6 * p["cache_read"] +
        counters["output"]      / 1e6 * p["output"]
    )
    print(f"\nToken usage:")
    print(f"  Input (non-cached): {counters['input']:,}")
    print(f"  Cache writes:       {counters['cache_write']:,}")
    print(f"  Cache reads:        {counters['cache_read']:,}")
    print(f"  Output:             {counters['output']:,}")
    print(f"  Actual cost:        ${actual_cost:.2f}")
    if counters["errors"]:
        print(f"  Errors:             {counters['errors']} rows (check enrich_error column)")


if __name__ == "__main__":
    main()
