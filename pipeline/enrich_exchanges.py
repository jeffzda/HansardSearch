"""
enrich_exchanges.py — Enrich Q&A exchanges from a search matches CSV.

Each exchange (grouped by exchange_id) is sent to the Claude API as a single
unit — the full question + answer text with speaker labels — and annotated
with the unified 16-field taxonomy (taxonomy_template.md v2.0).

Output is one row per exchange (not per speech), with a join key back to the
original matches file via exchange_id.

Usage:
    python enrich_exchanges.py \\
        --matches  ../case_studies/ARENA_5/matches.csv \\
        --out      ../case_studies/ARENA_5/exchanges_enriched.csv

    # with a custom taxonomy file (recommended for org-specific framing values):
    python enrich_exchanges.py \\
        --matches  ../case_studies/GRDC/matches.csv \\
        --taxonomy ../case_studies/GRDC/taxonomy_grdc.md \\
        --out      ../case_studies/GRDC/exchanges_enriched.csv

    # dry run (first 5 exchanges only):
    python enrich_exchanges.py --matches ... --out ... --dry-run

    # resume a partial run:
    python enrich_exchanges.py --matches ... --out ... --resume

Output columns:
    Metadata (13):
        exchange_id, date, chamber, debate_heading, q_in_writing,
        question_speaker, question_party, question_in_gov, question_gender,
        answer_speaker, answer_party, answer_in_gov, answer_gender

    Pre-computed (1):
        org_mentioned_in  — question / answer / both

    Universal layer (10):
        summary, policy_domain, mention_centrality, org_framing,
        speaker_valence, rhetorical_function, target_of_speech,
        policy_action_urged, org_name_form, notable_quote

    Exchange overlay (5):
        question_intent, minister_valence, answer_quality, answered,
        evasion_technique

    Meta (2):
        enrich_confidence_note, enrich_error
"""

from __future__ import annotations

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

# ── Taxonomy field lists ───────────────────────────────────────────────────────

# Fields the LLM returns (15 = 10 universal + 5 exchange overlay)
_LLM_FIELDS = [
    # Universal layer
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
    # Exchange overlay
    "question_intent",
    "minister_valence",
    "answer_quality",
    "answered",
    "evasion_technique",
]

# All enrichment output fields (pre-computed + LLM + meta)
EXCHANGE_FIELDS = (
    ["org_mentioned_in"]
    + _LLM_FIELDS
    + ["enrich_confidence_note", "enrich_error"]
)

# ── Generic embedded taxonomy (used when --taxonomy is not provided) ───────────

_GENERIC_TAXONOMY = """
You are annotating Q&A exchanges from the Australian Parliament (Senate and
House of Representatives). Each exchange consists of one or more question rows
followed by one or more answer rows, presented with speaker labels.

## Design rules

Apply these rules to every field before annotating.

1. Lean beats complete. Every field has a defined set of allowed values.
   Do not add fields or values not listed here.

2. Raw metadata is in the ingest layer. Do not re-state date, party, speaker,
   or chamber values as taxonomy annotations.

3. Fields are labelled extractable or inferred:
   - extractable: directly determinable from the text without interpretive judgement
   - inferred: requires interpretive judgement from available evidence

4. Only infer at medium confidence or better. For inferred fields: if the text
   does not clearly support the value, use null. A null is correct and useful;
   a low-confidence guess is misleading. When in doubt, null.

5. Null discipline: use JSON null (not the string "null") for any field that
   cannot be determined. Never fabricate a value to avoid a null.

6. Universal fields describe the exchange as a whole. For fields where question
   and answer differ, code the question's framing unless the field definition
   specifies otherwise (rhetorical_function and minister_valence apply to the
   answer side).

7. Exchange overlay fields are only populated when there is a Q&A exchange.

## Universal fields

### summary
Type: extractable. Mandatory — always populate.
One or two neutral sentences: who asked what, how the minister responded,
and the net outcome. State what each speaker *did* with the mention — what
claim they made, what argument they advanced. Do not echo the speaker's
rhetoric. "Senator X cited the organisation's 2014 investment mandate to argue
the government was dismantling the clean energy pipeline" not "the organisation
was mentioned in a speech about energy".

### policy_domain
Type: extractable. Target fill rate: ~95%.
The primary policy area in the question (not any topic the minister shifts to).
  org_core_mandate   — the organisation's own programs, funding, or operations
  governance         — governance, board, or financial accountability
  legislation        — bills and motions citing the organisation
  budget             — appropriations debates where the organisation is incidental
  policy_debate      — broader policy debates citing the organisation
  other              — procedural or unrelated mentions
Use the most specific domain available. Do not default to a general domain when
a more specific one applies.

### mention_centrality
Type: inferred. Target fill rate: ~98%.
How central is the organisation to this exchange?
  focal       — the organisation is a primary subject; substantial attention
  supporting  — cited as evidence or example for an argument about something else
  incidental  — passing mention, list entry, or brief aside
Judge from the question's framing. A question that squarely targets the
organisation is focal even if the answer shifts topic.

### org_framing
Type: inferred. Target fill rate: ~88%.
How the speaker(s) characterise the organisation. Code the question's framing;
note conflicts in summary if question and answer frame it differently.
  public_institution   — presented as a legitimate, effective public body
  policy_instrument    — valued instrumentally as a tool for policy goals
  accountability_target — subject of scrutiny; decisions or governance questioned
  governance_failure   — institutional functioning impaired or neglected
  sector_exemplar      — cited as a positive example of a model or capability
  political_football   — invoked primarily for political attack or defence
  neutral_reference    — named factually without evaluative framing
Leave null if the mention is too brief to determine framing at medium confidence.

### speaker_valence
Type: inferred. Target fill rate: ~85%.
The questioner's evaluative stance toward the organisation specifically (not
toward the policy under debate).
  positive      — endorses, praises, or defends the organisation
  neutral       — no clear evaluative stance
  negative      — criticises, questions, or attacks the organisation
  mixed         — both positive and negative elements; valence shifts
  instrumental  — uses the organisation to support a broader argument

### rhetorical_function
Type: inferred. Target fill rate: ~90%.
What argumentative work does the minister's answer do with the organisation
mention? Applies to the ANSWER side. The questioner's function is captured
by question_intent.
  evidence_citation     — data or activities cited as factual evidence
  authority_endorsement — standing invoked to legitimise a position
  deflection_rebuttal   — invoked to rebut a criticism or deflect scrutiny
  policy_justification  — cited to justify a specific policy action
  attack_opponent       — invoked to criticise or embarrass a political opponent
  coalition_building    — named among supporters to show breadth
  alibi                 — genuine achievements cited as cover for an unaddressed failure
                          (distinct from deflection_rebuttal: no rebuttal of premise;
                          the minister pivots to adjacent success without conceding)
  incidental_mention    — no argumentative function in the answer
  null                  — minister's answer contains no organisation mention

### target_of_speech
Type: inferred. Target fill rate: ~90%.
Who or what the question is primarily directed at.
  government_policy    — addressing or criticising specific government policy
  opposition_party     — attacking or responding to the opposition
  org_institution      — directly targeting the organisation's own decisions
  minister_or_official — holding a minister or official accountable
  legislative_chamber  — procedural context
  general_advocacy     — making a general case without a specific target

### policy_action_urged
Type: inferred. Target fill rate: ~80%.
What the question demands or implies (not what the minister offers).
  strengthen_accountability — greater accountability, transparency, or oversight
  increase_funding          — more resources or investment urged
  reduce_or_abolish         — cut funding, wind up, or diminish the role
  defend_status_quo         — existing policy settings defended
  reform_model              — structural change to governance or legislation
  no_clear_action           — descriptive, procedural, or incidental
Use no_clear_action (not null) when the speech is substantive but advocates no
specific direction. Leave null only for genuinely fragmentary turns.

### org_name_form
Type: extractable. Target fill rate: ~98%.
Which name variant(s) the questioner uses. Strict text extraction only.
  acronym   — short form only
  full_name — full name only
  mixed     — both forms used
Leave null if the row was pulled in by Q&A pairing and contains no direct mention.

### notable_quote
Type: extractable. Target fill rate: ~75%.
The single most analytically or rhetorically significant verbatim phrase from
either side. Choose for evasive formulations, explicit value judgements,
memorable rhetoric, or factual admissions. Extract verbatim — do not paraphrase.
Leave null if no phrase is analytically distinctive.

## Exchange overlay fields

### question_intent
The questioner's primary political purpose.
  factual        — seeking specific information; no evident political framing
  accountability — pressing the minister to justify a decision or outcome
  hostile        — directly attacking the minister or government; strongly adversarial
  supportive     — a Dorothy Dixer; inviting the minister to speak favourably
  rhetorical     — making a political point under the guise of a question
  clarifying     — following up on a previous answer

### minister_valence
The minister's evaluative stance toward the organisation in their answer.
  positive      — endorses, praises, or defends the organisation
  neutral       — no clear evaluative stance
  negative      — criticises or questions the organisation
  instrumental  — uses the organisation to support a broader argument

### answer_quality
How substantively the minister addressed the question.
  substantive — directly addressed the question with specific information
  partial     — addressed part of the question but avoided key elements
  deflection  — changed the subject without answering
  non_answer  — made no attempt to address the question

### answered
true  — the minister substantively addressed what was asked
false — the minister did not substantively address what was asked

### evasion_technique
If answered is false, the technique used. Null when answered is true.
  topic_shift        — pivoted to a related but different subject
  false_premise      — rejected the question's framing rather than answering
  general_commitment — responded with vague commitments instead of specifics
  procedural         — redirected to process (committee, review, inquiry)
  humour             — deflected with a joke or rhetorical flourish
  null               — answered is true
""".strip()


# ── Prompt building ────────────────────────────────────────────────────────────

def _build_json_template() -> str:
    lines = []
    for f in _LLM_FIELDS:
        if f == "summary":
            lines.append(f'  "{f}": "one or two sentence neutral summary"')
        elif f == "notable_quote":
            lines.append(f'  "{f}": "<verbatim quote or null>"')
        elif f == "answered":
            lines.append(f'  "{f}": <true|false>')
        else:
            lines.append(f'  "{f}": "<allowed value or null>"')
    lines.append('  "confidence_note": "<brief note only if genuinely difficult; otherwise null>"')
    return "{\n" + ",\n".join(lines) + "\n}"


def build_system_prompt(taxonomy_text: str | None) -> str:
    """Combine taxonomy definitions with the JSON template instruction."""
    taxonomy = taxonomy_text.strip() if taxonomy_text else _GENERIC_TAXONOMY
    instruction = f"""
---

## Your task

You will be given one parliamentary Q&A exchange with speaker labels.
Annotate it using the taxonomy above.

Return ONLY a JSON object with these exact keys:

{_build_json_template()}

Rules:
- Use ONLY the allowed values specified in the taxonomy for each field.
- Use null (not the string "null") for fields that cannot be determined.
- evasion_technique must be null when answered is true.
- Do not add any text outside the JSON object.
- Do not add extra keys.
""".strip()
    return taxonomy + "\n\n" + instruction


# ── Exchange assembly ──────────────────────────────────────────────────────────

def _safe(v: object) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s in ("nan", "None", "NaT") else s


def assemble_exchange(rows: list[dict]) -> dict:
    """
    Given a list of rows sharing the same exchange_id (sorted by row_order),
    return a flat dict of exchange-level metadata and formatted transcript text.

    org_mentioned_in is determined from match_source:
      - If any question row has match_source == "search" → question mentions org
      - If any answer row has match_source == "search"   → answer mentions org
      - Rows with match_source in (question_answer, answer_question) were pulled
        in by pairing logic and may not contain the search terms directly.
    """
    rows = sorted(rows, key=lambda r: float(r.get("row_order") or 0))

    q_rows = [r for r in rows if _safe(r.get("is_question")) == "1"]
    a_rows = [r for r in rows if _safe(r.get("is_answer"))   == "1"]

    q_ref = q_rows[0] if q_rows else rows[0]
    a_ref = a_rows[0] if a_rows else rows[-1]

    # Build transcript
    lines: list[str] = []
    for r in rows:
        is_q = _safe(r.get("is_question")) == "1"
        is_a = _safe(r.get("is_answer"))   == "1"
        role    = "QUESTION" if is_q else ("ANSWER" if is_a else "SPEECH")
        speaker = _safe(r.get("speaker_name")) or "Unknown"
        party   = _safe(r.get("party"))
        ts      = _safe(r.get("time_stamp"))
        meta    = f"{speaker} ({party})" if party else speaker
        if ts:
            meta += f" [{ts}]"
        lines.append(f"[{role} — {meta}]\n{_safe(r.get('body'))}")

    transcript = "\n\n".join(lines)

    # Determine org_mentioned_in from match_source
    q_has_match = any(_safe(r.get("match_source")) == "search" for r in q_rows)
    a_has_match = any(_safe(r.get("match_source")) == "search" for r in a_rows)
    # Fallback: if match_source column absent, assume both
    if not any(r.get("match_source") for r in rows):
        q_has_match = a_has_match = True
    if q_has_match and a_has_match:
        org_mentioned_in = "both"
    elif q_has_match:
        org_mentioned_in = "question"
    elif a_has_match:
        org_mentioned_in = "answer"
    else:
        org_mentioned_in = "both"  # pulled in by pairing; terms may appear in context

    return {
        # Metadata
        "exchange_id":       _safe(rows[0].get("exchange_id")),
        "date":              _safe(q_ref.get("date")),
        "chamber":           _safe(q_ref.get("chamber")),
        "debate_heading":    _safe(q_ref.get("debate_heading")),
        "q_in_writing":      _safe(q_ref.get("q_in_writing")),
        "question_speaker":  _safe(q_ref.get("speaker_name")),
        "question_party":    _safe(q_ref.get("party")),
        "question_in_gov":   _safe(q_ref.get("in_gov")),
        "question_gender":   _safe(q_ref.get("gender")),
        "answer_speaker":    _safe(a_ref.get("speaker_name")),
        "answer_party":      _safe(a_ref.get("party")),
        "answer_in_gov":     _safe(a_ref.get("in_gov")),
        "answer_gender":     _safe(a_ref.get("gender")),
        # Pre-computed
        "org_mentioned_in":  org_mentioned_in,
        # Transcript for LLM
        "_transcript":       transcript,
    }


# ── JSON extraction ────────────────────────────────────────────────────────────

def extract_json(text: str) -> dict:
    text  = re.sub(r"```(?:json)?\s*", "", text).strip()
    start = text.find("{")
    end   = text.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError(f"No JSON object found: {text[:200]}")
    return json.loads(text[start:end])


# ── API call ──────────────────────────────────────────────────────────────────

def annotate_exchange(
    client: anthropic.Anthropic,
    exchange: dict,
    system_prompt: str,
    model: str,
    no_cache: bool = False,
    max_retries: int = 3,
) -> dict:
    """Call the API and return a dict of exchange taxonomy fields."""
    user_content = (
        f"exchange_id: {exchange['exchange_id']}\n"
        f"date: {exchange['date']}  chamber: {exchange['chamber']}\n"
        f"debate_heading: {exchange['debate_heading'] or '(none)'}\n"
        f"q_in_writing: {exchange['q_in_writing']}\n\n"
        f"{exchange['_transcript']}"
    )

    system_block: dict = {"type": "text", "text": system_prompt}
    if not no_cache:
        system_block["cache_control"] = {"type": "ephemeral"}

    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=1024,
                temperature=0,
                system=[system_block],
                messages=[{"role": "user", "content": user_content}],
            )
            raw    = response.content[0].text
            parsed = extract_json(raw)

            result: dict = {
                "org_mentioned_in": exchange["org_mentioned_in"],
            }
            for f in _LLM_FIELDS:
                result[f] = parsed.get(f) if f == "answered" else (parsed.get(f) or "")
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
                return {f: "" for f in EXCHANGE_FIELDS} | {"enrich_error": str(e)}
        except (ValueError, json.JSONDecodeError) as e:
            return {f: "" for f in EXCHANGE_FIELDS} | {"enrich_error": f"parse_error: {e}"}

    return {f: "" for f in EXCHANGE_FIELDS} | {"enrich_error": "max_retries_exceeded"}


# ── Cost estimate ─────────────────────────────────────────────────────────────

PRICING = {
    "claude-sonnet-4-6": dict(input=3.00,  output=15.00, cache_write=3.75, cache_read=0.30),
    "claude-opus-4-6":   dict(input=15.00, output=75.00, cache_write=18.75, cache_read=1.50),
}

def estimate_cost(n: int, model: str,
                  mean_input_tokens: int = 800,
                  system_tokens: int = 2_000,
                  output_tokens: int = 400) -> float:
    p = PRICING.get(model, PRICING["claude-sonnet-4-6"])
    cache_write = system_tokens / 1e6 * p["cache_write"]
    cache_read  = system_tokens * (n - 1) / 1e6 * p["cache_read"]
    row_input   = mean_input_tokens * n / 1e6 * p["input"]
    out_cost    = output_tokens * n / 1e6 * p["output"]
    return cache_write + cache_read + row_input + out_cost


# ── Output columns ─────────────────────────────────────────────────────────────

_METADATA_COLUMNS = [
    "exchange_id", "date", "chamber", "debate_heading", "q_in_writing",
    "question_speaker", "question_party", "question_in_gov", "question_gender",
    "answer_speaker",   "answer_party",   "answer_in_gov",  "answer_gender",
]

OUTPUT_COLUMNS = _METADATA_COLUMNS + EXCHANGE_FIELDS


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Enrich Q&A exchanges from a search matches CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--matches",  type=Path, required=True,
                    help="matches.csv produced by search_corpus.py (must have exchange_id column)")
    ap.add_argument("--out",      type=Path, required=True,
                    help="Output path for exchanges_enriched.csv")
    ap.add_argument("--taxonomy", type=Path, default=None,
                    help="Optional taxonomy .md file. If omitted, uses the generic embedded taxonomy. "
                         "Provide an instantiated taxonomy_template.md for org-specific framing values.")
    ap.add_argument("--model",    default="claude-sonnet-4-6")
    ap.add_argument("--resume",   action="store_true",
                    help="Skip exchanges already in the JSONL checkpoint")
    ap.add_argument("--dry-run",  action="store_true",
                    help="Process first 5 exchanges only (no API cost)")
    ap.add_argument("--no-cache", action="store_true",
                    help="Disable prompt caching")
    ap.add_argument("--workers",  type=int, default=10,
                    help="Parallel API calls (default 10)")
    args = ap.parse_args()

    # Load taxonomy
    taxonomy_text: str | None = None
    if args.taxonomy:
        taxonomy_text = args.taxonomy.read_text(encoding="utf-8")
        print(f"Loaded taxonomy from {args.taxonomy}")
    else:
        print("No --taxonomy provided; using generic embedded taxonomy.")
    system_prompt = build_system_prompt(taxonomy_text)

    # Load matches
    with open(args.matches, encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))
    print(f"Loaded {len(all_rows)} rows from {args.matches}")

    # Group by exchange_id — skip rows without one
    exchange_groups: dict[str, list[dict]] = {}
    for row in all_rows:
        xid = _safe(row.get("exchange_id"))
        if xid:
            exchange_groups.setdefault(xid, []).append(row)

    exchanges = [assemble_exchange(rows) for rows in exchange_groups.values()]
    exchanges.sort(key=lambda e: (e["date"], e["exchange_id"]))
    print(f"Assembled {len(exchanges)} exchanges")

    # Checkpoint
    checkpoint = args.out.with_suffix(".jsonl")
    already_done: dict[str, dict] = {}
    if args.resume and checkpoint.exists():
        with open(checkpoint, encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                already_done[rec["exchange_id"]] = rec
        print(f"Resuming: {len(already_done)} exchanges already annotated")

    if args.dry_run:
        exchanges = exchanges[:5]
        print("DRY RUN — processing first 5 exchanges only")

    todo = [e for e in exchanges if e["exchange_id"] not in already_done]
    est  = estimate_cost(len(todo), args.model)
    print(f"\nExchanges to process: {len(todo)}  |  Model: {args.model}")
    print(f"Estimated cost: ~${est:.2f}")

    if not args.dry_run and len(todo) > 5:
        try:
            ans = input("Proceed? [y/N] ").strip().lower()
        except EOFError:
            ans = "y"
        if ans != "y":
            print("Aborted.")
            sys.exit(0)

    client   = anthropic.Anthropic()
    lock     = threading.Lock()
    ckpt_fh  = open(checkpoint, "a", encoding="utf-8")
    counters = {"done": 0, "errors": 0,
                "input": 0, "output": 0, "cache_write": 0, "cache_read": 0}

    def process(exchange: dict) -> None:
        result = annotate_exchange(client, exchange, system_prompt, args.model, args.no_cache)
        usage  = result.pop("_usage", {})
        xid    = exchange["exchange_id"]

        with lock:
            counters["done"]        += 1
            counters["input"]       += usage.get("input_tokens", 0)
            counters["output"]      += usage.get("output_tokens", 0)
            counters["cache_write"] += usage.get("cache_creation_input_tokens", 0)
            counters["cache_read"]  += usage.get("cache_read_input_tokens", 0)

            if result.get("enrich_error"):
                counters["errors"] += 1
                print(f"  [{counters['done']}/{len(todo)}] {xid}  ERROR: {result['enrich_error']}", flush=True)
            else:
                intent  = result.get("question_intent", "?")
                quality = result.get("answer_quality", "?")
                ans     = result.get("answered")
                print(f"  [{counters['done']}/{len(todo)}] {xid}  {intent} → {quality} | answered={ans}", flush=True)

            ckpt_fh.write(json.dumps({"exchange_id": xid, **result}) + "\n")
            ckpt_fh.flush()
            already_done[xid] = result

    print(f"Running {args.workers} workers in parallel…\n", flush=True)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(process, e) for e in todo]
        for fut in as_completed(futures):
            fut.result()

    ckpt_fh.close()

    # Write output CSV — one row per exchange
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for exchange in exchanges:
            xid      = exchange["exchange_id"]
            enriched = already_done.get(xid, {k: "" for k in EXCHANGE_FIELDS})
            row      = {**exchange, **enriched}
            writer.writerow({k: row.get(k, "") for k in OUTPUT_COLUMNS})

    print(f"\nWrote {len(exchanges)} exchanges → {args.out}")

    # Cost summary
    p = PRICING.get(args.model, PRICING["claude-sonnet-4-6"])
    actual = (
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
    print(f"  Actual cost:        ${actual:.2f}")
    if counters["errors"]:
        print(f"  Errors:             {counters['errors']} exchanges (check enrich_error column)")


if __name__ == "__main__":
    main()
