"""
build_taxonomy.py — Use Claude to propose a parliamentary-discourse taxonomy
for case-study analysis, using a stratified sample of matched rows and
a structural template + optional reference taxonomy as design guides.

Usage:
    # Recommended: with template + org-function context
    python build_taxonomy.py \
        --matches  ../case_studies/GRDC/matches.csv \
        --template taxonomy_template.md \
        --out       ../case_studies/GRDC/taxonomy_proposal.md \
        --subject   GRDC \
        --subject-full-name "Grains Research and Development Corporation (GRDC)" \
        --org-description "a statutory R&D corporation funded by matching grower levies \\
            and government contributions, investing in grains research and extension \\
            across Australia"

    # With reference taxonomy only (legacy behaviour):
    python build_taxonomy.py \
        --matches  ../case_studies/WWF/matches.csv \
        --reference ../ARENA_Taxonomy_v1.1.md \
        --out       ../case_studies/WWF/taxonomy_proposal.md \
        --subject   WWF \
        --subject-full-name "World Wildlife Fund / WWF"

    # With both template and reference (template takes structural precedence):
    python build_taxonomy.py \
        --matches  ../case_studies/ARENA/matches.csv \
        --template taxonomy_template.md \
        --reference ../ARENA_Taxonomy_v1.1.md \
        --out       ../case_studies/ARENA/taxonomy_proposal.md \
        --subject   ARENA \
        --subject-full-name "Australian Renewable Energy Agency (ARENA)"

When --template is supplied, Claude follows the template's fixed field structure
(fields 1-7 universal) and replaces the [ORG-SPECIFIC] slots with fields derived
from the organisation's function and the sample rows. This produces consistent,
comparable schemas across case studies.

When --org-description is supplied, the prompt derives 4-6 analytical questions
appropriate to that organisation's function before designing the taxonomy, rather
than applying a generic advocacy-NGO question set. This ensures the schema captures
what actually matters for the organisation type (e.g. levy governance and research
priorities for GRDC vs campaign pressure and party alignment for an advocacy NGO).

Writes the taxonomy proposal to the output path and prints it.
"""

import argparse
import csv
import random
import sys
import textwrap
from pathlib import Path

import anthropic

csv.field_size_limit(10_000_000)

# ── Sampling ──────────────────────────────────────────────────────────────────

def stratified_sample(rows: list[dict], n_per_decade: int = 6, seed: int = 42) -> list[dict]:
    """
    Return a stratified sample of spoken (non-QoN) rows, ~n_per_decade per
    five-year band, covering diverse parties, chambers and in_gov values.
    """
    random.seed(seed)
    spoken = [r for r in rows if r.get("q_in_writing") == "0"]

    bands = {
        "1998-2002": [r for r in spoken if "1998" <= r["date"] <= "2002-12-31"],
        "2003-2007": [r for r in spoken if "2003" <= r["date"] <= "2007-12-31"],
        "2008-2012": [r for r in spoken if "2008" <= r["date"] <= "2012-12-31"],
        "2013-2017": [r for r in spoken if "2013" <= r["date"] <= "2017-12-31"],
        "2018-2025": [r for r in spoken if "2018" <= r["date"]],
    }

    sample: list[dict] = []
    seen: set[str] = set()
    for pool in bands.values():
        for r in random.sample(pool, min(n_per_decade, len(pool))):
            if r["match_id"] not in seen:
                seen.add(r["match_id"])
                sample.append(r)

    return sample


# ── Prompt construction ───────────────────────────────────────────────────────

def format_row(r: dict, max_body: int = 800, max_ctx: int = 300) -> str:
    """Format a single matched row for the prompt."""
    body = r.get("body", "")
    if len(body) > max_body:
        body = body[:max_body] + "…"

    ctx_before = r.get("context_before", "")
    if len(ctx_before) > max_ctx:
        ctx_before = ctx_before[:max_ctx] + "…"

    ctx_after = r.get("context_after", "")
    if len(ctx_after) > max_ctx:
        ctx_after = ctx_after[:max_ctx] + "…"

    lines = [
        f"match_id: {r['match_id']}",
        f"date: {r['date']}  chamber: {r['chamber']}  party: {r['party']}  "
        f"in_gov: {r['in_gov']}  is_question: {r['is_question']}  is_answer: {r['is_answer']}",
        f"speaker: {r.get('speaker_name','')}  gender: {r.get('gender','')}",
        f"debate_heading: {r.get('debate_heading','(none)')}",
        f"body:\n{textwrap.fill(body, width=100, initial_indent='  ', subsequent_indent='  ')}",
    ]
    if ctx_before:
        lines.append(f"context_before: {ctx_before[:200]}")
    if ctx_after:
        lines.append(f"context_after: {ctx_after[:200]}")

    return "\n".join(lines)


def build_prompt(sample: list[dict], reference_taxonomy: str = "",
                 subject: str = "WWF",
                 subject_full: str = "World Wildlife Fund / WWF",
                 org_description: str = "",
                 taxonomy_template: str = "") -> str:
    rows_text = "\n\n---\n\n".join(format_row(r) for r in sample)

    # Org-function context block — inserted before the analytical questions section.
    # When a description is provided, Claude derives function-appropriate questions;
    # otherwise a generic fallback set is used.
    if org_description.strip():
        function_block = f"""## Organisation function

{subject_full} ({subject}) is: {org_description.strip()}

Before designing the taxonomy, reason about what kinds of parliamentary mentions this
organisation type typically attracts and what analytical questions would be most useful
for a researcher studying its parliamentary salience. For example:
- A statutory funding/R&D body will attract mentions about appropriations, levy governance,
  research priorities, and industry co-investment — not about protest or advocacy.
- An advocacy NGO will attract mentions about lobbying influence, campaign pressure, and
  alignment with party values.
- A peak industry body will attract mentions about industry conditions, trade, and
  government consultation.

Derive 4–6 analytical questions that are specifically appropriate to the function of
{subject}, grounded in what the sample rows reveal about how it is actually discussed
in parliament. State these questions explicitly before designing the taxonomy, then build
the taxonomy to answer them."""
    else:
        function_block = """## What the analysis needs to support

The goal is to answer questions like:
- How is {subject} framed by different parties over time — as a credible institution, a policy
  instrument, or a political target?
- Does government membership (`in_gov`) predict a more critical or dismissive framing?
- What policy domains trigger {subject} citations, and have those shifted over the years?
- Is {subject} the central subject of the speech turn, or an incidental reference?
- What rhetorical work is the mention doing — supporting a policy call, attacking an opponent,
  citing evidence, deflecting?""".format(subject=subject)

    # Build the structural guidance block — template takes precedence over reference.
    if taxonomy_template.strip():
        template_section = f"""## Structural template (follow this exactly)

The template below defines the **required output structure** for this taxonomy. You must:
1. Keep fields 1–7 as defined (substituting `{{SUBJECT}}` → `{subject}` and
   `{{subject}}` → `{subject.lower()}` throughout).
2. Replace all `[ORG-SPECIFIC: ...]` slots with content derived from the analytical questions
   above and the sample rows. Choose controlled vocabulary values that reflect how {subject}
   is *actually* discussed in parliament, not generic placeholders.
3. Replace `[field_8_name]`, `[field_9_name]`, `[field_10_name]` slots with real field names
   and definitions appropriate to {subject}'s function.
4. Populate the two example records using actual rows from the sample below.
5. Preserve all six design rules verbatim.

{taxonomy_template}"""
        if reference_taxonomy.strip():
            template_section += f"""

---

## Additional reference (design philosophy — do not copy fields)

{reference_taxonomy}"""
    elif reference_taxonomy.strip():
        template_section = f"""## Reference taxonomy (design philosophy only — fields must be replaced)

You should borrow its design philosophy — lean schema, extractable vs inferred distinction,
null-when-unclear discipline, ~60% fill-rate threshold — but the specific fields are entirely
wrong for this use case and must be replaced.

{reference_taxonomy}"""
    else:
        template_section = ""

    return f"""You are a political-science researcher designing a lean, rigorous annotation schema for
parliamentary discourse analysis.

## Task

You have been given:
1. A **structural template** (or reference taxonomy) defining the schema format and universal fields.
2. A **sample of parliamentary speech turns** from Australian Hansard (1998–2025) that mention
   {subject_full} ({subject}). These have already been matched and extracted; you are designing
   the schema that will be applied to each row to enable quantitative and narrative analysis.

{function_block}

## What to produce

Design a taxonomy with:
- **8–12 core fields** (including a mandatory `what_happened` summary field)
- Each field marked `extractable` or `inferred`
- Controlled vocabularies with 4–8 values where possible
- Clear rules for when to leave a field null
- A note on expected fill rate for each field
- Brief guidance on what "good" vs "bad" values look like for the trickiest fields

Also identify 2–3 optional overlay fields that would add value but cannot be reliably filled
for the full corpus.

Do not include fields that merely restate the existing CSV metadata (date, party, in_gov, etc.)
— those are already present. The taxonomy fields should add analytic value that is not already
in the structured metadata.

---

{template_section}

---

## Sample rows ({len(sample)} rows from the {subject} case study)

{rows_text}

---

## Output format

Write the taxonomy as a markdown document with:
- A brief statement of purpose and design rules (follow the template's six rules verbatim)
- The 4–6 analytical questions you derived (if org-function was provided)
- Each field as a numbered section with: name, type (extractable/inferred), purpose, allowed
  values, rule, and expected fill rate
- Two example records applying all fields to sample rows above
- A brief rationale (2–3 sentences) for any field you considered but excluded
- A summary table
"""


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Build a taxonomy proposal via Claude API.")
    ap.add_argument("--matches",   type=Path, required=True)
    ap.add_argument("--template",  type=Path, default=None,
                    help="Path to taxonomy_template.md. When provided, Claude follows its "
                         "fixed field structure (fields 1-7) and fills in the [ORG-SPECIFIC] "
                         "slots. Recommended for new case studies. Takes structural precedence "
                         "over --reference.")
    ap.add_argument("--reference", type=Path, default=None,
                    help="Path to a reference taxonomy (legacy). Design philosophy is borrowed "
                         "but all fields are replaced. If --template is also provided, the "
                         "reference is included as secondary context only.")
    ap.add_argument("--out",       type=Path, required=True)
    ap.add_argument("--model",            default="claude-opus-4-6")
    ap.add_argument("--seed",             type=int, default=42)
    ap.add_argument("--subject",          default="WWF",
                    help="Short name of the subject organisation (default: WWF)")
    ap.add_argument("--subject-full-name", default="World Wildlife Fund / WWF",
                    help="Full name of the subject organisation")
    ap.add_argument("--org-description",   default="",
                    help="One or two sentences describing the organisation's function and "
                         "institutional role. When provided, the prompt derives analytical "
                         "questions appropriate to that function rather than using a generic "
                         "advocacy-NGO question set. Example: 'a statutory R&D corporation "
                         "funded by matching grower levies and government contributions, "
                         "investing in grains research and extension across Australia.'")
    args = ap.parse_args()

    if args.template is None and args.reference is None:
        ap.error("At least one of --template or --reference must be provided.")

    # Load data
    with open(args.matches, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {len(rows)} rows from {args.matches}")

    template_text = ""
    if args.template is not None:
        template_text = args.template.read_text(encoding="utf-8")
        print(f"Loaded taxonomy template from {args.template}")

    reference = ""
    if args.reference is not None:
        reference = args.reference.read_text(encoding="utf-8")
        print(f"Loaded reference taxonomy from {args.reference}")

    sample = stratified_sample(rows, seed=args.seed)
    print(f"Sample size: {len(sample)} rows")

    # Build prompt
    prompt = build_prompt(sample, reference,
                          subject=args.subject,
                          subject_full=args.subject_full_name,
                          org_description=args.org_description,
                          taxonomy_template=template_text)
    print(f"\nPrompt length: {len(prompt):,} chars")
    print(f"Calling {args.model}…\n")

    # Call Claude
    client = anthropic.Anthropic()
    message = client.messages.create(
        model=args.model,
        max_tokens=8192,
        messages=[{"role": "user", "content": prompt}],
    )

    result = message.content[0].text

    # Write output
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(result, encoding="utf-8")

    print(result)
    print(f"\n\nTaxonomy proposal written to {args.out}")
    print(f"Input tokens: {message.usage.input_tokens:,}  Output tokens: {message.usage.output_tokens:,}")


if __name__ == "__main__":
    main()
