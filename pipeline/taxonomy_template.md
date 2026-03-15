# Parliamentary Mention Taxonomy — {ORG} (v2.0)

**Lean extraction schema for analysing how {ORG} is referenced in Australian parliamentary discourse**

Instantiate by replacing `{ORG}` throughout and customising `org_framing` and `policy_domain` allowed values for the subject.

Compatible with both enrichment pipelines:
- `enrich_case_study.py` — row-level (one enriched row per speech turn)
- `enrich_exchanges.py` — exchange-level (one enriched row per Q&A exchange)

---

## Purpose

This taxonomy extracts **analytic value from parliamentary speech turns or Q&A exchanges that mention {ORG}**, enabling quantitative and narrative analysis of:
- how {ORG} is framed by different political actors over time
- the rhetorical function of {ORG} citations
- the policy domains in which {ORG} appears
- the centrality and valence of {ORG} mentions within broader parliamentary argument
- patterns of accountability scrutiny versus routine institutional reference

[CUSTOMISE: add 2–3 sentences describing {ORG}'s institutional context — what it does, who it matters to, what kind of parliamentary attention it receives.]

---

## Design rules

These rules govern how every field in this taxonomy is populated. Read them before annotating.

**1. Lean beats complete.**
The schema uses a fixed set of high-fill fields rather than a larger sparse schema. Every field must earn its place through analytic utility and achievable fill rate. Do not add fields not defined here.

**2. Raw metadata is preserved separately.**
Existing ingest metadata (`match_id`, `date`, `chamber`, `party`, `in_gov`, `is_question`, `is_answer`, `speaker_name`, `gender`, `debate_heading`, `body`) lives in the ingest layer and is not duplicated in taxonomy fields. Do not re-state metadata values as taxonomy annotations.

**3. Separate facts from inference.**
Every field is labelled with one of two types:
- `extractable` — the value is directly stated or determinable from the text without interpretive judgement. If the text says it, extract it.
- `inferred` — the value requires interpretive judgement based on the available evidence. Apply the confidence rule below.

**4. Only infer at medium confidence or better.**
For `inferred` fields: if the text does not support the field value clearly enough to assign it with at least medium confidence, leave it `null`. A null is correct and useful; a low-confidence guess is misleading. When in doubt, null.

For `extractable` fields: if the information is simply absent from the text, leave it `null`. Do not infer what is not stated.

**5. Optimise for comparative retrieval.**
The schema must support cross-tabulation across: policy domain, framing type, rhetorical function, centrality, valence, and speech act. Allowed values for each field are chosen to enable aggregation — they must be mutually exclusive and collectively exhaustive for the expected content. Do not use free text where a controlled value is defined.

**6. ~60% fill rate is the core field threshold.**
Fields in this schema are expected to be populated in at least ~60% of rows. Fields that cannot reach this threshold are optional overlays, not core fields. If you cannot determine a core field's value, use `null` — do not force a value to maintain apparent completeness.

**Null discipline:**
- Use JSON `null` (not the string `"null"`) for any field that cannot be determined.
- `null` means: the text does not support this field at medium confidence or better.
- Never fabricate a value to avoid a null.

---

## Schema overview

Two layers. For row-level enrichment, only Layer 1 is populated. For exchange-level enrichment, both layers are populated.

**Layer 1 — Universal fields (10):** apply to any speech turn or Q&A exchange.
**Layer 2 — Exchange overlay (6):** NULL for non-Q&A rows. Only meaningful when there is a question and an answer to compare.

---

## Layer 1: Universal fields

---

### 1. `summary`
**Applies to:** any speech turn or exchange
**Type:** extractable

One or two neutral sentences: what was said, and the role the {ORG} mention plays.

For a **speech turn**: what the speaker did with the mention — what claim they made, what argument they advanced, and how {ORG} figured in that. State the *function* of the mention, not just that it occurred ("The speaker cited {ORG}'s 2014 funding to argue the government was dismantling the clean energy pipeline" not "{ORG} was mentioned in a speech about energy").

For an **exchange**: the question's intent and the answer's substance (or evasion) as a single narrative.

**Fill rule:** mandatory. Always populate.

**Good:** "Speaker cited {ORG}'s investment mandate to argue that the government's funding cuts would cripple the clean energy pipeline, positioning {ORG} as a critical public institution."
**Bad:** "Speaker talked about energy and mentioned {ORG}." (too vague — does not state the argumentative function)
**Bad:** "Speaker bravely defended {ORG}." (adopts the speaker's framing rather than describing it neutrally)

---

### 2. `policy_domain`
**Applies to:** any speech turn or exchange
**Type:** extractable

The primary policy area when {ORG} is mentioned. For exchanges, classify the domain of the *question*, not any topic the minister shifts to.

**[CUSTOMISE: replace with topic-specific domain values. Example structure below — delete or adapt as needed.]**

- `[org]_core_mandate` — {ORG}'s own programs, funding decisions, or operations
- `governance_and_accountability` — {ORG}'s governance, board, financial accountability
- `budget_and_appropriations` — appropriations debates where {ORG} appears incidentally
- `legislation` — bills and motions citing {ORG}
- `[related_policy_area_1]` — [define]
- `[related_policy_area_2]` — [define]
- `other` — procedural or unrelated mentions

**Fill rule:** extractable; populate in ~95% of rows. Use `other` for purely procedural mentions. Leave `null` only for completely fragmentary turns with no discernible policy content.

**Common error:** defaulting to the most general domain when a more specific one is available. Use the most specific domain that accurately describes the passage where {ORG} is mentioned.

---

### 3. `mention_centrality`
**Applies to:** any speech turn or exchange
**Type:** inferred

How central is {ORG} to the speech or exchange?

- `focal` — {ORG} is a primary subject; the speaker(s) devote substantial attention to {ORG} itself
- `supporting` — {ORG} cited as evidence or example to support an argument about something else; more than a passing mention but not the main topic
- `incidental` — {ORG} appears in a list, a procedural context, or a brief aside; removing the mention would not materially change the speech

For exchanges: judge from the question's framing. A question that squarely targets {ORG} is `focal` even if the answer shifts topic.

**Fill rule:** inferred; populate in ~98% of rows (evidence for centrality is almost always present).

---

### 4. `org_framing`
**Applies to:** any speech turn or exchange
**Type:** inferred

How the speaker(s) characterise {ORG} as an institution. The central field for organisational framing analysis.

For exchanges: code the dominant framing across the exchange. If question and answer frame {ORG} differently, code the question's framing and note the tension in `summary`.

**[CUSTOMISE: replace with values appropriate to the organisation's institutional role. Example structure below — delete or adapt.]**

- `public_institution` — presented as a legitimate, effective public body fulfilling its mandate
- `policy_instrument` — valued instrumentally as a tool for achieving policy goals
- `accountability_target` — the subject of scrutiny; decisions, expenditure, or governance questioned
- `governance_failure` — institutional functioning impaired or neglected
- `sector_exemplar` — cited as a positive example of a model or sector capability
- `political_football` — invoked primarily for political attack or defence, independent of its activities
- `neutral_reference` — named factually without evaluative framing (lists, schedules, tabling)

**Fill rule:** inferred; target ~88% fill. Leave `null` if the mention is too brief or opaque to determine framing at medium confidence. Do not force a framing value for incidental mentions.

---

### 5. `speaker_valence`
**Applies to:** any speech turn or exchange
**Type:** inferred

For a **speech turn**: the speaker's evaluative stance toward {ORG}.
For an **exchange**: the *questioner's* evaluative stance. The minister's stance is captured separately in `minister_valence` (field 12).

- `positive` — explicitly or clearly implicitly endorses, praises, or defends {ORG}
- `neutral` — no clear evaluative stance; factual or dispassionate mention
- `negative` — criticises, questions, dismisses, or attacks {ORG}'s decisions or legitimacy
- `mixed` — both positive and negative elements present; valence shifts within the turn
- `instrumental` — uses {ORG} to support a broader argument without clear approval or disapproval of {ORG} itself

**Fill rule:** inferred; target ~85%. Assess the speaker's stance toward {ORG} *specifically*, not toward the policy under debate. A speaker may oppose a bill overall while referencing {ORG} neutrally. Leave `null` if valence is genuinely indeterminate.

---

### 6. `rhetorical_function`
**Applies to:** any speech turn or exchange
**Type:** inferred

What argumentative work is the {ORG} mention doing?

For a **speech turn**: the function in the speaker's argument.
For an **exchange**: the function in the *minister's answer* — how does the minister use (or avoid) the {ORG} mention. The questioner's function is captured by `question_intent` (field 11).

- `evidence_citation` — {ORG}'s data, decisions, or activities cited as factual evidence for a claim
- `authority_endorsement` — {ORG}'s standing or involvement invoked to legitimise a position
- `deflection_rebuttal` — {ORG} invoked to rebut a criticism or deflect scrutiny
- `policy_justification` — {ORG} cited to justify a specific policy action or legislative measure
- `attack_opponent` — {ORG} invoked to criticise or embarrass a political opponent
- `coalition_building` — {ORG} named among a set of supporters to demonstrate breadth
- `alibi` — {ORG}'s genuine achievements cited as cover for a specific unaddressed failure (distinct from `deflection_rebuttal`: the minister concedes nothing and pivots to adjacent success)
- `incidental_mention` — procedural or list context; no argumentative function

For exchanges: leave `null` if the minister's answer contains no {ORG} mention.

**Fill rule:** inferred; target ~90%. Code the function of the {ORG} mention specifically, not the overall speech purpose. A speech may attack the government generally but the {ORG} mention within it might function as `evidence_citation`.

**Key distinction — `authority_endorsement` vs `evidence_citation`:** Use `evidence_citation` when specific {ORG}-funded projects, data, or outcomes are cited. Use `authority_endorsement` when the speaker invokes {ORG}'s institutional standing without citing specific evidence.

---

### 7. `target_of_speech`
**Applies to:** any speech turn or exchange
**Type:** inferred

Who or what the speaker is primarily directing the speech at. For exchanges, classify based on the question.

- `government_policy` — addressing, defending, or criticising specific government policy
- `opposition_party` — attacking or responding to the opposition
- `org_institution` — directly addressing {ORG}'s own decisions, operations, or governance
- `minister_or_official` — holding a minister or official accountable for decisions affecting {ORG}
- `legislative_chamber` — procedural context: committee referrals, scheduling, tabling
- `general_advocacy` — making a general case without a specific parliamentary target

**Fill rule:** inferred; target ~90%. Leave `null` for genuinely fragmentary turns (stage directions, truncated text) where the target cannot be assessed.

---

### 8. `policy_action_urged`
**Applies to:** any speech turn or exchange
**Type:** inferred

Whether and in what direction the speaker urges policy action. For exchanges, based on what the *question* demands or implies — not what the minister offers.

- `strengthen_accountability` — greater accountability, transparency, or oversight urged
- `increase_funding` — more resources or investment in {ORG} or its domain urged
- `reduce_or_abolish` — cut funding, wind up, or diminish {ORG}'s role
- `defend_status_quo` — existing policy settings or {ORG}'s operations defended
- `reform_model` — structural change to governance, legislation, or the model
- `no_clear_action` — descriptive, procedural, or incidental; no policy direction evident

**Fill rule:** inferred; target ~80%. Use `no_clear_action` — not `null` — when the speech is clearly substantive but advocates no specific direction. Leave `null` only for genuinely procedural or fragmentary turns.

---

### 9. `org_name_form`
**Applies to:** any speech turn or exchange
**Type:** extractable

Which name variant(s) the speaker uses. Strict text extraction only — do not infer from context.

**[CUSTOMISE: define the acronym and full name.]**

- `acronym` — short form only (e.g., "{ORG_ACRONYM}")
- `full_name` — full name only (e.g., "{ORG_FULL_NAME}")
- `mixed` — both forms used in the same turn

**Fill rule:** extractable; target ~98%. If neither form appears (e.g., the row was pulled in by Q&A pairing and does not itself mention {ORG}), leave `null`.

---

### 10. `notable_quote`
**Applies to:** any speech turn or exchange
**Type:** extractable

The single most analytically or rhetorically significant verbatim phrase from the text.

For a **speech turn**: the most revealing phrase about how the speaker frames {ORG}.
For an **exchange**: the most revealing phrase from either side — whichever is more analytically significant.

Choose for: evasive formulations, explicit value judgements, memorable rhetoric, factual admissions. Extract verbatim — do not paraphrase. Leave `null` if nothing stands out.

**Fill rule:** extractable; target ~75%. Do not force a quote if no phrase is analytically distinctive.

---

## Layer 2: Exchange overlay (NULL for non-Q&A rows)

These fields are only populated when the enrichment unit is a Q&A exchange. Leave all as `null` for single speech turn enrichment.

---

### 11. `question_intent`
**Applies to:** exchanges only
**Type:** inferred

The political intent of the question — what the questioner is trying to achieve.

- `factual` — seeking specific information; no evident political framing
- `accountability` — pressing the minister to justify a decision or outcome; moderate adversarial tone
- `hostile` — directly attacking the minister, government, or {ORG}; strongly adversarial
- `supportive` — inviting the minister to speak positively (Dorothy Dixer)
- `rhetorical` — making a political point through the question rather than seeking information
- `clarifying` — following up on a previous answer or statement

**Fill rule:** inferred; target ~98% for exchanges. The question's intent is almost always determinable.

---

### 12. `minister_valence`
**Applies to:** exchanges only
**Type:** inferred

The minister's evaluative stance toward {ORG} in their answer. Counterpart to `speaker_valence` (field 5) which captures the questioner's stance.

Same allowed values as `speaker_valence`: `positive` / `neutral` / `negative` / `mixed` / `instrumental`

**Fill rule:** inferred; target ~80% for exchanges. Leave `null` if the minister's answer contains no {ORG} reference.

---

### 13. `answer_quality`
**Applies to:** exchanges only
**Type:** inferred

How substantively the minister's answer addressed what was asked.

- `substantive` — directly addressed the question with specific information
- `partial` — addressed some but not all of the question; or answered in general terms where specifics were sought
- `deflection` — acknowledged the question but redirected to a different topic or frame
- `non_answer` — made no attempt to address the question

**Fill rule:** inferred; target ~98% for exchanges.

---

### 14. `answered`
**Applies to:** exchanges only
**Type:** inferred

Boolean — did the minister actually address what was asked?

- `true` — the minister substantively addressed the question's substance
- `false` — the minister did not substantively address the question

`true` corresponds to `substantive` or well-executed `partial` answer quality. `false` corresponds to `deflection` or `non_answer`. When `partial`, apply judgement: did the minister address the core of what was asked, even if incompletely?

**Fill rule:** inferred; mandatory for exchanges. Always populate.

---

### 15. `evasion_technique`
**Applies to:** exchanges only
**Type:** inferred

If `answered` is false, the technique the minister used. Must be `null` when `answered` is true.

- `topic_shift` — pivoted to a related but different subject
- `false_premise` — rejected the question's framing rather than answering it
- `general_commitment` — responded with vague future intentions instead of specific information
- `procedural` — redirected to process (committee, review, upcoming inquiry)
- `humour` — deflected with a joke or rhetorical flourish
- `null` — answered is true; no evasion occurred

**Fill rule:** inferred; target ~95% when answered is false. Leave `null` only when the evasion technique cannot be categorised above.

---

### 16. `{org}_mentioned_in`
**Applies to:** exchanges only
**Type:** pre-computed (not LLM-classified)

Which side of the exchange contains the {ORG} mention that triggered the match. Computed before enrichment from the `match_source` column.

- `question` — the direct match is in the question only
- `answer` — the direct match is in the answer only
- `both` — direct matches on both sides

**Fill rule:** pre-computed; 100% for exchanges.

---

## Field population summary

### Must always be populated
- `summary` (both row-level and exchange)
- `answered` (exchanges only)

### Populate when evidence supports medium confidence or better
All other fields. Medium confidence means: if asked to justify the classification, you could point to specific text. If you cannot, use `null`.

### Use null when
- The text does not support the field at medium confidence
- The turn is too fragmentary (stage directions, truncated body, omnibus procedural answers)
- For `extractable` fields: the information is simply absent from the text
- For exchange overlay fields: the row is not a Q&A exchange

### Never
- Guess to avoid a null
- Re-state metadata (date, party, speaker) as taxonomy field values
- Use the string `"null"` — use JSON `null`
- Use free text where a controlled value is defined

---

## Optional overlay fields

Fields with expected fill rate below ~60%, or requiring external knowledge. Not part of the core 16. Add as columns after validating fill rates.

### `org_specific_source`
**Type:** extractable
**Purpose:** The specific {ORG} project, grant round, publication, or spokesperson named.
**Why overlay:** Many mentions are generic with no specific source cited. Expected fill rate ~25–40%.

### `co_cited_organisations`
**Type:** extractable
**Purpose:** Other organisations named alongside {ORG} in the same turn. Enables network analysis of how {ORG} is grouped with peers.
**Why overlay:** Requires entity extraction. Variable fill rate depending on turn length (~40–50%).

---

## Mapping from existing schemas

### From ARENA parliamentary taxonomy v1.0 (case_studies/ARENA/taxonomy_proposal.md)

| ARENA v1.0 field | Unified field | Notes |
|---|---|---|
| `what_happened` | `summary` | Renamed |
| `policy_domain` | `policy_domain` | Kept; ARENA-specific domain values customised |
| `arena_framing` | `org_framing` | Name generalised; ARENA-specific values customised |
| `rhetorical_function` | `rhetorical_function` | Kept; added `alibi` value |
| `mention_valence` | `speaker_valence` | Renamed; added `mixed` value (was already in ARENA v1.0) |
| `mention_centrality` | `mention_centrality` | Kept; `central` renamed `focal` |
| `speech_act_type` | — | Removed from core; captured by `question_intent` + `rhetorical_function` |
| `target_of_criticism` | `target_of_speech` | Generalised; not only criticism |
| `arena_claim_type` | — | Moved to optional overlay as `org_specific_source` |
| `debate_topic_normalised` | `policy_domain` | Replaced by structured domain field |
| — | `policy_action_urged` | New |
| — | `org_name_form` | New |
| — | `notable_quote` | New |

### From GRDC parliamentary taxonomy (case_studies/GRDC/taxonomy_proposal.md)

| GRDC field | Unified field | Notes |
|---|---|---|
| `what_happened` | `summary` | Renamed |
| `policy_domain` | `policy_domain` | Kept |
| `mention_centrality` | `mention_centrality` | Kept |
| `grdc_framing` | `org_framing` | Name generalised |
| `speaker_valence_toward_grdc` | `speaker_valence` | Name generalised; added `mixed` value |
| `rhetorical_function` | `rhetorical_function` | Kept |
| `target_of_speech` | `target_of_speech` | Kept |
| `policy_action_urged` | `policy_action_urged` | Kept |
| `grdc_name_form` | `org_name_form` | Name generalised |
| `other_organisations_mentioned` | `co_cited_organisations` (overlay) | Moved to optional overlay |

---

## Instantiation checklist

1. Copy this file, rename to `taxonomy_{org_name}.md`
2. Replace `{ORG}` throughout with the organisation name/acronym
3. Set `{ORG_ACRONYM}` and `{ORG_FULL_NAME}` in `org_name_form`
4. Customise the Purpose section (2–3 sentences on institutional context)
5. Customise `policy_domain` allowed values for the organisation's subject area
6. Customise `org_framing` allowed values for the organisation's institutional character
7. Add 2–3 example records at the end (one focal/negative, one incidental, one exchange if applicable)
8. Pass to `enrich_case_study.py --taxonomy` (row-level) or `enrich_exchanges.py --taxonomy` (exchange-level)
