# ARENA Delivery Insight Taxonomy v1.1
**Lean extraction schema for project-delivery insights from ARENA knowledge documents**

---

## Purpose

This taxonomy is designed to extract **project-delivery-relevant knowledge** from ARENA knowledge-bank documents.

It is deliberately biased toward:
- delivery characteristics
- reference-class learning
- failure and delay patterns
- outcome patterns
- future transferability for similar projects

It is **not** primarily a technical-topic taxonomy.

---

## Design rules

1. **Lean beats complete.**
   Use 12 high-fill fields rather than a larger sparse schema.

2. **Preserve raw metadata separately.**
   Keep CSV/source metadata in an ingest layer, not in the core analytic schema.

3. **Separate facts from inference.**
   Every field must be labeled as either:
   - `extractable` — directly stated in source metadata or text
   - `inferred` — assigned by analyst/model from available evidence

4. **Only infer when confidence is medium or high.**
   If the document does not support a field clearly enough, leave it null.

5. **Optimise for reference-class retrieval.**
   The schema must support retrieval across:
   - project type
   - scale
   - phase
   - proponent type
   - delay category
   - failure mode
   - outcome

6. **Do not design for sparse information.**
   If a field cannot be populated in at least ~60% of documents, it should not be core.

---

## Ingest envelope (preserved, not part of the 12 core fields)

Retain these raw fields exactly as supplied by source metadata:

- `source_title_raw`
- `source_type_raw`
- `publish_date_raw`
- `project_name_raw`
- `category_raw`
- `project_status_raw`
- `year_raw`
- `source_url_raw` (if available)

These are not analytical fields. They are provenance and normalization support.

---

## Core analytic schema (12 fields)

### 1. `record_id`
**type:** extractable / system-generated  
**purpose:** stable unique identifier

### 2. `source_title`
**type:** extractable  
**purpose:** normalized source title for citation and traceability

### 3. `publish_date`
**type:** extractable  
**purpose:** publication date or best available source date

### 4. `project_name`
**type:** extractable  
**purpose:** canonical project name

### 5. `what_happened`
**type:** extractable  
**purpose:** one- or two-sentence neutral summary of the delivery-relevant event, lesson, issue, or result

**rule:**  
State the event in delivery language, not promotional language.

---

### 6. `project_type`
**type:** inferred  
**Flyvbjerg dimension:** project type  
**purpose:** reference-class grouping by delivery archetype

**allowed values:**
- generation
- storage
- network / grid
- DER / customer-side
- transport electrification
- industrial decarbonisation
- manufacturing / supply chain
- software / data / digital
- enabling infrastructure
- multi-technology / hybrid

**rule:**  
Classify by the dominant delivery object, not by every technology mentioned.

---

### 7. `project_scale_band`
**type:** inferred  
**Flyvbjerg dimension:** scale  
**purpose:** normalize projects for reference-class comparison

**allowed values:**
- lab / bench
- pilot
- demonstration
- first commercial / FOAK
- commercial expansion
- utility / large-scale
- programmatic / portfolio-level

**rule:**  
Use the smallest reliable scale band supported by the document.  
Do not force exact MW / MWh / $ values into the core schema.

---

### 8. `lifecycle_phase`
**type:** inferred  
**Flyvbjerg dimension:** phase  
**purpose:** identify where the insight arose in the project lifecycle

**allowed values:**
- concept / feasibility
- development / design
- approvals / contracting
- procurement
- construction / installation
- commissioning / integration
- operations
- variation / re-scope
- close-out / post-project review

**rule:**  
Choose the phase in which the main issue or insight occurred, not necessarily the project’s overall current phase.

---

### 9. `proponent_type`
**type:** inferred  
**Flyvbjerg dimension:** proponent type  
**purpose:** support comparison by delivery actor

**allowed values:**
- project developer
- utility / energy retailer
- network business
- industrial operator
- fleet / logistics operator
- manufacturer / OEM
- technology vendor
- research organisation / university
- consortium / multi-party venture
- government / public-sector body
- community / local body

**rule:**  
Use the lead delivery actor, not every participant.

---

### 10. `delay_category`
**type:** inferred  
**Flyvbjerg dimension:** delay category  
**purpose:** normalize schedule drag into reusable categories

**allowed values:**
- no material delay stated
- approvals / regulatory
- grid connection / system studies
- procurement / supply chain
- financing / commercial close
- construction / installation
- commissioning / integration
- data / validation / testing
- stakeholder / land / community
- internal governance / resourcing

**rule:**  
Use only when delay is a meaningful part of the lesson.  
If timing friction is implied but not clear, leave null.

---

### 11. `failure_mode`
**type:** inferred  
**Flyvbjerg dimension:** failure mode  
**purpose:** support retrieval of what actually went wrong

**allowed values:**
- no major failure stated
- technical underperformance
- integration failure
- schedule slippage
- cost overrun
- resource / capability shortfall
- commercial / demand failure
- regulatory misfit
- data quality / measurement failure
- design assumption failure
- governance / coordination failure

**rule:**  
Capture the dominant failure mode only.  
If the document presents a positive lesson with no failure, use `no major failure stated`.

---

### 12. `outcome_class`
**type:** inferred  
**Flyvbjerg dimension:** outcome  
**purpose:** classify the project-level result of the event or lesson

**allowed values:**
- successful demonstration
- partial success
- delayed but recoverable
- re-scoped / adapted
- knowledge generated despite setback
- discontinued / not progressed
- follow-on scale-up enabled
- policy / market influence only

**rule:**  
This is not a moral judgment.  
It is a compact description of what the project event ultimately produced.

---

## Field population rules

### Must always be populated
- `record_id`
- `source_title`
- `publish_date`
- `project_name`
- `what_happened`

### Populate when evidence supports medium confidence or better
- `project_type`
- `project_scale_band`
- `lifecycle_phase`
- `proponent_type`
- `delay_category`
- `failure_mode`
- `outcome_class`

### Leave null when unclear
Do not guess.

---

## What is deliberately excluded from the core schema

The following are useful, but should be optional overlays rather than core fields unless a fill-rate audit proves they exceed ~60%:

- exact technology subtype
- geography
- exact MW / MWh / $ scale
- barrier text
- enabler text
- recommendation text
- evidence excerpt
- transferability score
- policy relevance
- impact tags
- tradeoff

These can be added later as:
- optional overlay fields, or
- derived tags

---

## Minimal optional overlay layer

Only add these after validating fill rates.

### `technology_domain`
High-value retrieval field, but secondary to delivery structure.

**allowed values:**
- battery storage
- hydrogen
- solar PV
- solar thermal
- wind
- DER
- demand response
- EV
- bioenergy
- industrial renewables
- grid / system stability
- hybrid systems
- pumped hydro
- other

### `evidence_excerpt`
Short quote or data point grounding the record.

### `confidence_note`
Optional note when classification was difficult.

---

## Extraction guidance

### Good `what_happened`
“Grid connection approval took 14 months and required repeated resubmission due to unclear technical requirements.”

### Bad `what_happened`
“The project experienced important learnings in relation to stakeholder engagement and successful future deployment.”

---

## Example record

```yaml
record_id: ARENA-DLV-0001
source_title: "Origin - Mortlake Power Station Battery Project - Lessons Learnt No.1"
publish_date: 2024-08-15
project_name: "Mortlake Power Station Battery Project"
what_happened: "Grid connection approval took 14 months and required repeated resubmission due to unclear technical requirements."

project_type: storage
project_scale_band: first commercial / FOAK
lifecycle_phase: approvals / contracting
proponent_type: utility / energy retailer
delay_category: grid connection / system studies
failure_mode: schedule slippage
outcome_class: delayed but recoverable