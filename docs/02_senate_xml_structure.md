# Senate Hansard XML Structure

> **Status:** Reference document — implemented. The Senate parser (`pipeline/03_parse.py`) handles all three format eras documented here (v2.0, v2.1, and v2.2). The 1,482 XML files in `data/raw/senate/` span 1998-03-02 to 2025-11-27. Format 1 (pre-1998 uppercase/legacy) remains outside project scope.

Source: https://github.com/wragge/hansard-xml (covers 1901–2005); APH ParlInfo for 2006–present.

---

## Format eras

| Era | Years | File naming | Schema |
|---|---|---|---|
| **Format 1 — Uppercase/Legacy** | 1981–1997 | `senate_YYYY-MM-DD.xml` | `<!DOCTYPE hansard PUBLIC "-//PARLINFO//DTD HANSARD STORAGE//EN" []>` |
| **Format 2 — Lowercase v2.0** | ~1998–2003 | Numeric IDs (e.g. `NNNN-N.xml`) | `<hansard version="2.0" xsi:noNamespaceSchemaLocation="...">` |
| **Format 3 — Lowercase v2.1** | 2004–2005+ | `YYYYMMDD_senate_P_S_vN.xml` | `<hansard version="2.1" xsi:noNamespaceSchemaLocation="...">` |

The target range for this project is **1998 to present**, which means:
- 1998–2005: Format 2/3 from the GLAM Workbench harvest
- 2006–present: Format 2/3 downloaded directly from APH ParlInfo (same schema, continuing evolution)

Format 1 (pre-1998) is documented below for completeness but is **outside the project scope**.

---

## Format 2/3 — Lowercase (1998–present, target format)

### Root element

```xml
<?xml version="1.0" encoding="UTF-8"?>
<hansard xsi:noNamespaceSchemaLocation="../../hansard.xsd" version="2.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
```

- No namespace on elements; schema location is a relative path
- `version="2.0"` for ~1998–2003; `version="2.1"` for 2004–2005+
- No DOCTYPE declaration

### Session header

```xml
<session.header>
  <date>2000-02-15</date>           <!-- ISO 8601: YYYY-MM-DD -->
  <parliament.no>39</parliament.no>
  <session.no>1</session.no>
  <period.no>5</period.no>
  <chamber>SENATE</chamber>         <!-- always uppercase -->
  <page.no>11735</page.no>          <!-- opening Hansard page number -->
  <proof>0</proof>                  <!-- 0 = final, 1 = proof copy -->
</session.header>
```

### Document hierarchy

```
hansard
├── session.header
└── chamber.xscript
    ├── business.start
    │   ├── day.start  "2000-02-15"
    │   ├── separator/  (empty self-closing element, v2.1)
    │   └── para  "The PRESIDENT ... took the chair at 2.00 p.m., and read prayers."
    ├── debate  [repeating]
    │   ├── debateinfo
    │   │   ├── title
    │   │   ├── type
    │   │   ├── page.no
    │   │   └── id.no  (bill/document ID, optional)
    │   ├── cognate  [optional — for cognate bills]
    │   │   └── cognateinfo
    │   │       ├── title
    │   │       ├── page.no
    │   │       ├── type
    │   │       └── id.no
    │   └── subdebate.1  [repeating]
    │       ├── subdebateinfo
    │       │   ├── title
    │       │   └── page.no
    │       ├── question  [in QWN debates]
    │       │   └── talk.start
    │       │       ├── talker
    │       │       └── para
    │       ├── answer
    │       │   └── talk.start
    │       │       ├── talker
    │       │       └── para
    │       ├── speech
    │       │   └── talk.start
    │       │       ├── talker
    │       │       ├── para
    │       │       ├── motion > para  [formally moved motions]
    │       │       └── quote > para
    │       ├── interjection
    │       │   └── talk.start
    │       │       ├── talker
    │       │       └── para
    │       ├── continue  [speaker resumes after interjection]
    │       │   └── talk.start
    │       │       ├── talker
    │       │       └── para
    │       ├── motionnospeech  [procedural motion without speech wrapper]
    │       ├── amendments > amendment > para
    │       └── subdebate.2  [optional, max nesting depth]
    │           ├── subdebateinfo
    │           └── motionnospeech
    ├── division  [vote, can appear at any level]
    └── adjournment
        ├── adjournmentinfo
        │   ├── page.no
        │   └── time.stamp
        └── para  "Senate adjourned at 1.15 p.m."
```

### `<talker>` element structure

```xml
<talker>
  <time.stamp>14:01:00</time.stamp>      <!-- HH:MM:SS; may be empty -->
  <page.no>11735</page.no>
  <name role="metadata">Cook, Sen Peter</name>   <!-- sort form: Surname, Sen Firstname -->
  <name role="display">Senator COOK</name>        <!-- display form -->
  <name.id>RF4</name.id>                          <!-- PHID (unique speaker ID) -->
  <electorate>Western Australia</electorate>       <!-- STATE NAME, not division -->
  <party>ALP</party>
  <role>Deputy Leader of the Opposition in the Senate</role>
  <in.gov>0</in.gov>                    <!-- 0=opposition/crossbench, 1=government -->
  <first.speech>0</first.speech>        <!-- 1 if this is the senator's first speech -->
</talker>
```

**Critical note:** `<electorate>` contains the **full state or territory name** (e.g. `"Western Australia"`, `"New South Wales"`, `"Tasmania"`), not an electoral division. This is a fundamental structural difference from the House.

Special IDs:
- `10000` — The President (equivalent of House Speaker)
- `10001` — The Clerk

### `<motionnospeech>` element

Used for procedural motions by a named senator without a full speech wrapper:

```xml
<motionnospeech>
  <name>Senator FERRIS</name>
  <electorate>(South Australia)</electorate>
  <role></role>
  <time.stamp>12:31:00</time.stamp>
  <inline>—by leave—At the request of Senator Moore, I move:</inline>
  <motion>
    <para>That the Select Committee on the Administration of Indigenous Affairs
    be authorised to hold a public meeting during the sitting of the Senate
    today, to 1.30 p.m.</para>
  </motion>
  <para>Question agreed to.</para>
</motionnospeech>
```

Note: `<electorate>` here is in the format `(South Australia)` — with parentheses, unlike the `<talker>` form.

### `<division>` element (vote)

```xml
<division>
  <division.header>
    <time.stamp>13:53:00</time.stamp>
    <para>The Senate divided.</para>         <!-- or "The committee divided." -->
  </division.header>
  <para>(The Acting Deputy President—Senator P.F.S. Cook)</para>  <!-- presiding officer -->
  <division.data>
    <ayes>
      <num.votes>38</num.votes>
      <title>AYES</title>
      <names>
        <name>Bishop, T.M.</name>
        <name>Brandis, G.H.</name>
        <name>Eggleston, A. *</name>          <!-- * denotes teller -->
      </names>
    </ayes>
    <noes>
      <num.votes>8</num.votes>
      <title>NOES</title>
      <names>
        <name>Bartlett, A.J.J.</name>
        <name>Nettle, K. *</name>
      </names>
    </noes>
    <pairs>                                    <!-- paired absences; may be absent -->
      <title>PAIRS</title>
      <names>
        <name>Heffernan, W.</name>
        <name>Collins, J.M.A.</name>
      </names>
    </pairs>
  </division.data>
  <para>* denotes teller</para>
  <division.result>
    <para>Question agreed to.</para>
  </division.result>
</division>
```

Note: In v2.1 files (1901 reharvest), `<name>` elements inside `<names>` carry an `id` attribute: `<name id="L2E">Baker, Sir R.C.</name>`

### Inline formatting elements

```xml
<inline font-style="italic">text</inline>
<inline font-weight="bold">text</inline>
<inline ref="R2228">bill title text</inline>
<para class="block">...</para>
<para class="Definition">...</para>
<quote><para>...</para></quote>
<motion><para>That so much of the standing orders...</para></motion>
<table pgwide="1" border-style="solid">...</table>  <!-- CALS-derived table markup -->
```

### Debate type values

Values found in `<debateinfo><type>`:

| Value | Usage |
|---|---|
| `Questions Without Notice` | QWN session; subdebates are question topics |
| `Answers to Questions Without Notice` | Answers tabled separately |
| `Answers to Questions on Notice` | Written answers — may be separate sitting-day file |
| `Bills` | Second reading, committee stage, third reading |
| `Business` | Routine business, notices, tabling |
| `Miscellaneous` / `miscellaneous` | Opening of Parliament, ceremonial |
| `Adjournment` | Adjournment debate |
| `Committees` | Committee reports and meetings |
| `Parliamentary Zone` | Commonwealth parliamentary zone matters |
| `Ministerial Arrangements` | Ministerial changes |
| `Distinguished Visitors` | Formal visits |
| `special adjournment` | Special adjournment motions |

---

## Format 1 — Uppercase/Legacy (1981–1997, outside project scope)

Documented here for completeness only. This format is **not within the 1998–present target range**.

### Root element

```xml
<!DOCTYPE hansard PUBLIC "-//PARLINFO//DTD HANSARD STORAGE//EN" []>
<HANSARD DATE="08/05/1990" PROOF="No" PAGE="1" CHAMBER="Senate"
         PARLIAMENT.NO="36" SESSION.NO="1" PERIOD.NO="1">
```

All metadata as attributes on root; date format `DD/MM/YYYY`.

### Document structure (abbreviated)

```
HANSARD  [attrs: DATE, PROOF, PAGE, CHAMBER, PARLIAMENT.NO, SESSION.NO, PERIOD.NO]
└── CHAMBER.XSCRIPT
    ├── BUSINESS.START
    │   └── DAY.START  [attr: DATE="DD/MM/YYYY"]
    ├── DEBATE  [attrs: TYPE, PAGE]
    │   ├── TITLE
    │   ├── DEBATE.SUB1  [attr: PAGE]
    │   │   └── SPEECH  [attrs: PAGE, SPEAKER, NAMEID, PARTY, ELECTORATE, MINISTERIAL, GOV]
    │   │       ├── TALK.START > TALKER > NAME [attr: NAMEID]; ELECTORATE; ROLE
    │   │       └── INTERJECT  [attrs: CHAIR="0|1", SPEAKER, NAMEID]
    └── QWN  [attr: PAGE]
        └── QUESTION.BLOCK  [attr: PAGE]
            ├── TITLE
            ├── QUESTION  [attrs: PAGE, SPEAKER, NAMEID, PARTY, ELECTORATE]
            └── ANSWER   [attrs: PAGE, SPEAKER, NAMEID, PARTY, ELECTORATE, MINISTERIAL, GOV]
```

### Key differences from Format 2/3

| Feature | Format 1 (Uppercase) | Format 2/3 (Lowercase) |
|---|---|---|
| Tag case | ALL UPPERCASE | all lowercase |
| Root element metadata | Attributes on `<HANSARD>` | Child elements in `<session.header>` |
| Date format | `DD/MM/YYYY` | ISO 8601 `YYYY-MM-DD` |
| Speaker metadata | Attributes on `<SPEECH>` | Child elements in `<talker>` |
| Speaker name | Single `<NAME NAMEID="ID">` | Two `<name>` elements with `role` attribute |
| Interjections | `<INTERJECT CHAIR="0|1">` | `<interjection>` with full `<talk.start>/<talker>` |
| QWN structure | `<QWN> > <QUESTION.BLOCK> > <QUESTION>/<ANSWER>` | `<debate type="Questions..."> > <subdebate.1> > <question>/<answer>` |
| Subdebates | `<DEBATE.SUB1>`, `<DEBATE.SUB2>` | `<subdebate.1>`, `<subdebate.2>` |
| Time stamp | `(10.31)` text in `<TIME.STAMP>` | `HH:MM:SS` in `<time.stamp>` |
| Schema | DTD via DOCTYPE | XSD via `xsi:noNamespaceSchemaLocation` |

---

## Complete element inventory

### Format 2/3 (target format, lowercase)

**Structural elements:**
`hansard`, `session.header`, `date`, `parliament.no`, `session.no`, `period.no`, `chamber`, `page.no`, `proof`, `chamber.xscript`, `business.start`, `day.start`, `separator`

**Debate elements:**
`debate`, `debateinfo`, `title`, `type`, `id.no`, `subdebate.1`, `subdebate.2`, `subdebateinfo`, `cognate`, `cognateinfo`

**Speech elements:**
`speech`, `question`, `answer`, `interjection`, `continue`, `motionnospeech`, `talk.start`, `talker`, `time.stamp`, `name`, `name.id`, `electorate`, `party`, `role`, `in.gov`, `first.speech`, `para`, `inline`, `motion`, `quote`

**Division elements:**
`division`, `division.header`, `division.data`, `ayes`, `noes`, `pairs`, `num.votes`, `names`, `division.result`

**Other:**
`adjournment`, `adjournmentinfo`, `amendments`, `amendment`, `table`, `tgroup`, `thead`, `tbody`, `row`, `entry`, `graphic`

**Attributes:**
`version`, `xsi:noNamespaceSchemaLocation`, `xmlns:xsi`, `role` (on `name`: "metadata"/"display"), `id` (on `name` in divisions, v2.1), `class` (on `para`), `font-style`, `font-weight` (on `inline`), `ref` (on `inline`), `pgwide`, `border-style`, `border-color`, `border-width` (on table elements)

---

## Annotated sample XML — Senate sitting day 15 February 2000

```xml
<?xml version="1.0"?>
<hansard xsi:noNamespaceSchemaLocation="../../hansard.xsd" version="2.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <session.header>
    <date>2000-02-15</date>
    <parliament.no>39</parliament.no>
    <session.no>1</session.no>
    <period.no>5</period.no>
    <chamber>SENATE</chamber>
    <page.no>11735</page.no>
    <proof>0</proof>
  </session.header>
  <chamber.xscript>

    <!-- Business start: first row of every sitting day -->
    <business.start>
      <day.start>2000-02-15</day.start>
      <para>—————</para>
      <para>The PRESIDENT (Senator the Hon. Margaret Reid) took the chair at
      2.00 p.m., and read prayers.</para>
    </business.start>

    <!-- Questions Without Notice: topic-grouped Q&A pairs -->
    <debate>
      <debateinfo>
        <title>QUESTIONS WITHOUT NOTICE</title>
        <type>Questions Without Notice</type>
        <page.no>11735</page.no>
      </debateinfo>
      <subdebate.1>
        <subdebateinfo>
          <title>National Textiles</title>
          <page.no>11735</page.no>
        </subdebateinfo>
        <question>
          <talk.start>
            <talker>
              <time.stamp>14:01:00</time.stamp>
              <page.no>11735</page.no>
              <name role="metadata">Cook, Sen Peter</name>
              <name role="display">Senator COOK</name>
              <name.id>RF4</name.id>
              <electorate>Western Australia</electorate>
              <party>ALP</party>
              <role>Deputy Leader of the Opposition in the Senate</role>
              <in.gov>0</in.gov>
              <first.speech>0</first.speech>
            </talker>
            <para>—My question is to the Minister for Employment...</para>
          </talk.start>
        </question>
        <answer>
          <talk.start>
            <talker>
              <page.no>11735</page.no>
              <name role="metadata">Ellison, Sen Chris</name>
              <name role="display">Senator ELLISON</name>
              <name.id>9X5</name.id>
              <electorate>Western Australia</electorate>
              <party>LP</party>
              <role>Special Minister of State</role>
              <in.gov>1</in.gov>
              <first.speech>0</first.speech>
            </talker>
            <para>—I do not have any instructions on that...</para>
          </talk.start>
        </answer>
      </subdebate.1>
    </debate>

    <!-- Regular debate with interjection and continue -->
    <debate>
      <debateinfo>
        <title>SOME BILL 2000</title>
        <type>Bills</type>
        <page.no>11748</page.no>
        <id.no>R599</id.no>
      </debateinfo>
      <subdebate.1>
        <subdebateinfo>
          <title>Second Reading</title>
          <page.no>11748</page.no>
        </subdebateinfo>
        <speech>
          <talk.start>
            <talker>
              <time.stamp>15:04:00</time.stamp>
              <page.no>11748</page.no>
              <name role="metadata">Faulkner, Sen John</name>
              <name role="display">Senator FAULKNER</name>
              <name.id>5K4</name.id>
              <electorate>New South Wales</electorate>
              <party>ALP</party>
              <role>Leader of the Opposition in the Senate</role>
              <in.gov>0</in.gov>
              <first.speech>0</first.speech>
            </talker>
            <para>Speech content paragraph.</para>
            <motion>
              <para>That this bill be now read a second time.</para>
            </motion>
          </talk.start>
        </speech>
        <interjection>
          <talk.start>
            <talker>
              <name.id>ZW4</name.id>
              <name role="metadata">Sherry, Sen Nick</name>
              <name role="display">Senator SHERRY</name>
            </talker>
            <para>—Interjection text.</para>
          </talk.start>
        </interjection>
        <continue>
          <talk.start>
            <talker>
              <name.id>5K4</name.id>
              <name role="display">Senator FAULKNER</name>
            </talker>
            <para>—Resumed speech content.</para>
          </talk.start>
        </continue>
      </subdebate.1>
    </debate>

    <!-- Division (vote) -->
    <division>
      <division.header>
        <time.stamp>13:53:00</time.stamp>
        <para>The Senate divided.</para>
      </division.header>
      <para>(The Acting Deputy President—Senator P.F.S. Cook)</para>
      <division.data>
        <ayes>
          <num.votes>38</num.votes>
          <title>AYES</title>
          <names>
            <name>Bishop, T.M.</name>
            <name>Eggleston, A. *</name>
          </names>
        </ayes>
        <noes>
          <num.votes>8</num.votes>
          <title>NOES</title>
          <names>
            <name>Bartlett, A.J.J.</name>
            <name>Nettle, K. *</name>
          </names>
        </noes>
        <pairs>
          <title>PAIRS</title>
          <names>
            <name>Heffernan, W.</name>
            <name>Collins, J.M.A.</name>
          </names>
        </pairs>
      </division.data>
      <para>* denotes teller</para>
      <division.result>
        <para>Question agreed to.</para>
      </division.result>
    </division>

    <!-- Adjournment -->
    <adjournment>
      <adjournmentinfo>
        <page.no>11900</page.no>
        <time.stamp>13:15:00</time.stamp>
      </adjournmentinfo>
      <para>Senate adjourned at 1.15 p.m.</para>
    </adjournment>

  </chamber.xscript>
</hansard>
```
