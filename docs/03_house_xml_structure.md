# House of Representatives Hansard XML Structure

> **Status:** Reference document — implemented. The House parser (`pipeline/03b_parse_house.py`) handles all five schema eras documented here, including the Federation Chamber node rename from `maincomm.xscript` to `fedchamb.xscript`. The 1,722 XML files in `data/raw/house/` span 1998-03-02 to 2025-11-27. Format 2 (1981–1997 uppercase/SGML) remains outside project scope.

Source: https://github.com/wragge/hansard-xml (covers 1901–2005); APH ParlInfo for 2006–present.

---

## Format eras

| Era | Years | File naming | Schema |
|---|---|---|---|
| **Format 1 — Lowercase v2.x** | 1901–1980 + 1998–2005 | `YYYYMMDD_reps_P_VV_vN.xml` (early) or numeric IDs `NNNN-N.xml` (late) | `<hansard version="2.0|2.1" xsi:noNamespaceSchemaLocation="...">` |
| **Format 2 — Uppercase/SGML** | 1981–1997 | `reps_YYYY-MM-DD.xml` | `<!DOCTYPE hansard PUBLIC "-//PARLINFO//DTD HANSARD STORAGE//EN" []>` |

The Katz & Alexander project targets 1998–2025, which falls entirely within Format 1 (lowercase).

---

## Format 1 — Lowercase (1901–1980 + 1998–2005, target format)

### Root element

```xml
<?xml version="1.0" encoding="UTF-8"?>
<hansard xsi:noNamespaceSchemaLocation="../../hansard.xsd" version="2.1"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
```

- Same schema as Senate Format 2/3 — same DTD/XSD reference
- `<chamber>REPS</chamber>` (uppercase) in session header

### Session header

```xml
<session.header>
  <date>2000-04-04</date>
  <parliament.no>39</parliament.no>
  <session.no>1</session.no>
  <period.no>5</period.no>
  <chamber>REPS</chamber>
  <page.no>15113</page.no>
  <proof>0</proof>
</session.header>
```

### Document hierarchy

```
hansard
├── session.header
└── chamber.xscript
    ├── business.start
    │   └── day.start
    ├── debate  [repeating]
    │   ├── debateinfo (title, type, page.no, id.no?)
    │   ├── subdebate.1  [repeating]
    │   │   ├── subdebateinfo
    │   │   ├── question  [in QWN debates]
    │   │   │   ├── talk.start > talker + para
    │   │   │   ├── interjection
    │   │   │   └── continue
    │   │   ├── answer
    │   │   │   ├── talk.start > talker + para
    │   │   │   ├── interjection
    │   │   │   └── continue
    │   │   └── speech
    │   │       ├── talk.start > talker + para
    │   │       ├── interjection
    │   │       └── continue
    │   └── division  [optional]
    ├── maincomm.xscript  [Federation Chamber, pre-2012]
    │   └── (same structure as chamber.xscript)
    └── fedchamb.xscript  [Federation Chamber, post-2012]
        └── (same structure as chamber.xscript)
```

**Key difference from Senate:** The House has a **second chamber node** for the Federation Chamber (formerly Main Committee). The Senate has no equivalent — there is only `chamber.xscript`.

### `<talker>` element structure

```xml
<talker>
  <page.no>15113</page.no>
  <time.stamp>14:02:00</time.stamp>
  <name role="metadata">Beazley, Kim, MP</name>    <!-- sort form: Surname, Firstname, MP -->
  <name role="display">Mr BEAZLEY</name>            <!-- display form -->
  <name.id>PE4</name.id>                            <!-- PHID -->
  <electorate>Brand</electorate>                    <!-- ELECTORAL DIVISION, not state -->
  <party>ALP</party>
  <role></role>
  <in.gov>0</in.gov>
  <first.speech>0</first.speech>
</talker>
```

**Critical difference from Senate:** `<electorate>` contains the **electoral division name** (e.g. `"Brand"`, `"Bennelong"`, `"Melbourne"`), not a state.

Special IDs:
- `10000` — The Speaker / Chair
- `20000` — Unidentified member

### Questions Without Notice

```xml
<debate>
  <debateinfo>
    <title>QUESTIONS WITHOUT NOTICE</title>
    <type>Questions Without Notice</type>
    <page.no>15113</page.no>
  </debateinfo>
  <subdebate.1>
    <subdebateinfo>
      <title>Mandatory Sentencing</title>
      <page.no>15113</page.no>
    </subdebateinfo>
    <question>
      <talk.start>
        <talker>
          <time.stamp>14:02:00</time.stamp>
          <page.no>15113</page.no>
          <name role="metadata">Beazley, Kim, MP</name>
          <name role="display">Mr BEAZLEY</name>
          <name.id>PE4</name.id>
          <electorate>Brand</electorate>
          <party>ALP</party>
          <in.gov>0</in.gov>
          <first.speech>0</first.speech>
        </talker>
        <para>—My question is to the Prime Minister...</para>
      </talk.start>
      <interjection>...</interjection>
      <continue>...</continue>
    </question>
    <answer>
      <talk.start>
        <talker>
          <page.no>15113</page.no>
          <name role="metadata">Howard, John, MP</name>
          <name role="display">Mr HOWARD</name>
          <name.id>ZD4</name.id>
          <electorate>Bennelong</electorate>
          <party>LP</party>
          <role>Prime Minister</role>
          <in.gov>1</in.gov>
          <first.speech>0</first.speech>
        </talker>
        <para>—As I understand it...</para>
      </talk.start>
      <para>Additional answer paragraphs...</para>
      <interjection>...</interjection>
      <continue>...</continue>
    </answer>
  </subdebate.1>
  <!-- Next question-answer pair as another subdebate.1 -->
</debate>
```

### Questions in writing (`answers.to.questions`)

The House embeds written Q&A **within the same XML file** as a separate subtree:

```
hansard
└── chamber.xscript
    └── ...
└── answers.to.questions   ← separate subtree, not present in Senate XML
    └── debate
        └── subdebate.1
            ├── question > talk.start > talker + para
            └── answer > talk.start > talker + para
```

### Division markup (same as Senate)

```xml
<division>
  <division.header>
    <time.stamp>10:07:00</time.stamp>
    <para>The House divided.</para>
  </division.header>
  <para>(Mr Speaker—Mr Neil Andrew)</para>
  <division.data>
    <ayes>
      <num.votes>75</num.votes>
      <title>AYES</title>
      <names>
        <name>Abbott, A.J.</name>
        <name>Anderson, J.D. *</name>
      </names>
    </ayes>
    <noes>...</noes>
    <pairs>...</pairs>
  </division.data>
  <para>* denotes teller</para>
  <division.result>
    <para>Question so resolved in the affirmative.</para>
  </division.result>
</division>
```

---

## Format 2 — Uppercase/SGML (1981–1997)

### Root element

```xml
<!DOCTYPE hansard PUBLIC "-//PARLINFO//DTD HANSARD STORAGE//EN" []>
<HANSARD DATE="08/05/1990" PROOF="No" PAGE="1" CHAMBER="Reps"
         PARLIAMENT.NO="36" SESSION.NO="1" PERIOD.NO="1">
```

### Document hierarchy (abbreviated)

```
HANSARD  [attrs: DATE (DD/MM/YYYY), PROOF, PAGE, CHAMBER="Reps", PARLIAMENT.NO, SESSION.NO, PERIOD.NO]
└── CHAMBER.XSCRIPT
    ├── BUSINESS.START > DAY.START [attr: DATE] + PROCTEXT > PARA
    ├── DEBATE  [attrs: TYPE, PAGE]
    │   ├── TITLE
    │   ├── PROCTEXT > PARA
    │   ├── DEBATE.SUB1 > DEBATE.SUB2 > SPEECH ...
    │   └── SPEECH  [attrs: PAGE, SPEAKER, NAMEID, PARTY, ELECTORATE, GOV, MINISTERIAL, TIME.STAMP]
    │       ├── TALK.START > TALKER > NAME [attr: NAMEID]; ELECTORATE
    │       └── INTERJECT  [attrs: CHAIR="0|1", SPEAKER, NAMEID]
    └── QWN  [attr: PAGE]
        └── QUESTION.BLOCK  [attr: PAGE]
            ├── TITLE
            ├── QUESTION  [attrs: PAGE, SPEAKER, NAMEID, PARTY, ELECTORATE]
            └── ANSWER   [attrs: PAGE, SPEAKER, NAMEID, PARTY, ELECTORATE, MINISTERIAL, GOV]
```

### Division markup (SGML — uses TABLE structure)

In the SGML format, division member names are stored in a CALS table, not a flat `<names>/<name>` list:

```xml
<DIVISION TIME="10.01 a.m.">
  <DIVISION.HEADER>
    <TAB LEADING="NONE" TYPE="NORMAL">The House divided.
    <TIME.STAMP TIME="10.01 a.m.">[10.01 a.m.]</TIME.STAMP>
  </DIVISION.HEADER>
  <DIVISION.DATA>
    <AYES NUMVOTES="68">
      <TITLE>AYES</TITLE>
      <TABLE>
        <TGROUP COLS="2">
          <TBODY>
            <ROW>
              <ENTRY COLNAME="C1">Adams, D.</ENTRY>
              <ENTRY COLNAME="C2">Baldwin, P. J.</ENTRY>
            </ROW>
            <!-- two members per row, tellers marked with * -->
          </TBODY>
        </TGROUP>
      </TABLE>
    </AYES>
    <NOES NUMVOTES="50">...</NOES>
    <PAIRS>...</PAIRS>
  </DIVISION.DATA>
  <DIVISION.RESULT><PARA>Question so resolved in the affirmative.</PARA></DIVISION.RESULT>
</DIVISION>
```

---

## XPath reference — Format 1 (lowercase, 1998–present)

| Data | XPath |
|---|---|
| Document date | `//session.header/date` |
| Chamber | `//session.header/chamber` |
| Day start | `//business.start/day.start` |
| All debates | `//chamber.xscript/debate` |
| Debate title | `debate/debateinfo/title` |
| Debate type | `debate/debateinfo/type` |
| Subdebates level 1 | `debate/subdebate.1` |
| Subdebates level 2 | `debate/subdebate.1/subdebate.2` |
| All speeches | `//speech` |
| Speaker name.id | `speech/talk.start/talker/name.id` |
| Speaker metadata name | `speech/talk.start/talker/name[@role='metadata']` |
| Speaker display name | `speech/talk.start/talker/name[@role='display']` |
| Speaker electorate | `speech/talk.start/talker/electorate` |
| Speaker party | `speech/talk.start/talker/party` |
| Speaker role | `speech/talk.start/talker/role` |
| In government | `speech/talk.start/talker/in.gov` |
| Maiden speech flag | `speech/talk.start/talker/first.speech` |
| Speech page number | `speech/talk.start/talker/page.no` |
| Speech timestamp | `speech/talk.start/talker/time.stamp` |
| All questions | `//question` |
| All answers | `//answer` |
| All interjections | `//interjection` |
| Continue elements | `//continue` |
| Federation Chamber | `//maincomm.xscript` (pre-2012) or `//fedchamb.xscript` (post-2012) |
| Written Q&A | `//answers.to.questions//question` and `//answers.to.questions//answer` |
| All divisions | `//division` |
| Division timestamp | `division/division.header/time.stamp` |
| Ayes count | `division/division.data/ayes/num.votes` |
| Ayes member names | `division/division.data/ayes/names/name` |
| Division result | `division/division.result/para` |
