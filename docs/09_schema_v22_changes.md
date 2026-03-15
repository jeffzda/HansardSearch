# Schema Version 2.2 Changes (2014–present)

> **Status:** Reference document — implemented. Both the Senate parser (`03_parse.py`) and the House parser (`03b_parse_house.py`) implement v2.2-specific code paths. The Senate parser uses a single `V22` branch; the House parser uses a dedicated `HouseParserV22` class. Text extraction from `<talk.text>/<body>/<p class="HPS-*">`, display name extraction from `<span class="HPS-MemberSpeech">`, and interjection detection via `<a type="MemberInterjecting">` are all in production. The URL-era table below was confirmed empirically during the download stage.

Schema version 2.2 was introduced around 2014 (confirmed: 44th Parliament, 2014-02-11). It represents a complete rewrite of the text content model, driven by a migration of the source documents from a custom XML workflow to OOXML (Office Open XML, i.e. Microsoft Word `.docx` format). The structural skeleton (`<session.header>`, `<chamber.xscript>`, `<debate>`, `<speech>`, `<talker>`) is preserved, but text content is expressed in HTML/CSS-class markup derived from OOXML.

---

## Version by parliament (confirmed)

| Date | Parliament | Schema version | URL pattern |
|---|---|---|---|
| 2000-02-15 | 39th | 2.0 | date-based (`2000-02-15`) |
| 2004-02-11 | 40th | 2.0 | date-based |
| 2006-02-07 | 41st | 2.1 | date-based |
| 2008-02-12 | 42nd | 2.1 | date-based |
| 2011-02-08 | 43rd | 2.1 | date-based |
| 2014-02-11 | 44th | **2.2** | UUID |
| 2017-02-07 | 45th | **2.2** | UUID |
| 2020-02-04 | 46th | **2.2** | UUID |
| 2023-02-07 | 47th | **2.2** | numeric integer ID |

Schema version is read from the `version` attribute on the root `<hansard>` element.

---

## Root element differences

| Feature | v2.0 / v2.1 | v2.2 |
|---|---|---|
| Namespace prefix | `xmlns:xsi` + `xsi:noNamespaceSchemaLocation` | No XSI namespace; bare `noNamespaceSchemaLocation` attribute |
| `<chamber>` casing | `SENATE` (all-caps) | `Senate` (title-case) |

---

## `<talker>` child elements

| Element | v2.0 | v2.1 | v2.2 |
|---|---|---|---|
| `<time.stamp>` | yes | yes | yes (may be empty) |
| `<page.no>` | yes | yes | yes |
| `<name role="metadata">` | yes | yes | yes |
| `<name role="display">` | yes | yes | **REMOVED** |
| `<name.id>` | yes | yes | yes |
| `<electorate>` | yes | yes | yes (may be empty) |
| `<party>` | yes | yes | yes (may be empty) |
| `<role>` (ministerial title) | yes | yes | **REMOVED** |
| `<in.gov>` | yes | yes | yes (may be empty) |
| `<first.speech>` | yes | yes | yes (may be empty) |

In v2.2, the display name and ministerial role are encoded in the speech body text, not in the talker. See HPS class names below.

---

## `<debateinfo>` child elements

| Element | v2.0 / v2.1 | v2.2 |
|---|---|---|
| `<title>` | yes | yes |
| `<page.no>` | yes | yes |
| `<type>` | yes | yes |
| `<id.no>` (bill reference ID) | yes | **REMOVED** |
| `<cognate>` / `<cognateinfo>` | yes (variable) | **REMOVED** |
| `<time.stamp>` | yes (some files) | no |

---

## Text content model change

### v2.0 / v2.1 text model

Text content appears as `<para>` children of speech elements, with inline formatting in `<inline>` elements:

```xml
<speech>
  <talk.start>
    <talker>
      <name role="metadata">O'Brien, Sen Kerry</name>
      <name role="display">Senator O'BRIEN</name>
      <name.id>8O6</name.id>
      <electorate>Tasmania</electorate>
      <party>ALP</party>
      <role>Shadow Minister for Agriculture</role>
      <in.gov>0</in.gov>
      <first.speech>0</first.speech>
      <page.no>1</page.no>
      <time.stamp>12:31:00</time.stamp>
    </talker>
    <para>—Mr President, welcome back for the new year...</para>
  </talk.start>
  <para>Additional paragraph content.</para>
  <para>Another paragraph with <inline font-weight="bold">bold text</inline>.</para>
</speech>
```

### v2.2 text model

Text content is wrapped in `<talk.text>/<body>/<p>/<span>` structure with CSS class names derived from OOXML conversion. The speech body is entirely within `<talk.text>`:

```xml
<speech>
  <talk.start>
    <talker>
      <page.no>1</page.no>
      <time.stamp/>
      <name role="metadata">O'Brien, Sen Kerry</name>
      <name.id>8O6</name.id>
      <electorate>Tasmania</electorate>
      <party>ALP</party>
      <in.gov>0</in.gov>
      <first.speech>0</first.speech>
    </talker>
  </talk.start>
  <talk.text>
    <body xmlns:w="..." xmlns:a="..." ...>
      <p class="HPS-Normal" style="direction:ltr;unicode-bidi:normal;">
        <span class="HPS-Normal">
          <a href="8o6" type="MemberSpeech">
            <span class="HPS-MemberSpeech">Senator O'BRIEN</span>
          </a>
          <span class="HPS-Electorate"> (Tasmania)</span>
          (<span class="HPS-Time">12:31</span>):
          Mr President, welcome back for the new year...
        </span>
      </p>
      <p class="HPS-Normal">
        <span class="HPS-Normal">Additional paragraph content.</span>
      </p>
    </body>
  </talk.text>
</speech>
```

The `<body>` element carries 10 OOXML namespace declarations on every occurrence.

---

## `<a>` type attribute — speech type encoding

In v2.2, every speaker attribution within the body text is encoded as an `<a>` element with a `type` attribute and an `href` containing the lowercase PHID:

| `type` value | Meaning | Speaker class |
|---|---|---|
| `MemberSpeech` | Senator's speech | `HPS-MemberSpeech` |
| `OfficeSpeech` | Presiding officer's speech | `HPS-OfficeSpeech` |
| `MemberContinuation` | Senator continuing after interjection | `HPS-MemberContinuation` |
| `MemberInterjecting` | Senator interjecting | `HPS-MemberInterjecting` |
| `OfficeInterjecting` | Presiding officer interjecting | `HPS-OfficeInterjecting` |
| `MemberQuestion` | Senator asking a question (QWN) | `HPS-MemberQuestion` |
| `MemberAnswer` | Senator answering a question (QWN) | `HPS-MemberAnswer` |
| `Bill` | Bill title reference | — |
| `""` (empty) | Date anchor or structural | — |

**Key implication for parsing:** In v2.2, interjections are encoded as `<a type="MemberInterjecting">` within the body text stream, not necessarily as separate sibling `<interjection>` XML elements. The parser must scan `<a type>` attributes in the body to detect interjections.

The `href` value is the PHID in lowercase (e.g., `href="8o6"` corresponds to `name.id = "8O6"`). This provides a redundant member identification path that can supplement the `<talker>` data.

---

## HPS CSS class names (complete list)

These classes appear on `<p>` and `<span>` elements throughout v2.2 files:

| Class | Element | Meaning |
|---|---|---|
| `HPS-Normal` | `<p>`, `<span>` | Regular paragraph / normal text run |
| `HPS-Small` | `<p>`, `<span>` | Smaller text (e.g. procedural notes) |
| `HPS-Debate` | `<p>` | Debate heading line |
| `HPS-SubDebate` | `<p>` | Sub-debate heading |
| `HPS-SubSubDebate` | `<p>` | Sub-sub-debate heading |
| `HPS-MemberSpeech` | `<span>` inside `<a>` | Senator's display name at speech start |
| `HPS-OfficeSpeech` | `<span>` inside `<a>` | President/Chair display name at speech start |
| `HPS-MemberContinuation` | `<span>` inside `<a>` | Senator name at continuation |
| `HPS-MemberInterjecting` | `<span>` inside `<a>` | Interjecter's display name |
| `HPS-OfficeInterjecting` | `<span>` inside `<a>` | President/Chair interjecting display name |
| `HPS-MemberQuestion` | `<span>` inside `<a>` | Questioner's display name (QWN) |
| `HPS-MemberAnswer` | `<span>` inside `<a>` | Answerer's display name (QWN) |
| `HPS-Time` | `<span>` | Timestamp within speech |
| `HPS-Electorate` | `<span>` | State name within speech header |
| `HPS-MinisterialTitles` | `<span>` | Minister's portfolio title |
| `HPS-GeneralBold` | `<span>` | Bold formatting |
| `HPS-Bullet` | `<p>` | Bulleted list item |
| `HPS-SmallBullet` | `<p>` | Small bulleted list item |
| `HPS-TableLeftAlignSmall` | `<p>` | Table cell text |
| `HPS-SODJobDate` | `<p>` | Date heading in business.start |
| `HPS-Line` | `<p>` | Horizontal rule / separator line |
| `HPS-DivisionPreamble` | `<p>` | Division header text |
| `HPS-DivisionFooter` | `<p>` | Division result text |

---

## Structural changes to debate elements

### `<adjournment>` removed

In v2.0 / v2.1, `<adjournment>` was a standalone top-level element in `<chamber.xscript>`:
```xml
<adjournment>
  <adjournmentinfo>
    <time.stamp>13:15:00</time.stamp>
    <page.no>N</page.no>
  </adjournmentinfo>
  <para>Senate adjourned at 1.15 p.m.</para>
</adjournment>
```

In v2.2, adjournment is a regular `<debate>` element:
```xml
<debate>
  <debateinfo>
    <title>ADJOURNMENT</title>
    <page.no>N</page.no>
    <type>ADJOURNMENT</type>
  </debateinfo>
  <!-- speeches follow -->
</debate>
```

### New wrapper elements

| New element | Wraps |
|---|---|
| `<debate.text>` | Procedural text between debate opening and subdebates |
| `<subdebate.text>` | Procedural text at subdebate level |
| `<talk.text>` | Speech body text (replaces bare `<para>` children after `<talk.start>`) |

### Removed elements (v2.0/v2.1 → v2.2)

`<para>`, `<inline>`, `<motion>`, `<motionnospeech>`, `<quote>`, `<list>`, `<item>`, `<separator>`, `<interrupt>`, `<amendments>`, `<amendment>`, `<petition.group>`, `<petition>`, `<petitioninfo>`, `<petition.groupinfo>`, `<presenter>`, `<adjournment>`, `<adjournmentinfo>`, `<answers.to.questions>`, `<id.no>` (in debateinfo), `<cognate>`, `<cognateinfo>`, `<tgroup>`, `<thead>`, `<tbody>`, `<colspec>`, `<day.start>`, `<subtitle>`

---

## Implications for the parser

The parser requires **two distinct code paths** keyed on the `version` attribute:

### v2.0 / v2.1 path (1998–~2013)
- Extract text from `<para>` children of speech elements
- Extract display name from `<name role="display">` in `<talker>`
- Extract ministerial role from `<role>` in `<talker>`
- Extract interjections from `<interjection>` sibling elements
- Extract procedural motions from `<motionnospeech>` elements
- Parse `<motion>` and `<quote>` elements inline

### v2.2 path (2014–present)
- Extract text from `<p>` elements within `<talk.text>/<body>`, stripping attribution spans (`HPS-MemberSpeech`, `HPS-OfficeSpeech`, `HPS-Time`, `HPS-Electorate`, `HPS-MinisterialTitles`)
- Extract display name from `<span class="HPS-MemberSpeech|HPS-OfficeSpeech">` inside `<talk.text>`
- Extract speech type from `<a type="MemberSpeech|MemberInterjecting|...">` in body
- Extract PHID from `<a href="phid">` (lowercase; normalise to uppercase)
- Detect interjections via `<a type="MemberInterjecting|OfficeInterjecting">` in body
- Extract timestamps from `<span class="HPS-Time">` in body
- Extract state from `<span class="HPS-Electorate">` in body
- No `<adjournment>` element — detect adjournment debate by `<debateinfo><type>ADJOURNMENT</type>`
