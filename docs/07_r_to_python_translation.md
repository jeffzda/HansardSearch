# R to Python Translation Decisions

> **Status:** Reference document. This translation was completed as implemented in the pipeline scripts. The Senate parser (`03_parse.py`) and House parser (`03b_parse_house.py`) both use the node-iteration approach described here rather than the text-blob approach used in the original R code. The House R scripts for the 1998–2025 extension were translated into `03b_parse_house.py`.

This document maps every R package, function, and idiom used in the Katz & Alexander pipeline to its Python/pandas equivalent, and documents where the two approaches diverge.

---

## Package mapping

| R package | Python equivalent | Notes |
|---|---|---|
| `xml2` | `lxml.etree` | Primary XML parser. `read_xml()` → `etree.parse()`; `xml_find_all()` → `element.findall()` or `etree.XPath()`; `xml_text()` → `element.text`; `xml_name()` → `element.tag`; `xml_children()` → `list(element)` |
| `XML` (secondary) | `lxml.etree` | The R project uses both `xml2` and `XML` packages inconsistently. Python uses `lxml` for both. `xmlToDataFrame()` has no direct equivalent — use explicit iteration |
| `dplyr` | `pandas` | Data manipulation throughout |
| `tidyr` | `pandas` | `separate_rows()`, `pivot_wider()`, `fill()`, `unite()` |
| `stringr` | `re` + `pandas.str` | String detection, extraction, replacement |
| `purrr` | list comprehensions + `pandas` | `map_dfr()` → `pd.concat([...])` |
| `tibble` | `pandas.DataFrame` | Data frames |
| `arrow` | `pyarrow` + `pandas` | `write_parquet()` → `pyarrow.parquet.write_table()`; `read_parquet()` → `pd.read_parquet()` |
| `here` | `pathlib.Path` | Relative file paths |
| `hms` | `datetime.time` or `pd.to_timedelta` | Time-of-day storage |
| `heapsofpapers` | `httpx` + `asyncio` or `requests` + `tqdm` | Bulk async/parallel file downloading |
| `AustralianPoliticians` | Direct CSV from GitHub (no Python package) | `pd.read_csv()` from raw GitHub URLs |
| `ausPH` | Not needed — use `name.id` from XML + OA parser `people.csv` | No Python equivalent package; PHIDs are already in the XML |
| `fs` | `pathlib.Path.glob()` | File system operations: `dir_ls()` → `Path.glob()`; `file_size()` → `Path.stat().st_size` |
| `readxl` | `openpyxl` or `pd.read_excel()` | Lookup tables stored as Excel |
| `writexl` | `openpyxl` or `pd.ExcelWriter` | Writing Excel files |
| `googledrive` | Manual download or `gdown` | Correction spreadsheets hosted on Google Drive |
| `lubridate` | `pandas` date functions | Date parsing and arithmetic |

---

## Key function-level translations

### XML parsing

| R (xml2 / XML) | Python (lxml) |
|---|---|
| `read_xml(path)` | `etree.parse(path).getroot()` |
| `xml_find_all(doc, xpath)` | `root.findall(xpath)` or `root.xpath(xpath)` |
| `xml_find_first(doc, xpath)` | `root.find(xpath)` |
| `xml_text(node)` | `node.text or ''` (note: may be None) |
| `xml_name(node)` | `node.tag` |
| `xml_children(node)` | `list(node)` |
| `xml_attr(node, "attr")` | `node.get("attr")` |
| `xmlToDataFrame(node=getNodeSet(doc, xpath))` | Manual iteration — no direct equivalent |
| `getNodeSet(doc, xpath)` | `root.xpath(xpath)` (note: `//` works differently with lxml — use `xpath()` not `findall()` for `//` paths) |

**Important:** `lxml` distinguishes between `.find()` / `.findall()` (which use a subset of XPath) and `.xpath()` (which uses full XPath). The `//` abbreviation only works reliably with `.xpath()`.

**Text content in mixed-content nodes:** In R, `xml_text(node)` concatenates all text content recursively including child node text. In lxml, `node.text` gives only the direct text before the first child; `node.tail` gives text after the closing tag of the node within its parent. To replicate R's `xml_text()` behaviour:

```python
def xml_text(element):
    """Get all text content from element and descendants, concatenated."""
    return ''.join(element.itertext())
```

### Data manipulation

| R (tidyverse) | Python (pandas) |
|---|---|
| `tibble(...)` | `pd.DataFrame({...})` |
| `data.frame(...)` | `pd.DataFrame({...})` |
| `bind_rows(a, b, c)` | `pd.concat([a, b, c], ignore_index=True)` |
| `left_join(x, y, by="key")` | `x.merge(y, on='key', how='left')` |
| `left_join(x, y, by=c("a"="b"))` | `x.merge(y, left_on='a', right_on='b', how='left')` |
| `inner_join(x, y, by="key")` | `x.merge(y, on='key', how='inner')` |
| `select(df, col1, col2)` | `df[['col1', 'col2']]` |
| `filter(df, condition)` | `df[condition]` or `df.query(...)` |
| `mutate(df, new_col = expr)` | `df.assign(new_col=expr)` or `df['new_col'] = expr` |
| `rename(df, new = old)` | `df.rename(columns={'old': 'new'})` |
| `distinct(df)` | `df.drop_duplicates()` |
| `distinct(df, col)` | `df[['col']].drop_duplicates()` |
| `arrange(df, col)` | `df.sort_values('col')` |
| `arrange(df, desc(col))` | `df.sort_values('col', ascending=False)` |
| `group_by(df, col) %>% summarise(...)` | `df.groupby('col').agg(...)` |
| `group_by(df, col) %>% mutate(...)` | `df.groupby('col').transform(...)` |
| `rowid_to_column("order")` | `df.reset_index(drop=True); df.index + 1` or `df.assign(order=range(1, len(df)+1))` |
| `pull(df, col)` | `df['col'].tolist()` or `df['col'].values` |
| `nrow(df)` | `len(df)` |
| `ncol(df)` | `len(df.columns)` |
| `dim(df)` | `df.shape` |
| `stopifnot(cond)` | `assert cond` |
| `is.na(x)` | `pd.isna(x)` or `x.isna()` |
| `!is.na(x)` | `x.notna()` |
| `coalesce(a, b)` | `a.fillna(b)` or `a.combine_first(b)` |

### String operations

| R (stringr) | Python (pandas str / re) |
|---|---|
| `str_detect(x, pattern)` | `pd.Series.str.contains(pattern, regex=True, na=False)` |
| `str_extract(x, pattern)` | `pd.Series.str.extract(pattern)[0]` (returns DataFrame; take first group) |
| `str_extract_all(x, pattern)` | `pd.Series.str.findall(pattern)` |
| `str_remove(x, pattern)` | `pd.Series.str.replace(pattern, '', regex=True)` |
| `str_remove_all(x, pattern)` | `pd.Series.str.replace(pattern, '', regex=True)` (same — pandas replaces all by default) |
| `str_replace(x, pattern, repl)` | `pd.Series.str.replace(pattern, repl, n=1, regex=True)` |
| `str_replace_all(x, pattern, repl)` | `pd.Series.str.replace(pattern, repl, regex=True)` |
| `str_trim(x)` | `pd.Series.str.strip()` |
| `trimws(x)` | `str.strip()` or `pd.Series.str.strip()` |
| `str_to_lower(x)` | `pd.Series.str.lower()` |
| `str_to_upper(x)` | `pd.Series.str.upper()` |
| `str_to_title(x)` | `pd.Series.str.title()` |
| `str_length(x)` | `pd.Series.str.len()` |
| `str_c(a, b, sep="")` | `a + b` or `pd.Series.str.cat(sep='')` |
| `paste(a, b, sep=" ")` | `a + " " + b` |
| `paste0(a, b)` | `a + b` |
| `paste(vec, collapse="\|")` | `"\|".join(vec)` |
| `str_count(x, pattern)` | `pd.Series.str.count(pattern)` |
| `str_pad(x, width, side, pad)` | `pd.Series.str.ljust(width)` / `.rjust()` / `.center()` |
| `str_split(x, pattern)` | `pd.Series.str.split(pattern)` |
| `str_match(x, pattern)` | `pd.Series.str.extract(pattern)` |
| `sprintf("%05d", n)` | `f"{n:05d}"` |
| `grepl(pattern, x)` | `bool(re.search(pattern, x))` |
| `gsub(pattern, repl, x)` | `re.sub(pattern, repl, x)` |
| `sub(pattern, repl, x)` | `re.sub(pattern, repl, x, count=1)` |

### Pivoting and reshaping

| R (tidyr) | Python (pandas) |
|---|---|
| `pivot_wider(df, id_cols=id, names_from=n, values_from=v, values_fn=list)` | `df.groupby([id, n])[v].apply(list).unstack()` |
| `pivot_longer(df, cols, names_to="name", values_to="val")` | `df.melt(id_vars=..., value_vars=..., var_name='name', value_name='val')` |
| `unnest(df, col)` | `df.explode('col')` |
| `unnest_wider(df, col, names_sep="_")` | `pd.json_normalize(df['col'])` or `df['col'].apply(pd.Series)` |
| `unite(df, "new", c(col1, col2), na.rm=T, sep=" ")` | `df[['col1','col2']].fillna('').agg(' '.join, axis=1).str.strip()` |
| `fill(df, col, .direction="down")` | `df['col'].ffill()` |
| `fill(df, col, .direction="up")` | `df['col'].bfill()` |
| `fill(df, col, .direction="downup")` | `df['col'].ffill().bfill()` |
| `separate(df, col, into=c("a","b"), sep="-")` | `df['col'].str.split('-', n=1, expand=True).rename(columns={0:'a',1:'b'})` |

### The `separate_rows()` lookahead problem

This is the single hardest translation. R's `tidyr::separate_rows()` with a lookahead regex splits a string and retains the delimiter at the start of each subsequent element. Python's `str.split()` with a lookahead regex works the same way:

```python
import re

def separate_rows_lookahead(series: pd.Series, pattern: str) -> pd.Series:
    """
    Split each string in series at positions matching pattern (lookahead),
    keeping the delimiter at the start of the new element.
    Returns an exploded series.
    """
    return (
        series
        .apply(lambda x: re.split(f'(?={pattern})', x) if pd.notna(x) else [x])
        .explode()
        .reset_index(drop=True)
    )
```

For DataFrame-level split (preserving all other columns):

```python
def separate_rows_df(df: pd.DataFrame, col: str, pattern: str) -> pd.DataFrame:
    """Split column on lookahead pattern, exploding rows."""
    df = df.copy()
    df[col] = df[col].apply(
        lambda x: re.split(f'(?={pattern})', str(x)) if pd.notna(x) else [x]
    )
    return df.explode(col).reset_index(drop=True)
```

**Regex engine limits in Python vs. R:**

R's `separate_rows()` batches patterns in groups of 300 when the alternation is very long (to avoid regex engine limits). Python's `re` module has a 100-named-group limit but no hard pattern-length limit for unnamed alternation. The `regex` third-party package (a drop-in `re` replacement) handles very long alternation patterns more reliably:

```
pip install regex
import regex
```

Since this project uses **node-iteration** rather than text-blob splitting, the large alternation pattern problem does not arise. `separate_rows()` with lookahead is only needed for specific edge cases (anonymous interjection splitting).

### The `item_df()` helper

The R `item_df()` function handles XML nodes with duplicate child names by building a long data frame and pivoting wide. The Python equivalent:

```python
def item_df(root, xpath: str) -> pd.DataFrame:
    """
    For each element matching xpath, extract all child tag names and text content.
    Returns a DataFrame pivoted wide, with list values for duplicate tags.
    Equivalent to R's item_df() helper function.
    """
    items = root.xpath(xpath)
    rows = []
    for i, item in enumerate(items, 1):
        for child in item:
            rows.append({
                'itemindex': i,
                'nodenames': child.tag,
                'contents': (child.text or '').strip()
            })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return (
        df.groupby(['itemindex', 'nodenames'])['contents']
        .apply(list)
        .unstack(fill_value=None)
    )
```

### Date and time handling

| R | Python |
|---|---|
| `as.Date("2000-02-15")` | `pd.Timestamp("2000-02-15").date()` or `datetime.date.fromisoformat("2000-02-15")` |
| `as.Date(x, format="%d/%m/%Y")` | `pd.to_datetime(x, format="%d/%m/%Y").dt.date` |
| `format(date, "%Y-%m-%d")` | `date.strftime("%Y-%m-%d")` |
| `strptime("9.30 am", "%I.%M %p")` | `datetime.strptime("9.30 am", "%I.%M %p")` |
| `as_hms("14:01:00")` | `datetime.time.fromisoformat("14:01:00")` or store as string |
| `Sys.time()` | `datetime.datetime.now()` |
| `difftime(a, b, units="days")` | `(a - b).days` |
| `format(time, "%H:%M:%S")` | `time.strftime("%H:%M:%S")` |

**Storage recommendation:** Store `time_stamp` as a string `"HH:MM:SS"` in CSV/Parquet for compatibility. The `hms` type in R has no direct pandas equivalent; `pd.to_timedelta` can represent time-of-day as a duration from midnight, but this is less intuitive. String storage matches the R CSV output exactly.

### Factor handling

R uses `factor` for categorical columns. In pandas, the equivalent is `pd.Categorical`. However, for Parquet output, pyarrow will encode categoricals as dictionary-encoded columns automatically, which is equivalent.

```python
# R: as.factor(x)
# Python:
df['party'] = pd.Categorical(df['party'])

# For Parquet output, pyarrow handles this natively:
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.Table.from_pandas(df)
pq.write_table(table, 'output.parquet', compression='snappy')
```

### Grouped operations

```r
# R
main %>%
  group_by(speech_no) %>%
  arrange(order) %>%
  mutate(interject = case_when(
    order == min(order) ~ 0,
    str_detect(name, "SPEAKER") ~ 0,
    TRUE ~ NA_real_
  )) %>%
  ungroup()
```

```python
# Python
def flag_interject(group):
    group = group.sort_values('order')
    conditions = [
        group['order'] == group['order'].min(),
        group['name'].str.contains('PRESIDENT', na=False),
    ]
    choices = [0, 0]
    group['interject'] = np.select(conditions, choices, default=np.nan)
    return group

main = main.groupby('speech_no', group_keys=False).apply(flag_interject)
```

### File operations

| R | Python |
|---|---|
| `dir_ls(path, regexp="\\.xml$")` | `list(Path(path).glob("*.xml"))` |
| `file_size(path)` | `Path(path).stat().st_size` |
| `read_csv(path, col_types=...)` | `pd.read_csv(path, dtype=...)` |
| `write_csv(df, path)` | `df.to_csv(path, index=False)` |
| `write_parquet(df, path)` | `df.to_parquet(path, index=False)` or `pq.write_table(pa.Table.from_pandas(df), path)` |
| `read_parquet(path)` | `pd.read_parquet(path)` |
| `map_dfr(files, read_csv, .id="date")` | `pd.concat([pd.read_csv(f).assign(date=f.stem) for f in files], ignore_index=True)` |

---

## Architectural decision: node-iteration vs. text-blob splitting

### Why the R code uses text-blob splitting

The Katz & Alexander R pipeline does not iterate over XML speech nodes directly. Instead it collapses each `<debate>` to a single text blob and splits on regex patterns. This was necessary because **R's `xml2::xml_text()` on a parent node concatenates all descendant text without preserving the node boundaries** — it cannot reconstruct which text belongs to which child speech/interjection node.

### Why Python can use node-iteration

Python's `lxml` library provides:
1. **Document-order iteration**: `element.iter()` yields all descendants in document order
2. **Text/tail distinction**: `node.text` = text before first child; `node.tail` = text after this node's closing tag within its parent
3. **Mixed-content handling**: can reconstruct the interleaving of speech and interjection text

This means the Python pipeline can walk the XML tree directly:

```python
def walk_chamber(root):
    for debate in root.findall('.//debate'):
        debate_context = extract_debate_info(debate)
        for node in debate.iter():
            if node.tag in ('speech', 'question', 'answer', 'interjection',
                            'continue', 'motionnospeech'):
                yield parse_speech_node(node, debate_context)
```

### Benefits of node-iteration

- No regex pattern extraction, escaping, or batching
- No risk of splitting on wrong boundary
- Handles nested interjections and `<continue>` elements correctly by design
- Schema changes are handled by checking `node.tag` directly
- No need for the `item_df()` helper for core parsing (only needed for division member lists)

### Trade-off

The text-blob approach, despite its complexity, has one advantage: it forces all textual content through a uniform pipeline, making it easier to spot inconsistencies (if the blob doesn't split cleanly on a pattern, that pattern is wrong). The node-iteration approach requires careful handling of `node.text` vs `node.tail` to reconstruct full paragraph text.

---

## Column naming convention

The R pipeline uses R's idiomatic `dot.case` for column names (e.g. `name.id`, `page.no`, `time.stamp`, `in.gov`, `first.speech`, `q_in_writing`, `fedchamb_flag`). Python conventions favour `snake_case`.

**Decision:** Use `snake_case` throughout the Python pipeline for internal processing. The final output columns should match the Katz & Alexander schema names where possible, using underscores in place of dots:

| R column | Python column |
|---|---|
| `name.id` | `name_id` |
| `page.no` | `page_no` |
| `time.stamp` | `time_stamp` |
| `in.gov` | `in_gov` |
| `first.speech` | `first_speech` |
| `q_in_writing` | `q_in_writing` (unchanged — already underscored) |
| `fedchamb_flag` | Removed for Senate |
| `div_flag` | `div_flag` (unchanged) |
| `speech_no` | `speech_no` (unchanged) |
| `uniqueID` | `unique_id` |
| `partyfacts_id` | `partyfacts_id` (unchanged) |

This renaming should be documented prominently in the dataset README so users of both the House and Senate corpora can reconcile column names.
