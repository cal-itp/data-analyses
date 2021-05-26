---
jupyter:
  jupytext:
    formats: md//md,ipynb//ipynb
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.11.2
  kernelspec:
    display_name: venv-notebooks
    language: python
    name: venv-notebooks
---

```python
from siuba import *
from siuba.sql import LazyTbl
from siuba.dply import vector as vec

from sqlalchemy import create_engine

# limit at 1 Gb
engine = create_engine("bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000")
```

```python
# special ipython function to get the html formatter
import pandas as pd
pd.set_option("display.max_rows", 10)

# pandas hardcodes style tags onto its output, so we need to strip them off.
# this means that we change how ipython generates the html, and flag notebook=False
# in pandas' to_html method.
# see: https://stackoverflow.com/q/51460112/1144523
html_formatter = get_ipython().display_formatter.formatters['text/html']

# here, we avoid the default df._repr_html_ method, since it inlines css
# which does not work with markdown output
f_repr_df = lambda df: df.to_html(max_rows = pd.get_option("display.max_rows"), show_dimensions = True, border=0)

html_formatter.for_type(
    pd.DataFrame,
    f_repr_df,
)
```

```python
# siuba uses pandas' default html representation, so need to overload it too
from siuba.sql import LazyTbl
# hard-coding template from LazyTbl._repr_html
template = (
        "<div>"
        "<pre>"
        "# Source: lazy query\n"
        "# DB Conn: {}\n"
        "# Preview:\n"
        "</pre>"
        "{}"
        "<p># .. may have more rows</p>"
        "</div>"
        )


html_formatter.for_type(
    LazyTbl,
    lambda tbl: template.format(tbl.source.engine, f_repr_df(tbl._get_preview()))
)
```

```python
class AutoTable:
    def __init__(self, engine, table_formatter = None, table_filter = None):
        self._engine = engine
        self._table_names = self._engine.table_names()
        
        mappings = {}
        for name in self._table_names:
            if table_filter is not None and not table_filter(name):
                continue
                
            fmt_name = table_formatter(name)
            if fmt_name in mappings:
                raise Exception("multiple tables w/ formatted name: %s" %fmt_name)
            mappings[fmt_name] = name
        
        self._attach_mappings(mappings)
        
    def _attach_mappings(self, mappings):
        for k, v in mappings.items():
            loader = lambda self: self._load_table
            setattr(self, k, self._table_factory(v))
    
    def _table_factory(self, table_name):
        def loader():
            return self._load_table(table_name)
        
        return loader
    
    def _load_table(self, table_name):
        return LazyTbl(self._engine, table_name)
        
```
