# Library API

`calitp-portfolio` is importable as `calitp_portfolio` for **prepare scripts** —
the scripts that regenerate a site's `parts:` from warehouse data before a build.
These are the replacements for
`_shared_utils.portfolio_utils.create_portfolio_yaml_chapters_*` and the
per-project `deploy_portfolio_yaml.py` scripts built on them.

The workflow is **load → mutate → write**:

```python
from pathlib import Path
from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_flat

site = load_site(Path("sites/gtfs_digest.yml"))
site = generate_parts_flat(
    site,
    param_key="greetings",
    values=["Humboldt Transit Authority", "Tehama County"],
)
site.write_yaml(Path("sites/gtfs_digest.yml"))
```

Everything outside `parts:` (title, readme, notebook, deploy targets) is
preserved from the loaded yml; only the part/chapter tree is regenerated.

## Loading and writing

### `load_site(yml_path, output_dir=None) -> Site`

Parses a `site.yml` and validates it into a `Site` (a pydantic model — bad or
missing fields raise a `ValidationError` at load time, not mid-build). The
site's `name` is the yml filename stem.

`output_dir` (where `build` writes artifacts) resolves with this precedence,
highest first:

1. the explicit `output_dir` argument
2. the yml's `output_dir:` field, resolved relative to the yml's directory
3. default: `<yml dir>/<yml stem>/`

Prepare scripts can ignore `output_dir` — it is excluded from serialization, so
a load → write round-trip never injects it into the yml.

### `Site.to_yaml() -> str` and `Site.write_yaml(path) -> None`

`to_yaml()` serializes the `Site` back to a canonical `site.yml` string: keys in
model order, unset and default-valued fields omitted, and the `readme` path kept
in `./`-prefixed form so a reloaded yml resolves it identically (paths without
`./` are resolved relative to `directory:`; see `Site.default_readme`).
`write_yaml(path)` writes that string to disk.

## Generating `parts:`

Three pure helpers in `calitp_portfolio.mutations` cover the common shapes.
Shared semantics:

- **Pure** — each returns a new `Site` (`model_copy`); the input is untouched
  and nothing is written to disk until you call `write_yaml`.
- **Wholesale replacement** — the generated parts *replace* the existing
  `parts:` tree; anything hand-written there is dropped.
- **Values are stringified** — every param value passes through `str()`, so
  ints, dates, etc. are safe inputs.
- **Insertion order is preserved** — dict/list order in the inputs becomes
  part/chapter order in the TOC.

The examples below each reproduce one of the test fixtures under
`tests/fixtures/sites/`, so you can diff your output against a known-good yml.

### `generate_parts_flat(site, *, param_key, values)`

One `Part`, N `Chapter`s — one chapter per value, each with
`params={param_key: value}`. The shape for "one page per operator" sites.

```python
from pathlib import Path

from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_flat

site = load_site(Path("tests/fixtures/sites/_param_analyses_test.yml"))
site = generate_parts_flat(
    site,
    param_key="greetings",
    values=["Hi! So happy to see you here!!", "Bye! See you soon!!"],
)
site.write_yaml(Path("/tmp/param_out.yml"))
```

Generated `parts:`:

```yaml
parts:
  - chapters:
      - params:
          greetings: Hi! So happy to see you here!!
      - params:
          greetings: Bye! See you soon!!
```

### `generate_parts_grouped(site, *, param_key, groups)`

N captioned `Part`s, M `Chapter`s each. `groups` maps each part caption (a TOC
section heading) to its list of param values. The shape for "operators grouped
by district" sites.

```python
from pathlib import Path

from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_grouped

site = load_site(Path("tests/fixtures/sites/_group_and_params_analyses_test.yml"))
site = generate_parts_grouped(
    site,
    param_key="greetings",
    groups={
        "District 01 Eureka": [
            "Humboldt Transit Authority",
            "Lake Transit Authority",
            "Mendocino Transit Authority",
            "Redwood Coast Transit Authority",
        ],
        "District 02 Redding": [
            "Redding Area Bus Authority",
            "Tehama County",
        ],
    },
)
site.write_yaml(Path("/tmp/group_out.yml"))
```

Generated `parts:`:

```yaml
parts:
  - caption: District 01 Eureka
    chapters:
      - params:
          greetings: Humboldt Transit Authority
      - params:
          greetings: Lake Transit Authority
      - params:
          greetings: Mendocino Transit Authority
      - params:
          greetings: Redwood Coast Transit Authority
  - caption: District 02 Redding
    chapters:
      - params:
          greetings: Redding Area Bus Authority
      - params:
          greetings: Tehama County
```

### `generate_parts_sections(site, *, chapter_key, section_key, chapters)`

One `Part` whose `Chapter`s each carry **sub-sections**: a chapter becomes a
captioned TOC node with one rendered notebook per section beneath it. `chapters`
maps each chapter's param value to a spec dict with two keys:

| Key | Meaning |
|-----|---------|
| `caption` | The chapter's TOC heading. |
| `sections` | Param values for `section_key` — one rendered notebook each. |

Each chapter gets `params={chapter_key: <key>}` and
`sections=[{section_key: <value>}, ...]`; at build time every section's params
merge over the chapter's (so the notebook sees both keys).

```python
from pathlib import Path

from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_sections

site = load_site(Path("tests/fixtures/sites/_section_analyses_test.yml"))
site = generate_parts_sections(
    site,
    chapter_key="day_or_night",
    section_key="greetings",
    chapters={
        "01 - Day": {
            "caption": "Daily Greetings",
            "sections": ["Good Morning!", "Good Afternoon!"],
        },
        "02 - Night": {
            "caption": "Night Greetings",
            "sections": ["Sleep well!"],
        },
    },
)
site.write_yaml(Path("/tmp/section_out.yml"))
```

Generated `parts:`:

```yaml
parts:
  - chapters:
      - caption: Daily Greetings
        params:
          day_or_night: 01 - Day
        sections:
          - greetings: Good Morning!
          - greetings: Good Afternoon!
      - caption: Night Greetings
        params:
          day_or_night: 02 - Night
        sections:
          - greetings: Sleep well!
```
