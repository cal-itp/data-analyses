Index page

uv run calitp-portfolio index tests/fixtures/sites/sites.yml --output /tmp/index.html

### Basic site
uv run calitp-portfolio build tests/fixtures/sites/_basic_analyses_test.yml --output-dir /tmp/build-basic-real

### README-only site
uv run calitp-portfolio build tests/fixtures/sites/_readme_only_analyses_test.yml --output-dir /tmp/build-readme --no-execute

### Section chapters
uv run calitp-portfolio build tests/fixtures/sites/_section_analyses_test.yml --output-dir /tmp/build-sections --no-execute
ls /tmp/build-sections/   # day_or_night_01-day.md, day_or_night_02-night.md

### --hide-title-block diff
uv run calitp-portfolio build tests/fixtures/sites/_param_manual_title_analyses_test.yml --output-dir /tmp/build-on --no-execute --hide-title-block

### Build only selected chapters
uv run calitp-portfolio list tests/fixtures/sites/_group_and_params_analyses_test.yml   # grep for the slugs you want

uv run calitp-portfolio build tests/fixtures/sites/_group_and_params_analyses_test.yml \
  --output-dir /tmp/build-subset --no-execute \
  --only 00__notebook_with_params_2__greetings_humboldt-transit-authority,00__notebook_with_params_2__greetings_tehama-county

### Limit chapter count (quick smoke build)
uv run calitp-portfolio build tests/fixtures/sites/_group_and_params_analyses_test.yml \
  --output-dir /tmp/build-first3 --no-execute --limit 3

### Readme-only (landing page preview)
uv run calitp-portfolio build tests/fixtures/sites/_param_analyses_test.yml \
  --output-dir /tmp/build-readme --readme-only

### TOC-only (re-render structure after a yml edit, reuse prior notebook outputs)
uv run calitp-portfolio build tests/fixtures/sites/_param_analyses_test.yml \
  --output-dir /tmp/build-toc --toc-only

---

## Library API (prepare-script use)

shared_utils.portfolio_utils.create_portfolio_yaml_chapters_* replacements

Each example loads a fixture, calls the helper with inputs that reproduce that fixture's `parts:`, then re-dumps.

### Flat (one Part, N Chapters with params)

Reproduces `tests/fixtures/sites/_param_analyses_test.yml`.

```python
from pathlib import Path
from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_flat

src = Path("tests/fixtures/sites/_param_analyses_test.yml")
site = load_site(src)
site = generate_parts_flat(
    site,
    param_key="greetings",
    values=["Hi! So happy to see you here!!", "Bye! See you soon!!"],
)
site.write_yaml(Path("/tmp/param_out.yml"))
```

### Grouped (N Parts with captions, M Chapters each)

Reproduces `tests/fixtures/sites/_group_and_params_analyses_test.yml`.

```python
from pathlib import Path
from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_grouped

src = Path("tests/fixtures/sites/_group_and_params_analyses_test.yml")
site = load_site(src)
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

### Sections (one Part, N Chapters each with sub-sections)

Reproduces `tests/fixtures/sites/_section_analyses_test.yml`.

```python
from pathlib import Path
from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_sections

src = Path("tests/fixtures/sites/_section_analyses_test.yml")
site = load_site(src)
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

---
