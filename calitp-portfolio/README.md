# calitp-portfolio

A CLI for building, validating, and deploying Cal-ITP **portfolio sites** — the
public, statically-rendered websites generated from parameterized Jupyter
notebooks (GTFS Digest, RT Speeds, AHSC, District Digest, …).

It takes a declarative `site.yml`, fans a single notebook out over a list of
parameters (one chapter per transit operator, district, etc.), executes each via
[papermill](https://papermill.readthedocs.io/), renders a
[Jupyter Book v2 / MyST](https://next.jupyterbook.org/) table of contents, builds
static HTML, and uploads it to a public GCS bucket.

## Concepts

A portfolio is described by two kinds of YAML file:

- **`site.yml`** — one per portfolio site. Declares a notebook, a readme, a
  deploy target, and a tree of **parts → chapters**. Each chapter binds the
  notebook to a set of `params`; the same notebook becomes N rendered pages.
- **`sites.yml`** — the manifest of *all* sites, used to render the landing /
  index page that links to each built site.

The object model (`models.py`) mirrors that structure:

| Model | Role |
|-------|------|
| `Site` | A whole portfolio site: title, readme, default notebook, deploy targets, parts. |
| `Part` | A grouping of chapters, optionally with a `caption` (becomes a TOC section heading). |
| `Chapter` | One rendered page: a notebook + `params`. Notebook and params resolve up the chain (chapter → part → site), so you set defaults once and override per chapter. |

A chapter's **identifier** (e.g. `00__notebook_with_params_2__greetings_humboldt-transit-authority`)
is the stable handle used by `list` output and `build --only` — it matches the
parameterized notebook's filename stem.

### Example `site.yml`

```yaml
title: Group and Params Analyses Test
directory: tests/fixtures/portfolio
readme: ./tests/fixtures/portfolio/README_GP.md
notebook: tests/fixtures/portfolio/notebook_with_params_2.ipynb
parts:
  - caption: District 01 Eureka
    chapters:
      - params:
          greetings: Humboldt Transit Authority
      - params:
          greetings: Lake Transit Authority
  - caption: District 02 Redding
    chapters:
      - params:
          greetings: Tehama County
deploy:
  staging: gs://calitp-analysis-staging/_group_and_params_analyses_test
  # prod:  gs://calitp-analysis/group_and_params   # add when ready to release
```

This produces one page per `greetings` value, grouped under two TOC sections.

## Installation

`calitp-portfolio` is a [uv workspace](../../README.md) member of `data-analyses`.
From the repo root:

```bash
uv sync                 # installs the workspace, including this package
uv run calitp-portfolio --help
```

The build pipeline also shells out to a **`jupyter book`** binary (provided by
`jupyter-book==2.1.1`, a declared dependency) and, for accessibility scans, to
the [`@axe-core/cli`](https://github.com/dequelabs/axe-core-npm) Node binary
(`npx axe`), which must be on `PATH` for `axe-check`.

## Authentication

Executing notebooks (papermill) and deploying to GCS both require Google
Application Default Credentials. Authenticate once with the bundled Cal-ITP
login config:

```bash
uv run calitp-portfolio login
```

Commands that need credentials run a pre-flight check first and exit with a
pointer to `login` if none are found. `build --no-execute` and the readme/TOC
preview builds skip the check, so you can iterate on structure without auth.

## Commands

```
calitp-portfolio [COMMAND] [ARGS]...
```

| Command | Purpose |
|---------|---------|
| `build` | Build static HTML from a parameterized notebook portfolio. |
| `list` | Print the resolved part/chapter tree with slugs, params, and notebook paths. |
| `index` | Render the landing page from a `sites.yml` manifest (optionally deploy it). |
| `deploy` | Upload built HTML to the site's GCS deploy target. |
| `clean` | Remove a site's build artifacts (idempotent). |
| `login` | Authenticate to Google Cloud with the bundled Cal-ITP config. |
| `axe-check` | Run an accessibility (WCAG) scan against built HTML. |

### `build`

```bash
# Full build (executes notebooks → renders HTML)
uv run calitp-portfolio build sites/gtfs_digest.yml

# Structure-only iteration (no papermill, no auth needed)
uv run calitp-portfolio build sites/gtfs_digest.yml --output-dir /tmp/out --no-execute
```

Artifacts land in `--output-dir` (default `<yml dir>/<yml stem>/`): parameterized
notebooks per chapter, a generated `myst.yml` (config + TOC), bundled template
assets, the built site under `_build/html/`, plus a `build.log` and a
`build.json` manifest (tool version, timestamp, yml hash, error count).

Useful flags for fast iteration:

| Flag | Effect |
|------|--------|
| `--no-execute` | Skip papermill; render structure only. |
| `--readme-only` | Build just the landing page; skip all chapters. |
| `--toc-only` | Re-render `myst.yml` + rebuild, reusing prior notebook outputs (fast after a yml edit). |
| `--only <ids>` | Build only the comma-separated chapter identifiers (see `list`). |
| `--limit N` | Build only the first N chapters, in source order (quick smoke build). |
| `--hide-title-block` | Suppress the per-page title block. |
| `--continue-on-error` | Keep building remaining chapters after a papermill failure (reported at the end, non-zero exit). |
| `--show-stderr` | Keep cell stderr in outputs (stripped by default). |

### `list`

```bash
uv run calitp-portfolio list sites/gtfs_digest.yml
```

Prints the resolved tree — one line per chapter with its identifier, params, and
notebook. Use it to find the slugs to pass to `build --only`.

### `index`

```bash
# Render the landing page
uv run calitp-portfolio index sites.yml --output /tmp/index.html

# Render and upload to the manifest's deploy target
uv run calitp-portfolio index sites.yml --deploy --target prod
```

Test sites (`test_sites:` in the manifest) are linked only on the `staging`
target.

### `deploy`

```bash
# Upload <site>/_build/html to the yml's deploy target
uv run calitp-portfolio deploy sites/gtfs_digest.yml --target prod

# Deploy an arbitrary HTML directory to an arbitrary bucket prefix
uv run calitp-portfolio deploy --html /tmp/out/_build/html --target-url gs://bucket/prefix
```

`--target prod` errors if the yml has no `deploy.prod` set — a guard against
publishing a site that isn't marked ready for release.

### `clean`

```bash
uv run calitp-portfolio clean sites/gtfs_digest.yml          # removes _build/
uv run calitp-portfolio clean sites/gtfs_digest.yml --all    # also removes per-chapter notebook output dirs
```

### `axe-check`

```bash
# Scan a built site (serves over http so client-rendered charts/maps load)
uv run calitp-portfolio axe-check sites/gtfs_digest.yml --wcag aa

# Scan a static directory directly
uv run calitp-portfolio axe-check --html /tmp/out/_build/html --impact all --report
```

Per-rule deduping and a `--report` JSON dump are supported. When scanning a
`site.yml` it serves over http and waits for MyST/Vega/Folium to render before
auditing. The scan runs [`@axe-core/cli`](https://github.com/dequelabs/axe-core-npm/tree/develop/packages/cli)
under the hood (pinned version); each finding maps to an
[axe rule](https://dequeuniversity.com/rules/axe/) you can look up for an
explanation and remediation steps.

Two filters control what the scan tests for and what it shows you:

**`--wcag` — which conformance level to test against.** WCAG defines three
cumulative levels: **A** (must-have basics), **AA** (the standard most public
sites and procurement rules target, including
[California / Section 508](https://www.w3.org/WAI/standards-guidelines/wcag/)),
and **AAA** (the strictest, rarely required site-wide). Each level includes the
ones below it. The flag selects the axe rule tags applied:

| `--wcag` | axe tags enabled |
|----------|------------------|
| `a` | `wcag2a` |
| `aa` *(default)* | `wcag2a`, `wcag2aa`, `wcag21aa` |
| `aaa` | `wcag2a`, `wcag2aa`, `wcag21aa`, `wcag2aaa`, `wcag21aaa` |

**`--impact` — how severe a violation must be to be reported.** Impact is axe's
own rating of how much a failure affects users with disabilities — independent of
WCAG level. From least to most severe:

| Impact | Meaning |
|--------|---------|
| `minor` | Nuisance; minor annoyance for some users. |
| `moderate` | Some difficulty for some users; worth fixing. |
| `serious` | Significant barrier; blocks or severely frustrates affected users. |
| `critical` | Blocks access entirely for affected users. |

The default `serious,critical` keeps the report focused on what actually breaks
the experience. Pass a comma-separated subset, or `--impact all` to see
everything down to `minor`.

## Library API

For prepare-scripts that generate a `site.yml` programmatically (the replacements
for `_shared_utils.portfolio_utils.create_portfolio_yaml_chapters_*`), load a
`Site`, mutate its parts, and re-dump. See [`command_ref.md`](command_ref.md) for
worked examples reproducing each fixture.

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

Three generators cover the common shapes:

- `generate_parts_flat` — one part, N chapters varying a single param.
- `generate_parts_grouped` — N captioned parts, each with M chapters.
- `generate_parts_sections` — one part, N chapters each with sub-sections.

## Authoring notebooks

Notebooks receive their `params` via papermill. A notebook can also *report back*
computed values (e.g. a human-readable title derived from a param) into the
site's metadata using the bundled cell magic:

```python
%%capture_parameters
district_name = lookup_district(district)
district_name
```

The last line names the variables to capture; the build engine reads them out of
the cell output and merges them into the chapter's params.

## Development

```bash
uv run --group portfolio pytest          # 90 tests across 13 files
```

Tests run against bundled fixtures under `tests/fixtures/` (copied from the
legacy portfolio's test corpus). The highest-value coverage is the **TOC
snapshots** (`tests/snapshots/`, exercised by `test_toc.py`): TOC generation is
historically the layer that breaks under MyST upgrades, so snapshot tests guard
it explicitly.

Conventions follow the `data-analyses` repo: Black + isort (profile "black",
line length 120), enforced by pre-commit. Scope pre-commit runs to changed files
(`pre-commit run --files ...`) to avoid reformatting sibling projects.

## Build pipeline, end to end

```
site.yml ──▶ load_site() ──▶ Site / Part / Chapter
                               │
                               ├─▶ papermill: execute notebook once per chapter (params)
                               ├─▶ render myst.yml (config + generated TOC)
                               ├─▶ bundle template assets (logos, css, footer)
                               └─▶ jupyter book build --html  ──▶ _build/html/
                                                                      │
sites.yml ──▶ index ──▶ index.html                                    ▼
                                                          deploy ──▶ gs://… (public bucket)
```
