# calitp-portfolio-sites

The manifest for the Cal-ITP analysis portfolio **landing page**:

- production: <https://analysis.dds.dot.ca.gov/>
- staging: <https://analysis-staging.dds.dot.ca.gov/>

`sites.yml` lists every published portfolio site (plus the test sites, which
are linked on staging only). The [`calitp-portfolio`](../calitp-portfolio/README.md)
CLI renders it into the landing page with the `index` command — see the
[`index` docs](../calitp-portfolio/README.md#index) for the manifest schema and
how rendering/deploying works.

[`examples/`](examples/) holds the notebooks and readmes the test sites are
built from.

## Usage

Preview the landing page locally (from the repo root):

```bash
uv run calitp-portfolio index calitp-portfolio-sites/sites.yml --output /tmp/index.html
open /tmp/index.html
```

To add a site, append an entry to `sites:` — `title` is the link text, `name`
must match the final path segment of the site's GCS deploy target (the page
links to `/<name>/`), and `source` points at the site's source directory.

## Deploying

Requires Google credentials (`uv run calitp-portfolio login`, once):

```bash
# Staging (includes the test-site links)
uv run calitp-portfolio index calitp-portfolio-sites/sites.yml --deploy --output /tmp/index.html

# Production (test sites omitted)
uv run calitp-portfolio index calitp-portfolio-sites/sites.yml --deploy --target prod --output /tmp/index.html
```

Without `--output`, the rendered `index.html` is written next to `sites.yml` —
pass it to keep this directory free of build artifacts.
