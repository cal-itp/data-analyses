# Demo

## NTD Monthly Ridership

### Site yml file is now local to the site. Everything a site needs is in the package

### portfolio_utils -> calitp_portfolio.mutations

```python
    portfolio_utils.create_portfolio_yaml_chapters_no_sections(
        PORTFOLIO_SITE_YAML, chapter_name="rtpa", chapter_values=list(df.rtpa_name)
    )
```
```python
    site = load_site(PORTFOLIO_SITE_YAML)
    site = generate_parts_flat(site, param_key="rtpa", values=list(df.rtpa_name))
    site.write_yaml(PORTFOLIO_SITE_YAML)
```

### calitp_data_analysis.magics -> calitp_portfolio.magics
```python
%%capture_parameters
```

### Workflow

```bash
uv run calitp-portfolio login
```

```bash
uv run calitp-portfolio
uv run calitp-portfolio build --help
```

```bash
uv run calitp-portfolio list ntd_monthly_ridership.yml | head -5
```

```bash
uv run calitp-portfolio build ntd_monthly_ridership.yml --prepare-only --limit 5
```

```bash
uv run calitp-portfolio build ntd_monthly_ridership.yml --readme-only
uv run calitp-portfolio build ntd_monthly_ridership.yml --toc-only
```

```bash
uv run calitp-portfolio build ntd_monthly_ridership.yml --limit 1

uv run calitp-portfolio list ntd_monthly_ridership.yml | head -5
uv run calitp-portfolio build ntd_monthly_ridership.yml --only 00__monthly_ridership_report__rtpa_butte-county-association-of-governments
```

### Artifacts
build.json - when did I last successfully build this?

build.log - what happened when I built it?
