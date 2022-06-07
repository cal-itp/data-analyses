# data-analyses
Place for sharing quick reports, and works in progress

This repository is for quick sharing of works in progress and simple analyses. 
For collaborative short-term tasks, create a new folder and work off a separate branch.
For longer-term projects, consider making a new repository!

## Using this Repo

* Use [this link](https://docs.calitp.org/data-infra/analytics_tools/saving_code.html#onboarding-setup) to get started in JupyterHub, set up SSH, and start commiting to the repo!

## Quick Links - Get Started in Data Analysis

#### Data Analytics Documentation - Welcome

https://docs.calitp.org/data-infra/analytics_welcome/overview.html

#### Data Analytics Documentation - Introduction to Analytics Tools

https://docs.calitp.org/data-infra/analytics_tools/overview.html

## Publishing Reports
[The sites folder](./portfolio/sites/) contains the YAML files that drive sites
deployed to [https://analysis.calitp.org/](https://analysis.calitp.org/); the existing
sites can be used as examples/templates for deploying additional sites. Also, the
Data Services Documentation has a [specific chapter](https://docs.calitp.org/data-infra/publishing/overview.html)
dedicated to various ways to publish data.

### Caveats (when using the portfolio site)
Jupyter Book/Sphinx do not play nicely with Markdown headers written out in `display()`
calls. Therefore, [portfolio.py](./portfolio.py) uses a custom Papermill
engine to template Markdown cells directly, following Python formatted-string
syntax. For example, your Markdown cell could contain `# {district_name}` and
it will be templated by the underlying engine.
