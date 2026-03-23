# Introduction
This website contains data analysis and reports developed by Cal-ITP data analysts and scientists.

## Source code

All source code for these analyses and reports may be found [on GitHub](https://github.com/cal-itp/data-analyses).

## Setup

Portfolio has additional dependencies beyond the workspace baseline. Install them with:

```
uv sync --group portfolio
```

## Running locally (without JupyterHub)

```
uv sync --group portfolio
uv run python portfolio/portfolio.py build MY_NEW_REPORT
```

## Deployment
### Typical Workflow
Creating a parameterized notebook means creating a Jupyter notebook set up so that multiple notebooks can be dynamically created from it by passing different configurations.
See instructions here for authoring a notebook for paramaterization [pointers for styling notebooks](https://docs.calitp.org/data-infra/publishing/sections/4_notebooks_styling.html)

1. Setup your configurations.
   * Add a `site_name.yml` to `portfolio/sites/` - this controls the parameterization related to the notebooks.
   * JupyterBooks have a table of contents and organize chapters and sections. We allow both chapters and sections to be parameterized, and examples are given for the most common types of supported parameterized reports.
   * The path to the parameterized notebook must be defined (`project_folder/report.ipynb`)
   * The path to the README must be defiend (`project_folder/README.md`)
   * How to organize the chapters and sections. The 2 most common are
     * by Caltrans district (chapter) and each transit operator within the district gets a page (section), and
     * by Caltrans district (chapter) and each district is its own page (no section).
   * [various parameterization examples](https://docs.calitp.org/data-infra/publishing/sections/5_analytics_portfolio_site.html)

2. Build and deploy your parameterized notebook as a JupyterBook
   * (Optionally) Remove the local folder containing the previously generate portfolio site
     ```
     python portfolio/portfolio.py clean MY_NEW_REPORT
     ```
   * Parameterize the notebook specified in `portfolio/sites/MY_NEW_REPORT.yml`
     ```
     python portfolio/portfolio.py build MY_NEW_REPORT
     ```

     Add the option `--hide_title_block` if you want to build the site without the Title Block.

     ```
     python portfolio/portfolio.py build MY_NEW_REPORT --hide_title_block
     ```
      * local files are created in `portfolio/MY_NEW_REPORT/` (all files below are within this newly created `portfolio/MY_NEW_REPORT/` sub-directory)
      * JupyterBook necessary accessories: `myst.yml` and a README file.
      * There will be additional files or directories holding the parameterized notebooks and site build files.
   * During build you will see any accessibility violations that are detected in the generated site.

3. Deploy your portfolio site to the staging environment for review
> [!NOTE]
> The default target is staging, so `--target staging` can be supplied.

   ```
   python portfolio/portfolio.py deploy-site MY_NEW_REPORT --target staging
   ```

   This will deploy to `https://analysis-staging.dds.dot.ca.gov/MY_NEW_REPORT`. You can later deploy to production with `--target production`.

### Additional useful commands and info
> [!NOTE]
> The default target is staging, so `--target staging` can be supplied.

* Combine __production__ build and deploy steps for a specific portfolio site
  ```
  python portfolio/portfolio.py build MY_NEW_REPORT --deploy --target staging
  ```
  This will deploy to `https://analysis.dds.dot.ca.gov/MY_NEW_REPORT`.
* When we deploy, the HTML files in `portfolio/MY_NEW_REPORT/_build/html` are published either to the staging or production sites.
* Many of these steps are also documented in the [Makefile](https://github.com/cal-itp/data-analyses/blob/main/Makefile).
* Portfolio sites are now gitignored, no longer checked into git.

### Deploy Portfolio Index
> [!NOTE]
> The default target is staging, so `--target staging` can be supplied.

To deploy the Index page to the staging environment (https://analysis-staging.dds.dot.ca.gov/), run:
```
python portfolio/portfolio.py deploy-index --target staging
```
You can deploy to production with `--target production`.

There is only one difference between staging and production Index page. In the Staging Index page you will see a list of `Test Projects` that you can use as examples or experiment building new ones.

## Accessibility Testing
You will see accessibility violations at the end of building your site. If you have already built your site and wish to run only accessibilty checks, you can run `python portfolio/portfolio.py check-accessibility MY_NEW_REPORT`

### Accesibility Warning Severity
❔- Minor
⚠️- Moderate
🛑- Serious
🛑‼️- Critical
