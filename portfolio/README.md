# Introduction
This website contains data analysis and reports developed by Cal-ITP data analysts and scientists.

Creating a parameterized notebook means creating a Jupyter notebook set up so that multiple notebooks can be dynamically created from it by passing different configurations.

See instructions for authoring a notebook in [Getting Notebooks Ready for the Portfolio](https://docs.calitp.org/data-infra/publishing/sections/4_notebooks_styling.html) and [The Cal-ITP Analytics Portfolio](https://docs.calitp.org/data-infra/publishing/sections/5_analytics_portfolio_site.html).


## Source code

All source code for these analyses and reports may be found [on GitHub](https://github.com/cal-itp/data-analyses).


## Environments

> [!IMPORTANT]
> The default target is staging. To set the production target just add `--target production` to the commands.

Portfolios will be generated in your local environment and can be deployed to the following targets:

* [Production - https://analysis.dds.dot.ca.gov](https://analysis.dds.dot.ca.gov/)
* [Staging - https://analysis-staging.dds.dot.ca.gov](https://analysis-staging.dds.dot.ca.gov/)


## Typical Workflow

Many of these steps are also documented in the [Makefile](https://github.com/cal-itp/data-analyses/blob/main/Makefile).

### 1. Create a `README.md` file

The `README.md` file will be the landing page of your Porfolio site.
To create uniformity across portfolios, follow the [template](https://github.com/cal-itp/data-analyses/blob/main/portfolio/template_README.md).

### 2. Setup your configurations

JupyterBooks have a table of contents organized in chapters. Chapters can be grouped and parameterized.

To configure how your notebooks are organized and parameterized, you need to create a configuration file like `<my_report_site>.yml` in [portfolio/sites/](https://github.com/cal-itp/data-analyses/tree/main/portfolio/sites). Replace `<my_report_site>` by your site name.

You can create your file manually by following the example files (`_<example_name>_test.yml`) located in the [portfolio/sites/](https://github.com/cal-itp/data-analyses/tree/main/portfolio/sites) folder.

Or run the generator code from shared_utils:
- Create your own code generator in Python.
- Import `from shared_utils import portfolio_utils`.
- Use one of the functions available: [create_portfolio_yaml_chapters_no_sections](https://github.com/cal-itp/data-analyses/blob/ffc3dae8004cb7285958d1f2631f3ac87ae57f1c/_shared_utils/shared_utils/portfolio_utils.py#L24), [create_portfolio_yaml_chapters_with_groups](https://github.com/cal-itp/data-analyses/blob/ffc3dae8004cb7285958d1f2631f3ac87ae57f1c/_shared_utils/shared_utils/portfolio_utils.py#L56), or [create_portfolio_yaml_chapters_with_sections](https://github.com/cal-itp/data-analyses/blob/ffc3dae8004cb7285958d1f2631f3ac87ae57f1c/_shared_utils/shared_utils/portfolio_utils.py#L136).

### 3. Build your notebook as a JupyterBook

> [!IMPORTANT]
> If you are using **Prototype Image - 2026.3.18, Python 3.11** in JupyterHub, add `uv run` to the beginning of the command.<br/>
> For example: `uv run python portfolio/portfolio.py clean MY_REPORT_SITE`.


> [!NOTE]
> Files generated to build the site are [ignored](https://github.com/cal-itp/data-analyses/blob/2303a904302a8e9ff53b5710a7bc2a71d590593c/.gitignore#L141) and will not be tracked or commited to the github repo.

Replace `MY_REPORT_SITE` with the same name from the `my_report_site.yml`, created on Step 1.

* (Optional) Remove the local folder containing the previously generate portfolio site:
  ```bash
  $ python portfolio/portfolio.py clean MY_REPORT_SITE
  ```

* Parameterize the notebook specified in `portfolio/sites/MY_REPORT_SITE.yml`:
  ```bash
  $ python portfolio/portfolio.py build MY_REPORT_SITE
  ```

  Add the option `--hide_title_block` if you want to build the site without the `Title Block`:

  ```bash
  $ python portfolio/portfolio.py build MY_REPORT_SITE --hide_title_block
  ```

  Local files will be created in `portfolio/MY_REPORT_SITE/`:
   * JupyterBook necessary accessories: `myst.yml` and a README file.
   * Additional files or directories holding the parameterized notebooks and site.

* Verify the accessibility violations that are detected in the generated site. See more info in [Accessibility Testing](#accessibility_testing) bellow.


### 4. Deploy your portfolio site to Staging for review

Replace `MY_REPORT_SITE` with the same name from the `my_report_site.yml`, created on Step 1.

```bash
$ python portfolio/portfolio.py deploy-site MY_REPORT_SITE
```

This command will deploys to `https://analysis-staging.dds.dot.ca.gov/MY_REPORT_SITE`. You can later deploy to production.


### 5. (Optional) Update the Index page for Staging

> [!NOTE]
> Staging Index page displays a list of `Test Projects` that you can use for examples or experiments.

If you are creating a new site or updating the name, this step will include a new link to the Portfolio list.
If you are changing the [Index Template](https://github.com/cal-itp/data-analyses/blob/main/portfolio/templates/index.html), you need to re-build the Index page to see the changes.

To build the Index page locally, run:

```bash
$ python portfolio/portfolio.py index
```
This command will create `portfolio/index/index.html` in your local environment.


### 6. (Optional) Deploy the Index page to Staging for review

To deploy your local Index page to Staging, run:

```bash
$ python portfolio/portfolio.py deploy-index
```

Or if you want to build and deploy in one command, run:

```bash
$ python portfolio/portfolio.py index --deploy
```
This command will deploy to [https://analysis-staging.dds.dot.ca.gov/](https://analysis-staging.dds.dot.ca.gov/).


### 7. Deploy your portfolio site to Production

Once you reviewed your site on Staging and it is ready for Production, run:

```bash
$ python portfolio/portfolio.py build MY_REPORT_SITE --no-execute-papermill --deploy --target production
```

This command will deploy to `https://analysis.dds.dot.ca.gov/MY_REPORT_SITE`.


### 8. (Optional) Deploy the Index page to Production

> [!NOTE]
> When you build the Index page for Production, the page will not display `Test Projects`.

Once you reviewed your Index page on Staging and it is ready for Production, run:

```bash
$ python portfolio/portfolio.py index --deploy --target production
```

This command will deploy to [https://analysis.dds.dot.ca.gov/](https://analysis.dds.dot.ca.gov/).


## Accessibility Testing

You will see accessibility violations at the end of building your site. If you have already built your site and wish to run only accessibilty checks, you can run `python portfolio/portfolio.py check-accessibility MY_REPORT_SITE`

### Accesibility Warning Severity

❔- Minor
⚠️- Moderate
🛑- Serious
🛑‼️- Critical
