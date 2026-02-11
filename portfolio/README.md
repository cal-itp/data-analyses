# Introduction
This website contains data analysis and reports developed by Cal-ITP data analysts and scientists.

## Source code

All source code for these analyses and reports may be found [on GitHub](https://github.com/cal-itp/data-analyses).

## Workflow
1. Create a parameterized notebook.
   This is one Jupyter notebook, set up in a way captions, charts, maps, and headings are constructed by using parameters.
   * [pointers for styling notebooks](https://docs.calitp.org/data-infra/publishing/sections/4_notebooks_styling.html)

2. Add a `site_name.yml` to `portfolio/sites/` - this controls the parameterization related to the notebooks.
   JupyterBooks have a table of contents and organize chapters and sections ([read more on Jupyterbook structure](https://jupyterbook.org/en/stable/structure/configure.html)).
   We allow both chapters and sections to be parameterized, and examples are given for the most common types of supported parameterized reports.
   * the path to the parameterized notebook must be defined (`project_folder/report.ipynb`)
   * the path to the README must be defiend (`project_folder/README.md`)
   * how to organize the chapters and sections. The 2 most common are (1): by Caltrans district (chapter) and each transit operator within the district gets a page (section), and (2) by Caltrans district (chapter) and each district is its own page (no section).
   * [various parameterization examples](https://docs.calitp.org/data-infra/publishing/sections/5_analytics_portfolio_site.html)

3. Building and deploying a new parameterized JupyterBook
   * Remove the local folder containing a previously generate portfolio site
     ```
     python portfolio/portfolio.py clean MY_NEW_REPORT
     ```
   * Parameterize the notebook specified in `portfolio/sites/MY_NEW_REPORT.yml`
     ```
     python portfolio/portfolio.py build MY_NEW_REPORT`
     ```
      * local files are created in `portfolio/MY_NEW_REPORT/` (all files below are within this newly created `portfolio/MY_NEW_REPORT/` sub-directory)
      * JupyterBook necessary accessories: `myst.yml` and `README.md`
      * There will be additional files or directories holding the parameterized notebooks and site build files.
   * During build you will see any accessibility violations that are detected in the generated site. If you have already built your site and wish to run only the accessibilty checks: `python portfolio/portfolio.py check-accessibility MY_NEW_REPORT`

4. Deploy the portfolio index to the staging environment (https://analysis-staging.dds.dot.ca.gov/) `python portfolio/portfolio.py deploy-index --target staging`. You can deploy to production with `--target production`.
5. Deploy a specific portfolio site to the staging environment `python portfolio/portfolio.py deploy-site MY_NEW_REPORT --target staging` will deploy to `https://analysis-staging.dds.dot.ca.gov/MY_NEW_REPORT`. You can deploy to production with `--target production`.
6. Combined production build and deploy steps for a specific portfolio site `python portfolio/portfolio.py build MY_NEW_REPORT --deploy` will build and deploy to production. When we deploy, the HTML files in `portfolio/MY_NEW_REPORT/_build/html` are available at `https://analysis.dds.dot.ca.gov/MY_NEW_REPORT`.

All these steps are also documented in the [Makefile](https://github.com/cal-itp/data-analyses/blob/main/Makefile). Some of the steps that are commented out should be uncommented depending on your use case. Portfolio sites are now gitignored, no longer checked into git.
   ```
    build_portfolio_site:
    cd portfolio/ && pip install -r requirements.txt && cd ../
    python portfolio/portfolio.py clean $(site)
    python portfolio/portfolio.py build $(site) --deploy
    add portfolio/sites/$(site).yml
    # make production_portfolio #(deploy onto the main page)
   ```
