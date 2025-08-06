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
   * There are two commands: `clean` and `build`
   * `python portfolio/portfolio.py clean MY_NEW_REPORT` (removes the local folder `portfolio/MY_NEW_REPORT/`)
   * `python portfolio/portfolio.py build MY_NEW_REPORT` (this parameterizes the notebook specified in `portfolio/sites/MY_NEW_REPORT.yml`). JupyterBook docs on [this](https://jupyterbook.org/en/stable/start/build.html).
      * local files are created in `portfolio/MY_NEW_REPORT/` (all files below are within this newly created `portfolio/MY_NEW_REPORT/` sub-directory)
      * JupyterBook necessary accessories: `toc.yml`, `README.md`, and `config.yml` (this needs to get checked into GitHub).
      * There will be additional files or directories holding the parameterized notebooks. The names of the notebooks will be constructed programmatically, but for
     illustrative purposes, we'll call the 2 notebooks `first_operator` and `second_operator`. (This folder needs to get checked into GitHub).
        Example: `portfolio/MY_NEW_REPORT/district_01_eureka/first_operator.ipynb`, `portfolio/MY_NEW_REPORT/district_01_eureka/second_operator.ipynb`.
        
        `first_operator.ipynb` uses `project_folder/report.ipynb` to display the first operator's information in all the cells.
        `second_operator.ipynb` uses `project_folder/report.ipynb` to display the second operator's information in all the cells.
     * A second folder is created to [build the JupyterBook](https://jupyterbook.org/en/stable/start/build.html#aside-source-vs-build-files), and this is `portfolio/MY_NEW_REPORT/_build/`. Within the that directory, there are 2 more sub-directories (`_build/jupyter_execute/` and `_build/html`). This second folder is stored locally and **not** checked into GitHub. JupyterBook makes a distinction between the **source** and **build** files.
     * `_build/juptyer_execute/` files are a copy of the parameterized notebooks. The notebooks `portfolio/MY_NEW_REPORT/_build/jupyter_execute/district_01_eureka/first_operator.ipynb / second_operator.ipynb`, these are basically equivalent to the parameterized notebooks created in `portfolio/MY_NEW_REPORT/`
     * `_build/html` are the rendered HTML pages corresponding to the parameterized notebooks.
     * Instead of notebooks, now they are replaced with HTML files: `_build/html/district_01_eureka/first_operator / second_operator.html`
     * There are also additional folders within `_build/html`: `_sources`, `_sphyinx_design_static`, and `_static` and other files like `genindex.html`, `index.html`, `search.html`, `searchindex.js`, `README.html`, and `objects.inv`
  * During build you will see any accessibility violations that are detected in the generated site. If you have already built your site and wish to run only the accessibilty checks: `python portfolio/portfolio.py check-accessibility MY_NEW_REPORT`
  * `python portfolio/portfolio.py build MY_NEW_REPORT --deploy` (when we deploy, the HTML files in `portfolio/MY_NEW_REPORT/_build/html` are available at `https://analysis.dds.dot.ca.gov/MY_NEW_REPORT`

4. All these steps are documented in the [Makefile](https://github.com/cal-itp/data-analyses/blob/main/Makefile). Some of the steps that are commented out should be uncommented depending on your use case.
   * If you've already checked in your site to GitHub, the next month you deploy your portfolio, you should use `git rm portfolio/$(site)/ -rf` and `clean $(site)` where `$(site)` is the name of your site based on `portfolio/sites/site_name.yml`. The `git rm` cleans up whatever is checked in and the `clean` removes the local folders that are not checked in. **Both are needed.** Not doing both can result in your `toc.yml` and HTML being out of sync.
   * If you're testing changes to your site, finish that up before you run `make production_portfolio`. 
   * Check the files in with `make git_check_sections`, which adds all the parameterized notebooks in `portfolio/MY_NEW_REPORT/*.ipynb`, but doesn't check in the notebooks or HTML files in `_build/`.
     
   ```
    build_portfolio_site:
    cd portfolio/ && pip install -r requirements.txt && cd ../
    # need git rm because otherwise, just local removal, but git change is untracked
    rm portfolio/$(site)/ -rf
    python portfolio/portfolio.py clean $(site)
    python portfolio/portfolio.py build $(site) --deploy 
    add portfolio/sites/$(site).yml     
    # make production_portfolio #(deploy onto the main page)

    git_check_sections:
    git add portfolio/$(site)/*.ipynb # this one is most common, where operators nested under district
   ```

6. We use Git Large File Storage `git lfs` to store these parameterized notebooks. However, we are also moving to storing these parameterized notebooks in Google Cloud Storage in the long run.
   * Swap out the `git add` and `git rm` steps. If using `gcsfs`, we can use the `fs.put` and `fs.rm` to cache the parameterized notebooks and built HTML files for JupyterBook.
