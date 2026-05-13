# data-analyses

Place for sharing quick reports, and works in progress

This repository is for quick sharing of works in progress and simple analyses.
For collaborative short-term tasks, create a new folder and work off a separate branch.
For longer-term projects, consider making a new repository!


## Using this Repo

* Use [this link](https://docs.calitp.org/data-infra/analytics_tools/saving_code.html#onboarding-setup) to get started in JupyterHub, set up SSH, and start commiting to the repo!


### JupyterHub Developers

If you are developing in JupyterHub, follow the [JupyterHub setup docs](https://docs.calitp.org/data-infra/analytics_tools/jupyterhub.html).


### Contributing

Follow these steps to start contributing:

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this `data-analyses` repo.

2. From the `data-analyses/` path (repo root path), run `make install_env` (runs `uv sync --all-groups` + pre-commit setup)

3. In JupyterHub, select the **"Pyproject Local"** kernel when opening a notebook

> [!NOTE]
> If you run into the error `No such file or directory`, you may need to [install uv](https://docs.astral.sh/uv/getting-started/installation/) running `pip install uv`.

### uv

This repository uses [uv](https://docs.astral.sh/uv/concepts/projects/dependencies/) for package management.

Every time you rebase your branch, add, upgrade, or remove packages, you need to run `uv sync` to install/update any change.

* Dependencies from "\_shared_utils", "rt_segment_speeds", and "rt_delay" will be installed/updated too.

You can see all the current dependencies in [pyproject.toml](https://github.com/cal-itp/data-analyses/blob/main/pyproject.toml).

Basic commands:

* `uv sync` install missing packages, update existing ones, and remove unnecessary ones to ensure the environment matches the main lockfile.
* `uv sync --all-groups` install missing packages, update existing ones, and remove unnecessary ones to ensure the environment matches all group dependencies.
* `uv sync --group <group name>` install missing packages, update existing ones, and remove unnecessary ones to ensure the environment matches the main lockfile and the specified group.
* `uv add <package name>` include and install a new package to the main project.
* `uv add <package name> --dev` include and install new packages/dependencies to the `dev` group.
* `uv add <package name> --portfolio` include and install new packages/dependencies to the `portfolio` group.
* `uv add <package name> --test` include and install new packages/dependencies used only for testing under the `test` group.
* `uv remove <package name>` remove and uninstall packages/dependencies from the project.

### nbdime

[`nbdime`](https://github.com/jupyter/nbdime) provides command-line tools for diffing and merging notebooks.

Basic commands:

* `nbdiff` compare notebooks in a terminal-friendly way.
* `nbshow` present a single notebook in a terminal-friendly way.

### Pre-commit

This repository uses pre-commit hooks to format code, including [Black](https://black.readthedocs.io/en/stable/index.html). This ensures baseline consistency in code formatting.

Pre-commit checks will run before you can make commits locally. If a pre-commit check fails, it will need to be addressed before you can make your commit.
Many formatting issues are fixed automatically within the pre-commit actions, so check the changes made by pre-commit on failure -- they may have automatically addressed the issues that caused the failure, in which case you can simply re-add the files, re-attempt the commit, and the checks will then succeed.

Installing pre-commit locally saves time dealing with formatting issues on pull requests. There is a [GitHub Action](./.github/workflows/lint.yml)
that runs pre-commit on all files, not just changed ones, as part of our continuous integration.


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
