"""
Generates
"""
import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

import humanize
import nbformat
import papermill as pm
import typer
import yaml
from nbconvert import HTMLExporter
from papermill.engines import NBClientEngine, papermill_engines
from pydantic import BaseModel
from pydantic.class_validators import validator
from slugify import slugify

CONFIG_OPTION = typer.Option(
    f"{os.path.dirname(os.path.realpath(__file__))}/analyses.yml",
)

DEPLOY_OPTION = typer.Option(
    False,
    help="Actually deploy this component to netlify.",
)

app = typer.Typer(help="CLI to tie together papermill and jupyter book")

from jinja2 import Environment, FileSystemLoader, select_autoescape

env = Environment(loader=FileSystemLoader("./portfolio/templates/"), autoescape=select_autoescape())


def district_name(district, **_):
    return humanize.apnumber(district).title()


RESOLVERS = [
    district_name,
]


# class TOC(BaseModel):
#     def as_yaml(self) -> str:
#         return yaml.dump({
#             "format:": "jb-book",
#             "root": "README",
#             "parts": [{
#
#             } for part in parts]
#         })


def parameterize_filename(old_path: Path, params: Dict) -> Path:
    assert old_path.suffix == ".ipynb"
    return Path(old_path.stem + "__" + "__".join(f"{k}_{v}" for k, v in params.items()) + old_path.suffix)


class Site(BaseModel):
    name: str
    title: str
    notebook: Path
    params: Dict[str, List] = {}
    readme: Optional[Path] = None
    prepare_only: bool = False

    @property
    def slug(self) -> str:
        return slugify(self.title)

    @validator("readme", always=True)
    def convert_status(cls, readme, values):
        return readme or values["notebook"]



class PortfolioConfig(BaseModel):
    sites: List[Site]


class EngineWithParameterizedMarkdown(NBClientEngine):
    @classmethod
    def execute_managed_notebook(cls, nb_man, kernel_name, **kwargs):
        # call the papermill execution engine:
        super().execute_managed_notebook(nb_man, kernel_name, **kwargs)

        assert "original_parameters" in kwargs

        params = kwargs["original_parameters"]

        for func in RESOLVERS:
            params[func.__name__] = func(**kwargs["original_parameters"])

        for cell in nb_man.nb.cells:

            # display() calls for markdown break jupyterbook/sphinx
            # https://github.com/executablebooks/jupyter-book/issues/1610
            # so we have to manually parameterize headers in markdown cells; for example, "District {district}" in a
            # markdown cell vs "display(Markdown(f"## District: {district}))" in a code cell
            if cell.cell_type == "markdown":
                cell.source = cell.source.format(**params)

            # hide input (i.e. code) for all cells
            if cell.cell_type == "code":
                cell.metadata.tags.append("remove_input")


papermill_engines.register("markdown", EngineWithParameterizedMarkdown)
papermill_engines.register_entry_points()


def convert_to_html(path: Path) -> Path:
    html_output_path = path.with_suffix(".html")
    typer.echo(f"converting to html, {path} => {html_output_path}")

    html_exporter = HTMLExporter(template_name="lab")

    with open(path) as f:
        output_notebook = nbformat.reads(f.read(), as_version=4)

    body, _ = html_exporter.from_notebook_node(output_notebook)

    html_output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(html_output_path, "w") as f:
        f.write(body)
    return html_output_path


@app.command()
def clean() -> None:
    shutil.rmtree("./target/")


@app.command()
def index(
    config=CONFIG_OPTION,
    deploy: bool = DEPLOY_OPTION,
) -> None:
    with open(config) as f:
        portfolio_config = PortfolioConfig(analyses=yaml.safe_load(f))

    analyses = portfolio_config.sites
    for template in ["index.html", "_redirects"]:
        fname = f"./portfolio/index/{template}"
        with open(fname, "w") as f:
            typer.echo(f"writing out to {fname}")
            f.write(env.get_template(template).render(analyses=analyses))

    if deploy:
        subprocess.run(
            [
                "netlify",
                "deploy",
                "--site=cal-itp-data-analyses",
                "--dir=portfolio/index",
            ]
        ).check_returncode()


@app.command()
def build(
    report: str,
    config=CONFIG_OPTION,
    deploy: bool = DEPLOY_OPTION,
    target_dir: Path = "./target/",
    portfolio_dir: Path = "./portfolio/",
    execute_papermill: bool = typer.Option(
        True,
        help="If false, will skip calls to papermill.",
    ),
    prepare_only: bool = typer.Option(
        False,
        help="Pass-through flag to papermill; if true, papermill will not actually execute cells.",
    ),
) -> None:
    """
    Builds a static site from parameterized notebooks as defined in the config file (default ./reports_config.yml).

    Use the --report flag to only build specific reports.

    For example:
    $ python generate_reports.py build --report=dla
    """
    with open(config) as f:
        portfolio_config = PortfolioConfig(**yaml.safe_load(f))

    target_dir.mkdir(parents=True, exist_ok=True)

    analysis = next(site for site in portfolio_config.sites if site.name == report)
    site_dir = portfolio_dir / Path(report)

    analysis_root = analysis.notebook.parts[0]
    typer.echo(f"copying readme from {analysis_root} to {site_dir}")
    shutil.copy(analysis_root / Path("README.md"), site_dir / Path("README.md"))

    params = list(zip(*analysis.params.values()))
    (target_dir / analysis.notebook.parent).mkdir(parents=True, exist_ok=True)

    for param_set in params or [{}]:
        params_dict = {k: v for k, v in zip(analysis.params.keys(), param_set)}

        if params_dict:
            parameterized_filepath = Path(report) / parameterize_filename(analysis.notebook, params_dict)
        else:
            parameterized_filepath = analysis.notebook
        target_path = target_dir / parameterized_filepath
        typer.echo(f"executing papermill; writing {analysis.notebook} => {target_path}")

        if execute_papermill:
            pm.execute_notebook(
                input_path=analysis.notebook,
                output_path=target_path,
                parameters=params_dict,
                cwd=analysis.notebook.parent,
                engine_name="markdown",
                report_mode=True,
                prepare_only=prepare_only or analysis.prepare_only,
                original_parameters=params_dict,
            )
        else:
            typer.echo(f"execute_papermill={execute_papermill} so we are skipping actual execution")

        # html_output_path = convert_to_html(output_path)

        portfolio_path = portfolio_dir / parameterized_filepath.parent / Path("notebooks") / parameterized_filepath.name  # .with_suffix(".html")
        print(portfolio_dir, parameterized_filepath, portfolio_path)
        portfolio_path.parent.mkdir(parents=True, exist_ok=True)
        typer.echo(f"placing in portfolio; {target_path} => {portfolio_path}")
        shutil.copy(target_path, portfolio_path)

    for template in ["_config.yml", "_toc.yml"]:
        fname = f"./portfolio/{report}/{template}"
        with open(fname, "w") as f:
            typer.echo(f"writing out to {fname}")
            f.write(env.get_template(template).render(report=report, analysis=analysis))

    subprocess.run(
        [
            "jb",
            "build",
            "-W",
            "-n",
            "--keep-going",
            ".",
        ],
        cwd=f"./portfolio/{report}/",
    ).check_returncode()

    if deploy:
        subprocess.run(
            [
                "netlify",
                "deploy",
                "--site=cal-itp-data-analyses",
                "--dir=portfolio/index",
                f"--alias={report}",
            ]
        ).check_returncode()


if __name__ == "__main__":
    app()
