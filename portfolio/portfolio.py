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
from jinja2 import Environment, FileSystemLoader, select_autoescape
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

env = Environment(loader=FileSystemLoader("./portfolio/templates/"), autoescape=select_autoescape())


def district_name(district, **_):
    return humanize.apnumber(district).title()


RESOLVERS = [
    district_name,
]


def slugify_params(params: Dict) -> str:
    return "__".join(f"{k}_{slugify(str(v))}" for k, v in params.items())


def parameterize_filename(old_path: Path, params: Dict) -> Path:
    assert old_path.suffix == ".ipynb"
    return Path(old_path.stem + "__" + slugify_params(params) + old_path.suffix)


class Chapter(BaseModel):
    caption: str
    params: Dict = {}
    sections: List[Dict] = []


class Part(BaseModel):
    caption: Optional[str] = None
    notebook: Optional[Path] = None
    params: Dict = {}
    chapters: List[Chapter] = []

    @property
    def slug(self) -> str:
        return slugify_params(self.params)


class Site(BaseModel):
    name: str
    title: str
    directory: Path
    readme: Optional[Path] = None
    notebook: Optional[Path] = None
    parts: List[Part]
    prepare_only: bool = False

    @property
    def slug(self) -> str:
        return slugify(self.title)

    @validator("readme", always=True)
    def convert_status(cls, readme, values):
        return readme or values["notebook"]

    @property
    def toc_yaml(self) -> str:
        return yaml.dump({
            "format": "jb-book",
            "root": "README",
            "parts": [{
                "caption": part.caption,
                "chapters": [{
                    "file": f"{slugify_params({**part.params, **chapter.params})}.md",
                    "sections": [{
                        "glob": f"{slugify_params({**part.params, **chapter.params})}/*",
                    }],
                } for chapter in part.chapters]
            } for part in self.parts if part.chapters]
        }, indent=4)


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
            try:
                params[func.__name__] = func(**kwargs["original_parameters"])
            except TypeError:
                pass

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

            if "%%capture" in cell.source:
                cell.outputs = []


papermill_engines.register("markdown", EngineWithParameterizedMarkdown)
papermill_engines.register_entry_points()

@app.command()
def clean() -> None:
    shutil.rmtree("./target/")


@app.command()
def index(
    config=CONFIG_OPTION,
    deploy: bool = DEPLOY_OPTION,
    alias: str = None,
) -> None:
    with open(config) as f:
        portfolio_config = PortfolioConfig(**yaml.safe_load(f))

    analyses = portfolio_config.sites
    for template in ["index.html", "_redirects"]:
        fname = f"./portfolio/index/{template}"
        with open(fname, "w") as f:
            typer.echo(f"writing out to {fname}")
            f.write(env.get_template(template).render(analyses=analyses))

    args = [
        "netlify",
        "deploy",
        "--site=cal-itp-data-analyses",
        "--dir=portfolio/index",
    ]

    if alias:
        args.append(f"--alias={alias}")

    if deploy:
        subprocess.run(args).check_returncode()


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

    site = next(site for site in portfolio_config.sites if site.name == report)
    site_dir = portfolio_dir / Path(report)
    site_dir.mkdir(parents=True, exist_ok=True)
    analysis_root = site.directory

    typer.echo(f"copying readme from {analysis_root} to {site_dir}")
    shutil.copy(analysis_root / Path("README.md"), site_dir / Path("README.md"))

    (target_dir / site.notebook.parent).mkdir(parents=True, exist_ok=True)

    for part in site.parts:
        # TODO: handle this for non-parameterized files/introductions
        if not part.chapters:
            continue
        for chapter in part.chapters or [Chapter()]:
            chapter_slug = slugify_params({**part.params, **chapter.params})
            chapter_path = site_dir / Path(chapter_slug)
            for section in chapter.sections or [{}]:
                params = {**part.params, **chapter.params, **section}
                notebook = part.notebook or site.notebook

                # TODO: this should be cleaned up a bit
                if params:
                    parameterized_filepath = Path(report) / parameterize_filename(notebook, section)
                else:
                    parameterized_filepath = notebook

                portfolio_path = chapter_path / parameterized_filepath.name
                portfolio_path.parent.mkdir(parents=True, exist_ok=True)
                typer.secho(f"parameterizing {notebook} => {portfolio_path}", fg=typer.colors.GREEN)

                if execute_papermill:
                    pm.execute_notebook(
                        input_path=notebook,
                        output_path=portfolio_path,
                        parameters=params,
                        cwd=notebook.parent,
                        engine_name="markdown",
                        report_mode=True,
                        prepare_only=prepare_only or site.prepare_only,
                        original_parameters=params,
                    )
                else:
                    typer.secho(f"execute_papermill={execute_papermill} so we are skipping actual execution", fg=typer.colors.YELLOW)

            fname = f"./portfolio/{report}/{chapter_slug}.md"
            with open(fname, "w") as f:
                typer.secho(f"writing readme to {fname}", fg=typer.colors.GREEN)
                f.write(f"# {chapter.caption}")

    fname = f"./portfolio/{report}/_config.yml"
    with open(fname, "w") as f:
        typer.secho(f"writing config to {fname}", fg=typer.colors.GREEN)
        f.write(env.get_template("_config.yml").render(report=report, analysis=site))
    fname = f"./portfolio/{report}/_toc.yml"
    with open(fname, "w") as f:
        typer.secho(f"writing toc to {fname}", fg=typer.colors.GREEN)
        f.write(site.toc_yaml)

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
                f"--dir=portfolio/{report}/_build/html/",
                f"--alias={report}",
            ]
        ).check_returncode()


if __name__ == "__main__":
    app()
