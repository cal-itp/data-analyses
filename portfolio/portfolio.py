"""
Generates
"""
import enum
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

import humanize
import papermill as pm
import typer
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from papermill.engines import NBClientEngine, papermill_engines
from pydantic import BaseModel
from pydantic.class_validators import validator
from slugify import slugify

assert os.getcwd().endswith("data-analyses"), "this script must be run from the root of the data-analyses repo!"

PORTFOLIO_DIR = Path("./portfolio/")
SITES_DIR = PORTFOLIO_DIR / Path("sites")

SiteChoices = enum.Enum('SiteChoices', {
    f.replace(".yml", ""): f.replace(".yml", "")
    for f in os.listdir(SITES_DIR)
})

DEPLOY_OPTION = typer.Option(
    False,
    help="Actually deploy this component to netlify.",
)

app = typer.Typer(help="CLI to tie together papermill, jupyter book, and netlify")

env = Environment(loader=FileSystemLoader("./portfolio/templates/"), autoescape=select_autoescape())


def district_name(district, **_):
    return humanize.apnumber(district).title()


RESOLVERS = [
    district_name,
]


def slugify_params(params: Dict) -> str:
    return "__".join(f"{k}_{slugify(str(v))}" for k, v in params.items())


def parameterize_filename(i: int, old_path: Path, params: Dict) -> Path:
    assert old_path.suffix == ".ipynb"
    return Path(str(i) + "__" + old_path.stem + "__" + slugify_params(params) + old_path.suffix)


class Chapter(BaseModel):
    caption: str
    notebook: Optional[Path] = None
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
    title: str
    directory: Path
    readme: Optional[Path] = "README.md"
    notebook: Optional[Path] = None
    parts: List[Part]
    prepare_only: bool = False

    @validator('readme', pre=True, always=True)
    def default_readme(cls, v, *, values, **kwargs):
        return v or (values['directory'] / Path("README.md"))

    @property
    def slug(self) -> str:
        return slugify(self.title)

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
        no_stderr = kwargs["no_stderr"]

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
                
                # Consider importing this name from calitp.magics
                if '%%capture_parameters' in cell.source:
                    params = {**params, **json.loads(cell.outputs[0]['text'])}

                if "%%capture" in cell.source:
                    cell.outputs = []
                    
                if no_stderr:
                    cell.outputs = [output for output in cell.outputs if 'name' not in output.keys() or output['name'] != 'stderr']


papermill_engines.register("markdown", EngineWithParameterizedMarkdown)
papermill_engines.register_entry_points()


@app.command()
def index(
    deploy: bool = DEPLOY_OPTION,
    alias: str = None,
    prod: bool = False,
) -> None:
    sites = []
    for site in os.listdir("./portfolio/sites/"):
        with open(f"./portfolio/sites/{site}") as f:
            sites.append(Site(name=site.replace(".yml", ""), **yaml.safe_load(f)))

    for template in ["index.html", "_redirects"]:
        fname = f"./portfolio/index/{template}"
        with open(fname, "w") as f:
            typer.echo(f"writing out to {fname}")
            f.write(env.get_template(template).render(sites=sites))

    args = [
        "netlify",
        "deploy",
        "--site=cal-itp-data-analyses",
        "--dir=portfolio/index",
    ]

    if alias:
        args.append(f"--alias={alias}")

    if prod:
        args.append("--prod")

    if deploy:
        typer.secho(f"deploying with args {args}", fg=typer.colors.GREEN)
        subprocess.run(args).check_returncode()


@app.command()
def clean(
    site: str,
) -> None:
    """
    Cleans the portfolio folder for a given site.
    """
    shutil.rmtree(PORTFOLIO_DIR / Path(site))


@app.command()
def build(
    site: SiteChoices,
    deploy: bool = DEPLOY_OPTION,
    execute_papermill: bool = typer.Option(
        True,
        help="If false, will skip calls to papermill.",
    ),
    no_stderr: bool = typer.Option(
        False,
        help="If true, will clear stderr stream for cell outputs",
    ),
    prepare_only: bool = typer.Option(
        False,
        help="Pass-through flag to papermill; if true, papermill will not actually execute cells.",
    ),
) -> None:
    """
    Builds a static site from parameterized notebooks as defined in a site YAML file.
    """
    site_output_dir = PORTFOLIO_DIR / Path(site.value)
    site_output_dir.mkdir(parents=True, exist_ok=True)

    with open(SITES_DIR / Path(f"{site.value}.yml")) as f:
        site = Site(**yaml.safe_load(f))

    typer.echo(f"copying readme from {site.directory} to {site_output_dir}")
    shutil.copy(site.readme, site_output_dir / site.readme.name)

    for part in site.parts:
        for chapter in part.chapters:
            chapter_slug = slugify_params({**part.params, **chapter.params})
            chapter_path = site_output_dir / Path(chapter_slug)
            chapter_path.mkdir(parents=True, exist_ok=True)
            for i, section in enumerate(chapter.sections or [{}]):
                params = {**part.params, **chapter.params, **section}
                notebook = section.get('notebook') or chapter.notebook or part.notebook or site.notebook

                if not notebook:
                    raise ValueError("no notebook found at any level")

                if isinstance(notebook, str):
                    notebook = Path(notebook)

                parameterized_path = chapter_path / Path(parameterize_filename(i, notebook, params))

                typer.secho(f"parameterizing {notebook} => {parameterized_path}", fg=typer.colors.GREEN)

                if execute_papermill:
                    pm.execute_notebook(
                        input_path=notebook,
                        output_path=parameterized_path,
                        parameters=params,
                        cwd=notebook.parent,
                        engine_name="markdown",
                        report_mode=True,
                        prepare_only=prepare_only or site.prepare_only,
                        original_parameters=params,
                        no_stderr = no_stderr,
                    )
                else:
                    typer.secho(f"execute_papermill={execute_papermill} so we are skipping actual execution", fg=typer.colors.YELLOW)

            fname = site_output_dir / f"{chapter_slug}.md"
            with open(fname, "w") as f:
                typer.secho(f"writing readme to {fname}", fg=typer.colors.GREEN)
                f.write(f"# {chapter.caption}")

    fname = site_output_dir / Path("_config.yml")
    with open(fname, "w") as f:
        typer.secho(f"writing config to {fname}", fg=typer.colors.GREEN)
        f.write(env.get_template("_config.yml").render(site=site))
    fname = site_output_dir / Path("_toc.yml")
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
        cwd=site_output_dir,
    ).check_returncode()

    if deploy:
        args = [
            "netlify",
            "deploy",
            "--site=cal-itp-data-analyses",
            f"--dir=portfolio/{site.value}/_build/html/",
            f"--alias={site.value}",
        ]
        typer.secho(f"Running deploy:\n{' '.join(args)}", fg=typer.colors.GREEN)
        subprocess.run(args).check_returncode()


if __name__ == "__main__":
    app()
