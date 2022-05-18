"""
Generates
"""
import enum
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

import humanize
import papermill as pm
import typer
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from papermill import PapermillExecutionError
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
    caption: Optional[str]
    notebook: Optional[Path] = None
    params: Dict = {}
    sections: List[Dict] = []
    part: "Part" = None

    @property
    def resolved_notebook(self):
        return self.notebook or self.part.notebook or self.part.site.notebook

    @property
    def resolved_params(self):
        return {**self.part.params, **self.params}

    @property
    def slug(self):
        return slugify_params(self.resolved_params)

    @property
    def path(self):
        return self.part.site.output_dir / Path(self.slug)

    def generate(self, execute_papermill=True, continue_on_error=False, **papermill_kwargs) -> List[PapermillExecutionError]:
        errors = []
        self.path.mkdir(parents=True, exist_ok=True)

        if self.sections:
            fname = self.part.site.output_dir / f"{self.slug}.md"
            with open(fname, "w") as f:
                typer.secho(f"writing readme to {fname}", fg=typer.colors.GREEN)
                f.write(f"# {self.caption}")

            for i, section in enumerate(self.sections):
                params = {**self.resolved_params, **section}
                notebook = section.get('notebook') or self.resolved_notebook

                if not notebook:
                    raise ValueError("no notebook found at any level")

                if isinstance(notebook, str):
                    notebook = Path(notebook)

                parameterized_path = self.path / Path(parameterize_filename(i, notebook, params))

                typer.secho(f"parameterizing {notebook} => {parameterized_path}", fg=typer.colors.GREEN)

                if execute_papermill:
                    try:
                        pm.execute_notebook(
                            input_path=notebook,
                            output_path=parameterized_path,
                            parameters=params,
                            cwd=notebook.parent,
                            engine_name="markdown",
                            report_mode=True,
                            original_parameters=params,
                            **papermill_kwargs,
                        )
                    except PapermillExecutionError as e:
                        if continue_on_error:
                            typer.secho(f"error encountered during papermill execution", fg=typer.colors.RED)
                            errors.append(e)
                        else:
                            raise
                else:
                    typer.secho(f"execute_papermill={execute_papermill} so we are skipping actual execution", fg=typer.colors.YELLOW)

        else:
            notebook = self.resolved_notebook

            if not notebook:
                raise ValueError("no notebook found at any level")

            if isinstance(notebook, str):
                notebook = Path(notebook)

            parameterized_path = self.path / Path(parameterize_filename(0, notebook, self.resolved_params))

            typer.secho(f"parameterizing {notebook} => {parameterized_path}", fg=typer.colors.GREEN)

            if execute_papermill:
                try:
                    pm.execute_notebook(
                        input_path=notebook,
                        output_path=parameterized_path,
                        parameters=self.resolved_params,
                        cwd=notebook.parent,
                        engine_name="markdown",
                        report_mode=True,
                        original_parameters=self.resolved_params,
                        **papermill_kwargs,
                    )
                except PapermillExecutionError as e:
                    if continue_on_error:
                        typer.secho(f"error encountered during papermill execution", fg=typer.colors.RED)
                        errors.append(e)
                    else:
                        raise
            else:
                typer.secho(f"execute_papermill={execute_papermill} so we are skipping actual execution",
                            fg=typer.colors.YELLOW)

        return errors

    @property
    def toc(self):
        if self.sections:
            return {
                "file": f"{self.slug}.md",
                "sections": [{
                    "glob": f"{self.slug}/*",
                }],
            }

        return {
            "file": f"{self.slug}/{parameterize_filename(0, self.resolved_notebook, self.resolved_params)}",
        }


class Part(BaseModel):
    caption: Optional[str] = None
    notebook: Optional[Path] = None
    params: Dict = {}
    chapters: List[Chapter] = []
    site: "Site" = None

    def __init__(self, **data):
        super().__init__(**data)

        for chapter in self.chapters:
            chapter.part = self

    @property
    def slug(self) -> str:
        return slugify_params(self.params)

    @property
    def to_toc(self):
        return {
                "caption": self.caption,
                "chapters": [chapter.toc for chapter in self.chapters]
            }


class Site(BaseModel):
    output_dir: Path
    name: Optional[str]
    title: str
    directory: Path
    readme: Optional[Path] = "README.md"
    notebook: Optional[Path] = None
    parts: List[Part]
    prepare_only: bool = False

    def __init__(self, **data):
        super().__init__(**data)

        for part in self.parts:
            part.site = self

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
            "parts": [part.to_toc for part in self.parts if part.chapters]
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
            name = site.replace(".yml", "")
            site_output_dir = PORTFOLIO_DIR / Path(name)
            sites.append(Site(output_dir=site_output_dir, name=name, **yaml.safe_load(f)))

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
    continue_on_error: bool = False,
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
    site_name = site.value
    site_output_dir = PORTFOLIO_DIR / Path(site_name)
    site_output_dir.mkdir(parents=True, exist_ok=True)

    with open(SITES_DIR / Path(f"{site_name}.yml")) as f:
        site = Site(output_dir=site_output_dir, **yaml.safe_load(f))

    typer.echo(f"copying readme from {site.directory} to {site_output_dir}")
    shutil.copy(site.readme, site_output_dir / site.readme.name)


    fname = site_output_dir / Path("_config.yml")
    with open(fname, "w") as f:
        typer.secho(f"writing config to {fname}", fg=typer.colors.GREEN)
        f.write(env.get_template("_config.yml").render(site=site))
    fname = site_output_dir / Path("_toc.yml")
    with open(fname, "w") as f:
        typer.secho(f"writing toc to {fname}", fg=typer.colors.GREEN)
        f.write(site.toc_yaml)

    errors = []

    for part in site.parts:
        for chapter in part.chapters:
            errors.extend(chapter.generate(
                execute_papermill=execute_papermill,
                continue_on_error=continue_on_error,
                prepare_only=prepare_only,
                no_stderr=no_stderr,
            ))


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
        if continue_on_error and errors:
            ans = input(f"{len(errors)} encountered during papermill; enter that number to continue: ")
            assert int(ans) == len(errors)

        args = [
            "netlify",
            "deploy",
            "--site=cal-itp-data-analyses",
            f"--dir=portfolio/{site_name}/_build/html/",
            f"--alias={site_name}",
        ]
        typer.secho(f"Running deploy:\n{' '.join(args)}", fg=typer.colors.GREEN)
        subprocess.run(args).check_returncode()

    if errors:
        typer.secho(f"{len(errors)} errors encountered during papermill execution", fg=typer.colors.RED)
        sys.exit(1)


if __name__ == "__main__":
    app()
