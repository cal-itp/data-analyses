"""
Generates
"""

import enum
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import humanize
import papermill as pm
import typer
import yaml
from axe_selenium_python import Axe
from jinja2 import Environment, FileSystemLoader, select_autoescape
from papermill import PapermillExecutionError
from papermill.engines import NBClientEngine, papermill_engines
from pydantic import BaseModel, field_validator
from selenium import webdriver
from slugify import slugify
from typing_extensions import Annotated

assert os.getcwd().endswith("data-analyses"), "this script must be run from the root of the data-analyses repo!"

GOOGLE_ANALYTICS_TAG_ID = "G-JCX3Z8JZJC"
PORTFOLIO_DIR = Path("./portfolio/")
SITES_DIR = PORTFOLIO_DIR / Path("sites")
PORTFOLIO_PUBLISH_PRODUCTION = "gs://calitp-analysis"
PORTFOLIO_PUBLISH_STAGING = "gs://calitp-analysis-staging"

SiteChoices = enum.Enum("SiteChoices", {f.replace(".yml", ""): f.replace(".yml", "") for f in os.listdir(SITES_DIR)})

DEPLOY_OPTION = typer.Option(
    False,
    help="Actually deploy this component.",
)

app = typer.Typer(help="CLI to tie together papermill, jupyter book, and gcs")

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

    return Path(str(i).zfill(2) + "__" + old_path.stem + "__" + slugify_params(params) + old_path.suffix)


class TyperLoggerHandler(logging.Handler):
    """A custom logger handler that use Typer echo."""

    def emit(self, record: logging.LogRecord) -> None:
        typer.secho(self.format(record))


class TyperLoggerFormatter(logging.Formatter):
    """
    Log formatter that strips terminal colour escape codes from the log message.
    """

    PAPERMILL_FORMATTING_REGEX = re.compile(r"((\s{2,}|(-)+>\s+)[0-9]*)*\\x1b\[([0-9];?m?)*")

    def format(self, record):
        """Return message with terminal escapes removed."""
        return "%s - %s - %s - %s" % (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            record.levelname,
            record.name,
            re.sub(self.PAPERMILL_FORMATTING_REGEX, "", record.getMessage()),
        )


def configure_logging(verbose):
    logger = logging.getLogger()
    handler = TyperLoggerHandler()
    formatter = TyperLoggerFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    log_level = logging.DEBUG if verbose else logging.WARNING
    logger.setLevel(log_level)

    return logger


class Chapter(BaseModel):
    caption: Optional[Any] = None
    notebook: Optional[Path] = None
    params: Dict = {}
    sections: List[Dict] = []
    site: "Site" = None
    part: "Part" = None

    @property
    def resolved_notebook(self):
        return self.notebook or self.part.notebook or self.part.site.notebook

    @property
    def resolved_params(self):
        part_params = self.part.params if self.part else {}
        return {**part_params, **self.params}

    @property
    def slug(self):
        return slugify_params(self.resolved_params)

    @property
    def output_dir(self):
        return self.part.site.output_dir if self.part else self.site.output_dir

    @property
    def path(self):
        return self.output_dir / Path(self.slug)

    def generate(
        self, execute_papermill=True, continue_on_error=False, **papermill_kwargs
    ) -> List[PapermillExecutionError]:
        errors = []
        self.path.mkdir(parents=True, exist_ok=True)

        if self.sections:
            fname = self.output_dir / f"{self.slug}.md"
            with open(fname, "w") as f:
                typer.secho(f"writing readme to {fname}", fg=typer.colors.GREEN)
                f.write(f"# {self.caption}")

            for i, section in enumerate(self.sections):
                two_digit_i = str(i).format(width=2)
                params = {**self.resolved_params, **section}
                notebook = section.get("notebook") or self.resolved_notebook

                if not notebook:
                    raise ValueError("no notebook found at any level")

                if isinstance(notebook, str):
                    notebook = Path(notebook)

                parameterized_path = self.path / Path(parameterize_filename(two_digit_i, notebook, params))

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
                            typer.secho("error encountered during papermill execution", fg=typer.colors.RED)
                            errors.append(e)
                        else:
                            raise
                else:
                    typer.secho(
                        f"execute_papermill={execute_papermill} so we are skipping actual execution",
                        fg=typer.colors.YELLOW,
                    )

        else:
            notebook = self.resolved_notebook

            if not notebook:
                raise ValueError("no notebook found at any level")

            if isinstance(notebook, str):
                notebook = Path(notebook)

            parameterized_path = self.path / Path(parameterize_filename("00", notebook, self.resolved_params))

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
                        typer.secho("error encountered during papermill execution", fg=typer.colors.RED)
                        errors.append(e)
                    else:
                        raise
            else:
                typer.secho(
                    f"execute_papermill={execute_papermill} so we are skipping actual execution", fg=typer.colors.YELLOW
                )

        return errors

    @property
    def toc(self):
        if self.sections:
            return {
                "file": f"{self.slug}.md",
                "children": [
                    {
                        "glob": f"{self.slug}/*",
                    }
                ],
            }

        folder = f"{self.slug}/" if self.slug else ""
        return {
            "file": f"{folder}{parameterize_filename('00', self.resolved_notebook, self.resolved_params)}",
        }


class Part(BaseModel):
    caption: Optional[Any] = None
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
        return {"title": self.caption, "children": [chapter.toc for chapter in self.chapters]}


class YamlPartialDumper(yaml.Dumper):

    def increase_indent(self, flow=False, indentless=False):
        return super(YamlPartialDumper, self).increase_indent(flow, False)


class Site(BaseModel):
    output_dir: Path
    name: str
    title: str
    directory: Path
    readme: Optional[Path] = "README.md"
    notebook: Optional[Path] = None
    parts: List[Part] = []
    chapters: List[Chapter] = []
    prepare_only: bool = False

    def __init__(self, **data):
        super().__init__(**data)

        content = [*self.chapters, *self.parts]
        for child in content:
            child.site = self

    @field_validator("readme", mode="before", check_fields=False)
    @classmethod
    def default_readme(cls, v, info):
        if "./" in v:
            return Path(v)
        else:
            directory = info.data["directory"]
            return (directory / Path("README.md")) or (directory / Path(v))

    @property
    def slug(self) -> str:
        return slugify(self.title)

    @property
    def toc_yaml(self) -> str:
        chapters = [chapter.toc for chapter in self.chapters]
        parts = [part.to_toc for part in self.parts if part.chapters]
        children = [*chapters, *parts]
        return yaml.dump(
            {"toc": [{"file": "README.md"}, *children]},
            indent=4,
            Dumper=YamlPartialDumper,
        )


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
                cell.metadata.tags.append("remove-input")

                # Consider importing this name from calitp.magics
                if "%%capture_parameters" in cell.source:
                    params = {**params, **json.loads(cell.outputs[0]["text"])}

                if "%%capture" in cell.source:
                    cell.outputs = []

                if no_stderr:
                    cell.outputs = [
                        output for output in cell.outputs if "name" not in output.keys() or output["name"] != "stderr"
                    ]

                # right side widget to add "tags" (it reverts to "tags": ["tags"]),
                if cell.metadata.get("tags"):
                    # "%%full_width" in cell.source doesn't pick up
                    # when Jupyterbook builds, it says
                    # UsageError: Line magic function `%%full_width` not found.
                    cell.metadata.tags.append("full-width")


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

    Path("./portfolio/index").mkdir(parents=True, exist_ok=True)
    for template in ["index.html"]:
        fname = f"./portfolio/index/{template}"
        with open(fname, "w") as f:
            typer.echo(f"writing out to {fname}")
            f.write(env.get_template(template).render(sites=sites, google_analytics_id=GOOGLE_ANALYTICS_TAG_ID))

    if deploy:
        deploy_index("production")


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
    verbose_logging: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging, DEBUG"),
) -> None:
    """
    Builds a static site from parameterized notebooks as defined in a site YAML file.
    """
    configure_logging(verbose_logging)
    site_yml_name = site.value
    site_output_dir = PORTFOLIO_DIR / Path(site_yml_name)
    site_output_dir.mkdir(parents=True, exist_ok=True)

    with open(SITES_DIR / Path(f"{site_yml_name}.yml")) as f:
        portfolio_site = Site(output_dir=site_output_dir, name=site_yml_name, **yaml.safe_load(f))

    typer.echo(f"copying readme from {portfolio_site.directory} to {site_output_dir}")
    shutil.copy(portfolio_site.readme, site_output_dir / portfolio_site.readme.name)

    fname = site_output_dir / Path("myst.yml")
    with open(fname, "w") as f:
        typer.secho(f"writing config and toc to {fname}", fg=typer.colors.GREEN)
        toc = portfolio_site.toc_yaml
        f.write(
            env.get_template("myst.yml").render(
                site=portfolio_site, toc=toc, google_analytics_id=GOOGLE_ANALYTICS_TAG_ID
            )
        )

    errors = []

    for chapter in portfolio_site.chapters:
        errors.extend(
            chapter.generate(
                execute_papermill=execute_papermill,
                continue_on_error=continue_on_error,
                prepare_only=prepare_only,
                no_stderr=no_stderr,
            )
        )

    for part in portfolio_site.parts:
        for chapter in part.chapters:
            errors.extend(
                chapter.generate(
                    execute_papermill=execute_papermill,
                    continue_on_error=continue_on_error,
                    prepare_only=prepare_only,
                    no_stderr=no_stderr,
                )
            )

    environment = os.environ.copy()
    environment["BASE_URL"] = f"/{site.value}"
    subprocess.run(
        [
            "jupyter",
            "book",
            "build",
            "--html",
        ],
        cwd=site_output_dir,
        env=environment,
    ).check_returncode()

    accessibilty_errors = check_accessibility(site)

    if deploy:
        if continue_on_error and errors:
            ans = input(f"{len(errors)} encountered during papermill; enter that number to continue: ")
            assert int(ans) == len(errors)

        if accessibilty_errors > 0:
            ans = input(f"{accessibilty_errors} serious accessibility errors: type 'ignore' to deploy anyway: ")
            if ans != "ignore":
                return

        deploy_site(site, "production")

    if errors:
        typer.secho(f"{len(errors)} errors encountered during papermill execution", fg=typer.colors.RED)
        sys.exit(1)


@app.command()
def deploy_index(target: Annotated[str, typer.Option(help="Where to deploy the site [staging|production]")]):
    """
    Deploys index page for analysis portfolio.
    """
    assert target in ["staging", "production"]
    target_bucket = {"staging": PORTFOLIO_PUBLISH_STAGING, "production": PORTFOLIO_PUBLISH_PRODUCTION}[target]

    args = ["gcloud", "storage", "cp", "portfolio/index/index.html", f"{target_bucket}/"]
    typer.secho(f"Deploying portfolio index {args}", fg=typer.colors.GREEN)
    subprocess.run(args).check_returncode()


@app.command()
def deploy_site(
    site: SiteChoices, target: Annotated[str, typer.Option(help="Where to deploy the site [staging|production]")]
):
    """
    Deploys site.
    """
    assert target in ["staging", "production"]
    site_yml_name = site.value
    site_output_dir = PORTFOLIO_DIR / Path(site_yml_name)
    target_bucket = {"staging": PORTFOLIO_PUBLISH_STAGING, "production": PORTFOLIO_PUBLISH_PRODUCTION}[target]

    args = [
        "gcloud",
        "storage",
        "rsync",
        f"{site_output_dir}/_build/html/",
        f"{target_bucket}/{site_yml_name}/",
        "--recursive",
        "--delete-unmatched-destination-objects",
        "--checksums-only",
    ]

    typer.secho(f"Running deploy:\n{' '.join(args)}", fg=typer.colors.GREEN)
    subprocess.run(args).check_returncode()


@app.command()
def check_accessibility(site: SiteChoices):
    """
    Checks site for accessibility issues.
    """

    site_output_dir = PORTFOLIO_DIR / Path(site.value)
    directory_path = f"{Path.cwd()}/{site_output_dir}/_build/html/"
    opts = webdriver.ChromeOptions()
    opts.headless = True
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    # The /dev/shm partition is too small in certain VM environments, causing Chrome to fail or crash (see http://crbug.com/715363).
    # Use this flag to work-around this issue (a temporary directory will always be used to create anonymous shared memory files).
    opts.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=opts)
    axe = Axe(driver)

    serious_count = 0

    def find_key_recursive(obj, target_key):
        if isinstance(obj, dict):
            if target_key in obj:
                return obj[target_key]
            for key, value in obj.items():
                result = find_key_recursive(value, target_key)
                if result is not None:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = find_key_recursive(item, target_key)
                if result is not None:
                    return result
        return None

    html_files = list(Path(directory_path).rglob("*.html"))
    for file_path in html_files:

        driver.get(f"file:///{file_path}")
        axe.inject()
        results = axe.run(options={"resultTypes": ["violations"]})

        # print violations
        if results["violations"]:
            print(f"Accessibility violations found in {file_path}:")
            for violation in results["violations"]:
                if violation["impact"] in ("serious", "critical"):
                    serious_count += 1
                impact = {"minor": "❔", "moderate": "⚠️", "serious": "🛑", "critical": "🛑‼️"}.get(violation["impact"])
                print(f"- {impact}  {violation['help']} ({violation['helpUrl']}):")
                print(f"  {find_key_recursive(violation, 'failureSummary')}")
                print(f"  {find_key_recursive(violation, 'html')}")
                print("\n")
        else:
            print(f"✅ {file_path}.\n")

    driver.quit()
    return serious_count


if __name__ == "__main__":
    app()
