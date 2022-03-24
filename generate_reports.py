"""
Generates
"""
import copy
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import humanize
import nbformat
import papermill as pm
import typer
import yaml
from nbconvert import HTMLExporter
from nbformat.v4 import new_output
from papermill.engines import NBClientEngine, papermill_engines
from pydantic import BaseModel

app = typer.Typer(help="CLI to tie together papermill and jupyter book")


def district_name(district, **_):
    return humanize.apnumber(district).title()


RESOLVERS = [
    district_name,
]


class Analysis(BaseModel):
    notebook: Path
    params: Dict[str, List] = {}
    prepare_only: bool = False


class DocsConfig(BaseModel):
    notebooks: Dict[str, Analysis]


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


def parameterize_filename(old_path: Path, params: Dict) -> Path:
    assert old_path.suffix == ".ipynb"
    return Path(
        old_path.stem
        + "__"
        + "__".join(f"{k}_{v}" for k, v in params.items())
        + old_path.suffix
    )


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
def build(
    config: Path = "./reports_config.yml",
    output_dir: Path = "./target/",
    docs_dir: Path = "./docs/",
    report: str = typer.Option(
        None,
        help="If provided, the name of the report to build.",
    ),
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
        docs_config = DocsConfig(notebooks=yaml.safe_load(f))

    if report:
        assert report in docs_config.notebooks.keys()

    output_dir.mkdir(parents=True, exist_ok=True)

    for name, analysis in docs_config.notebooks.items():
        if report and name != report:
            continue
        params = list(zip(*analysis.params.values()))
        (output_dir / analysis.notebook.parent).mkdir(parents=True, exist_ok=True)

        for param_set in params or [{}]:
            params_dict = {k: v for k, v in zip(analysis.params.keys(), param_set)}

            if params_dict:
                parameterized_filepath = (
                    analysis.notebook.parent
                    / parameterize_filename(analysis.notebook, params_dict)
                )
            else:
                parameterized_filepath = analysis.notebook
            output_path = output_dir / parameterized_filepath
            typer.echo(
                f"executing papermill; writing {analysis.notebook} => {output_path}"
            )

            if execute_papermill:
                pm.execute_notebook(
                    input_path=analysis.notebook,
                    output_path=output_path,
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

            docs_output_path = (
                docs_dir / parameterized_filepath
            )  # .with_suffix(".html")
            docs_output_path.parent.mkdir(parents=True, exist_ok=True)
            typer.echo(f"placing in docs; {output_path} => {docs_output_path}")
            shutil.copy(output_path, docs_output_path)

        subprocess.run(
            [
                "jb",
                "build",
                "-W",
                "-n",
                "--keep-going",
                str(docs_dir),
            ]
        )


if __name__ == "__main__":
    app()
