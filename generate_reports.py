"""
Generates
"""
import copy
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import nbformat
import papermill as pm
import typer
import yaml
from nbconvert import HTMLExporter
from nbformat.v4 import new_output
from papermill.engines import NBClientEngine, papermill_engines
from pydantic import BaseModel

app = typer.Typer()


class Analysis(BaseModel):
    notebook: Path
    params: Dict[str, List]


class DocsConfig(BaseModel):
    notebooks: Dict[str, Analysis]


class EngineWithParameterizedMarkdown(NBClientEngine):
    @classmethod
    def execute_managed_notebook(cls, nb_man, kernel_name, **kwargs):
        # call the papermill execution engine:
        super().execute_managed_notebook(nb_man, kernel_name, **kwargs)

        assert 'original_parameters' in kwargs

        for cell in nb_man.nb.cells:
            if cell.cell_type == "markdown":
                cell.source = cell.source.format(**kwargs['original_parameters'])
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
    report: str = None,
) -> None:
    with open(config) as f:
        docs_config = DocsConfig(notebooks=yaml.safe_load(f))

    if report:
        assert report in docs_config.notebooks.keys()

    output_dir.mkdir(parents=True, exist_ok=True)

    for name, analysis in docs_config.notebooks.items():
        if report and name != report:
            continue
        params = zip(*analysis.params.values())
        (output_dir / analysis.notebook.parent).mkdir(parents=True, exist_ok=True)

        for param_set in params:
            params_dict = {k: v for k, v in zip(analysis.params.keys(), param_set)}
            parameterized_filepath = analysis.notebook.parent / parameterize_filename(
                analysis.notebook, params_dict
            )
            output_path = output_dir / parameterized_filepath
            typer.echo(
                f"executing papermill; writing {analysis.notebook} => {output_path}"
            )
            pm.execute_notebook(
                input_path=analysis.notebook,
                output_path=output_path,
                parameters=params_dict,
                cwd=analysis.notebook.parent,
                engine_name="markdown",
                report_mode=True,
                original_parameters=params_dict,
            )

            # html_output_path = convert_to_html(output_path)

            docs_output_path = docs_dir / parameterized_filepath #.with_suffix(".html")
            docs_output_path.parent.mkdir(parents=True, exist_ok=True)
            typer.echo(f"placing in docs; {output_path} => {docs_output_path}")
            shutil.copy(output_path, docs_output_path)

        subprocess.run(
            [
                "jb",
                "build",
                "docs",
            ]
        )


if __name__ == "__main__":
    app()
