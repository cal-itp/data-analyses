"""
Generates
"""
import copy
from pathlib import Path
from typing import Dict, List

import nbformat
import papermill as pm
import typer
import yaml
from nbconvert import HTMLExporter
from pydantic import BaseModel


class Analysis(BaseModel):
    notebook: Path
    params: Dict[str, List]


class DocsConfig(BaseModel):
    notebooks: Dict[str, Analysis]


def parameterize_filename(old_path: Path, params: Dict) -> Path:
    assert old_path.suffix == ".ipynb"
    return Path(
        old_path.stem
        + "__"
        + "__".join(f"{k}_{v}" for k, v in params.items())
        + old_path.suffix
    )


def main(
    config: Path = "./reports_config.yml", output_dir: Path = Path("./target/")
) -> None:
    with open(config) as f:
        docs_config = DocsConfig(notebooks=yaml.safe_load(f))

    output_dir.mkdir(parents=True, exist_ok=True)

    for name, analysis in docs_config.notebooks.items():
        params = zip(*analysis.params.values())
        (output_dir / analysis.notebook.parent).mkdir(parents=True, exist_ok=True)

        for param_set in params:
            params_dict = {k: v for k, v in zip(analysis.params.keys(), param_set)}
            output_path = (
                output_dir
                / analysis.notebook.parent
                / parameterize_filename(analysis.notebook, params_dict)
            )
            typer.echo(
                f"executing papermill; writing {analysis.notebook} => {output_path}"
            )
            pm.execute_notebook(
                input_path=analysis.notebook,
                output_path=output_path,
                parameters=params_dict,
                cwd=analysis.notebook.parent,
            )

            html_output_path = output_path.with_suffix('.html')
            typer.echo(f"converting to html, {output_path} => {html_output_path}")

            html_exporter = HTMLExporter(template_name="classic")

            with open(output_path) as f:
                output_notebook = nbformat.reads(f.read(), as_version=4)

            body, _ = html_exporter.from_notebook_node(output_notebook)

            with open(output_path, 'w') as f:
                f.write(body)


if __name__ == "__main__":
    typer.run(main)
