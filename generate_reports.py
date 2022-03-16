"""
Generates
"""
from pathlib import Path
from typing import Dict, List

import papermill as pm
import typer
import yaml
from pydantic import BaseModel


class Analysis(BaseModel):
    notebook: Path
    params: Dict[str, List]


class DocsConfig(BaseModel):
    notebooks: Dict[str, Analysis]


def params_to_filepath(old_path: Path, params: Dict) -> Path:
    assert old_path.suffix == '.ipynb'
    return old_path.stem / Path('__'.join(f"{k}_{v}" for k, v in params.items()))


def main(config: Path = "./reports_config.yml", output_dir: Path = Path('./target/')) -> None:
    with open(config) as f:
        docs_config = DocsConfig(notebooks=yaml.safe_load(f))

    output_dir.mkdir(parents=True, exist_ok=True)

    for name, analysis in docs_config.notebooks.items():
        params = zip(*analysis.params.values())
        (output_dir / analysis.notebook.parent).mkdir(parents=True, exist_ok=True)

        for param_set in params:
            params_dict = {k: v for k, v in zip(analysis.params.keys(), param_set)}
            pm.execute_notebook(
                input_path=analysis.notebook,
                output_path=output_dir / analysis.notebook.parent /,
                parameters=params_dict,
                cwd=analysis.notebook.parent,
            )


if __name__ == "__main__":
    typer.run(main)
