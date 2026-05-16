"""End-to-end build tests - no stubs.

Two fixtures cover the meaningful pipeline paths:

- `_basic_analyses_test.yml`: chapter loop runs, papermill spins up a kernel,
  jupyter-book compiles multi-page HTML. Notebooks are markdown-only so no
  external Python deps (like `calitp_data_analysis.magics`) are needed.
- `_readme_only_analyses_test.yml`: zero chapters; tests a single-page site.

Check:
- MyST/jupyter-book version coupling
- Papermill API drift / engine registration failures
- TOC↔output-file path coupling
"""

import shutil
from pathlib import Path

from typer.testing import CliRunner

from calitp_portfolio.cli import app

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"


def _materialize_fixture(yml_name: str, dest: Path) -> Path:
    """Stage the fixture so build resolves for tests"""
    portfolio_dir = dest / "tests" / "fixtures" / "portfolio"
    portfolio_dir.mkdir(parents=True, exist_ok=True)
    for f in (FIXTURES / "portfolio").iterdir():
        if f.is_file():
            shutil.copy(f, portfolio_dir / f.name)
    yml_dest = dest / yml_name
    shutil.copy(FIXTURES / "sites" / yml_name, yml_dest)
    return yml_dest


def test_integration_basic_build(tmp_path, monkeypatch):
    """Full pipeline against a chapter-bearing site with markdown-only notebooks."""
    yml = _materialize_fixture("_basic_analyses_test.yml", tmp_path)
    monkeypatch.chdir(tmp_path)
    output_dir = tmp_path / "build"

    result = runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir)])

    assert result.exit_code == 0, result.stdout
    # papermill wrote parameterized notebook outputs
    assert (output_dir / "00__notebook_1__.ipynb").exists()
    assert (output_dir / "00__notebook_2__.ipynb").exists()
    # jupyter-book compiled the site
    assert (output_dir / "_build" / "html" / "index.html").exists()


def test_integration_readme_only_build(tmp_path, monkeypatch):
    """Empty-parts pipeline: jupyter-book compiles a single-page site."""
    yml = _materialize_fixture("_readme_only_analyses_test.yml", tmp_path)
    monkeypatch.chdir(tmp_path)
    output_dir = tmp_path / "build"

    result = runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir)])

    assert result.exit_code == 0, result.stdout
    assert (output_dir / "_build" / "html" / "index.html").exists()
