"""End-to-end check tests — real @axe-core/cli via npx, real Chrome.

Two fixtures cover the meaningful paths:

- `clean/`: WCAG-compliant HTML; verifies the success path (exit 0).
- `violating/`: image-alt + color-contrast violations across two pages;
  verifies failure detection, dedup, impact filtering, and report output.

Catches regressions in:
- npx command construction (`AXE_CLI_SPEC`, chrome flags)
- axe-cli JSON shape parsing
- dedup-across-pages logic
"""

import json
from pathlib import Path

from typer.testing import CliRunner

from calitp_portfolio.cli import app

runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures" / "built_html"


def test_clean_dir_passes():
    result = runner.invoke(app, ["axe-check", "--html", str(FIXTURES / "clean")])
    assert result.exit_code == 0
    assert "no violations" in result.stdout


def test_violating_dir_fails_with_dedup_and_report(tmp_path, monkeypatch):
    # `--report` is a bare flag that writes axe-report.json to the cwd.
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app,
        ["axe-check", "--html", str(FIXTURES / "violating"), "--report"],
    )
    assert result.exit_code == 1
    assert "image-alt" in result.stdout
    assert "2 pages affected" in result.stdout  # dedup across page1.html + page2.html

    saved = json.loads((tmp_path / "axe-report.json").read_text())
    assert len(saved) == 2  # one entry per scanned file
    rule_ids = {v["id"] for page in saved for v in page.get("violations", [])}
    assert "image-alt" in rule_ids
    # report is trimmed to actionable categories — axe's bulky `passes` (a result
    # per element that passed each rule) is dropped to keep the file small.
    assert all("passes" not in page for page in saved)
