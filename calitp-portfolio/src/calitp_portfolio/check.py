"""Accessibility scan via @axe-core/cli.

Invokes the `axe` Node CLI through `npx` with a pinned version, walks every
`.html` file under a target directory, parses the JSON output, applies impact
+ WCAG filters, and prints a focused report. Container-safe chrome options are
passed through so the scan works in JupyterHub pods.

We use `npx -y` (not a global install) so users don't have to set up the npm
ecosystem; the package downloads once and caches in ~/.npm. The version is
pinned in `AXE_CLI_SPEC` for reproducibility across users and CI runs.
"""

import json
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import typer

AXE_CLI_SPEC = "@axe-core/cli@4.11.3"

IMPACT_DISPLAY = {
    "minor": "❔",
    "moderate": "⚠️",
    "serious": "🛑",
    "critical": "🛑‼️",
}
BLOCKING_IMPACTS = {"serious", "critical"}
CONTAINER_SAFE_CHROME_OPTIONS = ["no-sandbox", "disable-dev-shm-usage"]


@dataclass
class Violation:
    rule_id: str
    impact: str
    help_text: str
    help_url: str
    pages: List[Path]
    sample_html: str
    sample_failure_summary: str


def preflight_axe() -> None:
    """Verify `npx` is on PATH; exit 2 with install guidance if not."""
    if shutil.which("npx") is None:
        typer.secho("error: `npx` not found on PATH.", fg=typer.colors.RED, err=True)
        typer.secho(
            "  The accessibility scanner runs via Node. Install Node 18+ (https://nodejs.org\n"
            "  or via your platform manager). `npx` is bundled with Node and will fetch the\n"
            f"  pinned scanner ({AXE_CLI_SPEC}) on first run.\n"
            "  Re-run with --skip-axe-check to bypass this preflight.",
            err=True,
        )
        raise typer.Exit(2)


def _resolve_html_dir(yml_path: Optional[Path], html_dir: Optional[Path]) -> Path:
    if (yml_path is None) == (html_dir is None):
        typer.secho("error: pass exactly one of <site.yml> or --html <dir>", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    if html_dir is not None:
        return html_dir.resolve()
    # site.yml mode: load Site, derive <output_dir>/_build/html/
    from calitp_portfolio.models import load_site

    site = load_site(yml_path)
    return (site.output_dir / "_build" / "html").resolve()


def _run_axe(html_files: Iterable[Path], tags: List[str]) -> List[dict]:
    file_urls = [f.resolve().as_uri() for f in html_files]
    if not file_urls:
        return []
    cmd = ["npx", "-y", AXE_CLI_SPEC, *file_urls, "--tags", ",".join(tags), "--stdout"]
    for opt in CONTAINER_SAFE_CHROME_OPTIONS:
        cmd.extend(["--chrome-options", opt])
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0 and not result.stdout.strip().startswith("["):
        typer.secho(f"axe CLI failed:\n{result.stderr}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    return json.loads(result.stdout)


def _collect(raw_results: List[dict], impacts: set, dedupe: bool) -> List[Violation]:
    grouped: dict = defaultdict(list)
    for page_result in raw_results:
        page_path = Path(page_result["url"].replace("file://", ""))
        for violation in page_result.get("violations", []):
            if violation.get("impact") not in impacts:
                continue
            key = violation["id"] if dedupe else (violation["id"], str(page_path))
            grouped[key].append((page_path, violation))

    out: List[Violation] = []
    for entries in grouped.values():
        page_paths = [p for p, _ in entries]
        sample_violation = entries[0][1]
        first_node = sample_violation.get("nodes", [{}])[0]
        out.append(
            Violation(
                rule_id=sample_violation["id"],
                impact=sample_violation["impact"],
                help_text=sample_violation.get("help", ""),
                help_url=sample_violation.get("helpUrl", ""),
                pages=page_paths,
                sample_html=first_node.get("html", "")[:100],
                sample_failure_summary=first_node.get("failureSummary", ""),
            )
        )

    impact_order = {"critical": 0, "serious": 1, "moderate": 2, "minor": 3}
    out.sort(key=lambda v: (impact_order.get(v.impact, 99), v.rule_id))
    return out


def _print_report(violations: List[Violation], dedupe: bool) -> None:
    if not violations:
        typer.secho("✅ no violations at the requested impact levels", fg=typer.colors.GREEN)
        return
    for v in violations:
        emoji = IMPACT_DISPLAY.get(v.impact, "?")
        if dedupe and len(v.pages) > 1:
            scope = f"{len(v.pages)} pages affected"
        else:
            scope = str(v.pages[0])
        typer.secho(f"\n{emoji}  {v.rule_id}  ({v.impact})  — {scope}")
        typer.secho(f"   {v.help_text} ({v.help_url})", fg=typer.colors.BLUE)
        if v.sample_failure_summary:
            typer.echo(f"   {v.sample_failure_summary.splitlines()[0]}")
        if v.sample_html:
            typer.echo(f"   sample HTML: {v.sample_html}")


def run(
    *,
    yml_path: Optional[Path],
    html_dir: Optional[Path],
    tags: List[str],
    impacts: set,
    dedupe: bool,
    skip_axe_check: bool,
    report_path: Optional[Path] = None,
) -> int:
    if not skip_axe_check:
        preflight_axe()
    scan_root = _resolve_html_dir(yml_path, html_dir)
    if not scan_root.is_dir():
        typer.secho(f"error: not a directory: {scan_root}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)

    html_files = sorted(scan_root.rglob("*.html"))
    if not html_files:
        typer.secho(f"no .html files under {scan_root}", fg=typer.colors.YELLOW)
        return 0

    typer.secho(f"scanning {len(html_files)} files under {scan_root}", fg=typer.colors.GREEN)
    raw = _run_axe(html_files, tags)

    if report_path is not None:
        report_path.write_text(json.dumps(raw, indent=2))
        typer.secho(f"wrote full axe report to {report_path}", fg=typer.colors.GREEN)

    violations = _collect(raw, impacts, dedupe)
    _print_report(violations, dedupe)

    return 1 if any(v.impact in BLOCKING_IMPACTS for v in violations) else 0
