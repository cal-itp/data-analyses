import subprocess
from importlib.resources import files
from pathlib import Path
from typing import Optional

import typer

from calitp_portfolio.builder import build_site
from calitp_portfolio.indexer import load_manifest, render_index

LOGIN_CONFIG = str(files("calitp_portfolio.auth") / "login.json")

app = typer.Typer(
    name="calitp-portfolio",
    help="Build, validate, and deploy Cal-ITP portfolio sites.",
    no_args_is_help=True,
)


@app.callback()
def main() -> None:
    """Root callback so Typer materializes a runnable app while subcommands are pending."""


@app.command()
def index(
    sites_yml: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Path to write rendered index.html."),
    target: str = typer.Option("staging", "--target", help="Deploy target: staging or prod."),
) -> None:
    """Render the portfolio landing page from a sites.yml manifest."""
    if target not in ("staging", "prod"):
        raise typer.BadParameter("--target must be 'staging' or 'prod'")

    manifest = load_manifest(sites_yml)
    html = render_index(manifest, target=target)

    output_path = output or sites_yml.parent / "index.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
    typer.echo(f"wrote {output_path}")


def _adc_authorized() -> bool:
    """Check for valid credentials"""
    result = subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
    )
    return result.returncode == 0


@app.command()
def build(
    site_yml: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True),
    output_dir: Optional[Path] = typer.Option(
        None, "--output-dir", "-o", help="Where to write build artifacts. Defaults to the yml's parent directory."
    ),
    execute: bool = typer.Option(True, "--execute/--no-execute", help="Skip calls to papermill when --no-execute."),
    show_stderr: bool = typer.Option(
        False, "--show-stderr", help="Keep stderr stream in cell outputs (default: strip)."
    ),
    prepare_only: bool = typer.Option(False, help="Pass-through to papermill; if true, cells are not executed."),
    continue_on_error: bool = typer.Option(False, help="Continue building remaining chapters on papermill error."),
    hide_title_block: bool = typer.Option(False, help="If true, will hide the title block for all pages."),
    skip_auth_check: bool = typer.Option(False, "--skip-auth-check", help="Skip the auth pre-flight."),
) -> None:
    """Build a static site from a parameterized notebook portfolio."""
    if execute and not prepare_only and not skip_auth_check and not _adc_authorized():
        typer.secho(
            "error: Auth check failed.\n" "  No valid credentials found.\n" "  Run: uv run calitp-portfolio login",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    output = output_dir or site_yml.parent
    exit_code = build_site(
        yml_path=site_yml,
        output_dir=output,
        execute_papermill=execute,
        no_stderr=not show_stderr,
        prepare_only=prepare_only,
        continue_on_error=continue_on_error,
        hide_title_block=hide_title_block,
    )
    if exit_code != 0:
        raise typer.Exit(code=exit_code)


@app.command()
def login() -> None:
    """Authenticate to Google Cloud using the Cal-ITP login config bundled with this tool."""
    cmd = ["gcloud", "auth", "application-default", "login", f"--login-config={LOGIN_CONFIG}"]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise typer.Exit(code=result.returncode)


if __name__ == "__main__":
    app()
