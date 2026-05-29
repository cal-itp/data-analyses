import shutil
from pathlib import Path
from typing import Optional

import typer

from calitp_portfolio import auth, deployer
from calitp_portfolio.builder import build_site
from calitp_portfolio.indexer import load_manifest, render_index
from calitp_portfolio.models import load_site

AUTH_FAIL_MSG = "error: Auth check failed.\n" "  No valid credentials found.\n" "  Run: uv run calitp-portfolio login"

app = typer.Typer(
    name="calitp-portfolio",
    help="Build, validate, and deploy Cal-ITP portfolio sites.",
    no_args_is_help=True,
    add_completion=False,
)


@app.callback()
def main() -> None:
    """Root callback so Typer materializes a runnable app while subcommands are pending."""


def _require_auth() -> None:
    if not auth.is_valid():
        typer.secho(AUTH_FAIL_MSG, fg=typer.colors.RED)
        raise typer.Exit(code=2)


def _resolve_target_url(deploy, target: str, source_label: str) -> str:
    if target == "staging":
        return deploy.staging
    if deploy.prod is None:
        typer.secho(
            f"error: --target prod requested but {source_label} has no deploy.prod set.\n"
            f"  Add a deploy.prod entry when the site is ready to release.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    return deploy.prod


@app.command()
def index(
    sites_yml: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Path to write rendered index.html."),
    target: str = typer.Option("staging", "--target", help="Deploy target: staging or prod."),
    deploy: bool = typer.Option(
        False, "--deploy", help="After rendering, upload index.html to the manifest's deploy target."
    ),
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

    if deploy:
        target_url = _resolve_target_url(manifest.deploy, target, source_label="sites.yml")
        _require_auth()
        deployer.upload_file(target_url, output_path)
        typer.echo(f"deployed {output_path} -> {target_url}")


@app.command(name="list")
def list_(
    site_yml: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True),
) -> None:
    """Print the resolved part/chapter tree with slugs, params, and notebook paths."""
    site = load_site(site_yml)
    typer.echo(site.title)
    if not any(part.chapters for part in site.parts):
        typer.echo("(no chapters)")
        return
    for part in site.parts:
        for chapter in part.chapters:
            part_label = f"[{part.caption}] " if part.caption else ""
            params = dict(chapter.resolved_params) or "{}"
            notebook = chapter.resolved_notebook
            typer.echo(f"{part_label}{chapter.identifier}  params={params}  notebook={notebook}")


@app.command()
def build(
    site_yml: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Where to write build artifacts. Defaults to `<yml dir>/<yml stem>/`.",
    ),
    execute: bool = typer.Option(True, "--execute/--no-execute", help="Skip calls to papermill when --no-execute."),
    show_stderr: bool = typer.Option(
        False, "--show-stderr", help="Keep stderr stream in cell outputs (default: strip)."
    ),
    prepare_only: bool = typer.Option(False, help="Pass-through to papermill; if true, cells are not executed."),
    continue_on_error: bool = typer.Option(False, help="Continue building remaining chapters on papermill error."),
    hide_title_block: bool = typer.Option(False, help="If true, will hide the title block for all pages."),
    only: Optional[str] = typer.Option(
        None,
        "--only",
        help="Comma-separated chapter identifiers to build (see `calitp-portfolio list`).",
    ),
    limit: Optional[int] = typer.Option(None, "--limit", help="Build only the first N chapters in source order."),
    readme_only: bool = typer.Option(False, "--readme-only", help="Build just the landing page; skip all chapters."),
    toc_only: bool = typer.Option(
        False, "--toc-only", help="Re-render myst.yml and run jupyter-book; skip papermill and readme copy."
    ),
) -> None:
    """Build a static site from a parameterized notebook portfolio."""
    if readme_only and toc_only:
        typer.secho("error: --readme-only and --toc-only are mutually exclusive", fg=typer.colors.RED)
        raise typer.Exit(code=2)
    if readme_only and (only or limit is not None):
        typer.secho("error: --readme-only cannot be combined with --only or --limit", fg=typer.colors.RED)
        raise typer.Exit(code=2)
    if toc_only and (only or limit is not None):
        typer.secho("error: --toc-only cannot be combined with --only or --limit", fg=typer.colors.RED)
        raise typer.Exit(code=2)

    papermill_runs = execute and not prepare_only and not readme_only and not toc_only
    if papermill_runs:
        _require_auth()

    only_slugs = [s.strip() for s in only.split(",")] if only else None
    if only_slugs:
        available = {c.identifier for p in load_site(site_yml).parts for c in p.chapters}
        unknown = [s for s in only_slugs if s not in available]
        if unknown:
            typer.secho(
                f"error: --only references unknown chapter(s): {', '.join(unknown)}\n"
                f"  Run `calitp-portfolio list {site_yml}` to see available slugs.",
                fg=typer.colors.RED,
            )
            raise typer.Exit(code=1)

    exit_code = build_site(
        yml_path=site_yml,
        output_dir=output_dir,
        execute_papermill=execute,
        no_stderr=not show_stderr,
        prepare_only=prepare_only,
        continue_on_error=continue_on_error,
        hide_title_block=hide_title_block,
        only=only_slugs,
        limit=limit,
        readme_only=readme_only,
        toc_only=toc_only,
    )
    if exit_code != 0:
        raise typer.Exit(code=exit_code)


@app.command()
def deploy(
    site_yml: Optional[Path] = typer.Argument(
        None,
        exists=True,
        dir_okay=False,
        readable=True,
        help="Site yml for deployment.",
    ),
    target: str = typer.Option("staging", "--target", help="Deploy target: staging or prod."),
    html: Optional[Path] = typer.Option(
        None,
        "--html",
        exists=True,
        file_okay=False,
        dir_okay=True,
        help="Directory of HTML to upload if not using site yml",
    ),
    target_url: Optional[str] = typer.Option(
        None, "--target-url", help="gs://bucket/prefix to upload to if not using site yml"
    ),
) -> None:
    """Upload built HTML to GCS. Defaults to <site>/_build/html and the yml's deploy target."""
    if target not in ("staging", "prod"):
        raise typer.BadParameter("--target must be 'staging' or 'prod'")

    generic = html is not None or target_url is not None
    if generic:
        if html is None or target_url is None:
            raise typer.BadParameter("--html and --target-url must be provided together")
        if site_yml is not None:
            raise typer.BadParameter("provide either a site yml or --html/--target-url, not both")
        resolved_url = target_url
        html_dir = html
    else:
        if site_yml is None:
            raise typer.BadParameter("provide a site yml, or --html with --target-url")
        site = load_site(site_yml)
        resolved_url = _resolve_target_url(site.deploy, target, source_label=str(site_yml))
        html_dir = site.output_dir / "_build" / "html"
        if not html_dir.is_dir():
            typer.secho(
                f"error: built HTML not found at {html_dir}. Run `calitp-portfolio build` first.",
                fg=typer.colors.RED,
            )
            raise typer.Exit(code=1)

    _require_auth()

    count = deployer.upload_directory(resolved_url, html_dir)
    typer.echo(f"deployed {count} files from {html_dir} -> {resolved_url}")


@app.command()
def clean(
    site_yml: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True),
    all_: bool = typer.Option(False, "--all", help="Also remove parameterized notebook output dirs."),
) -> None:
    """Remove a site's build artifacts. Idempotent."""
    site = load_site(site_yml)
    build_dir = site.output_dir / "_build"
    shutil.rmtree(build_dir, ignore_errors=True)
    typer.echo(f"removed {build_dir}")

    if all_:
        for part in site.parts:
            for chapter in part.chapters:
                if chapter.slug:
                    shutil.rmtree(chapter.path, ignore_errors=True)
                    typer.echo(f"removed {chapter.path}")


@app.command()
def login() -> None:
    """Authenticate to Google Cloud using the Cal-ITP login config bundled with this tool."""
    returncode = auth.login()
    if returncode != 0:
        raise typer.Exit(code=returncode)


if __name__ == "__main__":
    app()
