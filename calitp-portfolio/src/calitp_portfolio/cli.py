from pathlib import Path
from typing import Optional

import typer

from calitp_portfolio.indexer import load_manifest, render_index

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


if __name__ == "__main__":
    app()
