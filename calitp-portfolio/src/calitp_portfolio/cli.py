import typer

app = typer.Typer(
    name="calitp-portfolio",
    help="Build, validate, and deploy Cal-ITP portfolio sites.",
    no_args_is_help=True,
)


@app.callback()
def main() -> None:
    """Root callback so Typer materializes a runnable app while subcommands are pending."""


if __name__ == "__main__":
    app()
