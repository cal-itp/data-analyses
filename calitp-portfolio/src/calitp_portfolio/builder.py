import hashlib
import importlib.metadata
import json
import shutil
import subprocess
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

import typer
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape

# importing engine has the side effect of registering the "markdown" papermill engine
from calitp_portfolio import engine  # noqa: F401
from calitp_portfolio.models import Site

TEMPLATES_DIR = Path(__file__).parent / "templates"


def _load_site(yml_path: Path, output_dir: Path) -> Site:
    with open(yml_path) as f:
        return Site(output_dir=output_dir, name=yml_path.stem, **yaml.safe_load(f))


def _render_myst_yml(site: Site, hide_title_block: bool) -> str:
    env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=select_autoescape(["html"]),
    )
    return env.get_template("myst.yml").render(
        site=site,
        toc=site.toc_yaml,
        hide_title_block=hide_title_block,
        google_analytics_id="",
    )


def _bundle_template_assets(output_dir: Path) -> None:
    """Copy the chrome the myst.yml template references (logos, css, footer)
    into `<output_dir>/_templates/` so the build is self-contained.
    """
    dest = output_dir / "_templates"
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    shutil.copytree(TEMPLATES_DIR / "assets", dest / "assets")
    shutil.copytree(TEMPLATES_DIR / "partials", dest / "partials")
    shutil.copy(TEMPLATES_DIR / "custom.css", dest / "custom.css")


def _write_manifest(site: Site, yml_path: Path, output_dir: Path, errors_count: int) -> None:
    manifest = {
        "tool_version": importlib.metadata.version("calitp-portfolio"),
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "site_name": site.name,
        "site_title": site.title,
        "yml_path": str(yml_path),
        "yml_sha256": hashlib.sha256(yml_path.read_bytes()).hexdigest(),
        "deploy": site.deploy.model_dump(),
        "errors_count": errors_count,
    }
    (output_dir / "build.json").write_text(json.dumps(manifest, indent=2))


class _Tee:
    """File-like object that mirrors writes to multiple underlying streams."""

    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            s.write(data)
        return len(data)

    def flush(self):
        for s in self._streams:
            s.flush()

    def isatty(self):
        return bool(self._streams) and self._streams[0].isatty()


def _run_subprocess_tee(cmd, cwd, log_file, terminal) -> None:
    """Run a subprocess, streaming its combined stdout+stderr to terminal and log_file line-by-line."""
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    for line in proc.stdout:
        terminal.write(line)
        terminal.flush()
        log_file.write(line)
        log_file.flush()
    proc.wait()
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


def build_site(
    yml_path: Path,
    output_dir: Path,
    execute_papermill: bool,
    no_stderr: bool,
    prepare_only: bool,
    continue_on_error: bool,
    hide_title_block: bool,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "build.log"
    terminal_stdout = sys.stdout
    terminal_stderr = sys.stderr

    with open(log_path, "w") as log_file:
        tee_out = _Tee(terminal_stdout, log_file)
        tee_err = _Tee(terminal_stderr, log_file)
        with redirect_stdout(tee_out), redirect_stderr(tee_err):
            site = _load_site(yml_path, output_dir)

            typer.echo(f"copying {site.readme.name} from {site.directory} to {output_dir}")
            shutil.copy(site.readme, output_dir / site.readme.name)

            myst_path = output_dir / "myst.yml"
            typer.secho(f"writing config and toc to {myst_path}", fg=typer.colors.GREEN)
            myst_path.write_text(_render_myst_yml(site, hide_title_block=hide_title_block))

            _bundle_template_assets(output_dir)

            errors = []
            for part in site.parts:
                for chapter in part.chapters:
                    errors.extend(
                        chapter.generate(
                            execute_papermill=execute_papermill,
                            continue_on_error=continue_on_error,
                            prepare_only=prepare_only,
                            no_stderr=no_stderr,
                        )
                    )

            _run_subprocess_tee(
                ["jupyter", "book", "build", "--html", "--ci"],
                cwd=output_dir,
                log_file=log_file,
                terminal=terminal_stdout,
            )

            _write_manifest(site, yml_path, output_dir, errors_count=len(errors))

            if errors:
                typer.secho(f"{len(errors)} errors encountered during papermill execution", fg=typer.colors.RED)
                return 1

    return 0
