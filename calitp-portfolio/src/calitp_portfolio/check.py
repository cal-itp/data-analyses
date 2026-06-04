"""Accessibility scan via @axe-core/cli.

Invokes the `axe` Node CLI through `npx` with a pinned version, walks every
`.html` file under a target directory, parses the JSON output, applies impact
+ WCAG filters, and prints a focused report. Container-safe chrome options are
passed through so the scan works in JupyterHub pods.

We use `npx -y` (not a global install) so users don't have to set up the npm
ecosystem; the package downloads once and caches in ~/.npm. The version is
pinned in `AXE_CLI_SPEC` for reproducibility across users and CI runs.

MyST/Jupyter-Book sites embed notebook outputs (Altair/Vega charts, Folium maps)
as separate HTML fragments that the page loads client-side via *absolute* URLs
(`/<prefix>/build/<hash>.html`, where `<prefix>` is the site's BASE_URL). Those
only resolve when the site is served over http at that prefix — under `file://`
they 404 and the charts never render, so a file:// scan silently misses every
issue in rendered chart/map content. When scanning a site (`<site.yml>` mode) we
therefore serve the build dir over a short-lived local http server rooted at the
site's prefix, and wait `--load-delay` ms for the client render before auditing.
"""

import json
import os
import posixpath
import shutil
import subprocess
import threading
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import typer

AXE_CLI_SPEC = "@axe-core/cli@4.11.3"

# Default filename for `--report` (written to the current directory).
DEFAULT_REPORT_FILENAME = "axe-report.json"

# axe logs a full element-level result (with HTML) for every rule check —
# including the thousands that *pass* on a content-rich page. Persisting those
# turns a 2-issue report into multiple MB, of which >90% is "here's everything
# that's fine". We drop the bulky non-actionable categories from the saved
# report, keeping page metadata + `violations`/`incomplete`.
REPORT_DROP_CATEGORIES = ("passes", "inapplicable")

# Wait (ms) after page load before auditing, so MyST hydration + Vega/Altair/
# Folium render completes. Applied only when serving over http (file:// pages
# have no client-loaded content to wait for).
DEFAULT_SERVE_LOAD_DELAY_MS = 3000

IMPACT_DISPLAY = {
    "minor": "❔",
    "moderate": "⚠️",
    "serious": "🛑",
    "critical": "🛑‼️",
}
BLOCKING_IMPACTS = {"serious", "critical"}
CONTAINER_SAFE_CHROME_OPTIONS = ["no-sandbox", "disable-dev-shm-usage"]

# MyST writes notebook HTML outputs (charts/maps) into a top-level `build/`
# fragment dir. The real pages load these as outputs; scanning them standalone
# only surfaces document-scaffolding artifacts (a bare snippet has no <title>
# or lang), so we skip them when serving the real, rendered pages.
FRAGMENT_DIR = "build"


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


def _resolve_scan_target(yml_path: Optional[Path], html_dir: Optional[Path]) -> Tuple[Path, Optional[str]]:
    """Return `(html_dir_to_scan, url_prefix)`.

    `url_prefix` is the BASE_URL prefix the site is served under (used by the
    http-serve mode); `None` in `--html` mode, where we don't know the prefix."""
    if (yml_path is None) == (html_dir is None):
        typer.secho("error: pass exactly one of <site.yml> or --html <dir>", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    if html_dir is not None:
        return html_dir.resolve(), None
    # site.yml mode: load Site, derive <output_dir>/_build/html/ + the BASE_URL prefix.
    from calitp_portfolio.models import load_site

    site = load_site(yml_path)
    return (site.output_dir / "_build" / "html").resolve(), site.base_url.strip("/")


@contextmanager
def _serve_http(directory: Path, prefix: str) -> Iterator[str]:
    """Serve `directory` over http at `/<prefix>/` on an ephemeral localhost port.

    Yields the base URL (including the prefix). MyST asset and chart-fragment
    URLs are absolute (`/<prefix>/...`); the handler strips the prefix so they
    resolve to files under `directory`."""
    prefix = prefix.strip("/")
    base_dir = str(directory)

    class _Handler(SimpleHTTPRequestHandler):
        def translate_path(self, path: str) -> str:
            path = unquote(path.split("?", 1)[0].split("#", 1)[0])
            if prefix and (path == f"/{prefix}" or path.startswith(f"/{prefix}/")):
                path = path[len(prefix) + 1 :]
            path = posixpath.normpath(path)
            parts = [p for p in path.split("/") if p and p not in (".", "..")]
            return os.path.join(base_dir, *parts)

        def log_message(self, *args):  # silence per-request logging
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        yield f"http://127.0.0.1:{port}" + (f"/{prefix}" if prefix else "")
    finally:
        server.shutdown()
        server.server_close()


def _run_axe(urls: List[str], tags: List[str], load_delay_ms: int = 0) -> List[dict]:
    if not urls:
        return []
    cmd = ["npx", "-y", AXE_CLI_SPEC, *urls, "--tags", ",".join(tags), "--stdout"]
    if load_delay_ms:
        cmd += ["--load-delay", str(load_delay_ms)]
    for opt in CONTAINER_SAFE_CHROME_OPTIONS:
        cmd.extend(["--chrome-options", opt])
    # Have the chromedriver npm package download the driver matching the
    # installed Chrome, instead of its own (often newer) pinned default.
    env = {**os.environ, "DETECT_CHROMEDRIVER_VERSION": "true"}
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    # axe exits non-zero when it finds violations (normal), and --load-delay
    # prints a "Waiting for N milliseconds..." preamble ahead of the JSON, so
    # locate the array start rather than assuming stdout begins with "[".
    start = result.stdout.find("[")
    if start == -1:
        typer.secho(f"axe CLI failed:\n{result.stderr or result.stdout}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    return json.loads(result.stdout[start:])


def _display_path(url: str) -> Path:
    """Human-readable page path from an axe result URL (file:// or http://)."""
    parsed = urlparse(url)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    return Path(unquote(parsed.path).lstrip("/"))


def _collect(raw_results: List[dict], impacts: set, dedupe: bool) -> List[Violation]:
    grouped: dict = defaultdict(list)
    for page_result in raw_results:
        page_path = _display_path(page_result["url"])
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
    serve: Optional[bool] = None,
    load_delay_ms: Optional[int] = None,
    base_url: Optional[str] = None,
) -> int:
    if not skip_axe_check:
        preflight_axe()
    scan_root, site_prefix = _resolve_scan_target(yml_path, html_dir)
    if not scan_root.is_dir():
        typer.secho(f"error: not a directory: {scan_root}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)

    # Serve over http by default when scanning a site via its yml (its charts
    # only render over http); plain --html dirs default to file:// — fast, and
    # fixtures / arbitrary HTML have no client-loaded, absolutely-pathed content.
    if serve is None:
        serve = yml_path is not None
    if load_delay_ms is None:
        load_delay_ms = DEFAULT_SERVE_LOAD_DELAY_MS if serve else 0

    html_files = sorted(scan_root.rglob("*.html"))
    if serve:
        html_files = [f for f in html_files if f.relative_to(scan_root).parts[:1] != (FRAGMENT_DIR,)]

    if not html_files:
        typer.secho(f"no .html files under {scan_root}", fg=typer.colors.YELLOW)
        return 0

    if serve:
        prefix = base_url if base_url is not None else (site_prefix or "")
        typer.secho(
            f"scanning {len(html_files)} files under {scan_root} "
            f"(http-serve at /{prefix or ''}, load-delay {load_delay_ms}ms)",
            fg=typer.colors.GREEN,
        )
        with _serve_http(scan_root, prefix) as base:
            urls = [f"{base}/{f.relative_to(scan_root).as_posix()}" for f in html_files]
            raw = _run_axe(urls, tags, load_delay_ms)
    else:
        typer.secho(f"scanning {len(html_files)} files under {scan_root}", fg=typer.colors.GREEN)
        urls = [f.resolve().as_uri() for f in html_files]
        raw = _run_axe(urls, tags, load_delay_ms)

    if report_path is not None:
        trimmed = [{k: v for k, v in page.items() if k not in REPORT_DROP_CATEGORIES} for page in raw]
        report_path.write_text(json.dumps(trimmed, indent=2))
        typer.secho(
            f"wrote axe report (violations + incomplete) to {report_path}",
            fg=typer.colors.GREEN,
        )

    violations = _collect(raw, impacts, dedupe)
    _print_report(violations, dedupe)

    return 1 if any(v.impact in BLOCKING_IMPACTS for v in violations) else 0
