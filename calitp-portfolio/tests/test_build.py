import json
from pathlib import Path

from typer.testing import CliRunner

from calitp_portfolio.cli import app
from calitp_portfolio.models import GOOGLE_ANALYTICS_TAG_ID

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"


def _stub_jupyter_book_build(mocker):
    """Patch the build's subprocess tee runner so we don't actually shell out to `jupyter book build`."""

    def fake_run(cmd, cwd, log_file, terminal, env=None):
        html_dir = Path(cwd) / "_build" / "html"
        html_dir.mkdir(parents=True, exist_ok=True)
        (html_dir / "index.html").write_text("<html><title>built</title></html>")
        log_file.write("[stub] jupyter book build --html --ci\n")

    return mocker.patch("calitp_portfolio.builder._run_subprocess_tee", side_effect=fake_run)


def test_build_aborts_when_adc_unauthorized(tmp_path, mocker):
    """Pre-flight ADC probe fails -> build exits 2 with a clear pointer at `calitp-portfolio login`."""
    yml = FIXTURES / "sites" / "_basic_analyses_test.yml"
    output_dir = tmp_path / "build"
    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=False)
    build_site = mocker.patch("calitp_portfolio.cli.build_site")

    # No --no-execute / --prepare-only / --skip-auth-check, so pre-flight is active.
    result = runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir)])

    assert result.exit_code == 2
    assert "Auth check failed" in result.stdout
    assert "calitp-portfolio login" in result.stdout
    build_site.assert_not_called()


def test_build_produces_artifacts(tmp_path, mocker):
    yml = FIXTURES / "sites" / "_basic_analyses_test.yml"
    output_dir = tmp_path / "build"
    fake_run = _stub_jupyter_book_build(mocker)

    result = runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir), "--no-execute"])

    assert result.exit_code == 0, result.stdout
    assert (output_dir / "myst.yml").exists()
    assert (output_dir / "README.md").exists()
    assert (output_dir / "build.json").exists()
    assert (output_dir / "build.log").exists()
    assert (output_dir / "_build" / "html" / "index.html").exists()

    # subprocess was called with the right command and cwd
    call = fake_run.call_args
    assert call.args[0] == ["jupyter", "book", "build", "--html", "--ci"]
    assert call.kwargs["cwd"] == output_dir


def test_build_myst_yml_includes_toc_and_title(tmp_path, mocker):
    yml = FIXTURES / "sites" / "_basic_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)

    runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir), "--no-execute"])

    myst = (output_dir / "myst.yml").read_text()
    assert "title: Basic Analyses Test" in myst
    assert "file: README.md" in myst  # toc entry for the readme


def test_build_myst_yml_includes_google_analytics_tag(tmp_path, mocker):
    """Rendered myst.yml carries the GA tag so deployed sites keep tracking."""
    yml = FIXTURES / "sites" / "_basic_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)

    runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir), "--no-execute"])

    assert f"analytics_google: '{GOOGLE_ANALYTICS_TAG_ID}'" in (output_dir / "myst.yml").read_text()


def test_build_hide_title_block_renders_in_myst_yml(tmp_path, mocker):
    """Without --hide-title-block, jupyter-book renders two redundant titles per page. see: _param_manual_title fixture"""
    yml = FIXTURES / "sites" / "_param_manual_title_analyses_test.yml"
    _stub_jupyter_book_build(mocker)

    on_dir = tmp_path / "on"
    runner.invoke(app, ["build", str(yml), "--output-dir", str(on_dir), "--no-execute", "--hide-title-block"])
    assert "hide_title_block: True" in (on_dir / "myst.yml").read_text()

    off_dir = tmp_path / "off"
    runner.invoke(app, ["build", str(yml), "--output-dir", str(off_dir), "--no-execute"])
    assert "hide_title_block: False" in (off_dir / "myst.yml").read_text()


def test_build_manifest_records_basic_metadata(tmp_path, mocker):
    yml = FIXTURES / "sites" / "_basic_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)

    result = runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir), "--no-execute"])

    assert result.exit_code == 0, result.stdout
    manifest = json.loads((output_dir / "build.json").read_text())
    assert manifest["site_name"] == "_basic_analyses_test"
    assert manifest["site_title"] == "Basic Analyses Test"
    assert len(manifest["yml_sha256"]) == 64
    assert manifest["tool_version"]
    assert manifest["timestamp_utc"].endswith("Z")
    assert manifest["deploy"]["staging"].startswith("gs://")
    assert manifest["errors_count"] == 0


def test_build_bundles_template_assets(tmp_path, mocker):
    """Build copies templates into <output_dir>/_templates/ so paths resolve regardless of where output_dir lives."""
    yml = FIXTURES / "sites" / "_basic_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)

    result = runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir), "--no-execute"])

    assert result.exit_code == 0, result.stdout
    bundle = output_dir / "_templates"
    assert (bundle / "custom.css").exists()
    assert (bundle / "partials" / "footer.md").exists()
    assert (bundle / "assets" / "favicon.ico").exists()
    # logos under assets/
    logos = list((bundle / "assets").glob("*Logo*"))
    assert len(logos) >= 2


def test_build_writes_section_readme_for_section_chapters(tmp_path, mocker):
    """Section chapters trigger `Chapter.generate`'s sections branch, which writes
    `<output_dir>/<chapter_slug>.md` with `# <caption>` content before papermill runs."""
    yml = FIXTURES / "sites" / "_section_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)

    result = runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir), "--no-execute"])

    assert result.exit_code == 0, result.stdout
    daily = output_dir / "day_or_night_01-day.md"
    night = output_dir / "day_or_night_02-night.md"
    assert daily.read_text() == "# Daily Greetings"
    assert night.read_text() == "# Night Greetings"


def test_build_copies_readme_with_its_actual_filename(tmp_path, mocker):
    """The readme is copied not always as `README.md`."""
    yml = FIXTURES / "sites" / "_param_manual_title_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)

    result = runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir), "--no-execute"])

    assert result.exit_code == 0, result.stdout
    assert (output_dir / "README_TP.md").exists()
    assert not (output_dir / "README.md").exists()


def test_build_works_for_readme_only_site(tmp_path, mocker):
    """A site with no parts (README-only) should still build"""
    yml = FIXTURES / "sites" / "_readme_only_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)

    result = runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir), "--no-execute"])

    assert result.exit_code == 0, result.stdout
    assert (output_dir / "myst.yml").exists()
    assert (output_dir / "README.md").exists()
    assert (output_dir / "build.json").exists()


def test_build_passes_base_url_env_derived_from_deploy_staging(tmp_path, mocker):
    """BASE_URL env var on the build subprocess derives from `site.deploy.staging`'s path.

    Without it, the deployed staging site shows myst's "Site not loading correctly"
    warning because the build emitted absolute asset paths but the site is served
    under a `/<prefix>/` path."""
    yml = FIXTURES / "sites" / "_readme_only_analyses_test.yml"
    output_dir = tmp_path / "build"
    fake_run = _stub_jupyter_book_build(mocker)

    runner.invoke(app, ["build", str(yml), "--output-dir", str(output_dir), "--no-execute"])

    env = fake_run.call_args.kwargs["env"]
    assert env["BASE_URL"] == "/_readme_only_analyses_test"


def test_build_cli_output_dir_overrides_yml_output_dir(tmp_path, mocker):
    """CLI `--output-dir` trumps yml's `output_dir:` field."""
    yml = tmp_path / "_readme_only_analyses_test.yml"
    base = (FIXTURES / "sites" / "_readme_only_analyses_test.yml").read_text()
    yml.write_text(base + "output_dir: from-yml\n")
    _stub_jupyter_book_build(mocker)

    result = runner.invoke(app, ["build", str(yml), "--no-execute", "--output-dir", str(tmp_path / "from-cli")])

    assert result.exit_code == 0, result.stdout
    assert (tmp_path / "from-cli" / "myst.yml").exists()
    assert not (tmp_path / "from-yml" / "myst.yml").exists()


def test_build_only_filters_to_matching_chapter(tmp_path, mocker):
    """`build --only <slug>` skips chapters whose identifier doesn't match."""
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)
    generate = mocker.patch("calitp_portfolio.models.Chapter.generate", return_value=[])

    only = "00__notebook_with_params_2__greetings_hi-so-happy-to-see-you-here"
    result = runner.invoke(
        app,
        ["build", str(yml), "--output-dir", str(output_dir), "--no-execute", "--only", only],
    )

    assert result.exit_code == 0, result.stdout
    assert generate.call_count == 1


def test_build_limit_caps_chapter_count(tmp_path, mocker):
    """`--limit 1` builds only the first chapter in source order."""
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"  # 2 chapters
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)
    generate = mocker.patch("calitp_portfolio.models.Chapter.generate", return_value=[])

    result = runner.invoke(
        app,
        ["build", str(yml), "--output-dir", str(output_dir), "--no-execute", "--limit", "1"],
    )

    assert result.exit_code == 0, result.stdout
    assert generate.call_count == 1


def test_build_limit_spans_parts(tmp_path, mocker):
    """`--limit N` counts across parts, not within each part."""
    yml = FIXTURES / "sites" / "_group_and_params_analyses_test.yml"  # 4 chapters in part 1, 2 in part 2
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)
    generate = mocker.patch("calitp_portfolio.models.Chapter.generate", return_value=[])

    result = runner.invoke(
        app,
        ["build", str(yml), "--output-dir", str(output_dir), "--no-execute", "--limit", "5"],
    )

    assert result.exit_code == 0, result.stdout
    assert generate.call_count == 5


def test_build_readme_only_skips_chapters(tmp_path, mocker):
    """`--readme-only` copies the readme, renders a minimal TOC, never calls Chapter.generate."""
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)
    generate = mocker.patch("calitp_portfolio.models.Chapter.generate", return_value=[])

    result = runner.invoke(
        app,
        ["build", str(yml), "--output-dir", str(output_dir), "--readme-only"],
    )

    assert result.exit_code == 0, result.stdout
    generate.assert_not_called()
    # Readme still gets copied.
    assert (output_dir / "README_PARAMS.md").exists()
    # myst.yml exists but TOC has no chapter entries.
    myst = (output_dir / "myst.yml").read_text()
    assert "file: README_PARAMS.md" in myst
    assert "greetings_hi" not in myst


def test_build_toc_only_skips_papermill_and_readme_copy(tmp_path, mocker):
    """`--toc-only` re-renders myst.yml and runs jupyter-book; no readme copy, no papermill."""
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)
    generate = mocker.patch("calitp_portfolio.models.Chapter.generate", return_value=[])

    result = runner.invoke(
        app,
        ["build", str(yml), "--output-dir", str(output_dir), "--toc-only"],
    )

    assert result.exit_code == 0, result.stdout
    generate.assert_not_called()
    assert not (output_dir / "README_PARAMS.md").exists()
    # myst.yml still has the full TOC including chapters.
    myst = (output_dir / "myst.yml").read_text()
    assert "greetings_hi" in myst


def test_build_readme_only_and_toc_only_are_mutually_exclusive(tmp_path):
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"
    result = runner.invoke(
        app,
        ["build", str(yml), "--output-dir", str(tmp_path / "b"), "--readme-only", "--toc-only"],
    )
    assert result.exit_code != 0
    assert "mutually exclusive" in result.stdout.lower()


def test_build_readme_only_rejects_only_flag(tmp_path):
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"
    result = runner.invoke(
        app,
        ["build", str(yml), "--output-dir", str(tmp_path / "b"), "--readme-only", "--only", "x"],
    )
    assert result.exit_code != 0
    assert "--readme-only" in result.stdout


def test_build_toc_only_rejects_limit_flag(tmp_path):
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"
    result = runner.invoke(
        app,
        ["build", str(yml), "--output-dir", str(tmp_path / "b"), "--toc-only", "--limit", "1"],
    )
    assert result.exit_code != 0
    assert "--toc-only" in result.stdout


def test_build_only_errors_on_unknown_slug(tmp_path, mocker):
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)

    result = runner.invoke(
        app,
        ["build", str(yml), "--output-dir", str(output_dir), "--no-execute", "--only", "nonsense-slug"],
    )

    assert result.exit_code != 0
    assert "nonsense-slug" in result.stdout


def test_build_exits_nonzero_when_papermill_errors(tmp_path, mocker):
    """continue_on_error collects errors; build should exit nonzero if any happened."""
    yml = FIXTURES / "sites" / "_basic_analyses_test.yml"
    output_dir = tmp_path / "build"
    _stub_jupyter_book_build(mocker)

    # simulate a papermill error captured by continue_on_error
    from papermill import PapermillExecutionError

    fake_error = PapermillExecutionError(
        cell_index=0, exec_count=1, source="boom", ename="X", evalue="y", traceback=["t"]
    )
    mocker.patch(
        "calitp_portfolio.models.Chapter.generate",
        return_value=[fake_error],
    )

    result = runner.invoke(
        app,
        [
            "build",
            str(yml),
            "--output-dir",
            str(output_dir),
            "--no-execute",
            "--continue-on-error",
        ],
    )
    assert result.exit_code == 1
    manifest = json.loads((output_dir / "build.json").read_text())
    # _basic has 2 chapters; each call to Chapter.generate is mocked to return [fake_error]
    assert manifest["errors_count"] == 2
