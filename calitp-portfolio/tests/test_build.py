import json
from pathlib import Path

from typer.testing import CliRunner

from calitp_portfolio.cli import app

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"


def _stub_jupyter_book_build(mocker):
    """Patch the build's subprocess tee runner so we don't actually shell out to `jupyter book build`."""

    def fake_run(cmd, cwd, log_file, terminal):
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
