from pathlib import Path

from typer.testing import CliRunner

from calitp_portfolio.cli import app
from calitp_portfolio.models import load_site

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"


def _make_site_dir(tmp_path: Path, fixture_name: str) -> tuple[Path, Path]:
    """Copy a fixture yml to tmp_path and return (yml_path, site_output_dir)."""
    site_yml = tmp_path / f"{fixture_name}.yml"
    site_yml.write_text((FIXTURES / "sites" / f"{fixture_name}.yml").read_text())
    output_dir = tmp_path / fixture_name
    output_dir.mkdir()
    return site_yml, output_dir


def test_clean_removes_build_dir(tmp_path):
    site_yml, output_dir = _make_site_dir(tmp_path, "_basic_analyses_test")
    build_dir = output_dir / "_build"
    build_dir.mkdir()
    (build_dir / "html").mkdir()
    (build_dir / "html" / "index.html").write_text("<html/>")

    result = runner.invoke(app, ["clean", str(site_yml)])

    assert result.exit_code == 0, result.stdout
    assert not build_dir.exists()
    # output_dir itself stays (input yml's parent / stem)
    assert output_dir.exists()


def test_clean_is_idempotent_when_build_dir_missing(tmp_path):
    site_yml, _ = _make_site_dir(tmp_path, "_basic_analyses_test")

    result = runner.invoke(app, ["clean", str(site_yml)])

    assert result.exit_code == 0, result.stdout


def test_clean_all_also_removes_parameterized_chapter_dirs(tmp_path):
    site_yml, output_dir = _make_site_dir(tmp_path, "_param_analyses_test")
    site = load_site(site_yml, output_dir)
    chapter_paths = [c.path for p in site.parts for c in p.chapters]
    assert chapter_paths, "fixture should produce slugged chapter dirs"

    (output_dir / "_build").mkdir()
    for cp in chapter_paths:
        cp.mkdir(parents=True)
        (cp / "00__notebook.ipynb").write_text("{}")
    (output_dir / "README.md").write_text("readme")  # not a chapter dir

    result = runner.invoke(app, ["clean", str(site_yml), "--all"])

    assert result.exit_code == 0, result.stdout
    assert not (output_dir / "_build").exists()
    for cp in chapter_paths:
        assert not cp.exists()
    assert (output_dir / "README.md").exists()


def test_clean_without_all_leaves_chapter_dirs(tmp_path):
    site_yml, output_dir = _make_site_dir(tmp_path, "_param_analyses_test")
    site = load_site(site_yml, output_dir)
    chapter_paths = [c.path for p in site.parts for c in p.chapters]

    (output_dir / "_build").mkdir()
    for cp in chapter_paths:
        cp.mkdir(parents=True)

    result = runner.invoke(app, ["clean", str(site_yml)])

    assert result.exit_code == 0, result.stdout
    assert not (output_dir / "_build").exists()
    for cp in chapter_paths:
        assert cp.exists()
