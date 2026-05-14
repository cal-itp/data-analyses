from pathlib import Path

from typer.testing import CliRunner

from calitp_portfolio.cli import app

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"


def test_index_renders_index(tmp_path):
    sites_yml = FIXTURES / "sites" / "sites.yml"
    output = tmp_path / "index.html"

    result = runner.invoke(app, ["index", str(sites_yml), "--output", str(output)])

    assert result.exit_code == 0, result.stdout
    html = output.read_text()
    assert '<a href="/gtfs_digest/">GTFS Digest</a>' in html
    assert '<a href="/rt/">RT Speeds</a>' in html
    assert '<a href="/ahsc/">AHSC</a>' in html
    assert "Test Projects" in html
    assert '<a href="/_basic_analyses_test/">Basic Analyses Test</a>' in html
    assert '<a href="/_section_analyses_test/">Section Analyses Test</a>' in html
    assert html.index("GTFS Digest") < html.index("RT Speeds") < html.index("AHSC")


def test_index_prod_target_excludes_test_sites(tmp_path):
    sites_yml = FIXTURES / "sites" / "sites.yml"
    output = tmp_path / "index.html"

    result = runner.invoke(app, ["index", str(sites_yml), "--output", str(output), "--target", "prod"])

    assert result.exit_code == 0, result.stdout
    html = output.read_text()
    assert "GTFS Digest" in html
    assert "Test Projects" not in html
    assert "_basic_analyses_test" not in html
