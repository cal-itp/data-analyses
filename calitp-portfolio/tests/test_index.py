from pathlib import Path

from typer.testing import CliRunner

from calitp_portfolio.cli import app

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"


def _mock_storage(mocker):
    client = mocker.MagicMock()
    bucket = mocker.MagicMock()
    client.bucket.return_value = bucket
    bucket.blob.side_effect = lambda name: mocker.MagicMock(name=f"blob:{name}")
    mocker.patch("calitp_portfolio.deployer.storage.Client", return_value=client)
    return client, bucket


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


def test_index_deploy_uploads_rendered_html_to_staging(mocker, tmp_path):
    sites_yml = FIXTURES / "sites" / "sites.yml"
    output = tmp_path / "index.html"

    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=True)
    client, bucket = _mock_storage(mocker)

    result = runner.invoke(app, ["index", str(sites_yml), "--output", str(output), "--deploy"])

    assert result.exit_code == 0, result.stdout
    assert output.exists()
    client.bucket.assert_called_once_with("calitp-analysis-staging")
    bucket.blob.assert_called_once_with("index.html")


def test_index_deploy_prod_errors_when_no_prod_target(mocker, tmp_path):
    sites_yml = FIXTURES / "sites" / "sites.yml"
    output = tmp_path / "index.html"

    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=True)
    client_factory = mocker.patch("calitp_portfolio.deployer.storage.Client")

    result = runner.invoke(
        app,
        ["index", str(sites_yml), "--output", str(output), "--deploy", "--target", "prod"],
    )

    assert result.exit_code == 1
    assert "deploy.prod" in result.stdout
    client_factory.assert_not_called()


def test_index_deploy_exits_2_when_unauthorized(mocker, tmp_path):
    sites_yml = FIXTURES / "sites" / "sites.yml"
    output = tmp_path / "index.html"

    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=False)
    client_factory = mocker.patch("calitp_portfolio.deployer.storage.Client")

    result = runner.invoke(app, ["index", str(sites_yml), "--output", str(output), "--deploy"])

    assert result.exit_code == 2
    assert "Auth check failed" in result.stdout
    client_factory.assert_not_called()
