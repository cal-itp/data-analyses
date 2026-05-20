from pathlib import Path

import pytest
from typer.testing import CliRunner

from calitp_portfolio.cli import app
from calitp_portfolio.deployer import parse_gs_url

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"


def _mock_storage(mocker):
    """Patch google.cloud.storage.Client where deployer references it. Returns the bucket mock."""
    client = mocker.MagicMock()
    bucket = mocker.MagicMock()
    client.bucket.return_value = bucket
    bucket.blob.side_effect = lambda name: mocker.MagicMock(name=f"blob:{name}")
    mocker.patch("calitp_portfolio.deployer.storage.Client", return_value=client)
    return client, bucket


def test_parse_gs_url_splits_bucket_and_prefix():
    assert parse_gs_url("gs://my-bucket/some/prefix") == ("my-bucket", "some/prefix")
    assert parse_gs_url("gs://my-bucket/some/prefix/") == ("my-bucket", "some/prefix")
    assert parse_gs_url("gs://my-bucket") == ("my-bucket", "")


def test_parse_gs_url_rejects_non_gs_url():
    with pytest.raises(ValueError):
        parse_gs_url("https://example.com/foo")


def test_deploy_uploads_built_html_to_staging(mocker, tmp_path):
    # Synthesize a yml in tmp_path whose output_dir resolves to tmp_path/<stem>/_build/html
    site_yml = tmp_path / "_basic_analyses_test.yml"
    site_yml.write_text((FIXTURES / "sites" / "_basic_analyses_test.yml").read_text())

    html_dir = tmp_path / "_basic_analyses_test" / "_build" / "html"
    html_dir.mkdir(parents=True)
    (html_dir / "index.html").write_text("<html>hi</html>")
    (html_dir / "sub").mkdir()
    (html_dir / "sub" / "page.html").write_text("<html>page</html>")

    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=True)
    client, bucket = _mock_storage(mocker)

    result = runner.invoke(app, ["deploy", str(site_yml)])

    assert result.exit_code == 0, result.stdout
    client.bucket.assert_called_once_with("calitp-analysis-staging")
    uploaded = sorted(call.args[0] for call in bucket.blob.call_args_list)
    assert uploaded == [
        "_basic_analyses_test/index.html",
        "_basic_analyses_test/sub/page.html",
    ]


def test_deploy_announces_progress_preamble(mocker, tmp_path):
    """Deploy prints a preamble with file count + source + target before uploading,
    so the user knows the upload is running (was dead-air during the 1264-file ahsc deploy)."""
    site_yml = tmp_path / "_basic_analyses_test.yml"
    site_yml.write_text((FIXTURES / "sites" / "_basic_analyses_test.yml").read_text())
    html_dir = tmp_path / "_basic_analyses_test" / "_build" / "html"
    html_dir.mkdir(parents=True)
    (html_dir / "index.html").write_text("<html>hi</html>")
    (html_dir / "page.html").write_text("<html>page</html>")

    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=True)
    _mock_storage(mocker)

    result = runner.invoke(app, ["deploy", str(site_yml)])

    assert result.exit_code == 0, result.stdout
    assert "uploading 2 files" in result.stdout
    assert str(html_dir) in result.stdout
    assert "gs://calitp-analysis-staging/_basic_analyses_test" in result.stdout


def test_deploy_exits_2_when_not_authorized(mocker, tmp_path):
    site_yml = tmp_path / "_basic_analyses_test.yml"
    site_yml.write_text((FIXTURES / "sites" / "_basic_analyses_test.yml").read_text())
    html_dir = tmp_path / "_basic_analyses_test" / "_build" / "html"
    html_dir.mkdir(parents=True)

    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=False)
    client_factory = mocker.patch("calitp_portfolio.deployer.storage.Client")

    result = runner.invoke(app, ["deploy", str(site_yml)])

    assert result.exit_code == 2
    assert "Auth check failed" in result.stdout
    assert "calitp-portfolio login" in result.stdout
    client_factory.assert_not_called()


def test_deploy_prod_errors_when_no_prod_target(mocker, tmp_path):
    site_yml = tmp_path / "_basic_analyses_test.yml"
    site_yml.write_text((FIXTURES / "sites" / "_basic_analyses_test.yml").read_text())
    html_dir = tmp_path / "_basic_analyses_test" / "_build" / "html"
    html_dir.mkdir(parents=True)

    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=True)
    client_factory = mocker.patch("calitp_portfolio.deployer.storage.Client")

    result = runner.invoke(app, ["deploy", str(site_yml), "--target", "prod"])

    assert result.exit_code == 1
    assert "deploy.prod" in result.stdout
    client_factory.assert_not_called()


def test_deploy_generic_mode_requires_both_html_and_target_url(mocker, tmp_path):
    html_dir = tmp_path / "html"
    html_dir.mkdir()

    result = runner.invoke(app, ["deploy", "--html", str(html_dir)])
    assert result.exit_code != 0
