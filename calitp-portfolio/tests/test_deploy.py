from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from calitp_portfolio.cli import app
from calitp_portfolio.deployer import parse_gs_url

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"


def _mock_storage(mocker, existing_blobs=()):
    """Patch google.cloud.storage.Client where deployer references it.

    `existing_blobs` seeds bucket.list_blobs(...) so tests can simulate prior deploys.
    Per-name blob mocks are cached on `bucket._blobs` so tests can assert per-blob
    upload/delete calls (e.g. `bucket._blobs["a/b.css"].delete.called`).
    """
    client = mocker.MagicMock()
    bucket = mocker.MagicMock()
    client.bucket.return_value = bucket

    blobs: dict = {}

    def _blob(name):
        if name not in blobs:
            blobs[name] = mocker.MagicMock(name=f"blob:{name}")
        return blobs[name]

    bucket.blob.side_effect = _blob
    bucket._blobs = blobs
    bucket.list_blobs.return_value = [SimpleNamespace(name=n) for n in existing_blobs]

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


def _seed_site(tmp_path: Path) -> tuple[Path, Path]:
    site_yml = tmp_path / "_basic_analyses_test.yml"
    site_yml.write_text((FIXTURES / "sites" / "_basic_analyses_test.yml").read_text())
    html_dir = tmp_path / "_basic_analyses_test" / "_build" / "html"
    html_dir.mkdir(parents=True)
    return site_yml, html_dir


def test_deploy_skips_upload_for_unchanged_hashed_assets(mocker, tmp_path):
    """myst content-hashes asset filenames, so a name match in the bucket means a content
    match. Re-uploading is wasted bandwidth; skip it."""
    site_yml, html_dir = _seed_site(tmp_path)
    (html_dir / "build").mkdir()
    (html_dir / "build" / "root-HKSGTRTI.css").write_text("h1{}")
    (html_dir / "index.html").write_text("<html></html>")

    existing = [
        "_basic_analyses_test/build/root-HKSGTRTI.css",
        "_basic_analyses_test/index.html",
    ]
    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=True)
    _, bucket = _mock_storage(mocker, existing_blobs=existing)

    result = runner.invoke(app, ["deploy", str(site_yml)])

    assert result.exit_code == 0, result.stdout
    touched_names = [call.args[0] for call in bucket.blob.call_args_list]
    assert "_basic_analyses_test/build/root-HKSGTRTI.css" not in touched_names
    assert "_basic_analyses_test/index.html" in touched_names  # non-hashed re-uploaded
    assert "skipped 1 unchanged hashed assets" in result.stdout


def test_deploy_reuploads_non_hashed_files_even_on_name_match(mocker, tmp_path):
    """HTML / non-hashed filenames are stable across builds but their contents change.
    Name match alone isn't enough to skip; only hashed assets get the skip."""
    site_yml, html_dir = _seed_site(tmp_path)
    (html_dir / "index.html").write_text("<html>new</html>")

    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=True)
    _, bucket = _mock_storage(mocker, existing_blobs=["_basic_analyses_test/index.html"])

    result = runner.invoke(app, ["deploy", str(site_yml)])

    assert result.exit_code == 0, result.stdout
    assert bucket._blobs["_basic_analyses_test/index.html"].upload_from_filename.called
    assert "skipped" not in result.stdout


def test_deploy_deletes_stale_blobs_not_in_local_set(mocker, tmp_path):
    """The bucket accumulates old hashed assets across deploys; cleanup deletes anything
    at the prefix that the current build doesn't produce."""
    site_yml, html_dir = _seed_site(tmp_path)
    (html_dir / "build").mkdir()
    (html_dir / "build" / "root-NEWHASH1.css").write_text("body{}")
    (html_dir / "index.html").write_text("<html></html>")

    existing = [
        "_basic_analyses_test/build/root-OLDHASH9.css",  # stale
        "_basic_analyses_test/build/root-NEWHASH1.css",  # current
        "_basic_analyses_test/index.html",
    ]
    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=True)
    _, bucket = _mock_storage(mocker, existing_blobs=existing)

    result = runner.invoke(app, ["deploy", str(site_yml)])

    assert result.exit_code == 0, result.stdout
    deleted = [name for name, blob in bucket._blobs.items() if blob.delete.called]
    assert deleted == ["_basic_analyses_test/build/root-OLDHASH9.css"]
    assert "deleting 1 stale files" in result.stdout


def test_deploy_lists_blobs_with_trailing_slash_to_scope_prefix(mocker, tmp_path):
    """list_blobs(prefix='ahsc') would also match 'ahsc-2/...' — must use 'ahsc/' to
    avoid deleting siblings of the deploy prefix."""
    site_yml, html_dir = _seed_site(tmp_path)
    (html_dir / "index.html").write_text("<html></html>")

    mocker.patch("calitp_portfolio.cli.auth.is_valid", return_value=True)
    _, bucket = _mock_storage(mocker)

    result = runner.invoke(app, ["deploy", str(site_yml)])

    assert result.exit_code == 0, result.stdout
    bucket.list_blobs.assert_called_once_with(prefix="_basic_analyses_test/")
