from importlib.resources import files

from typer.testing import CliRunner

from calitp_portfolio.cli import app

runner = CliRunner()

BUNDLED_LOGIN_CONFIG = str(files("calitp_portfolio.auth") / "login.json")


def test_login_runs_application_default_login_with_bundled_config(mocker):
    run = mocker.patch("subprocess.run", return_value=mocker.MagicMock(returncode=0))

    result = runner.invoke(app, ["login"])

    assert result.exit_code == 0, result.stdout
    assert run.call_count == 1
    cmd = run.call_args_list[0].args[0]
    assert cmd[:4] == ["gcloud", "auth", "application-default", "login"]
    assert f"--login-config={BUNDLED_LOGIN_CONFIG}" in cmd
