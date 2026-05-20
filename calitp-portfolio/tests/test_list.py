from pathlib import Path

from typer.testing import CliRunner

from calitp_portfolio.cli import app

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"


def test_list_param_site_prints_slugs_in_source_order():
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"
    result = runner.invoke(app, ["list", str(yml)])

    assert result.exit_code == 0, result.stdout
    hi = "00__notebook_with_params_2__greetings_hi-so-happy-to-see-you-here"
    bye = "00__notebook_with_params_2__greetings_bye-see-you-soon"
    assert hi in result.stdout
    assert bye in result.stdout
    assert result.stdout.index(hi) < result.stdout.index(bye)


def test_list_shows_params_and_resolved_notebook():
    yml = FIXTURES / "sites" / "_param_analyses_test.yml"
    result = runner.invoke(app, ["list", str(yml)])

    assert result.exit_code == 0, result.stdout
    assert "notebook_with_params_2.ipynb" in result.stdout
    assert "greetings" in result.stdout
    assert "Hi! So happy to see you here!!" in result.stdout


def test_list_shows_part_caption_when_present():
    yml = FIXTURES / "sites" / "_group_and_params_analyses_test.yml"
    result = runner.invoke(app, ["list", str(yml)])

    assert result.exit_code == 0, result.stdout
    assert "District 01 Eureka" in result.stdout
    assert "District 02 Redding" in result.stdout
