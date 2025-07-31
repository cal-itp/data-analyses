import pytest
from pathlib import Path

from shared_utils.catalog_utils import get_catalog_file

#
# @pytest.fixture
# def cwd_setup(mocker):
#     mocker.patch("Path.cwd", return_value=Path(''))

def test_get_catalog_when_repo_in_home_directory(tempdir, mocker):
    catalog_name = "test-file"
    path = tempdir.mkdir("data-analyses").mkdir("test-project")
    path.join(f"{catalog_name}.yml")
    mocker.patch("Path.home", return_value=path.parent)
    mocker.patch("Path.cwd", return_value=path)

    file = get_catalog_file(catalog_name)
    assert file == path.join("test-file")

def test_get_catalog_when_repo_in_home_directory_nested():
    pass

def test_get_catalog_when_file_doesnt_exist():
    pass