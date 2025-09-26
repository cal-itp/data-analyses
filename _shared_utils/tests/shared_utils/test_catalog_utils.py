from pathlib import Path
from typing import Callable

import pytest
import shared_utils.catalog_utils as catalog_utils


class TestCatalogUtils:
    @pytest.fixture()
    def setup(self, mocker, tmp_path: Path) -> Callable:
        def setup_with(nested_directory: str = "", in_home_directory: bool = True) -> list[Path]:
            home_path = tmp_path.joinpath("home")
            home_path.mkdir()

            if in_home_directory:
                repo_path = home_path.joinpath(f"{nested_directory}data-analyses")
            else:
                repo_path = tmp_path.joinpath(f"{nested_directory}data-analyses")

            repo_path.mkdir(parents=True)

            current_path = repo_path.joinpath("test-project")
            current_path.mkdir(parents=True)
            shared_utils_path = repo_path.joinpath("_shared_utils/shared_utils")
            shared_utils_path.mkdir(parents=True)

            mocker.patch("shared_utils.catalog_utils.Path.home", return_value=home_path)
            mocker.patch("shared_utils.catalog_utils.Path.cwd", return_value=current_path)

            return [current_path, shared_utils_path]

        return setup_with

    def test_get_catalog_file_nonexistent(
        self,
        setup: Callable,
    ) -> None:
        setup()

        with pytest.raises(FileNotFoundError, match="No such catalog file found"):
            catalog_utils.get_catalog_file("test-file")

    def test_get_catalog_file_repo_in_home_directory(
        self,
        setup: Callable,
    ) -> None:
        _, shared_utils_path = setup()
        shared_utils_path.joinpath("test-file.yml").touch()

        filename = catalog_utils.get_catalog_file("test-file")
        assert filename == shared_utils_path.joinpath("test-file.yml")

    def test_get_catalog_file_repo_nested_in_home_directory(
        self,
        setup: Callable,
    ) -> None:
        _, shared_utils_path = setup("caltrans/")
        shared_utils_path.joinpath("test-file.yml").touch()

        filename = catalog_utils.get_catalog_file("test-file")
        assert filename == shared_utils_path.joinpath("test-file.yml")

    def test_get_catalog_file_repo_outside_of_home_directory(
        self,
        setup: Callable,
    ) -> None:
        setup(in_home_directory=False)

        with pytest.raises(RuntimeError, match="The data-analyses repo should be located in your home directory."):
            catalog_utils.get_catalog_file("test-file")
