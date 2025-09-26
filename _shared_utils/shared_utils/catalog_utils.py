"""
Functions for opening yaml catalogs in shared_utils.
"""
from pathlib import Path
from typing import Literal

import intake
from omegaconf import OmegaConf  # this is yaml parser

shared_utils_directory = "data-analyses/_shared_utils/shared_utils/"


def get_catalog_file(catalog_name):
    filename = f"{shared_utils_directory}{catalog_name}.yml"
    parent_directory = Path.cwd()

    if Path.home() not in Path.cwd().parents:
        raise RuntimeError("The data-analyses repo should be located in your home directory.")

    while True:
        test_path = parent_directory.joinpath(filename)

        if test_path.is_file():
            return test_path

        if parent_directory == Path.home():
            raise FileNotFoundError(f"No such catalog file found: {filename}")

        parent_directory = parent_directory.parent


def get_catalog(catalog_name: Literal["shared_data_catalog", "gtfs_analytics_data"]) -> Path:
    """
    Grab either the shared_data_catalog (uses intake driver) or
    gtfs_analytics_data catalog (uses OmegaConf yaml parser).

    """
    catalog_path = get_catalog_file(catalog_name)

    if catalog_name == "gtfs_analytics_data":
        return OmegaConf.load(catalog_path)

    else:
        return intake.open_catalog(catalog_path)
