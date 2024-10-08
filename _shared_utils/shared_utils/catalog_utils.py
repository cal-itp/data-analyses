"""
Functions for opening yaml catalogs in shared_utils.
"""
from pathlib import Path
from typing import Literal

import intake
from omegaconf import OmegaConf  # this is yaml parser

repo_name = "data-analyses/"
shared_utils_dir = "_shared_utils/shared_utils/"


def get_catalog(catalog_name: Literal["shared_data_catalog", "gtfs_analytics_data"]) -> Path:
    """ """
    catalog_file_path = Path(f"{repo_name}{shared_utils_dir}{catalog_name}.yml")
    catalog_path = Path.cwd().home().joinpath(catalog_file_path)

    if catalog_name == "gtfs_analytics_data":
        return OmegaConf.load(catalog_path)

    else:
        return intake.open_catalog(catalog_path)
