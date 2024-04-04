"""
Functions for opening yaml catalogs in shared_utils.
"""
from pathlib import Path
from typing import Literal

import intake
import yaml
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


def get_parameters(config_file: str, key: str) -> dict:
    """
    Parse the config.yml file to get the parameters needed
    for working with route or stop segments.
    These parameters will be passed through the scripts when working
    with vehicle position data.

    Returns a dictionary of parameters.
    """
    # https://aaltoscicomp.github.io/python-for-scicomp/scripts/
    with open(config_file) as f:
        my_dict = yaml.safe_load(f)
        params_dict = my_dict[key]

    return params_dict
