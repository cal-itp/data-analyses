import sys

# https://docs.python.org/3/library/warnings.html
if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")

# import datetime as dt
import os
from pathlib import Path

import pandas as pd

# import pyaml
import yaml

# from shared_utils import portfolio_utils
from update_vars_index import PROGRESS_PATH

# import time


def stage_portfolio():

    os.chdir("/home/jovyan/data-analyses")
    os.system("python3 portfolio/portfolio.py clean rt")
    os.system("python3 portfolio/portfolio.py build rt --no-stderr")


def deploy_portfolio():

    os.chdir("/home/jovyan/data-analyses")
    os.system("python3 portfolio/portfolio.py build rt --no-execute-papermill --deploy --target staging")
    print(
        "deployed to staging, check and use python3 portfolio/portfolio.py build rt --no-execute-papermill --deploy --target production to deploy to prod"
    )


def generate_jb2_yaml(
    portfolio_site_yaml: Path,
    df: pd.DataFrame,
    chapter_info: dict = {
        "column": "caltrans_district",
        "name": "district",
        "caption_prefix": "District ",
        "caption_suffix": "",
    },
    section_info: dict = {
        "column": "organization_name",
        "name": "organization_name",
    },
):

    chapter_col = chapter_info["column"]
    chapter_values = sorted(list(df[chapter_col].unique()))

    # Eric's example
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/04_generate_all.ipynb
    with open(portfolio_site_yaml) as f:
        site_yaml_dict = yaml.load(f, yaml.Loader)

    # Loop through each chapter (district), grab the sections (operators)
    section_col = section_info["column"]
    caption_prefix = chapter_info["caption_prefix"]
    caption_suffix = chapter_info["caption_suffix"]

    parts_list = [
        {
            **{
                "caption": f"{caption_prefix}{one_chapter_value}{caption_suffix}",
                "chapters": [
                    {"params": {section_info["name"]: str(one_section_value)}}
                    for one_section_value in df[df[chapter_col] == one_chapter_value][section_col].unique().tolist()
                ],
            }
        }
        for one_chapter_value in chapter_values
    ]

    # Make this into a list item
    site_yaml_dict["parts"] = parts_list

    # dump this dict into the yaml and overwrite existing file
    output = yaml.dump(site_yaml_dict)

    with open(portfolio_site_yaml, "w") as f:
        f.write(output)

    print(f"{portfolio_site_yaml} generated")

    return


if __name__ == "__main__":

    speedmaps_index_joined = pd.read_parquet(PROGRESS_PATH).dropna().sort_values(["caltrans_district", "analysis_name"])
    # portfolio_utils.create_portfolio_yaml_chapters_with_sections(
    #     portfolio_site_yaml="../portfolio/sites/rt.yml",
    #     df=speedmaps_index_joined,
    #     section_info={"column": "analysis_name", "name": "analysis_name"},
    # )
    generate_jb2_yaml(
        portfolio_site_yaml="../portfolio/sites/rt.yml",
        df=speedmaps_index_joined,
        section_info={"column": "analysis_name", "name": "analysis_name"},
    )
    stage_portfolio()
    deploy_portfolio()
