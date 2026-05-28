import sys

# https://docs.python.org/3/library/warnings.html
if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")

import os

import pandas as pd
from shared_utils import portfolio_utils
from update_vars_index import PROGRESS_PATH


def data_analyses_root_dir():

    if "jovyan" in os.getcwd():
        os.chdir("/home/jovyan/data-analyses")
    else:
        os.chdir("..")


def stage_portfolio():

    data_analyses_root_dir()
    os.system("python3 portfolio/portfolio.py clean rt")
    os.system("python3 portfolio/portfolio.py build rt --no-stderr")


def deploy_portfolio():

    #    data_analyses_root_dir()
    os.system(
        "python3 portfolio/portfolio.py build rt --no-execute-papermill --hide-title-block --deploy --target staging"
    )
    print(
        "deployed to staging, check and use python3 portfolio/portfolio.py build rt --no-execute-papermill --hide-title-block --deploy --target production to deploy to prod"
    )


if __name__ == "__main__":

    speedmaps_index_joined = pd.read_parquet(PROGRESS_PATH).dropna().sort_values(["caltrans_district", "analysis_name"])
    portfolio_utils.create_portfolio_yaml_chapters_with_groups(
        portfolio_site_yaml="../portfolio/sites/rt.yml",
        df=speedmaps_index_joined,
        param_info={"column": "analysis_name", "name": "analysis_name"},
    )
    stage_portfolio()
    deploy_portfolio()
