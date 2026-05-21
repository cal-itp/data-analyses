import sys

# https://docs.python.org/3/library/warnings.html
if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")

import os
from pathlib import Path

import pandas as pd
from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_grouped
from update_vars_index import PROGRESS_PATH

SITE_YML = Path(__file__).parent / "rt.yml"


def stage_portfolio():

    os.chdir("/home/jovyan/data-analyses")
    os.system("python3 portfolio/portfolio.py clean rt")
    os.system("python3 portfolio/portfolio.py build rt --no-stderr")


def deploy_portfolio():

    os.chdir("/home/jovyan/data-analyses")
    os.system(
        "python3 portfolio/portfolio.py build rt --no-execute-papermill --hide-title-block --deploy --target staging"
    )
    print(
        "deployed to staging, check and use python3 portfolio/portfolio.py build rt --no-execute-papermill --hide-title-block --deploy --target production to deploy to prod"
    )


if __name__ == "__main__":

    speedmaps_index_joined = pd.read_parquet(PROGRESS_PATH).dropna().sort_values(["caltrans_district", "analysis_name"])
    groups = {
        f"District {district}": speedmaps_index_joined[speedmaps_index_joined["caltrans_district"] == district][
            "analysis_name"
        ]
        .unique()
        .tolist()
        for district in sorted(speedmaps_index_joined["caltrans_district"].unique())
    }
    site = load_site(SITE_YML)
    site = generate_parts_grouped(site, param_key="analysis_name", groups=groups)
    site.write_yaml(SITE_YML)
    stage_portfolio()
    deploy_portfolio()
