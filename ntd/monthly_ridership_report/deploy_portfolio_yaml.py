"""
Creates site .yml with chapters for each unique RTPA in the report data,
then places the new .yml in the portfolio/sites directory

Yaml structure is not nested by district, it is just all RTPAs
in the navigation panel.
"""

import importlib
import os
import sys
from functools import cache
from pathlib import Path

from calitp_data_analysis.gcs_pandas import GCSPandas
from shared_utils import portfolio_utils

# from update_vars import GCS_FILE_PATH, MONTH, YEAR

sys.path.append(os.path.abspath("../"))
update_vars = importlib.import_module("update_vars")


@cache
def gcs_pandas():
    return GCSPandas()


PORTFOLIO_SITE_YAML = Path("../../portfolio/sites/ntd_monthly_ridership.yml")

if __name__ == "__main__":

    df = (
        gcs_pandas()
        .read_parquet(
            f"{update_vars.GCS_FILE_PATH}ca_monthly_ridership_{update_vars.YEAR}_{update_vars.MONTH}.parquet"
        )["rtpa_name"]
        .sort_values()
        .drop_duplicates()
        .to_frame()
    )

    portfolio_utils.create_portfolio_yaml_chapters_no_sections(
        PORTFOLIO_SITE_YAML, chapter_name="rtpa", chapter_values=list(df.rtpa_name)
    )
