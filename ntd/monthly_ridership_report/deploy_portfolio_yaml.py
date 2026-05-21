"""
Creates site .yml with chapters for each unique RTPA in the report data,
then places the new .yml in the portfolio/sites directory

Yaml structure is not nested by district, it is just all RTPAs
in the navigation panel.
"""

import os
import sys
from functools import cache
from pathlib import Path

from calitp_data_analysis.gcs_pandas import GCSPandas
from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_flat
from update_vars import GCS_FILE_PATH, MONTH, YEAR

sys.path.append(os.path.abspath("../"))
# update_vars = importlib.import_module("update_vars")


@cache
def gcs_pandas():
    return GCSPandas()


PORTFOLIO_SITE_YAML = Path(__file__).parent / "ntd_monthly_ridership.yml"

if __name__ == "__main__":

    df = (
        gcs_pandas()
        .read_parquet(f"{GCS_FILE_PATH}ca_monthly_ridership_{YEAR}_{MONTH}.parquet")["rtpa_name"]
        .sort_values()
        .drop_duplicates()
        .to_frame()
    )

    site = load_site(PORTFOLIO_SITE_YAML)
    site = generate_parts_flat(site, param_key="rtpa", values=list(df.rtpa_name))
    site.write_yaml(PORTFOLIO_SITE_YAML)
