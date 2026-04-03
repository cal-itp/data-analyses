"""
Create the GTFS Digest yaml that
sets the parameterization for the analysis site.
"""

from functools import cache

import pandas as pd
from calitp_data_analysis.gcs_pandas import GCSPandas
from shared_utils import portfolio_utils
from update_vars import GTFS_DATA_DICT, file_name


@cache
def gcs_pandas():
    return GCSPandas()


SITE_YML = "../portfolio/sites/gtfs_digest.yml"


def generate_operator_grain_yaml() -> pd.DataFrame:
    """
    Generate the yaml for our Operator grain portfolio.
    """
    FILEPATH_URL = f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.crosswalk}_{file_name}.parquet"

    # Keep only organizations with RT and schedule OR only schedule.
    df = (
        gcs_pandas()
        .read_parquet(FILEPATH_URL, columns=["caltrans_district", "analysis_name"])
        .dropna(subset=["caltrans_district"])
        .sort_values(["caltrans_district", "analysis_name"])
        .reset_index(drop=True)
        .drop_duplicates()
    )

    return df


if __name__ == "__main__":

    final = generate_operator_grain_yaml()

    portfolio_utils.create_portfolio_yaml_chapters_with_groups(
        SITE_YML,
        final,
        param_info={"column": "analysis_name", "name": "analysis_name"},
    )
