"""
Create the GTFS Digest yaml that
sets the parameterization for the analysis site.
"""

from functools import cache
from pathlib import Path

import pandas as pd
from calitp_data_analysis.gcs_pandas import GCSPandas
from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_grouped
from update_vars import GTFS_DATA_DICT, file_name


@cache
def gcs_pandas():
    return GCSPandas()


SITE_YML = Path(__file__).parent / "gtfs_digest.yml"


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

    groups = {
        f"District {district}": final[final["caltrans_district"] == district]["analysis_name"].unique().tolist()
        for district in sorted(final["caltrans_district"].unique())
    }
    site = load_site(SITE_YML)
    site = generate_parts_grouped(site, param_key="analysis_name", groups=groups)
    site.write_yaml(SITE_YML)
