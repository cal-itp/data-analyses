"""
Create the GTFS Digest yaml that 
sets the parameterization for the analysis site.
"""
import pandas as pd
from shared_utils import portfolio_utils, publish_utils
from update_vars import GTFS_DATA_DICT

import google.auth
import pandas_gbq
from calitp_data_analysis.sql import get_engine
from calitp_data_analysis.tables import tbls
db_engine = get_engine()
credentials, project = google.auth.default()

SITE_YML = "../portfolio/sites/gtfs_digest.yml"
    
def generate_operator_grain_yaml(filename: str) -> pd.DataFrame:
    """
    Generate the yaml for our Operator grain portfolio.
    """
    FILEPATH_URL = f"{GTFS_DATA_DICT.digest_tables.dir}{filename}.parquet"
    
    # Keep only organizations with RT and schedule OR only schedule.
    df = (pd.read_parquet(
        FILEPATH_URL, 
        filters=[[("sched_rt_category", "in", ["schedule_and_vp", "schedule_only"])]],
        columns = ["caltrans_district", "analysis_name"]
    ).dropna(subset=["caltrans_district"])
    .sort_values(["caltrans_district", "analysis_name"]).reset_index(drop=True)
         )
                     
    return df


if __name__ == "__main__":
    
    OPERATOR_PROFILE_REPORT = GTFS_DATA_DICT.digest_tables.operator_profiles

    final = generate_operator_grain_yaml(OPERATOR_PROFILE_REPORT)

    portfolio_utils.create_portfolio_yaml_chapters_with_sections(
        SITE_YML,
        final,
        chapter_info = {
            "column": "caltrans_district",
            "name": "district",
            "caption_prefix": "District ",
            "caption_suffix": "",
        },
        section_info = {
            "column": "analysis_name",
            "name": "analysis_name",
        },
    )