"""
Deploy portfolio yaml based on ntd id to rtpa crosswalk, all reporter types.

Since the names of RTPAs change so much depending on the crosswalk
we use, let's just generate the yaml.

This Yaml structure is structured by RTPA names, instead of CT districts.
RTPA names will become the navigation panel on the portfolio site
"""
import sys

sys.path.append("../")  # up one level

import pandas as pd

from pathlib import Path

from shared_utils import portfolio_utils
from update_vars import GCS_FILE_PATH

PORTFOLIO_SITE_YAML = Path("../../portfolio/sites/new_transit_metrics.yml")

if __name__ == "__main__":
    
    df = pd.read_parquet(
        f"{GCS_FILE_PATH}ntd_id_rtpa_crosswalk_all_reporter_types.parquet",
        columns = ["RTPA"]
    ).drop_duplicates().sort_values("RTPA").reset_index(drop=True)
    
    portfolio_utils.create_portfolio_yaml_chapters_no_sections(
        PORTFOLIO_SITE_YAML, 
        chapter_name = "rtpa",
        chapter_values =list(df.RTPA)
    )

