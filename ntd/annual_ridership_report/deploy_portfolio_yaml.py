# updated script to get RTPAs from dim_orgs
"""
Creates site .yml with chapters for each RTPA in the ntd/rtpa crosswalk, places it in the portfolio/sites directory

Yaml structure is not nested by district, it is just all RTPAs
in the navigation panel.
"""

import sys

sys.path.append("../")  # up one level

from pathlib import Path

import pandas as pd
from shared_utils import portfolio_utils
from update_vars import GCS_FILE_PATH

PORTFOLIO_SITE_YAML = Path("../../portfolio/sites/ntd_annual_ridership_report.yml")

# read in rtpa data from dim_orgs
if __name__ == "__main__":
    df = (
        (
            tbls.mart_transit_database.dim_organizations()
            >> filter(
                _._is_current == True,
                # _.ntd_id_2022.notna(),
                _.rtpa_name.notna(),
            )
            >> select(_.name, _.ntd_id_2022, _.rtpa_name, _.mpo_name)
            >> collect()
        )["rtpa_name"]
        .sort_values()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    portfolio_utils.create_portfolio_yaml_chapters_no_sections(
        PORTFOLIO_SITE_YAML, chapter_name="rtpa", chapter_values=list(df.rtpa_name)
    )