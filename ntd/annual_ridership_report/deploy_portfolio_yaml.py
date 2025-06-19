# updated script to get RTPAs from dim_orgs
"""
Creates site .yml with chapters for each RTPA in the ntd/rtpa crosswalk, places it in the portfolio/sites directory

Yaml structure is not nested by district, it is just all RTPAs
in the navigation panel.
"""

import sys

sys.path.append("../")  # up one level

from pathlib import Path
from calitp_data_analysis.tables import tbls
import pandas as pd
from shared_utils import portfolio_utils
from siuba import _, collect, filter, select, show_query
from update_vars import GCS_FILE_PATH
from annual_ridership_module import ntd_id_to_rtpa_crosswalk

PORTFOLIO_SITE_YAML = Path("../../portfolio/sites/ntd_annual_ridership_report.yml")

# read in rtpa data from dim_orgs
if __name__ == "__main__":
    df = ntd_id_to_rtpa_crosswalk(split_scag=True)["rtpa_name"].drop_duplicates().to_frame()
    # add row for LADPW
    ladpw= pd.DataFrame({"rtpa_name":["Los Angeles County Department of Public Works"]})
    df = pd.concat([df, ladpw], ignore_index=True).sort_values(by="rtpa_name")
    portfolio_utils.create_portfolio_yaml_chapters_no_sections(
        PORTFOLIO_SITE_YAML, chapter_name="rtpa", chapter_values=list(df.rtpa_name)
    )