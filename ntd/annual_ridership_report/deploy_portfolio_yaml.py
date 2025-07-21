"""
Creates site .yml with chapters for each unique RTPA in the report data, 
then places the new .yml in the portfolio/sites directory

Yaml structure is not nested by district, it is just all RTPAs
in the navigation panel.
"""

import sys

sys.path.append("../monthly_ridership_report")  # up one level
sys.path.append("../")
from pathlib import Path
from calitp_data_analysis.tables import tbls
import pandas as pd
from shared_utils import portfolio_utils
from update_vars import GCS_FILE_PATH
from _01_ntd_ridership_utils import ntd_id_to_rtpa_crosswalk

PORTFOLIO_SITE_YAML = Path("../../portfolio/sites/ntd_annual_ridership_report.yml")

# read in rtpa data from dim_orgs
if __name__ == "__main__":
    df = pd.read_parquet(f"{GCS_FILE_PATH}annual_ridership_report_data.parquet")["rtpa_name"].sort_values().drop_duplicates().to_frame()
    portfolio_utils.create_portfolio_yaml_chapters_no_sections(
        PORTFOLIO_SITE_YAML, chapter_name="rtpa", chapter_values=list(df.rtpa_name)
    )