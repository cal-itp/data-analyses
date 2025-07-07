"""
Creates site .yml with chapters for each unique RTPA in the report data, 
then places the new .yml in the portfolio/sites directory

Yaml structure is not nested by district, it is just all RTPAs
in the navigation panel.
"""
import pandas as pd
import sys
sys.path.append("../")
from pathlib import Path

from shared_utils import portfolio_utils
from update_vars import GCS_FILE_PATH, YEAR, MONTH
from _01_ntd_ridership_utils import ntd_id_to_rtpa_crosswalk # getting rtpa_name from dim_organizations now

PORTFOLIO_SITE_YAML = Path("../../portfolio/sites/ntd_monthly_ridership.yml")

if __name__ == "__main__":
    
    df = pd.read_parquet(f"{GCS_FILE_PATH}ca_monthly_ridership_{YEAR}_{MONTH}.parquet")["rtpa_name"].sort_values().drop_duplicates().to_frame()
    
    portfolio_utils.create_portfolio_yaml_chapters_no_sections(
        PORTFOLIO_SITE_YAML, 
        chapter_name = "rtpa",
        chapter_values =list(df.rtpa_name)
    )
    
