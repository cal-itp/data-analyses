"""
Deploy portfolio yaml.

Since the names of RTPAs change so much depending on the crosswalk
we use, let's just generate the yaml.

Yaml structure is not nested by district, it is just all RTPAs
in the navigation panel.
"""
import pandas as pd

from pathlib import Path

from shared_utils import portfolio_utils
from update_vars import GCS_FILE_PATH

PORTFOLIO_SITE_YAML = Path("../portfolio/sites/ntd_monthly_ridership_test.yml")

if __name__ == "__main__":
    
    df = pd.read_parquet(
        f"{GCS_FILE_PATH}ntd_id_rtpa_crosswalk.parquet",
        columns = ["RTPA"]
    ).drop_duplicates().sort_values("RTPA").reset_index(drop=True)
    
    portfolio_utils.create_portfolio_yaml_chapters_no_sections(
        PORTFOLIO_SITE_YAML, 
        chapter_name = "rtpa",
        chapter_values =list(df.RTPA)
    )
    
