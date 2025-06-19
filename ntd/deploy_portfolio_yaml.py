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
from annual_ridership_report.annual_ridership_module import ntd_id_to_rtpa_crosswalk # getting rtpa_name from dim_organizations now

PORTFOLIO_SITE_YAML = Path("../portfolio/sites/ntd_monthly_ridership.yml")

if __name__ == "__main__":
    
    df = ntd_id_to_rtpa_crosswalk(split_scag=True)["rtpa_name"].drop_duplicates().to_frame()
    # add row for LADPW
    ladpw= pd.DataFrame({"rtpa_name":["Los Angeles County Department of Public Works"]})
    df = pd.concat([df, ladpw], ignore_index=True).sort_values(by="rtpa_name")
    
    portfolio_utils.create_portfolio_yaml_chapters_no_sections(
        PORTFOLIO_SITE_YAML, 
        chapter_name = "rtpa",
        chapter_values =list(df.RTPA)
    )
    
