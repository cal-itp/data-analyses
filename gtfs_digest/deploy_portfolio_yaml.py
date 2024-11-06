"""
Create the GTFS Digest yaml that 
sets the parameterization for the analysis site.
"""
import pandas as pd

from pathlib import Path

import _operators_prep as op_prep
from segment_speed_utils.project_vars import RT_SCHED_GCS
from shared_utils import portfolio_utils

PORTFOLIO_SITE_YAML = Path("../portfolio/sites/gtfs_digest.yml")

if __name__ == "__main__":
    
    df = (op_prep.operators_schd_vp_rt()
          .sort_values(["caltrans_district", "organization_name"])
          .reset_index(drop=True)
         )
    
    portfolio_utils.create_portfolio_yaml_chapters_with_sections(
        PORTFOLIO_SITE_YAML ,
        df,
        chapter_info = {
            "column": "caltrans_district",
            "name": "district",
            "caption_prefix": "District ",
            "caption_suffix": "",
        },
        section_info = {
            "column": "organization_name",
            "name": "organization_name",
        },
    )