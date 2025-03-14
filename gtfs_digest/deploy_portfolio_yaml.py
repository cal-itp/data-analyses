"""
Create the GTFS Digest yaml that 
sets the parameterization for the analysis site.
"""
import pandas as pd
import yaml

from shared_utils import portfolio_utils
from _operators_prep import operators_schd_vp_rt

SITE_YML = "../portfolio/sites/gtfs_digest.yml"

if __name__ == "__main__":
    schd_vp_df2, schd_vp_df3, final = operators_schd_vp_rt()

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
            "column": "organization_name",
            "name": "organization_name",
        },
    )