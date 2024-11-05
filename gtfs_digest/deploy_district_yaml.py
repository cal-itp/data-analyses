import pandas as pd

from pathlib import Path
from typing import Literal

from shared_utils import portfolio_utils
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS, SHARED_GCS

DISTRICT_SITE = Path("../portfolio/sites/district_digest.yml")
LEG_DISTRICT_SITE = Path("../portfolio/sites/legislative_district_digest.yml")

def overwrite_yaml(
    digest_type: Literal["district", "legislative_district"],
) -> None:
    """
    Create yamls for either district or legislative district 
    GTFS digest.
    """
    if digest_type == "district":

        OPERATOR_FILE = GTFS_DATA_DICT.digest_tables.operator_profiles

        df = pd.read_parquet(
            f"{RT_SCHED_GCS}{OPERATOR_FILE}.parquet",
            columns = ["caltrans_district"]
        ).dropna("caltrans_district").drop_duplicates()
        
        portfolio_utils.create_portfolio_yaml_chapters_no_sections(
            DISTRICT_SITE, 
            chapter_name = "district",
            chapter_values = sorted(list(df.caltrans_district))
        )  
        
    elif digest_type == "legislative_district":
        
        df = pd.read_parquet(
            f"{SHARED_GCS}crosswalk_transit_operators_legislative_districts.parquet",
            columns = ["legislative_district"]
        ).drop_duplicates()
        
        portfolio_utils.create_portfolio_yaml_chapters_no_sections(
            LEG_DISTRICT_SITE, 
            chapter_name = "district",
            chapter_values = sorted(list(df.legislative_district))
        )
        
        
if __name__ == "__main__":
    
    #overwrite_yaml("district")
    
    overwrite_yaml("legislative_district")