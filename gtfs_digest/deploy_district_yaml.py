"""
Create the yamls for district / legislative GTFS Digest.

Since these yamls do not use sections, we can
generate them similarly using Makefile commands.

Try out typer to make CLI a little easier to use, since 
this only takes 1 argument with 2 possible values.
Base it off of this tutorial:
https://typer.tiangolo.com/tutorial/options/required/
"""
import pandas as pd
import typer

from pathlib import Path

from shared_utils import portfolio_utils
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS, SHARED_GCS

DISTRICT_SITE = Path("../portfolio/sites/district_digest.yml")
LEG_DISTRICT_SITE = Path("../portfolio/sites/legislative_district_digest.yml")

app = typer.Typer()

@app.command()
def overwrite_yaml(
    name: str = typer.Argument(default=None)
):
    """
    Create yamls for either district or legislative district 
    GTFS digest.
    """
    if name is None:
        raise ValueError("digest_type can be 'district', 'legislative_district'")
    
    elif name == "district":
        
        OPERATOR_FILE = GTFS_DATA_DICT.digest_tables.operator_profiles

        df = pd.read_parquet(
            f"{RT_SCHED_GCS}{OPERATOR_FILE}.parquet",
            columns = ["caltrans_district"]
        ).dropna(subset="caltrans_district").drop_duplicates()
                
        portfolio_utils.create_portfolio_yaml_chapters_no_sections(
            DISTRICT_SITE, 
            chapter_name = "district",
            chapter_values = sorted(list(df.caltrans_district))
        )  
         
    elif name == "legislative_district":
        
        df = pd.read_parquet(
            f"{SHARED_GCS}crosswalk_transit_operators_legislative_districts.parquet",
            columns = ["legislative_district"]
        ).drop_duplicates()
        
        portfolio_utils.create_portfolio_yaml_chapters_no_sections(
            LEG_DISTRICT_SITE, 
            chapter_name = "district",
            chapter_values = sorted(list(df.legislative_district))
        ) 
        
    return


if __name__ == "__main__":
    app()
