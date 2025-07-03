"""
Script to save any custom operator request
to include in our workflow.
For now, this is Muni's list of bus stops to include
as BRT.
Initially, we tagged all of the routes on which
these stops took place, but that flagged over 1_000 stops
as major_stop_brt.
Here, it's just 136 stops.
"""
import pandas as pd

from segment_speed_utils import helpers
from shared_utils import rt_dates
from update_vars import GCS_FILE_PATH

def get_muni_stops(analysis_date: str) -> pd.DataFrame:
    """
    From our stops table, grab Muni stop_ids and
    stop_names.
    If stop_id ever changes, maybe we can match 
    on stop_names.
    """
    muni_feed = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key"],
        filters = [[("name", "==", "Bay Area 511 Muni Schedule")]],
        get_pandas = True
    ).feed_key.iloc[0]
    
    stops = helpers.import_scheduled_stops(
        analysis_date,
        columns = ["stop_id", "stop_name"],
        filters = [[("feed_key", "==", muni_feed)]],
        get_pandas = True
    )
    
    return stops

if __name__ == "__main__":
    
    sfmta_date = "2024-02-01"
    analysis_date = rt_dates.DATES["feb2024"]
    
    all_muni_stops = get_muni_stops(analysis_date)
    
    FILE = f"SFMTA_muni_high_quality_transit_stops_{sfmta_date}.csv"

    muni_stops = (
        pd.read_csv(f"{GCS_FILE_PATH}operator_input/{FILE}", 
                    dtype={"bs_id": "str"})
        .drop(columns=["latitude", "longitude"])
        .rename(columns={"bs_id": "stop_id"})
    )
    
    muni_stops_with_names = pd.merge(
        muni_stops,
        all_muni_stops,
        on = "stop_id",
        how = "inner"
    )
    
    muni_stops_with_names.to_parquet(
        f"{GCS_FILE_PATH}operator_input/"
        f"muni_brt_stops.parquet"
    )
        
    print(f"saved muni stops")
    

