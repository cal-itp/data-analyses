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

from update_vars import GCS_FILE_PATH

if __name__ == "__main__":
    FILE = "SFMTA_muni_high_quality_transit_stops_2024-02-01.csv"

    muni_stops = (
        pd.read_csv(f"{GCS_FILE_PATH}operator_input/{FILE}", 
                    dtype={"bs_id": "str"})
        .drop(columns=["latitude", "longitude"])
        .rename(columns={"bs_id": "stop_id"})
    )
    
    muni_stops.to_parquet(
        f"{GCS_FILE_PATH}operator_input/"
        f"muni_brt_stops.parquet"
    )
    
    print(f"saved muni stops")