"""
Mock up time-of-day averages across multiple Oct 2024 dates
"""
import geopandas as gpd
import pandas as pd

from dask import delayed, compute

from update_vars import SEGMENT_GCS, GTFS_DATA_DICT

from quarter_year_averages import concatenate_single_day_summaries, get_aggregation

if __name__ == "__main__":
    
    from shared_utils import rt_dates 
    
    FILE2 = GTFS_DATA_DICT["stop_segments"]["shape_stop_single_segment"] + "_test"

    time_of_day_cols = [
        "route_id", "direction_id", 
        "stop_pair", "stop_pair_name", 
        'name', 
        "time_of_day"
    ] 
    
    quarter_timeofday_df = concatenate_single_day_summaries(
        FILE2,
        rt_dates.oct2024_week, 
        time_of_day_cols
    ).pipe(
        get_aggregation, 
        time_of_day_cols + ["year", "quarter"]
    )
    
    quarter_timeofday_df = quarter_timeofday_df.compute()
    quarter_timeofday_df.to_parquet(
        f"{SEGMENT_GCS}{FILE2}_quarter_time_of_day.parquet"
    )
    