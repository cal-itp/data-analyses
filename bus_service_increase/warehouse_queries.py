"""
GTFS schedule data is downloaded in gtfs_funnel/.

Pull those parquets and combine / aggregate.
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils, geography_utils
from bus_service_utils import utils as bus_utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from shared_utils import rt_dates

DATA_PATH = f"{bus_utils.GCS_FILE_PATH}2023_Oct/"

#---------------------------------------------------------------#
# Set dates for analysis
#---------------------------------------------------------------#
dates = {
    "wed": rt_dates.DATES["oct2023"], 
    "sat": rt_dates.DATES["oct2023a"],
    "sun": rt_dates.DATES["oct2023b"],
}

#---------------------------------------------------------------#
# Warehouse Queries for B1_service_opportunities_tract.ipynb
#---------------------------------------------------------------#
def process_daily_stop_times(selected_date: str) -> pd.DataFrame:

    stop_cols = ["feed_key", "stop_id"]
    
    stop_times = helpers.import_scheduled_stop_times(
        selected_date,
        columns = stop_cols + ["arrival_sec"]
    )
    
    # Aggregate to count daily stop times
    # Count the number of times a bus arrives at that stop daily    
    stop_arrivals = gtfs_schedule_wrangling.stop_arrivals_per_stop(
        stop_times,
        group_cols = stop_cols,
        count_col = "arrival_sec"
    ).compute()
    
    stops = helpers.import_scheduled_stops(
        selected_date,
        columns = stop_cols + ["stop_name", "geometry"],
        get_pandas = True,
        crs = geography_utils.WGS84
    )
    
    aggregated_stops_with_geom = pd.merge(
        stops,
        stop_arrivals,
        on = stop_cols,
        how = "inner"
    )
    
    utils.geoparquet_gcs_export(
        aggregated_stops_with_geom,
        DATA_PATH,
        f"aggregated_stops_with_geom_{selected_date}"
    )
    
    
if __name__ == "__main__":
    # Run this to get the static parquet files
    # Analysis is for a particular day, so don't need to hit warehouse constantly
    
    # (1) Get existing service 
    trips_combined = pd.concat(
        [helpers.import_scheduled_trips(
            analysis_date, 
            columns = None,
            get_pandas = True) 
         for analysis_date in dates.values()
        ], axis=0
    ).reset_index(drop=True)
    
    trips_combined.to_parquet(f"{DATA_PATH}trips.parquet")
    print(f"combined trips and exported")
        
    
    # (2) Get daily bus stop arrivals with geometry
    # Only do it for a weekday
    process_daily_stop_times(dates["wed"])
    