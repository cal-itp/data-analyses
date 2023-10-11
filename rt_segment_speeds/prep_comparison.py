"""
Prep the data used to compare segment methodologies 
between Eric and Tiffany.

Get at why speeds are coming out differently.

   - segments do not exactly match
   - points over which speeds are calculated aren't exactly the same, 
     since understanding of direction is not exactly the same
   - peel all that back and start at the trip-level to see what's going into 
     averages, start with simpler shapes (no loop, no inlining)
"""
import geopandas as gpd
import pandas as pd

from shared_utils import rt_dates, rt_utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, GCS_FILE_PATH
from calitp_data_analysis import utils

RT_DELAY_GCS = f"{GCS_FILE_PATH}rt_delay/v2_segment_speed_views/"
analysis_date = rt_dates.DATES["sep2023"]

def prep_eric_data(analysis_date: str) -> gpd.GeoDataFrame:
    itp_ids = [
        182,
        300,
    ]

    # Don't narrow down time-of-day yet, we might select a trip from any 
    # of these
    time_of_day = [
        "AM_Peak", "Midday", "PM_Peak"
    ]

    eric_dfs = [
        gpd.read_parquet(
            f"{RT_DELAY_GCS}{itp_id}_{analysis_date}_{time}.parquet")
          for itp_id, time in zip(itp_ids, time_of_day)
         ]

    df_eric = pd.concat(eric_dfs, axis=0).reset_index(drop=True)
    
    return df_eric


def prep_tiff_data(
    analysis_date: str, 
    subset_df: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    
    shape_trips = subset_df[["shape_id", "trip_id"]].drop_duplicates()

    scheduled_trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = [
            "gtfs_dataset_key", "name", 
            "trip_id", "trip_instance_key",
            "shape_id", "shape_array_key",
            "route_id", "direction_id"],
        get_pandas = True
    ).rename(columns = {"gtfs_dataset_key": "schedule_gtfs_dataset_key"})

    # Grab the trip_instance_keys we need and use it
    # to filter the speeds parquet down
    subset_trips = scheduled_trips.merge(
        shape_trips,
        on = ["shape_id", "trip_id"],
        how = "inner"
    )

    trip_instances = subset_trips.trip_instance_key.unique().tolist()
    subset_shapes = subset_trips.shape_array_key.unique().tolist()

    segments = gpd.read_parquet(
        f"{SEGMENT_GCS}stop_segments_{analysis_date}.parquet",
        filters = [[("shape_array_key", "in", subset_shapes)]]
    ).drop(columns = ["geometry_arrowized", "district_name"])

    filtered_trip_speeds = pd.read_parquet(
        f"{SEGMENT_GCS}speeds_stop_segments_{analysis_date}.parquet",
        filters = [[("trip_instance_key", "in", trip_instances)]]
    ).merge(
        subset_trips,
        on = ["trip_instance_key", "shape_array_key"],
        how = "inner"
    )

    df_tiff = pd.merge(
        segments,
        filtered_trip_speeds,
        on = ["schedule_gtfs_dataset_key", "shape_array_key", "stop_sequence"],
        how = "inner"
    )
    
    return df_tiff


def map_one_trip(gdf: gpd.GeoDataFrame, one_trip: str):
    gdf2 = gdf[gdf.trip_id==one_trip]

    m1 = gdf2.explore(
         "speed_mph", 
        tiles = "CartoDB Positron",
        cmap = rt_utils.ZERO_THIRTY_COLORSCALE
    )
    
    return m1

if __name__ == "__main__":
    df_eric = prep_eric_data(analysis_date)
    df_tiff = prep_tiff_data(analysis_date, df_eric)
    
    utils.geoparquet_gcs_export(
        df_eric,
        SEGMENT_GCS,
        f"speeds_eric_{analysis_date}"
    )

    utils.geoparquet_gcs_export(
        df_tiff,
        SEGMENT_GCS,
        f"speeds_tiff_{analysis_date}"
    )
    
    # stop_sequence doesn't exactly merge, but that's fine, 
    # since Eric cuts shorter segments, so stop_sequence can have 
    # values like 1.25, 1.50, etc.
    # Leave it in the merge for now, and allow left_only merges
    identifier_cols = [
        "trip_id", "shape_id", "stop_id", "stop_sequence",
        "route_id", "direction_id",
    ]

    
    speed_df = pd.merge(
        df_eric[identifier_cols + ["speed_mph"]].rename(
            columns = {"speed_mph": "eric_speed_mph"}),
        df_tiff[identifier_cols + ["speed_mph"]].rename(
            columns = {"speed_mph": "tiff_speed_mph"}),
        on = identifier_cols,
        how = "left",
        indicator = True
    )

    speed_df.to_parquet(f"{SEGMENT_GCS}speeds_comparison_{analysis_date}.parquet")