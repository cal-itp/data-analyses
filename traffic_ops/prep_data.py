"""
Functions to query GTFS schedule data, 
save locally as parquets, 
then clean up at the end of the script.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(130_000_000_000)

import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd

from siuba import *
from typing import Union, Literal

from shared_utils import geography_utils, gtfs_utils_v2, utils, rt_dates

ANALYSIS_DATE = gtfs_utils.format_date(rt_dates.DATES["dec2022"])

GCS = "gs://calitp-analytics-data/data-analyses/"
TRAFFIC_OPS_GCS = f"{GCS}traffic_ops/"
COMPILED_CACHED_GCS = f"{GCS}rt_delay/compiled_cached_views/"
DATA_PATH = "./data/"
    

def concatenate_dataset_and_export(
    dataset: Literal["trips", "routelines", "stops", "st"], 
    date: Union[str, datetime.date],
    export_path: str = COMPILED_CACHED_GCS,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Grab the cached file on selected date for trips, stops, routelines, st.
    Concatenate Amtrak.
    Save a new cached file in GCS.
    """
    
    if dataset in ["trips", "st"]:
        amtrak = pd.read_parquet(f"amtrak_{dataset}.parquet")
        df = pd.read_parquet(f"{export_path}{dataset}_{date}.parquet")
        
    elif dataset in ["routelines", "stops"]:
        amtrak = gpd.read_parquet(
            f"amtrak_{dataset}.parquet"
        ).to_crs(geography_utils.WGS84)
        
        df = gpd.read_parquet(
            f"{export_path}{dataset}_{date}.parquet"
        ).to_crs(geography_utils.WGS84)

    full_df = (pd.concat([df, amtrak], axis=0)
        .drop_duplicates()
        .reset_index(drop=True)
    )    
    
    if isinstance(full_df, pd.DataFrame):
        full_df.to_parquet(f"{export_path}{dataset}_{date}_all.parquet")
        
    elif isinstance(full_df, gpd.GeoDataFrame):
        utils.geoparquet_gcs_export(
            full_df,
            export_path,
            f"{dataset}_{date}_all"
        )

    
def remove_local_parquets():
    # Remove Amtrak now that full dataset made
    for dataset in ["trips", "routelines", "stops", "st"]:
        os.remove(f"amtrak_{dataset}.parquet")


def export_to_subfolder(file_name: str, analysis_date: str):
    """
    We always overwrite the same geoparquets each month, and point our
    shared_utils/shared_data_catalog.yml to the latest file.
    
    But, save historical exports just in case.
    """
    file_name_sanitized = file_name.replace('.parquet', '')
    
    gdf = gpd.read_parquet(f"{TRAFFIC_OPS_GCS}{file_name_sanitized}.parquet")
        
    utils.geoparquet_gcs_export(
        gdf, 
        f"{TRAFFIC_OPS_GCS}export/", 
        f"{file_name_sanitized}_{analysis_date}"
    )
        
        
#----------------------------------------------------#        
# Functions are used in 
# `create_routes_data.py` and `create_stops_data.py`
#----------------------------------------------------#
# Define column names, must fit ESRI 10 character limits
RENAME_COLS = {
    "name": "agency",
    "route_name_used": "route_name",
}


if __name__ == "__main__":
    """
    Amtrak is always excluded from queries for hqta and rt_delay
    
    Add back in now for our open data portal dataset
    """
    
    amtrak = "Amtrak Schedule"
    
    keep_stop_cols = [
        'feed_key', 'stop_id', 'stop_name', 
        'stop_key'
    ]
    
    amtrak_stops = gtfs_utils_v2.get_stops(
        selected_date = selected_date, 
        operator_feeds = [amtrak],
        stop_cols = keep_stop_cols,
        get_df = True,
        crs = geography_utils.CA_NAD83Albers, # this is the CRS used for rt_delay
    )
    
    amtrak_stops.to_parquet("amtrak_stops.parquet")
    
    keep_trip_cols = [
        'feed_key', 'service_date', 
        'trip_key','trip_id', 
        'route_id', 'route_type',
        'direction_id', 'shape_id',
        'route_short_name', 'route_long_name', 'route_desc'
    ]
    
    amtrak_trips = gtfs_utils_v2.get_trips(
        selected_date = selected_date, 
        operator_feeds = [amtrak],
        trip_cols = keep_trip_cols,
        get_df = True,
    )
    
    amtrak_trips.to_parquet("amtrak_trips.parquet")
    
    amtrak_routelines = gtfs_utils_v2.get_shapes(
        selected_date = selected_date,
        operator_feeds = [amtrak],
        get_df = True,
        crs = geography_utils.CA_NAD83Albers,
    )
    
    amtrak_routelines.to_parquet("amtrak_routelines.parquet")
    
    amtrak_st = gtfs_utils_v2.get_stop_times(
        selected_date = selected_date,
        operator_feeds = [amtrak],
        get_df = True,
        trip_df = amtrak_trips
    )
    
    amtrak_st.to_parquet("amtrak_st.parquet")
    
    # Concatenate Amtrak
    for d in ["trips", "routelines", "stops", "st"]:
        concatenate_dataset_and_export(
            dataset = d, 
            date = ANALYSIS_DATE,
            export_path = COMPILED_CACHED_GCS,
        )
    
    remove_local_parquets()