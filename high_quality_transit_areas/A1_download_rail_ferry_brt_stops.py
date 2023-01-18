"""
Download rail, ferry, BRT stops.

From rail_ferry_brt.ipynb into script.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from siuba import *

from shared_utils import utils
from update_vars import analysis_date, COMPILED_CACHED_VIEWS, TEMP_GCS

keep_stop_cols = [
    "feed_key",
    "stop_id", "stop_name", "stop_key", 
    "geometry", 
    "route_type"
]

def grab_stops(analysis_date: str,
               route_types: list = []) -> gpd.GeoDataFrame:
    """
    Merge stops table to get point geom attached to stop_ids 
    for specified route_types.
    """
    stops = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet")
    
    route_type_cols = [f"route_type_{i}" for i in route_types]

    # Grab stops for just the specific route types
    stops = stop.assign(
        route_type_present = stops[route_type_cols].sum(axis=1)
    )
    
    # Only keep stops for subset of route types
    stops_for_type = stops[stops.route_type_present > 0].reset_index(drop=True)
    
    ## TODO: test this line
    stops_for_type = stops_for_type.assign(
        route_type = pd.from_dummies(stops_for_type[route_type_cols])
    )
    
    stops_for_type = stops_for_type[keep_stop_cols]

    return stops_for_route_type
    

def grab_rail_data(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Grab all the rail routes by subsetting stops table 
    down to certain route types.
    
    Returns gpd.GeoDataFrame.
    """
    # Grab the different route types for rail from route tables
    rail_route_types = ['0', '1', '2']
            
    rail_stops = grab_stops(analysis_date, rail_route_types)
                
    utils.geoparquet_gcs_export(
        rail_stops,
        TEMP_GCS,
        "rail_stops"
    )
    

    
def filter_to_brt_trips(analysis_date: str) -> pd.DataFrame:
    """
    Start with trips table and filter to specific routes that
    are BRT
    """
    trips = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")
    
    BRT_ROUTE_FILTERING = {
        "Bay Area 511 AC Transit Schedule": {"route_id": 
                                             ["1T"]},
        "LA Metro Bus Schedule": {"route_desc": 
                                  ["METRO SILVER LINE", "METRO ORANGE LINE"]},
        "Bay Area 511 Muni Schedule": {"route_short_name": 
                                       ['49']},
        # Omni BRT -- too infrequent!
        "OmniTrans Schedule": {"route_short_name": ["sbX"]}
    }             
    
    all_brt_trips = pd.DataFrame()
    
    for name, filtering_cond in BRT_ROUTE_FILTERING.items():
        for col, filtering_list in filtering_cond.items():
            trips_subset = trips[
                (trips.name == name) & 
                (trips[col].isin(filtering_list))]
            
            all_brt_trips = pd.concat([all_brt_trips, trips_subset], axis=0)
    
    keep_cols = ["feed_key", "name", "trip_id", "route_type"]
    
    return all_brt_trips[keep_cols]


def filter_to_brt_stop_ids(
    analysis_date: str, 
    trip_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Start with all operators' stop_times, and narrow down to the trip_ids
    present for BRT.
    """
    stop_times = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}st_{analysis_date}.parquet")
    
    stops_for_trips = dd.merge(
        stop_times,
        trip_df,
        on = ["feed_key", "trip_id"],
        how = "inner"
    )[["feed_key", 
       "stop_id", "route_type"]].drop_duplicates().reset_index(drop=True)
    
    stops_present = stops_for_trips.compute()
    
    return stops_present
    
    
    
def grab_operator_brt(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Grab BRT routes, stops data for certain operators in CA by analysis date.
    
    Use different method than rail or ferry because
    BRT route_type would be '3' for bus.
    """
                             
    brt_trips = filter_to_brt_trips(analysis_date)
    brt_stops_present = filter_to_brt_stop_ids(analysis_date, brt_trips)
    
    stops = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet")
    
    brt_stops = pd.merge(
        stops,
        brt_stops_present,
        on = ["feed_key", "stop_id"],
        how = "inner"
    )[keep_stop_cols].drop_duplicates().reset_index(drop=True)
        
        
    utils.geoparquet_gcs_export(
        brt_stops,
        TEMP_GCS,
        "brt_stops"
    )
    

def additional_brt_filtering_out_stops(
    df: gpd.GeoDataFrame, filtering_dict: dict
) -> gpd.GeoDataFrame:
    """
    df: geopandas.GeoDataFrame
        Input BRT stops data (combined across operators)
    filtering_dict: dict
        key: feed_key / name
        value: list of stop_ids that need filtering
        Note: Metro is filtering for stops to drop 
            Muni is filtering for stops to keep
    """
    operators_to_filter = list(filtering_dict.keys())
    
    metro = df[df.name == "LA Metro Bus Schedule"]
    muni = df[df.name == "Bay Area 511 Muni Schedule"]
    subset_no_filtering = df[~df.name.isin(operators_to_filter)]
    
    # For Metro, unable to filter out non-station stops using GTFS, manual list
    metro2 = metro >> filter(-_.stop_id.isin(
        filtering_dict["LA Metro Bus Schedule"]))
    
    muni2 = muni >> filter(_.stop_id.isin(
        filtering_dict["Bay Area 511 Muni Schedule"]))

    brt_df_stops = pd.concat(
        [metro2, muni2, subset_no_filtering], 
        axis=0
    ).sort_values(["feed_key", "name"]).reset_index(drop=True)
    
    return brt_df_stops


def grab_ferry_data(analysis_date: str):
    ferry_route_types = ['4']
        
    ferry_stops = grab_stops(analysis_date, ferry_route_types)
        
    # only stops without bus service
    angel_and_alcatraz = ['2483552', '2483550', '43002'] 
    
    ferry_stops = ferry_stops >> filter(-_.stop_id.isin(angel_and_alcatraz))
    
    utils.geoparquet_gcs_export(
        ferry_stops,
        TEMP_GCS,
        "ferry_stops"
    )
