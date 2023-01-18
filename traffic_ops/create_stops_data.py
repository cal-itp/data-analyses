"""
Create stops file with identifiers including
route_id, route_name, agency_id, agency_name.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from datetime import datetime

import prep_data
from shared_utils import utils, geography_utils, portfolio_utils
from create_routes_data import import_trips

def import_stops(analysis_date: str) -> gpd.GeoDataFrame:
    # Instead of keeping route_type_0, route_type_1, etc
    # keep stops table long, instead of wide
    # attach route_id, route_type as before
    keep_cols = [
        "feed_key",
        "stop_id", "stop_name", 
    ] 
    
    stops = gpd.read_parquet(
        f"{prep_data.COMPILED_CACHED_GCS}"
        f"stops_{analysis_date}_all.parquet",
        columns = keep_cols
    )
    
    
def import_stop_times(analysis_date: str) -> pd.DataFrame:
    keep_cols = ["feed_key", "trip_id", "stop_id"]
    
    stop_times = pd.read_parquet(
        f"{prep_data.COMPILED_CACHED_GCS}"
        f"st_{analysis_date}_all.parquet",
        columns = keep_cols
    ).drop_duplicates().reset_index(drop=True)
    
    return stop_times
    
    
def attach_route_info_to_stops(
    stops: gpd.GeoDataFrame, 
    trips: pd.DataFrame, 
    stop_times: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Attach all the various route information (route_id, route_type)
    to the stops file.
    """            
    # In stop_times table, find the trip ids that are present that day
    # then find unique stop_ids present that day
    trip_cols = ["feed_key", "name", 
                 "trip_id", 
                 "route_id", "route_type", 
                ]
    
    stops_with_route_info = (
        pd.merge(
            stop_times,
            trips[trip_cols], 
            on = ["feed_key", "trip_id"]
        ).drop_duplicates(subset=["feed_key", "stop_id",
                                  "route_id", "route_type"])
        .drop(columns = "trip_id")
        .reset_index(drop=True)
    )
    
    stops_with_geom = pd.merge(
        stops,
        stops_with_route_info,
        on = ["feed_key", "stop_id"],
        how = "inner",
    )
    
    # Drop feed_key and just sort on name
    stops_assembled = (stops_with_geom.drop(columns = "feed_key")
                       .sort_values(["name", "route_id", "stop_id"])
                       .reset_index(drop=True)
                      )
    
    return stops_assembled


def create_stops_file_for_export(analysis_date: str) -> gpd.GeoDataFrame:
    time0 = datetime.now()

    # Read in local parquets
    stops = import_stops(analysis_date)

    trips = import_trips(analysis_date)
    stop_times = import_stop_times(analysis_date)
    
    time1 = datetime.now()
    print(f"Get rid of duplicates: {time1-time0}")
    
    stops_assembled = attach_route_info_to_stops(stops, trips, stop_times)
    
    time2 = datetime.now()
    print(f"Attach route and operator info to stops: {time2-time1}")
    
    return stops_assembled


if __name__ == "__main__":
    time0 = datetime.now()

    stops = create_stops_file_for_export(prep_data.ANALYSIS_DATE)  
    
    utils.geoparquet_gcs_export(
        stops, 
        prep_data.TRAFFIC_OPS_GCS, 
        "ca_transit_stops"
    )
    
    prep_data.export_to_subfolder(
        "ca_transit_stops", prep_data.ANALYSIS_DATE)
    
    time1 = datetime.now()
    print(f"Execution time for stops script: {time1-time0}")
