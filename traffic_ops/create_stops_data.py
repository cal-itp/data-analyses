"""
Create stops file with identifiers including
route_id, route_name, agency_id, agency_name.
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from datetime import datetime

import prep_data
from shared_utils import utils
    
    
def attach_route_info_to_stops(
    stops: gpd.GeoDataFrame, 
    trips: pd.DataFrame, 
    stop_times: dd.DataFrame
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
        dd.merge(
            stop_times,
            trips[trip_cols], 
            on = ["feed_key", "trip_id"]
        ).drop_duplicates(subset=["feed_key", "stop_id",
                                  "route_id", "route_type"])
        .drop(columns = "trip_id")
        .reset_index(drop=True)
    )
    
    stops_with_geom = dd.merge(
        stops,
        stops_with_route_info,
        on = ["feed_key", "stop_id"],
        how = "inner",
    ).compute()
    
    # Drop feed_key and just sort on name
    stops_assembled = (stops_with_geom.drop(columns = "feed_key")
                       .sort_values(["name", "route_id", "stop_id"])
                       .reset_index(drop=True)
                      )
    
    return stops_assembled


def create_stops_file_for_export(analysis_date: str) -> gpd.GeoDataFrame:
    time0 = datetime.now()

    # Read in parquets
    stops = prep_data.import_stops(analysis_date)
    trips = prep_data.import_trips(analysis_date)
    stop_times = prep_data.import_stop_times(analysis_date)
        
    stops_assembled = attach_route_info_to_stops(stops, trips, stop_times)
    
    time1 = datetime.now()
    print(f"Attach route and operator info to stops: {time1-time0}")
    
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
