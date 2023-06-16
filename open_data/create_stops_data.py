"""
Create stops file with identifiers including
route_id, route_name, agency_id, agency_name.
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from datetime import datetime

import prep_traffic_ops
from shared_utils import utils, geography_utils, schedule_rt_utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from update_vars import analysis_date, TRAFFIC_OPS_GCS

    
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
    ).compute()
    
    stops_with_geom = gtfs_schedule_wrangling.attach_stop_geometry(
        stops_with_route_info,
        stops
    )
  
    stops_assembled = (stops_with_geom
                       .sort_values(["name", "route_id", "stop_id"])
                       .reset_index(drop=True)
                      )
    
    stops_assembled2 = prep_traffic_ops.standardize_operator_info_for_exports(
            stops_assembled, analysis_date).to_crs(geography_utils.WGS84)
    
    return stops_assembled2


def finalize_export_df(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame: 
    """
    Suppress certain columns used in our internal modeling for export.
    """
    # Change column order
    route_cols = [
        'organization_source_record_id', 'organization_name',
        'route_id', 'route_type']
    stop_cols = ['stop_id', 'stop_name']
    agency_ids = ['base64_url', 'uri']
    
    col_order = route_cols + stop_cols + agency_ids + ['geometry']
    
    df2 = (df[col_order]
           .reindex(columns = col_order)
           .rename(columns = prep_traffic_ops.RENAME_COLS)
    )
    
    return df2


def create_stops_file_for_export(analysis_date: str) -> gpd.GeoDataFrame:
    time0 = datetime.now()

    # Read in parquets
    stops = helpers.import_scheduled_stops(
        analysis_date,
        columns = prep_traffic_ops.keep_stop_cols,
        get_pandas = True
    )
    
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = prep_traffic_ops.keep_trip_cols,
        get_pandas = True
    )
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = prep_traffic_ops.keep_stop_time_cols
    )
        
    stops_assembled = attach_route_info_to_stops(stops, trips, stop_times)
    
    time1 = datetime.now()
    print(f"Attach route and operator info to stops: {time1-time0}")
    
    return stops_assembled


if __name__ == "__main__":
    time0 = datetime.now()

    stops = create_stops_file_for_export(analysis_date)  
    
    stops = finalize_export_df(stops)
    
    utils.geoparquet_gcs_export(
        stops, 
        TRAFFIC_OPS_GCS, 
        "ca_transit_stops"
    )
    
    prep_traffic_ops.export_to_subfolder(
        "ca_transit_stops", analysis_date)
    
    time1 = datetime.now()
    print(f"Execution time for stops script: {time1-time0}")
