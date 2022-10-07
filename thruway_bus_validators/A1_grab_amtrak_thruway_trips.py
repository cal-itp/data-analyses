"""
Use cached parquets to find regional bus network.

Since Amtrak thruway buses don't have shape_id, construct it
using stops and stop_times.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import intake
import pandas as pd

from shapely.geometry import LineString
from shared_utils import geography_utils, utils
from _utils import SELECTED_DATE, COMPILED_CACHED_VIEWS, GCS_FILE_PATH

catalog = intake.open_catalog("../high_quality_transit_areas/*.yml")
ITP_ID = 13
trip_cols = ["calitp_itp_id", "trip_id"]


def grab_amtrak_thruway_trips(analysis_date: str) -> pd.DataFrame: 
    """
    Grab trips associated with Amtrak Thruway Connecting Service
    """
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}_all.parquet")

    thruway_bus = "Amtrak Thruway Connecting Service"
    amtrak_thruway_trips = trips[(trips.calitp_itp_id == ITP_ID) & 
                                 (trips.route_long_name == thruway_bus)
                                ].compute()
    
    return amtrak_thruway_trips


def grab_stops_stop_times_via_trips(
    trip_df: pd.DataFrame, analysis_date: str
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Grab the Amtrak stops and stop_times associated with 
    Amtrak Thruway Connecting Service
    """
    # Subset for Amtrak stops, then clip to CA
    # get rid of stops for all the other trips
    stops = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}_all.parquet")

    amtrak_stops = stops[stops.calitp_itp_id==ITP_ID].compute()

    ca = catalog.ca_boundary.read()
    ca_amtrak_stops = amtrak_stops.clip(ca)
    
    # Now, combine the CA stops along with what trip_ids are present
    # for Amtrak Thruway Connecting Service
    stop_times = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}st_{analysis_date}_all.parquet")
    
    amtrak_stop_times = stop_times[
        (stop_times.calitp_itp_id==ITP_ID) & 
        (stop_times.trip_id.isin(trip_df.trip_id)) & 
        (stop_times.stop_id.isin(ca_amtrak_stops.stop_id))
    ].compute()
        
    return ca_amtrak_stops, amtrak_stop_times
    

def array_stop_times_into_linestrings(
    stops: gpd.GeoDataFrame, stop_times: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Merge in stop point geom into stop_times.
    Do a groupby trip_id (no route-level info in dataset)
    and array those points into line geom.
    """    
    # Attach the point geom onto stop_times
    stop_times_gdf = pd.merge(
        stops,
        stop_times, 
        on = ["calitp_itp_id", "stop_id"],
        how = "inner",
        validate = "1:m"
    )
    
    # At the trip-level, sort the stops in correct order, then 
    # change points into linestring at the trip-level
    # https://stackoverflow.com/questions/51071365/convert-points-to-lines-geopandas
    gdf = (stop_times_gdf.sort_values(
        trip_cols + ["stop_sequence_rank"])
           .groupby(trip_cols)['geometry']
           .apply(lambda x: LineString(x.tolist()))
          ).reset_index()
    
    
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", 
                           crs = geography_utils.WGS84)
    
    origin = get_origin_and_destination(stop_times_gdf, category="origin")
    destination = get_origin_and_destination(stop_times_gdf, category="destination")
    
    gdf_with_od = pd.merge(
        gdf, 
        origin,
        on = trip_cols,
        how = "inner",
        validate = "1:1"
    ).merge(destination,
            on = trip_cols,
            how = "inner",
            validate = "1:1"
    )
    
    return gdf_with_od


def get_origin_and_destination(stop_times: gpd.GeoDataFrame, 
                               category="origin") -> gpd.GeoDataFrame:
    """
    Get the origin or destination by for each trip. 
    Merge this into stop_times with line geom.
    
    Might want origin/destination to find other local bus routes that run
    along same cities.
    """
    if category=="origin": 
        stop = (stop_times.groupby(trip_cols).stop_sequence_rank.min()
                .reset_index()
               )
    elif category=="destination": 
        stop = (stop_times.groupby(trip_cols).stop_sequence_rank.max()
                .reset_index()
               )

    one_stop_with_geom = pd.merge(
        stop_times, 
        stop,
        on = trip_cols + ["stop_sequence_rank"],
        how = "inner",
        validate = "1:1"
    ).rename(columns = {"geometry": category, 
                        "stop_name": f"{category}_stop_name"
                       })
    
    one_stop_with_geom = one_stop_with_geom[trip_cols + 
                                            [category, f"{category}_stop_name"]]
    
    return one_stop_with_geom


def merge_trips_with_linestring(
    trips: pd.DataFrame, trip_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame: 
    """
    Merge in trip-level line geom with the trips table.
    """
    drop_cols = ["calitp_extracted_at", "calitp_deleted_at"]
    
    gdf = pd.merge(
        trip_gdf, 
        trips, 
        on = trip_cols,
        how = "inner",
        validate = "1:1"
    ).drop(columns = drop_cols)
    
    return gdf


if __name__=="__main__":   
    # Grab Amtrak thruway bus trips
    trips = grab_amtrak_thruway_trips(SELECTED_DATE)

    # Find corresponding stops / stop times for those trips
    stops, stop_times = grab_stops_stop_times_via_trips(trips, SELECTED_DATE)
    
    # At the trip-level, string together stop's point geom into line geom
    # Also add in origin/destination points to help group
    # trips going to the same places (similar to how route info is)
    trips_with_line_geom = array_stop_times_into_linestrings(stops, stop_times)
    
    # Combine trip's line geom with trip-level df
    gdf = merge_trips_with_linestring(trips, trips_with_line_geom)
    
    utils.geoparquet_gcs_export(
        gdf, 
        GCS_FILE_PATH,
        f"amtrak_thruway_trips_{SELECTED_DATE}"
    )