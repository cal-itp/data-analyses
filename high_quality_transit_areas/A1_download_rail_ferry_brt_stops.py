"""
Download rail, ferry, BRT stops.

From rail_ferry_brt.ipynb into script.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from segment_speed_utils import helpers
from update_vars import analysis_date, COMPILED_CACHED_VIEWS, TEMP_GCS


def filter_trips_to_route_type(analysis_date: str, 
                               route_types: list = []) -> pd.DataFrame:
    """
    Can use route_type_* from stops table, but since BRT needs to start 
    from trips, might as well just get it from trips.
    """
    
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "name", "trip_id", 
                 "route_id", "route_type"],
    )
    
    if isinstance(route_types, list):
        trips_subset = trips[trips.route_type.isin(route_types)]
    
    elif route_types == "brt": 
        trips_subset = filter_to_brt_trips(trips)
        
    trips_subset = (trips_subset
                    .drop_duplicates()
                    .reset_index(drop=True)
                   )
    
    return trips_subset
    

def filter_to_brt_trips(trips: pd.DataFrame) -> pd.DataFrame:
    """
    Start with trips table and filter to specific routes that
    are BRT
    """    
    BRT_ROUTE_FILTERING = {
        "Bay Area 511 AC Transit Schedule": {"route_id": 
                                             ["1T"]},
        "LA Metro Bus Schedule": {"route_desc": 
                                  ["METRO SILVER LINE", "METRO ORANGE LINE", 
                                   "METRO J LINE", "METRO G LINE"
                                  ]},
        "Bay Area 511 Muni Schedule": {"route_short_name": 
                                       ['49']},
        # Omni BRT -- too infrequent!
        #"OmniTrans Schedule": {"route_short_name": ["sbX"]}
    }             
    
    all_brt_trips = pd.DataFrame()
    
    for name, filtering_cond in BRT_ROUTE_FILTERING.items():
        for col, filtering_list in filtering_cond.items():
            trips_subset = trips[
                (trips.name == name) & 
                (trips[col].isin(filtering_list))]
            
            all_brt_trips = pd.concat([all_brt_trips, trips_subset], axis=0)
    
    return all_brt_trips
    

def filter_unique_stops_for_trips(
    analysis_date: str, 
    trip_df: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Start with all operators' stop_times, and narrow down to the trip_ids
    present for the route_type and keep the unique stops.
    
    Then attach the stop's point geometry.
    """
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        with_direction = False,
        get_pandas = True
    )
    
    keep_stop_cols = [
        "feed_key", "name", 
        "stop_id", 
        "route_id", "route_type",         
        # let's keep route_id, since we double check in a notebook
    ]
    
    stops_for_trips = pd.merge(
        stop_times,
        trip_df,
        on = ["feed_key", "trip_id"],
        how = "inner"
    )[keep_stop_cols].drop_duplicates().reset_index(drop=True)
        
    # Attach stop geometry
    stops = helpers.import_scheduled_stops(
        analysis_date,
    )
    
    stops_with_geom = pd.merge(
        stops, 
        stops_for_trips,
        on = ["feed_key", "stop_id"],
        how = "inner"
    )[keep_stop_cols + ["stop_name", "geometry"]]
    
    return stops_with_geom
    

def grab_rail_data(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Grab all the rail stops.
    """
    rail_route_types = ['0', '1', '2']            
    
    rail_trips = filter_trips_to_route_type(analysis_date, rail_route_types)
    rail_stops = filter_unique_stops_for_trips(analysis_date, rail_trips)
                    
    utils.geoparquet_gcs_export(
        rail_stops,
        TEMP_GCS,
        "rail_stops"
    )
    
    
def grab_brt_data(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Grab BRT routes, stops data for certain operators in CA by analysis date.
    """
                             
    brt_trips = filter_trips_to_route_type(analysis_date, "brt")
    brt_stops = filter_unique_stops_for_trips(analysis_date, brt_trips)
            
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
    metro_stops_exclude = filtering_dict["LA Metro Bus Schedule"]
    metro2 = metro[~metro.stop_id.isin(metro_stops_exclude)]
    
    muni_stops_include = filtering_dict["Bay Area 511 Muni Schedule"]
    muni2 = muni[muni.stop_id.isin(muni_stops_include)]

    brt_df_stops = (pd.concat([
            metro2,
            muni2, 
            subset_no_filtering
        ], axis=0)
        .sort_values(["feed_key", "name"])
        .reset_index(drop=True)
    )
    
    return brt_df_stops


def grab_ferry_data(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Grab all the ferry stops.
    """
    ferry_route_types = ['4']
        
    ferry_trips = filter_trips_to_route_type(analysis_date, ferry_route_types)
    ferry_stops = filter_unique_stops_for_trips(analysis_date, ferry_trips)

    # only stops without bus service
    angel_and_alcatraz = ['2483552', '2483550', '43002'] 
    
    ferry_stops = ferry_stops[
        ~ferry_stops.stop_id.isin(angel_and_alcatraz)
    ].reset_index(drop=True)
    
    utils.geoparquet_gcs_export(
        ferry_stops,
        TEMP_GCS,
        "ferry_stops"
    )
