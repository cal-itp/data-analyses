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

from calitp.tables import tbl
from siuba import *

import utilities
from update_vars import analysis_date, COMPILED_CACHED_VIEWS


def trip_keys_for_route_type(analysis_date: str, 
                             route_type_list: list) -> pd.DataFrame: 
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")
    
    # Choose to output df instead of list because we need route_type later on
    trip_keys_for_type = (trips[trips.route_type.isin(route_type_list)]
                          [["trip_key", "route_type"]]
                          .drop_duplicates()
                          .compute()
                         )
    
    return trip_keys_for_type


def trip_keys_to_stop_keys(trip_key_df: pd.DataFrame) -> pd.DataFrame:
    
    stop_keys_list = (tbl.views.gtfs_schedule_index_feed_trip_stops() 
                      >> filter(_.trip_key.isin(trip_key_df.trip_key))
                      >> select(_.trip_key, _.stop_key)
                      >> distinct()
                      >> collect()
    )
    
    stop_keys_df = pd.merge(stop_keys_list, 
                            trip_key_df,
                            on = "trip_key",
                            how = "left",
    )
    
    return stop_keys_df


def grab_stops_by_stop_key(analysis_date: str, 
                           trip_stop_df: pd.DataFrame
                          ) -> gpd.GeoDataFrame:
    stops = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet")
    
    stops_for_route_type = dd.merge(
        stops,
        trip_stop_df,
        on = "stop_key",
        how = "inner"
    )
    
    # Drop trip_key and drop duplicates
    stops_for_route_type2 = (stops_for_route_type
                             .drop(columns = "trip_key")
                             .drop_duplicates()
                             .reset_index(drop=True)
                             .compute()
                            )
    
    return stops_for_route_type2
    

def grab_rail_data(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Grab all the rail routes by subsetting routes table down to certain route types.
    
    Combine it routes with stop point geom.
    Returns gpd.GeoDataFrame.
    """
    # Grab the different route types for rail from route tables
    rail_route_types = ['0', '1', '2']
    
    # Grab trip_keys associated with this route_type
    rail_trip_keys = trip_keys_for_route_type(analysis_date, rail_route_types)
    
    # Go from trip_keys to stop_keys to stops
    rail_stop_keys = trip_keys_to_stop_keys(rail_trip_keys)
    
    rail_stops = grab_stops_by_stop_key(analysis_date, rail_stop_keys)
    
    rail_stops = utilities.clip_to_ca(rail_stops)
    
    return rail_stops


def grab_operator_brt(itp_id: int, analysis_date: str):
    """
    Grab BRT routes, stops data for certain operators in CA by analysis date.
    """
        
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")
    
    operator_trips = trips[trips.calitp_itp_id==itp_id]
    
    # Filter within specific operator, each operator has specific filtering condition
    # If it's not one of the ones listed, raise an error
    BRT_OPERATORS = [
        182, 4, 282, 
        # 232, # Omni BRT too infrequent 
    ]
    
    BRT_ROUTE_FILTERING = {
        # LA Metro BRT
        182: {"route_desc": ["METRO SILVER LINE", "METRO ORANGE LINE"]},
              #["901", "910"]
        # AC Transit BRT
        4: {"route_id": ['1T']},
        # Omni BRT -- too infrequent!
        #232: {"route_short_name": ['sbX']},
        # Muni
        282: {"route_short_name": ['49']}
    }
    
    col, filtering_list = list(BRT_ROUTE_FILTERING[itp_id].items())[0]     
    brt_trips = operator_trips[operator_trips[col].isin(filtering_list)]
        
    # Grab trip_keys associated with this operator's BRT routes
    brt_trip_keys = (brt_trips[["trip_key", "route_type"]]
                     .drop_duplicates()
                     .compute()
                    )
    
    # Go from trip_keys to stop_keys to stops
    brt_stop_keys = trip_keys_to_stop_keys(brt_trip_keys)
    
    brt_stops = grab_stops_by_stop_key(analysis_date, brt_stop_keys)
    
    operator_brt_stops = utilities.clip_to_ca(brt_stops)    
    
    if itp_id not in BRT_OPERATORS:
        raise KeyError("Operator does not have BRT route filtering condition set.")
    
    return operator_brt_stops


def additional_brt_filtering_out_stops(df: gpd.GeoDataFrame, 
                                       itp_id: int, 
                                       filtering_list: list) -> gpd.GeoDataFrame:
    """
    df: geopandas.GeoDataFrame
        Input BRT stops data
    itp_id: int
    filtering_list: list of stop_ids
    """
    if itp_id == 182:
        brt_df_stops = df >> filter(-_.stop_id.isin(filtering_list))
        
    elif itp_id == 282:
        brt_df_stops = df >> filter(_.stop_id.isin(filtering_list))
    
    return brt_df_stops


def grab_ferry_data(analysis_date: str):
    ferry_route_types = ['4']
    
    # Grab trip_keys associated with this route_type
    ferry_trip_keys = trip_keys_for_route_type(analysis_date, ferry_route_types)

    # Go from trip_keys to stop_keys to stops
    ferry_stop_keys = trip_keys_to_stop_keys(ferry_trip_keys)
    
    ferry_stops = grab_stops_by_stop_key(analysis_date, ferry_stop_keys)
        
    # TODO: only stops without bus service, implement algorithm
    angel_and_alcatraz = ['2483552', '2483550', '43002'] 
    
    ferry_stops = ferry_stops >> filter(-_.stop_id.isin(angel_and_alcatraz))
    
    return ferry_stops