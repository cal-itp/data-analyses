"""
Download rail, ferry, BRT stops.

From rail_ferry_brt.ipynb into script.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000)

import datetime as dt
import geopandas as gpd
import intake
import pandas as pd
import siuba

from calitp.tables import tbl
from siuba import *

import utilities
from shared_utils import geography_utils, gtfs_utils
from update_vars import analysis_date

catalog = intake.open_catalog("./*.yml")

def operators_with_route_type(route_type_list: list) -> list:
    """
    Function to just find a subset of operators, 
    given any route_type list of values.
    
    Use this to pare down which operators to use when
    calling `gtfs_utils`.
    """

    ids_with_route_type = (
        tbl.views.gtfs_schedule_dim_routes() 
        >> filter(_.route_type.isin(route_type_list))
        >> select(_.calitp_itp_id)
        # Always exclude 200 in favor of agency feeds
        >> filter(_.calitp_itp_id != 200)
        >> distinct()
        >> collect()
    ).calitp_itp_id.tolist()
    
    return ids_with_route_type


def routes_to_stops(routes_tbl: siuba.sql.verbs.LazyTbl, 
                    analysis_date: dt.date) -> gpd.GeoDataFrame:
    """
    Takes routes LazyTbl, and bring in trips and index tables
    to find which stops were present on selected date.
    
    Clip stop point geom to CA boundary.
    
    Returns gpd.GeoDataFrame
    """
    # Get list of ITP IDs to filter trips against, and exclude 200
    subset_itp_ids = (routes_tbl 
         >> filter(_.calitp_itp_id != 200)
         >> select(_.calitp_itp_id)
         >> distinct()
         >> collect()
        ).calitp_itp_id.tolist()

    # Query to find trips on selected day for those rail routes selected 
    trips_query = (tbl.views.gtfs_schedule_fact_daily_trips()
                   >> filter(_.calitp_extracted_at <= analysis_date, 
                             _.calitp_deleted_at >= analysis_date, 
                             _.service_date == analysis_date, 
                             _.is_in_service == True)
                   >> select(_.calitp_itp_id, _.service_date, 
                            _.route_id, _.trip_key)
                   >> filter(_.calitp_itp_id != 200)
                  >> inner_join(_, routes_tbl, 
                            on = ['calitp_itp_id', 'route_id'])
                  >> inner_join(_, 
                                tbl.views.gtfs_schedule_index_feed_trip_stops() 
                                >> select(_.trip_key, _.stop_key), 
                                on = "trip_key")
                 )
    
    # Get list of stop_keys to do custom filtering
    subset_stop_keys = (trips_query 
         >> select(_.stop_key) 
         >> distinct()
         >> collect()
        ).stop_key.tolist()
    
    keep_stop_cols = [
        "calitp_itp_id", "stop_id", 
        "stop_lat", "stop_lon", 
        "stop_name", "stop_key", 
    ]
    
    # Get gdf of stops
    stops = gtfs_utils.get_stops(
        selected_date = analysis_date,
        itp_id_list = subset_itp_ids,
        get_df = True,
        stop_cols = keep_stop_cols,
        crs = geography_utils.CA_NAD83Albers,
        custom_filtering={"stop_key": subset_stop_keys}
    ) 
    
    # trips_query is where route_type is stored
    keep_trip_cols = [
        "calitp_itp_id", "service_date",
        "route_type", "route_id",
        "route_short_name", "route_long_name", "route_desc",
        "stop_key", # need this to join to stops
    ]
    
    trip_info = (trips_query 
                 >> filter(_.stop_key.isin(subset_stop_keys))
                 >> select(*keep_trip_cols)
                 >> distinct()
                 >> collect()
                )
    
    # Merge that back in to stops
    stops = pd.merge(stops,
                     trip_info,
                     on = ["calitp_itp_id", "stop_key"],
                     how = "inner",
    ).drop(columns = "stop_key") # can drop stop_key once we've joined everything
    
    # Clip to CA
    ca = catalog.ca_boundary.read().to_crs(geography_utils.CA_NAD83Albers)

    return stops.clip(ca)


def grab_rail_data(analysis_date: dt.date) -> gpd.GeoDataFrame:
    """
    Grab all the rail routes by subsetting routes table down to certain route types.
    
    Combine it routes with stop point geom.
    Returns gpd.GeoDataFrame.
    """
    # Grab the different route types for rail from route tables
    rail_route_types = ['0', '1', '2']
    
    # Grab the subset of operators that have these route types
    # and use in gtfs_utils.get_route_info
    rail_operators = operators_with_route_type(rail_route_types)
    
    keep_route_cols = [
        "feed_key", "route_key", 
        "calitp_itp_id", "date", "route_id", 
        "route_short_name", "route_long_name", "route_desc", "route_type",
        "calitp_extracted_at", "calitp_deleted_at"
    ]

    rail_routes = gtfs_utils.get_route_info(
        selected_date = analysis_date,
        itp_id_list = rail_operators,
        route_cols = keep_route_cols,
        get_df = False,
        custom_filtering = {"route_type": rail_route_types}
    )
            
    # Grab rail stops and clip to CA    
    rail_stops = routes_to_stops(rail_routes, analysis_date)
    
    return rail_stops


def grab_operator_brt(itp_id: int, analysis_date: dt.date):
    """
    Grab BRT routes, stops data for certain operators in CA by analysis date.
    """
    
    # Filter within specific operator, each operator has specific filtering condition
    # If it's not one of the ones listed, raise an error
    BRT_OPERATORS = [
        182, 4, 282, 
        # 232, # Omni BRT too infrequent 
    ]
    
    keep_route_cols = [
        "calitp_itp_id", "route_id", 
        "route_short_name", "route_long_name", 
        "route_desc", "route_type",
    ]
    
    BRT_ROUTE_FILTERING = {
        # LA Metro BRT
        182: {"route_id": ["901", "910"]},
        # AC Transit BRT
        4: {"route_id": ['1T']},
        # Omni BRT -- too infrequent!
        #232: {"route_short_name": ['sbX']},
        # Muni
        282: {"route_short_name": ['49']}
    }

    operator_brt = gtfs_utils.get_route_info(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        route_cols = keep_route_cols,
        get_df = False,
        custom_filtering = BRT_ROUTE_FILTERING[itp_id]
    )
        
    if itp_id not in BRT_OPERATORS:
        raise KeyError("Operator does not have BRT route filtering condition set.")

    operator_brt_stops = routes_to_stops(operator_brt, analysis_date)
    
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


def grab_ferry_data(analysis_date: dt.date):
    ferry_route_types = ['4']
    ferry_operators = operators_with_route_type(ferry_route_types)

    # For analysis date, grab the different route types from route tables    
    keep_route_cols = [
        "feed_key", "route_key", 
        "calitp_itp_id", "date", "route_id", 
        "route_short_name", "route_long_name", "route_desc", "route_type",
        "calitp_extracted_at", "calitp_deleted_at"
    ]
    
    ferry = gtfs_utils.get_route_info(
        selected_date = analysis_date,
        itp_id_list = ferry_operators,
        route_cols = keep_route_cols,
        get_df = False,
        custom_filtering = {"route_type": ['4']}
    )    
    
    # Grab ferry stops and clip to CA    
    ferry_stops = routes_to_stops(ferry, analysis_date)

    # TODO: only stops without bus service, implement algorithm
    angel_and_alcatraz = ['2483552', '2483550', '43002'] 
    
    ferry_stops = ferry_stops >> filter(-_.stop_id.isin(angel_and_alcatraz))
    
    return ferry_stops