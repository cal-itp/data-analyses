"""
Move rail_ferry_brt.ipynb into script.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(900_000_000_000) ## 800GB?

import datetime as dt
import geopandas as gpd
import intake
import pandas as pd

from calitp.tables import tbl
from siuba import *

import utilities
from shared_utils import rt_utils, geography_utils, gtfs_utils
from update_vars import analysis_date

catalog = intake.open_catalog("./*.yml")

def routes_to_stops(rail_routes_tbl, analysis_date):
    # Get list of ITP IDs to filter trips against, and exclude 200
    rail_itp_ids = list(
        (rail_routes_tbl 
         >> filter(_.calitp_itp_id != 200)
         >> select(_.calitp_itp_id)
         >> distinct()
         >> collect()
        ).calitp_itp_id)

    # Query to find trips on selected day for those rail routes selected 
    rail_trips = (tbl.views.gtfs_schedule_fact_daily_trips()
                  >> filter(_.calitp_extracted_at <= analysis_date, 
                            _.calitp_deleted_at >= analysis_date, 
                            _.service_date == analysis_date, 
                            _.is_in_service == True)
                  >> select(_.trip_key, _.service_date, _.route_id, _.calitp_itp_id)
                  >> inner_join(_, rail_routes_tbl, 
                            on = ['calitp_itp_id', 'route_id'])
                  >> inner_join(_, 
                                tbl.views.gtfs_schedule_index_feed_trip_stops() 
                                >> select(_.trip_key, _.stop_key), 
                                on = "trip_key")
                  >> filter(_.calitp_itp_id != 200)
                 )
    
    # Get list of stop_keys to do custom filtering
    rail_stop_keys = list(
        (rail_trips 
         >> select(_.stop_key) 
         >> distinct()
         >> collect()
        ).stop_key)
    
    keep_stop_cols = [
        "calitp_itp_id", "stop_id", 
        "stop_lat", "stop_lon", 
        "stop_name", "stop_key"
    ]
    
    # Get gdf of rail stops
    rail_stops = gtfs_utils.get_stops(
        selected_date = analysis_date,
        itp_id_list = rail_itp_ids,
        get_df = True,
        stop_cols = keep_stop_cols,
        crs = geography_utils.CA_NAD83Albers,
        custom_filtering={"stop_key": rail_stop_keys}
    ) 
    
    # Clip to CA
    ca = catalog.ca_boundary.read().to_crs(geography_utils.CA_NAD83Albers)

    return stops.clip(ca)


def grab_rail_data(analysis_date):
    # Grab the different route types for rail from route tables
    rail_route_types = ['0', '1', '2']
    
    keep_route_cols = [
        "feed_key", "route_key", 
        "calitp_itp_id", "date", "route_id", 
        "route_short_name", "route_long_name", "route_desc", "route_type"
        "calitp_extracted_at", "calitp_deleted_at"
    ]

    rail_routes = gtfs_utils.get_route_info(
        selected_date = analysis_date,
        itp_id_list = None,
        route_cols = keep_route_cols,
        get_df = False,
        custom_filtering = {"route_type": rail_route_types}
    )
            
    # Grab rail stops and clip to CA    
    rail_stops = routes_to_stops(rail_routes, analysis_date)
    
    return rail_stops


def grab_operator_brt(itp_id, analysis_date):
    """
    Grab BRT routes, stops data for certain operators in CA by analysis date.
    """
    operator_routes = rt_utils.get_routes(itp_id, analysis_date)
    
    # Filter within specific operator, each operator has specific filtering condition
    # If it's not one of the ones listed, raise an error
    BRT_OPERATORS = [182, 4, 282, 
                     # 232, # Omni BRT too infrequent 
                    ]
    # LA Metro BRT
    if itp_id == 182:
        operator_brt = (operator_routes 
                        >> filter(_.route_id.str.contains('901') |
                                  _.route_id.str.contains('910'))
                       )
    # AC Transit BRT
    elif itp_id == 4:
        operator_brt = (operator_routes 
                        >> filter(_.route_id == '1T')
                       )
    # Omni BRT -- too infrequent!
    elif itp_id == 232:
        operator_brt = (operator_routes 
                        >> filter(_.route_short_name == 'sbX')
                       )
    elif itp_id == 282:
        operator_brt = (operator_routes 
                        >> filter(_.route_short_name == '49')
                       )  
        
    elif itp_id not in BRT_OPERATORS:
        raise KeyError("Operator does not have BRT route filtering condition set.")

    operator_brt_stops = routes_to_stops(operator_brt, analysis_date)
    
    return operator_brt_stops


def additional_brt_filtering_out_stops(df, itp_id, filtering_list):
    """
    df: pandas.DataFrame
        Input BRT stops data
    itp_id: int
    filtering_list: list of stop_ids
    """
    if itp_id == 182:
        brt_df_stops = df >> filter(-_.stop_id.isin(filtering_list))
        
    elif itp_id == 282:
        brt_df_stops = df >> filter(_.stop_id.isin(filtering_list))
    
    return brt_df_stops



def grab_ferry_data(analysis_date):
    # For analysis date, grab the different route types from route tables
    ferry = get_routes_by_type(['4'], analysis_date)
        
    # Grab rail stops and clip to CA    
    ferry_stops = routes_to_stops(ferry, analysis_date)

    # TODO: only stops without bus service, implement algorithm
    angel_and_alcatraz = ['2483552', '2483550', '43002'] 
    
    ferry_stops = ferry_stops >> filter(-_.stop_id.isin(angel_and_alcatraz))
    
    return ferry_stops