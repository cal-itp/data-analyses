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
from shared_utils import rt_utils, geography_utils

catalog = intake.open_catalog("./*.yml")
analysis_date = dt.date(2022, 6, 15) ## Wed, June 15


def get_routes_by_type(route_types, analysis_date):
    routes_on_date = (tbl.views.gtfs_schedule_fact_daily_feed_routes()
         >> filter(_.date == analysis_date)
         >> filter(_.calitp_extracted_at <= analysis_date, 
                   _.calitp_deleted_at >= analysis_date)
        )

    dim_routes = tbl.views.gtfs_schedule_dim_routes()
    routes_date_joined = (routes_on_date
         >> inner_join(_, (dim_routes 
                           >> select(_.route_id, _.route_key, _.route_short_name,
                                     _.route_long_name, _.route_desc, 
                                     _.route_type, _.calitp_itp_id)),
                       on = 'route_key')
         # >> distinct(_.calitp_itp_id, _.route_id, _.route_short_name, 
         #                 _.route_long_name, _.route_desc, _.route_type)
         >> filter(_.calitp_itp_id != 200) # avoid MTC feed in favor of individual operator feeds
         >> filter((_.route_type.isin(route_types)))
         # >> collect()
        )
    
    return routes_date_joined

def routes_to_stops(routes_tbl, analysis_date):
    
    trips_query = (tbl.views.gtfs_schedule_fact_daily_trips()
                   >> filter(_.calitp_extracted_at <= analysis_date, 
                             _.calitp_deleted_at >= analysis_date)
                   >> filter(_.service_date == analysis_date)
                   >> filter(_.is_in_service == True)
                   >> select(_.trip_key, _.service_date, _.route_id, _.calitp_itp_id)
                   >> inner_join(_, routes_tbl, 
                                 on = ['calitp_itp_id', 'route_id'])
    )
    
    trips_ix_query = (trips_query
                      >> inner_join(_, 
                                    tbl.views.gtfs_schedule_index_feed_trip_stops(), 
                                    on = 'trip_key')
                      >> select(-_.calitp_url_number, 
                                -_.calitp_extracted_at, -_.calitp_deleted_at)
    )
    
    stops = (tbl.views.gtfs_schedule_dim_stops()
             >> distinct(_.calitp_itp_id, _.stop_id,
                         _.stop_lat, _.stop_lon, _.stop_name, _.stop_key)
             >> inner_join(_, 
                           (trips_ix_query 
                            >> distinct(_.stop_key, _.route_type)), 
                           on = 'stop_key')
             >> collect()
             >> distinct(_.calitp_itp_id, _.stop_id, _keep_all=True) 
             ## should be ok to drop duplicates, but must use stop_id for future joins...
             >> select(-_.stop_key)
            
    )

    stops = (gpd.GeoDataFrame(stops, 
                             geometry=gpd.points_from_xy(
                                 stops.stop_lon, stops.stop_lat
                             ), crs=geography_utils.WGS84)
             .to_crs(geography_utils.CA_NAD83Albers)
            )
    
    # Clip to CA
    ca = catalog.ca_boundary.read().to_crs(geography_utils.CA_NAD83Albers)

    return stops.clip(ca)


def grab_rail_data(analysis_date):
    # Grab the different route types for rail from route tables
    rail_route_types = ['0', '1', '2']
    new_routes = get_routes_by_type(rail_route_types, analysis_date)
        
    # Grab rail stops and clip to CA    
    rail_stops = routes_to_stops(new_routes, analysis_date)
    
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