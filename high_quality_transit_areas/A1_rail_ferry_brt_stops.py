"""
Assemble major transit stops for rail, BRT, and ferry 
and export to GCS.

Turn rail_ferry_brt.ipynb and combine_and_visualize.ipynb 
into scripts.
"""
import datetime
import geopandas as gpd
import intake
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis import utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import COMPILED_CACHED_VIEWS
from update_vars import GCS_FILE_PATH, analysis_date

catalog = intake.open_catalog("*.yml")

ac_transit_route_id = ["1T"]
metro_route_desc = ["METRO SILVER LINE", "METRO ORANGE LINE", 
                    "METRO J LINE", "METRO G LINE"]

muni_route_id = [
    '1', '1X', '2',
    '8', '8AX', '8BX', '9', '9R',
    '12', '14', '14R', '15', '19', '22', '27', '28', 
    '30', '33', '36', '38', '38R', '45', '49', '55',
    '90', '91', '714','TBUS',              
]

muni_brt_include = pd.read_parquet(
    f"{GCS_FILE_PATH}operator_input/muni_brt_stops.parquet"
).stop_id.tolist()

'''
Eric double checked for bus shelters
'15820', '13460', #  Flower/Adams, Fig/Adams; no shelter
'1813', #  Flower/23rd; no shelter
'2378', #  Flower/Washington; no shelter
'2377', #  Flower/Pico; no shelter
'4675', #  Flower/Olympic; no shelter
'3674', #  Flower/7th; no shelter
'15713', #  6th/Flower; no shelter
'5378', #  Olive/5th; no shelter
'70500012', #  Olive/2nd; no shelter
'8704', #  Arcadia/Los Angeles; no shelter
'''

# Certain stops from Metro must be excluded
metro_j_exclude = [
    # Flower from 110 going towards DTLA -- no shelters, yes bus lanes
    '15820', '13460', '1813', '2378', '2377', '4675', '3674',
    # Grand DTLA - no bus lanes 
    '13560', 
    # Grand DTLA - no shelters, yes bus lanes
    '13561',
    # 6th St DTLA -- no shelters, yes bus lanes
    '15713',    
    # Olive St DTLA -- no shelters, yes bus lanes
    '5378', '70500012', 
    # 1st St DTLA
    '5377', '15612',
    # Spring St DTLA on grand park side...no bus lanes?
    '12416', 
    # Aliso -- no shelters, yes bus lanes
    '8704',
    # Figueroa St Gardena/Carson before it gets onto 110
    '65300038', '65300039',
    # Beacon St Harbor Gateway area
    '378', '3124', '3153', '2603', 
    # Pacific Ave Harbor Gateway area
    '3821', '12304', '13817', '5408', '5410', '5411', 
    '13802', '5395', '13803', '5396', '13804', '5397',
    '13805', '141012',
]

def assemble_stops(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Start with stop_times, attach stop geometry, 
    and also route info (route_type) from trips table.
    """
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "schedule_gtfs_dataset_key",
                   "stop_id", "trip_instance_key"],
        with_direction = True,
        get_pandas = True
    )
    
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = [
            "name",
            "trip_instance_key", 
            "route_id", "route_type", "route_desc"
        ],
        get_pandas = True
    )
       
    stops_with_route = pd.merge(
        stop_times,
        trips,
        on = "trip_instance_key",
        how = "inner"
    ).drop(
        columns = "trip_instance_key"
    ).drop_duplicates().reset_index(drop=True)
        
    # Attach stop geometry
    stops = helpers.import_scheduled_stops(
        analysis_date,
        columns = ["feed_key", "stop_id", "stop_name", "geometry"],
        get_pandas = True
    )
    
    stops_with_geom = pd.merge(
        stops, 
        stops_with_route,
        on = ["feed_key", "stop_id"],
        how = "inner"
    )
    
    return stops_with_geom


def grab_rail_stops(
    gdf: gpd.GeoDataFrame, 
    route_types: list = ['0', '1', '2']
) -> gpd.GeoDataFrame:
    """
    Grab all the rail stops.
    """           
    return gdf[
        gdf.route_type.isin(route_types)
    ].reset_index(drop=True).assign(hqta_type = "major_stop_rail")
  

def grab_ferry_stops(
    gdf: gpd.GeoDataFrame, 
    route_types: list = ['4']
) -> gpd.GeoDataFrame:
    """
    Grab all the ferry stops.
    """    
    # only stops without bus service
    angel_and_alcatraz = ['2483552', '2483550', '43002']         
    
    return gdf[
        (gdf.route_type.isin(route_types)) & 
        ~(gdf.stop_id.isin(angel_and_alcatraz))
    ].reset_index(drop=True).assign(hqta_type = "major_stop_ferry")


def grab_brt_stops(
    gdf: gpd.GeoDataFrame, 
    route_types: list = ["3"]
) -> gpd.GeoDataFrame:
    """
    Start with the stops that has route information
    and start filtering based on operator name, route_id / route_desc, 
    and stop_ids to include or exclude.
    
    The stop id lists were manually provided (by Muni) and/or verified by us.
    """
    metro_name = "LA Metro Bus Schedule"
    muni_name = "Bay Area 511 Muni Schedule"
    ac_transit_name = "Bay Area 511 AC Transit Schedule"
    # Omni BRT -- too infrequent! "route_short_name": ["sbX"]
    
    BRT_ROUTE_FILTERING = {
        "Bay Area 511 AC Transit Schedule": {"route_id": ac_transit_route_id},
        "LA Metro Bus Schedule": {"route_desc": metro_route_desc},
    }   
    
    brt_operator_stops = gdf[
        (gdf.route_type.isin(route_types)) & 
        (gdf.name.isin([metro_name, muni_name, ac_transit_name])) 
    ]
    
    muni_brt = brt_operator_stops[
        (brt_operator_stops.name == muni_name) & 
        (brt_operator_stops.route_id.isin(muni_route_id)) & 
        (brt_operator_stops.stop_id.isin(muni_brt_include))
    ]
    
    # For Metro, unable to filter out non-station stops using GTFS, manual list
    metro_brt = brt_operator_stops[
        (brt_operator_stops.name == metro_name) & 
        (brt_operator_stops.route_desc.isin(metro_route_desc)) & 
        ~(brt_operator_stops.stop_id.isin(metro_j_exclude))
    ]
    
    ac_transit_brt = brt_operator_stops[
        (brt_operator_stops.name == ac_transit_name) & 
        (brt_operator_stops.route_id.isin(ac_transit_route_id))
    ]
  
    brt_stops = pd.concat(
        [muni_brt, metro_brt, ac_transit_brt], axis=0
    ).reset_index(drop=True).assign(hqta_type = "major_stop_brt")
    
    return brt_stops


def compile_rail_ferry_brt_stops(
     list_of_files: list
 ) -> gpd.GeoDataFrame:
    """
    Prepare the rail / ferry / BRT stops to be assembled with
    the bus_hqta types and saved into the hqta_points file.
    """
    df = pd.concat(
        list_of_files, 
        axis=0, ignore_index=True
    )
    
    keep_cols = [
        "schedule_gtfs_dataset_key", "feed_key", 
        "stop_id", "stop_name",
        "route_id", "route_type",
        "hqta_type", "geometry"
    ]
    
    df2 = (df[keep_cols]
           .sort_values(["feed_key", "stop_id"]).reset_index(drop=True)
           .rename(columns = {
               "feed_key": "feed_key_primary",
               "schedule_gtfs_dataset_key": "schedule_gtfs_dataset_key_primary"
           })
    )

    return df2 


if __name__ == "__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")

    start = datetime.datetime.now()
    
    stops_route_gdf = assemble_stops(analysis_date)
    
    rail_stops = grab_rail_stops(stops_route_gdf)
    ferry_stops = grab_ferry_stops(stops_route_gdf)
    brt_stops = grab_brt_stops(stops_route_gdf)
    
    major_transit_stops = compile_rail_ferry_brt_stops(
        [rail_stops, ferry_stops, brt_stops]
    )
    
    utils.geoparquet_gcs_export(
        major_transit_stops, 
        GCS_FILE_PATH, 
        "rail_brt_ferry"
    )
    
    end = datetime.datetime.now()
    
    logger.info(
        f"A1_rail_ferry_brt_stops {analysis_date} "
        f"execution time: {end - start}"
    )