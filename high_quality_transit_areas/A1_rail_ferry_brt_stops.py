"""
Download rail, ferry, BRT stops.
Export combined rail/ferry/BRT data into GCS.

Clean up the combined rail/ferry/BRT points
and get it ready to be combined with other bus-related points.

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
from update_vars import GCS_FILE_PATH, analysis_date, TEMP_GCS

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

def filter_trips_to_route_type(
    analysis_date: str, 
    route_types: list
) -> pd.DataFrame:
    """
    Can use route_type_* from stops table, but since BRT needs to start 
    from trips, might as well just get it from trips.
    """
    
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "name", "trip_id", 
                   "route_id", "route_type", "route_desc"],
    )
    
    if isinstance(route_types, list):
        trips_subset = trips[trips.route_type.isin(route_types)]
    
    elif route_types == "brt": 
        trips_subset = filter_to_brt_trips(trips)
        
    trips_subset = (trips_subset
                    .drop(columns = "route_desc")
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
        "Bay Area 511 AC Transit Schedule": {"route_id": ac_transit_route_id},
        "LA Metro Bus Schedule": {"route_desc": metro_route_desc},
        "Bay Area 511 Muni Schedule": {"route_id": muni_route_id},
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
    

def grab_rail_data(analysis_date: str):
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
    
    
def grab_brt_data(analysis_date: str):
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
    df: gpd.GeoDataFrame, 
) -> gpd.GeoDataFrame:
    """
    df: geopandas.GeoDataFrame
        Input BRT stops data (combined across operators)
    """
    metro_name = "LA Metro Bus Schedule"
    muni_name = "Bay Area 511 Muni Schedule"
    
    muni = df[df.name == muni_name].query(
        'stop_id in @muni_brt_include'
    )
    
    # For Metro, unable to filter out non-station stops using GTFS, manual list
    metro = df[df.name == metro_name].query(
        'stop_id not in @metro_j_exclude')
    
    muni_metro = pd.concat([muni, metro], axis=0)
    
    other_operators = df[~df.name.isin([metro_name, muni_name])]

    brt_df_stops = pd.concat(
        [muni_metro, other_operators], axis=0
    ).sort_values(["feed_key", "name"]).reset_index(drop=True)
    
    return brt_df_stops


def grab_ferry_data(analysis_date: str):
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

    
def clip_to_ca(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Clip to CA boundaries. 
    """    
    ca = catalog.ca_boundary.read().to_crs(gdf.crs)

    gdf2 = gdf.clip(ca, keep_geom_type = False).reset_index(drop=True)

    return gdf2
    
    
def get_rail_ferry_brt_extract() -> gpd.GeoDataFrame:
    """
    Prepare the rail / ferry / BRT stops to be assembled with
    the bus_hqta types and saved into the hqta_points file.
    """
    df = catalog.rail_brt_ferry_initial.read()
    
    keep_cols = ["feed_key", "name", "stop_id", 
                 "route_type", "geometry"]
    
    rail_types = ["0", "1", "2"]
    bus_types = ["3"]
    ferry_types = ["4"]
    
    df2 = (df[keep_cols].assign(
            hqta_type = df.route_type.map(
                lambda x: "major_stop_rail" if x in rail_types
                else "major_stop_brt" if x in bus_types
                else "major_stop_ferry" if x in ferry_types 
                else "missing" # add flag to make it easier to check results
            )
        ).rename(columns = {"feed_key": "feed_key_primary"})
           .drop(columns = ["route_type", "name"])
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
    
    # Rail
    grab_rail_data(analysis_date)
    rail_stops = gpd.read_parquet(f"{TEMP_GCS}rail_stops.parquet")

    # BRT
    grab_brt_data(analysis_date)
    brt_stops = gpd.read_parquet(f"{TEMP_GCS}brt_stops.parquet")
    brt_stops = additional_brt_filtering_out_stops(
        brt_stops)
    
    # Ferry
    grab_ferry_data(analysis_date)
    ferry_stops = gpd.read_parquet(f"{TEMP_GCS}ferry_stops.parquet")
        
    # Concatenate datasets that need to be clipped to CA
    rail_brt = pd.concat([
        rail_stops,
        brt_stops
    ], axis=0, ignore_index= True).pipe(clip_to_ca)
        
    # Concatenate all together
    rail_brt_ferry = pd.concat([
        rail_brt,
        ferry_stops
    ], axis=0, ignore_index=True)
    
    # Export to GCS
    utils.geoparquet_gcs_export(
        rail_brt_ferry, 
        GCS_FILE_PATH, 
        "rail_brt_ferry"
    )
    
    end = datetime.datetime.now()
    logger.info(f"A1_rail_ferry_brt_stops {analysis_date} "
                f"execution time: {end - start}")