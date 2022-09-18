"""
Script to create PMAC data
"""
import datetime
import geopandas as gpd
import os
import pandas as pd
import sys

os.environ["CALITP_BQ_MAX_BYTES"] = str(130_000_000_000)

from loguru import logger

from bus_service_utils import create_parallel_corridors, gtfs_build
from shared_utils import gtfs_utils, geography_utils, portfolio_utils
from update_vars import ANALYSIS_DATE, BUS_SERVICE_GCS, COMPILED_CACHED_GCS


logger.add("./logs/A1_generate_routes_on_shn_data.log")
logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")


def merge_routelines_with_trips(selected_date: str) -> gpd.GeoDataFrame: 
    """
    Merge routes and trips to get line geometry.
    """
    routelines = gpd.read_parquet(
        f"{COMPILED_CACHED_GCS}routelines_{selected_date}_all.parquet")
    trips = pd.read_parquet(f"{COMPILED_CACHED_GCS}trips_{selected_date}_all.parquet")
    
    EPSG_CODE = routelines.crs.to_epsg()
    
    df = gtfs_build.merge_routes_trips(
        routelines, trips, 
        merge_cols = ["calitp_itp_id", "calitp_url_number", "shape_id"],
        crs = f"EPSG: {EPSG_CODE}")
    
    df2 = (df[df._merge=="both"]
          .rename(columns = {"calitp_itp_id": "itp_id"})
          .reset_index(drop=True)
         )
    
    return df2


def get_total_service_hours(selected_date):
    # Run a query that aggregates service hours to shape_id level
    trip_cols = ["calitp_itp_id", "calitp_url_number", 
                 "route_id", "shape_id"]
    
    # exclude 200!
    itp_ids_on_day = (portfolio_utils.add_agency_name(selected_date)
                      >> filter(_.calitp_itp_id != 200)
                      >> select(_.calitp_itp_id)
                      >> distinct()
                     )
    
    ITP_IDS = itp_ids_on_day.calitp_itp_id.tolist()
    
    trips_with_hrs = gtfs_utils.get_trips(
        selected_date = selected_date,
        itp_id_list = ITP_IDS,
        trip_cols = None,
        get_df = True # only when it's True can the Metrolink fix get applied
    ) 
    
    trips_with_hrs.to_parquet(f"./data/trips_with_hrs_staging_{selected_date}.parquet")
    
    aggregated_hrs = (trips_with_hrs.groupby(trip_cols)
                      .agg({"service_hours": "sum"})
                      .reset_index()
                      .rename(columns = {"service_hours": "total_service_hours"})
                      .drop_duplicates()
    )

    aggregated_hrs.to_parquet(
        f"{BUS_SERVICE_GCS}trips_with_hrs_{selected_date}.parquet")
    
    # Once aggregated dataset written to GCS, remove local cache
    os.remove(f"./data/trips_with_hrs_staging_{selected_date}.parquet")

    
    
if __name__ == "__main__":    
    
    start = datetime.datetime.now()
    
    # Use concatenated routelines and trips from traffic_ops work
    # Use the datasets with Amtrak added back in (rt_delay always excludes Amtrak)
    trips_with_geom = merge_routelines_with_trips(ANALYSIS_DATE)
    
    # 50 ft buffers, get routes that are on SHN
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet=50, 
        alternate_df = trips_with_geom,
        pct_route_threshold = 0.2, 
        pct_highway_threshold = 0,
        DATA_PATH = BUS_SERVICE_GCS, 
        FILE_NAME = f"routes_on_shn_{ANALYSIS_DATE}"
    )  
    
    time1 = datetime.datetime.now()
    logger.info(f"routes on SHN created: {time1 - start}")

    # Grab other routes where at least 35% of route is within 0.5 mile of SHN
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet= geography_utils.FEET_PER_MI * 0.5, 
        alternate_df = trips_with_geom,
        pct_route_threshold = 0.2,
        pct_highway_threshold = 0,
        DATA_PATH = BUS_SERVICE_GCS, 
        FILE_NAME = f"parallel_or_intersecting_{ANALYSIS_DATE}"
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"routes within half mile buffer created: {time2 - time1}")
    
    # Get aggregated service hours by shape_id
    #get_total_service_hours(ANALYSIS_DATE)
    
    time3 = datetime.datetime.now()
    logger.info(f"downloaded service hours at shape_id level: {time3 - time2}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")