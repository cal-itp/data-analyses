"""
Script to create PMAC data
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(130_000_000_000)

import geopandas as gpd
import pandas as pd

from calitp.tables import tbl
from siuba import *

import create_parallel_corridors
import utils
from shared_utils import gtfs_utils, geography_utils, portfolio_utils, rt_dates

COMPILED_CACHED_GCS = "gs://calitp-analytics-data/data-analyses/rt_delay/compiled_cached_views/"
ANALYSIS_DATE = rt_dates.PMAC["Q2_2022"] 

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
        f"{utils.GCS_FILE_PATH}trips_with_hrs_{selected_date}.parquet")
    
    # Once aggregated dataset written to GCS, remove local cache
    os.remove(f"./data/trips_with_hrs_staging_{selected_date}.parquet")


if __name__ == "__main__":    
    
    # Use concatenated routelines and trips from traffic_ops work
    # Use the datasets with Amtrak added back in (rt_delay always excludes Amtrak)
    routelines = gpd.read_parquet(
        f"{COMPILED_CACHED_GCS}routelines_{ANALYSIS_DATE}_all.parquet")
    trips = pd.read_parquet(f"{COMPILED_CACHED_GCS}trips_{ANALYSIS_DATE}_all.parquet")
    
    shape_id_cols = ["calitp_itp_id", "calitp_url_number", "shape_id"]
    
    trips_with_geom = pd.merge(
        routelines[shape_id_cols + ["geometry"]].drop_duplicates(subset=shape_id_cols),
        trips,
        on = shape_id_cols,
        how = "inner",
        validate = "1:m",
    ).rename(columns = {"calitp_itp_id": "itp_id"})

    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet= geography_utils.FEET_PER_MI, 
        alternate_df = trips_with_geom,
        pct_route_threshold = 0.3,
        pct_highway_threshold = 0.1,
        DATA_PATH = utils.GCS_FILE_PATH, 
        FILE_NAME = f"parallel_or_intersecting_{ANALYSIS_DATE}"
    )
    
    # 50 ft buffers, get routes that are 
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet=50, 
        alternate_df = trips_with_geom,
        pct_route_threshold = 0.1, 
        pct_highway_threshold = 0,
        DATA_PATH = utils.GCS_FILE_PATH, 
        FILE_NAME = f"routes_on_shn_{ANALYSIS_DATE}"
    )    
    
    # Get aggregated service hours by shape_id
    #get_total_service_hours(ANALYSIS_DATE)