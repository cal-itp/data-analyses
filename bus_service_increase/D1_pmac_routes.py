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
from shared_utils import gtfs_utils, geography_utils, rt_dates

TRAFFIC_OPS_GCS = "gs://calitp-analytics-data/data-analyses/traffic_ops/"
ANALYSIS_DATE = rt_dates.PMAC["Q2_2022"] 

def get_total_service_hours(selected_date):
    # Run a query that aggregates service hours to shape_id level
    trip_cols = ["calitp_itp_id", "calitp_url_number", 
                 "route_id", "shape_id"]
    
    trips_with_hrs = (tbl.views.gtfs_schedule_fact_daily_trips()
             >> filter(_.service_date == selected_date, 
                       _.is_in_service==True, 
                       _.calitp_itp_id != 200)
             >> select(_.trip_key, _.service_date, _.service_hours)
             >> inner_join(_, 
                           tbl.views.gtfs_schedule_dim_trips()
                           >> select(*trip_cols, _.trip_key)
                           >> distinct(), 
                           on = "trip_key")
             >> group_by(_.calitp_itp_id, _.calitp_url_number, _.shape_id, _.route_id)
             >> mutate(total_service_hours = _.service_hours.sum())
             >> select(*trip_cols, _.total_service_hours)
             >> distinct()
             >> collect()
            )

    trips_with_hrs.to_parquet(
        f"{utils.GCS_FILE_PATH}trips_with_hrs_{selected_date}.parquet")


if __name__ == "__main__":    
    
    # Use concatenated routelines and trips from traffic_ops work
    # Use the datasets with Amtrak added back in (rt_delay always excludes Amtrak)
    routelines = gpd.read_parquet(f"{TRAFFIC_OPS_GCS}routelines_{ANALYSIS_DATE}_all.parquet")
    trips = pd.read_parquet(f"{TRAFFIC_OPS_GCS}trips_{ANALYSIS_DATE}_all.parquet")
    
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