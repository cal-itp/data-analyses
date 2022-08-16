import datetime
import geopandas as gpd
import pandas as pd

import create_parallel_corridors
from shared_utils import gtfs_utils, geography_utils
from calitp.storage import get_fs
fs = get_fs()

analysis_date = datetime.date(2022, 5, 4)
date_str = gtfs_utils.format_date(analysis_date)
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/traffic_ops/"


if __name__ == "__main__":
    '''
    # Grab trips table for selected date
    
    gtfs_utils.all_trips_or_stoptimes_with_cached(
        dataset="trips",
        analysis_date = analysis_date,
        export_path = GCS_FILE_PATH
    )
    
    # Grab routelines for selected date (line geom)
    
    gtfs_utils.all_routelines_or_stops_with_cached(
        dataset="routelines",
        analysis_date = analysis_date,
        export_path = GCS_FILE_PATH
    )
    '''
    
    routelines = gpd.read_parquet(f"{GCS_FILE_PATH}routelines_{date_str}.parquet")
    trips = pd.read_parquet(f"{GCS_FILE_PATH}trips_{date_str}.parquet")
    
    trips_with_geom = pd.merge(
        routelines,
        trips,
        on = ["calitp_itp_id", "calitp_url_number", "shape_id"],
        how = "inner",
        validate = "1:m",
    ).rename(columns = {"calitp_itp_id": "itp_id"})

    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet= geography_utils.FEET_PER_MI, 
        alternate_df = trips_with_geom,
        pct_route_threshold = 0.3,
        pct_highway_threshold = 0.1,
        DATA_PATH = GCS_FILE_PATH, 
        FILE_NAME = f"parallel_or_intersecting_{date_str}"
    )
    
    # 50 ft buffers, get routes that are 
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet=50, 
        alternate_df = trips_with_geom,
        pct_route_threshold = 0.1, 
        pct_highway_threshold = 0,
        DATA_PATH = GCS_FILE_PATH, 
        FILE_NAME = f"routes_on_shn_{date_str}"
    )    