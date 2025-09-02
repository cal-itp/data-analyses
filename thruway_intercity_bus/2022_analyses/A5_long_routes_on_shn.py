"""
Investigate which local bus routes run mostly on the SHN.

Use 50%+ as threshold, see if some of the long distance
buses that don't travel exactly to Amtrak thruway origin or destination
can be captured. These would be "legs", maybe in the middle
of origin or destination.
"""
import geopandas as gpd
import pandas as pd

import dask_geopandas as dg
import dask.dataframe as dd

from bus_service_utils import create_parallel_corridors, gtfs_build
from shared_utils import rt_utils, rt_dates

COMPILED_CACHED_GCS = f"{rt_utils.GCS_FILE_PATH}compiled_cached_views/"
ANALYSIS_DATE = rt_dates.DATES["sep2022"]

if __name__ == "__main__":
    routelines = dg.read_parquet(
        f"{COMPILED_CACHED_GCS}routelines_{ANALYSIS_DATE}_all.parquet")
    trips = dd.read_parquet(f"{COMPILED_CACHED_GCS}trips_{ANALYSIS_DATE}_all.parquet")

    EPSG_CODE = routelines.crs.to_epsg()

    df = gtfs_build.merge_routes_trips(
        routelines, trips, 
        merge_cols = ["calitp_itp_id", "calitp_url_number", "shape_id"],
        crs = f"EPSG: {EPSG_CODE}",
        join = "inner"
    ).rename(columns = {"calitp_itp_id": "itp_id"})

    # 50 ft buffers, get routes that are on SHN
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet=50, 
        alternate_df = df,
        pct_route_threshold = 0.5, 
        pct_highway_threshold = 0,
        DATA_PATH = "./", 
        FILE_NAME = f"routes_on_shn_{ANALYSIS_DATE}"
    )  