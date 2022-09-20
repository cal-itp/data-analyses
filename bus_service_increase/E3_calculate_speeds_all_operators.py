"""
Get the generated speedmap metrics.

Calculate once for each operator, then concatenate and save.

Based on rt_delay/rt_filter_map_plot.py,
and just expand to do grouping across trips / operators.

Need this script to keep it at stop-id level, speed for stop-to-stop segments.
This is still point data, calculated at the stop-id level.
"""
import geopandas as gpd
import glob
import os
import pandas as pd

from shared_utils import (rt_utils, geography_utils, 
                          gtfs_utils, utils)
from E0_bus_oppor_vars import ANALYSIS_DATE, COMPILED_CACHED_GCS
from G2_aggregated_route_stats import ANALYSIS_MONTH_DAY

def generate_speeds(df, itp_id):
    trip_cols = [
        "calitp_itp_id", "route_id", 
        "shape_id", "trip_id",
    ]

    sort_cols = trip_cols + ["stop_sequence"]

    df = (df.assign(
            calitp_itp_id = itp_id,
            arrival_time = pd.to_datetime(df.arrival_time),
            actual_time = pd.to_datetime(df.actual_time),
        ).sort_values(sort_cols)
          .reset_index(drop=True)
    )
    
    # Do the shift to grab prior metric, so we can calculate change
    df = df.assign(
        actual_time_last = (df.sort_values(sort_cols)
                            .groupby(trip_cols)["actual_time"]
                            .apply(lambda x: x.shift(1))
                           ),
        last_loc = (df.sort_values(sort_cols)
                    .groupby(trip_cols)["shape_meters"]
                    .apply(lambda x: x.shift(1))
                   ),
        delay_prior = (df.sort_values(sort_cols)
                       .groupby(trip_cols)["delay_seconds"]
                       .apply(lambda x: x.shift(1))
                      )
    )
    
    
    df = df.assign(
        seconds_from_last = df.apply(
            lambda x: pd.Timedelta(x.actual_time-x.actual_time_last).seconds, 
            axis=1),
        meters_from_last = df.shape_meters - df.last_loc,
        delay_chg_sec = df.delay_seconds - df.delay_prior,
    )
    
    df = df.assign(
        speed_mph = (df.meters_from_last / df.seconds_from_last) * rt_utils.MPH_PER_MPS
    )
    
    keep_cols = sort_cols + ["stop_id", "stop_name",
                             "shape_meters",
                             "actual_time", "arrival_time", 
                             "seconds_from_last", 
                             "meters_from_last", "delay_chg_sec", 
                             "speed_mph", "geometry"]
    
    df2 = df[keep_cols].astype({
        "stop_sequence": int, 
        "seconds_from_last": "Int64", 
        "delay_chg_sec": "Int64"
    })
    
    return df2


if __name__ == "__main__":
    
    ALL_ITP_IDS = gtfs_utils.ALL_ITP_IDS
    
    # (1) If there's a stop_delay df for the operator, run it through more processing
    # Concatenate for all operators
    gdf = gpd.GeoDataFrame()
    
    for itp_id in ALL_ITP_IDS:
        filename = f"{itp_id}_{ANALYSIS_MONTH_DAY}.parquet"
        path = rt_utils.check_cached(filename, 
                                     GCS_FILE_PATH = rt_utils.GCS_FILE_PATH, 
                                     subfolder = "stop_delay_views/")

        if path:    
            df = gpd.read_parquet(path)
            
            df2 = generate_speeds(df, itp_id).to_crs(geography_utils.WGS84)
            print(f"{itp_id}: finished")
            
            df.to_parquet(f"./data/speeds_{itp_id}_{ANALYSIS_DATE}.parquet")
  
            gdf = pd.concat([gdf, df2], axis=0, ignore_index=True)

    
    gdf = gdf.sort_values(["calitp_itp_id", "route_id", 
                           "trip_id", "stop_sequence"]).reset_index(drop=True)
    
    utils.geoparquet_gcs_export(gdf, 
                                COMPILED_CACHED_GCS, 
                                f"all_operators_segment_speed_views_{ANALYSIS_DATE}"
                               )
    
    print("Concatenated, exported all operators to GCS")
    
    for f in glob.glob("./data/speeds_*.parquet"):
        os.remove(f)
    
    print("Remove all local parquets")        
    
    

