"""
Get the generated speedmap metrics.

Calculate once for each operator, then concatenate and save.

Based on rt_delay/rt_filter_map_plot.py,
and just expand to do grouping across trips / operators.

The rt_trips parquets have speed by trip-level
#df = pd.read_parquet(f"{GCS_FILE_PATH}rt_trips/{itp_id}_{date}.parquet")

Need this script to keep it at stop-id level, speed for stop-to-stop segments.
This is still point data, calculated at the stop-id level.
"""
import geopandas as gpd
import glob
import os
import pandas as pd

from shared_utils import (rt_utils, rt_dates, 
                          geography_utils, gtfs_utils, utils)

analysis_date = rt_dates.DATES["may2022"]

month = analysis_date.split('-')[1]
day = analysis_date.split('-')[2]


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
    
    keep_cols = sort_cols + ["actual_time", "arrival_time", 
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

    # get the date format, which uses underscore, not hyphens
    date = f"{month}_{day}"
    
    # (1) If there's a stop_delay df for the operator, run it through more processing
    # Concatenate for all operators
    gdf = gpd.GeoDataFrame()
    
    for itp_id in ALL_ITP_IDS:

        filename = f"{itp_id}_{date}.parquet"

        path = rt_utils.check_cached(filename, 
                                     GCS_FILE_PATH = rt_utils.GCS_FILE_PATH, 
                                     subfolder = "stop_delay_views/")

        if path:    
            df = gpd.read_parquet(path)
            
            df2 = generate_speeds(df, itp_id).to_crs(geography_utils.WGS84)
            print(f"{itp_id}: finished")
            
            df.to_parquet(f"./data/speeds_{itp_id}_{date}.parquet")
  
            gdf = pd.concat([gdf, df2], axis=0, ignore_index=True)

    
    gdf = gdf.sort_values(["calitp_itp_id", "route_id", 
                           "trip_id", "stop_sequence"]).reset_index(drop=True)
    
    utils.geoparquet_gcs_export(gdf, 
                                f"{rt_utils.GCS_FILE_PATH}segment_speed_views/", 
                                f"all_operators_{analysis_date}"
                               )
    
    print("Concatenated, exported all operators to GCS")
    
    for f in glob.glob("./data/speeds_*.parquet"):
        os.remove(f)
    
    print("Remove all local parquets")        

    
    # (2) If there's a rt_trips df for the operator, concatenate for all operators
    df = pd.DataFrame()
    
    for itp_id in ALL_ITP_IDS:
        try:
            trip_df = pd.read_parquet(
                f"{rt_utils.GCS_FILE_PATH}rt_trips/{itp_id}_{date}.parquet")

            df = pd.concat([df, trip_df], axis=0, ignore_index=True)
        except:
            continue
        
    df = df.sort_values(["calitp_itp_id", "route_id", "trip_id"]).reset_index(drop=True)
    
    df.to_parquet(f"{rt_utils.GCS_FILE_PATH}rt_trips/all_operators_{analysis_date}.parquet")

    print("Concatenated all parquets for rt_trips")        

