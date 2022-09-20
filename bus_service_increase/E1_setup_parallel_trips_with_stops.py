"""
Grab stashed trips parquet file.
Add in service hours, narrow it down to 1 trip per route,
and subset it to parallel routes only.

Create a df that has the selected trip per route
with all the stop info (and stop sequence) attached.
"""
import datetime
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import os
import pandas as pd

os.environ["CALITP_BQ_MAX_BYTES"] = str(130_000_000_000)

import shared_utils
from bus_service_utils import utils

ANALYSIS_DATE = shared_utils.rt_dates.PMAC["Q2_2022"]
COMPILED_CACHED = f"{shared_utils.rt_utils.GCS_FILE_PATH}compiled_cached_views/"


def grab_service_hours(selected_date: str, 
                       valid_trip_keys: list) -> pd.DataFrame:
    daily_service_hours = shared_utils.gtfs_utils.get_trips(
        selected_date = selected_date,
        itp_id_list = None,
        # Keep more columns, route_id, shape_id, direction_id so the metrolink fix 
        # can be propagated
        trip_cols = ["calitp_itp_id", "service_date", "trip_key",
                     "route_id", "shape_id", "direction_id", "service_hours"],
        get_df = True,
        custom_filtering = {"trip_key": valid_trip_keys}
    )
    
    daily_service_hours.to_parquet(
        f"{utils.GCS_FILE_PATH}service_hours_{selected_date}.parquet")

    
def merge_trips_with_service_hours(selected_date: str)-> pd.DataFrame:
    
    trips = dd.read_parquet(
        f"{COMPILED_CACHED}trips_{selected_date}.parquet")
    
    daily_service_hours = pd.read_parquet(
        f"{utils.GCS_FILE_PATH}service_hours_{selected_date}.parquet")

    df = dd.merge(
        trips, 
        daily_service_hours[["trip_key", "service_hours"]],
        on = "trip_key",
        how = "inner",
    ).compute()
    
    return df
    
def select_one_trip(df: pd.DataFrame) -> pd.DataFrame:
        
    # Across trip_ids, for the same route_id, there are differing max_stop_sequence
    # Can't use max_stop_sequence to find longest route_length
    # Use service hours instead to find faster trip during free-flowing traffic
    group_cols = ["calitp_itp_id", "route_id"]
    
    # Select a trip that closest to 25th percentile (lower means faster!)
    # This groupby ruins the index, throws an error, so just merge in as separate df
    quantile = (df.groupby(group_cols)["service_hours"]
                .quantile(0.25).reset_index()
                .rename(columns = {"service_hours": "p25"})
               )
    
    df = pd.merge(df, quantile, 
                  on = group_cols,
                  how = "inner",
                  validate = "m:1"
            )
    
    # Select trip that is closest to 25th percentile (min_diff)
    df["difference"] = abs(df.service_hours - df.p25)
    df["min_diff"] = df.groupby(group_cols)["difference"].transform("min")

    df['faster_trip'] = df.apply(
        lambda x: 1 if x.difference == x.min_diff else 0, axis=1)
    
    # If there are multiple trips selected for a route, do a sort/drop duplicates
    df2 = (df[df.faster_trip==1]
           .sort_values(group_cols + ["trip_id"], ascending=[True, True, True])
           .drop_duplicates(subset=group_cols)
           .drop(columns = ["faster_trip", "difference", "min_diff", "p25"])
           .reset_index(drop=True)
          )

    return df2


def grab_stops_for_trip_selected(trip_df: dd.DataFrame, 
                                 selected_date: str) -> gpd.GeoDataFrame:
    
    stop_times = dd.read_parquet(
        f"{COMPILED_CACHED}st_{selected_date}.parquet")
    
    stop_times_for_trip = dd.merge(
        trip_df,
        stop_times[["trip_key", "stop_id", "stop_sequence"]],
        on = "trip_key",
        how = "inner"
    )
    
    # Add in stop geom
    stops = dg.read_parquet(
        f"{COMPILED_CACHED}stops_{selected_date}.parquet"
    )
    
    stop_times_with_geom = dd.merge(
        stops[["calitp_itp_id", "stop_id", "stop_name", "geometry"]].drop_duplicates(),
        stop_times_for_trip,
        on = ["calitp_itp_id", "stop_id"],
        how = "inner"
    ).to_crs(shared_utils.geography_utils.WGS84)
    
    
    stop_times_with_geom2 = (stop_times_with_geom.drop(
        columns = ["calitp_extracted_at", "calitp_deleted_at"])
        .sort_values(["calitp_itp_id", "route_id", "trip_id", "stop_sequence"])
        .reset_index(drop=True)
    ).compute()
    
    return stop_times_with_geom2


if __name__ == "__main__":
    
    trips = dd.read_parquet(
        f"{COMPILED_CACHED}trips_{ANALYSIS_DATE}.parquet")
    
    valid_trip_keys = trips.trip_key.unique().compute().tolist()
    #grab_service_hours(ANALYSIS_DATE, valid_trip_keys)
    
    df = merge_trips_with_service_hours(ANALYSIS_DATE)
    
    one_trip = select_one_trip(df)

    trips_with_stops = grab_stops_for_trip_selected(one_trip, ANALYSIS_DATE)
    
    shared_utils.utils.geoparquet_gcs_export(
        trips_with_stops,
        utils.GCS_FILE_PATH,
        f"trips_with_stops_{ANALYSIS_DATE}"
    )