"""
Quick script to put together Big Blue Bus speedmap
segment speeds.

Use this for exploratory work on where 20th, 80th 
percentile speeds seem low...be able to pull out
the full distribution of speeds going into that.

Aligned earlier steps to factor in dwell time, 
only interpolate between the "before" and "after" stop
position point.

Adapt this later into `stop_arrivals_to_speeds` and 
`average_segment_speeds`.
"""
import geopandas as gpd
import pandas as pd

from segment_speed_utils import (gtfs_schedule_wrangling, 
                                 helpers, segment_calcs)
                                 
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT
from shared_utils import rt_dates, rt_utils

analysis_date = rt_dates.DATES["apr2024"]

def calculate_speeds(df: pd.DataFrame, trip_stop_cols: list):
    # Truncate this function for now - this sits in `stop_arrivals_to_speeds`
    # set our own trip_stop_cols for speedmap segments
    trip_cols = ["trip_instance_key"]
    
    
    df = segment_calcs.convert_timestamp_to_seconds(
        df, ["arrival_time"]
    ).sort_values(trip_stop_cols).reset_index(drop=True)
    
    df = df.assign(
        subseq_arrival_time_sec = (df.groupby(trip_cols, 
                                             observed=True, group_keys=False)
                                  .arrival_time_sec
                                  .shift(-1)
                                 ),
        subseq_stop_meters = (df.groupby(trip_cols, 
                                        observed=True, group_keys=False)
                             .stop_meters
                             .shift(-1)
                            )
    )

    speed = df.assign(
        meters_elapsed = df.subseq_stop_meters - df.stop_meters, 
        sec_elapsed = df.subseq_arrival_time_sec - df.arrival_time_sec,
    ).pipe(
        segment_calcs.derive_speed, 
        ("stop_meters", "subseq_stop_meters"), 
        ("arrival_time_sec", "subseq_arrival_time_sec")
    )
    
    return speed


def bbb_speeds_by_trip(analysis_date: str):
    bbb_arrivals = pd.read_parquet(
        f"{SEGMENT_GCS}test_arrivals_{analysis_date}.parquet"
    )
    
    # double check this and make sure list is partially in GTFS_DATA_DICT.trip_stop_cols
    trip_stop_cols = [
        "trip_instance_key", "stop_sequence", 
        "stop_sequence1", "stop_pair"
    ] + ["shape_array_key"]
  

    # Needed to reconcile speedmap segments stop_sequenc1 (which can be missing
    # and fill it in with stop_sequence
    # otherwise merges downstream will drop too many rows
    speeds = calculate_speeds(
        bbb_arrivals, 
        trip_stop_cols + ["stop_meters"]
    ).pipe(
        gtfs_schedule_wrangling.fill_missing_stop_sequence1
    )

    time_of_day = (
        gtfs_schedule_wrangling.get_trip_time_buckets(analysis_date)   
        [["trip_instance_key", "time_of_day"]]
    )

    speeds2 = pd.merge(
        speeds.assign(service_date = pd.to_datetime(analysis_date)),
        time_of_day,
        on = "trip_instance_key",
        how = "inner"
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    )

    return speeds2


def average_bbb_segment_speeds_with_geom(
    analysis_date: str, 
    speeds: pd.DataFrame
) -> gpd.GeoDataFrame:

    # double check this and make sure list is partially in GTFS_DATA_DICT.trip_stop_cols
    # use same list as above
    trip_stop_cols = [
        "trip_instance_key", "stop_sequence", 
        "stop_sequence1", "stop_pair"
    ] + ["shape_array_key"]
    
    
    SPEEDMAP_SEGMENTS = GTFS_DATA_DICT.speedmap_segments.segments_file

    segment_geom = gpd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDMAP_SEGMENTS}_{analysis_date}.parquet"
    )

    speeds_with_geom = pd.merge(
        segment_geom,
        speeds[speeds.speed_mph <= 80], 
        # this filtering would be present in averaging already
        on = trip_stop_cols
    )

    avg_speeds = segment_calcs.calculate_avg_speeds(
        speeds_with_geom, 
        ["route_id", "direction_id", "stop_pair", "segment_id", "peak_offpeak"]
    )     

    segment_gdf = pd.merge(
        segment_geom[["route_id", "direction_id", 
          "stop_id1", "stop_sequence", "stop_id2", "stop_sequence1",
          "shape_array_key",
          "segment_id",
          "stop_pair"] + ["geometry"]],
        avg_speeds,
        on = ["route_id", "direction_id", 
              "segment_id",
              "stop_pair"]
    )

    segment_gdf = segment_gdf.assign(
        geometry = segment_gdf.apply(
            lambda x: rt_utils.try_parallel(x.geometry), 
            axis=1)
    )
    
    return segment_gdf

if __name__ == "__main__":

    bbb_speeds = bbb_speeds_by_trip(analysis_date)
    bbb_segment_speeds = average_bbb_segment_speeds_with_geom(analysis_date, bbb_speeds)
    
    bbb_speeds.to_parquet(
        f"{SEGMENT_GCS}bbb_speeds_by_trip_{analysis_date}.parquet"
    )
    
    bbb_segment_speeds.to_parquet(
        f"{SEGMENT_GCS}bbb_segment_speeds_gdf_{analysis_date}.parquet"
    )
