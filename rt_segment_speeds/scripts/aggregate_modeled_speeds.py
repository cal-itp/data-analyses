"""
Convert speeds stored as arrays into df.

Turn results from wide to long and attach natural identifiers 
like in `stop_arrivals_to_speeds.py` so we can
continue that with `speeds_by_time_of_day.py`.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from calitp_data_analysis import utils
from loguru import logger

from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT
from shared_utils import rt_dates
from stop_arrivals_to_speed import attach_operator_natural_identifiers


def explode_rt_stop_times_speeds(
    analysis_date: str,
    segment_type: str = "modeled_rt_stop_times"
):
    """
    Turn results stored as arrays into long df.
    """
    SPEEDS_FILE = GTFS_DATA_DICT[segment_type].speeds_wide
    STOP_TIMES_FILE = GTFS_DATA_DICT[segment_type].stop_times_projected
    trip_stop_cols = [*GTFS_DATA_DICT[segment_type].trip_stop_cols]
    
    speeds = pd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDS_FILE}_{analysis_date}.parquet"
    )
    
    speeds = speeds.assign(
        stop_sequence = speeds.apply(lambda x: list(x.stop_sequence)[:-1], axis=1),
        # skip last stop because there's no subseq stop
    )
        
    stop_times = pd.read_parquet(
        f"{SEGMENT_GCS}{STOP_TIMES_FILE}_{analysis_date}.parquet",
        columns = trip_stop_cols
    ).explode(["stop_sequence"]).reset_index(drop=True)
    
    speeds_long = speeds.explode(
        ["meters_elapsed", "seconds_elapsed", "speed_mph", "stop_sequence"]
    ).reset_index(drop=True)
    
    
    # Add what is derived from cut_stop_segments 
    # this does assume part of stop_times_direction exists, 
    # but don't need to derive so many columns
    stop_segments = pd.read_parquet(
        f"{SEGMENT_GCS}segment_options/stop_segments_{analysis_date}.parquet",
        columns = [
            "trip_instance_key", "shape_array_key", 
            "stop_sequence"]
    )

    df = pd.merge(
        stop_times,
        speeds_long,
        on = trip_stop_cols, # only last stop doesn't merge? is this right?
        how = "inner",
    ).merge(
        stop_segments,
        on = trip_stop_cols,
        how = "inner"
    )
    
    return df


def aggregate_speeds(
    df: pd.DataFrame,
    group_cols: list = ["schedule_gtfs_dataset_key", 
                        "route_id", "direction_id",
                        "stop_pair", "time_of_day"]
) -> pd.DataFrame:
    """
    Aggregate by operator route-direction segments + time_of_day.
    """
    # Basic counts of n_trips, n_routes, n_operators
    df2 = (
        df
       .groupby(group_cols, group_keys=False)
       .agg({
            "speed_mph": "mean",
            "trip_instance_key": "nunique",
        }).reset_index()
        .rename(columns = {
            "trip_instance_key": "n_vp_trips",
       })
    )
    
    return df2


def merge_in_stop_segment_geom(
    df: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Merge in stop segments created in cut_stop_segments
    """
    stop_segments = gpd.read_parquet(
        f"{SEGMENT_GCS}segment_options/stop_segments_{analysis_date}.parquet",
        columns = [
            "schedule_gtfs_dataset_key", 
            "route_id", "direction_id",
            "stop_pair", "geometry"]
    ).drop_duplicates()

    gdf = pd.merge(
        stop_segments,
        df,
        on = ["schedule_gtfs_dataset_key", 
              "route_id", "direction_id", "stop_pair"],
        how = "inner"
    )
    
    return gdf


if __name__ == "__main__":
    
    LOG_FILE = "../logs/modeled_speeds.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    #from segment_speed_utils.project_vars import test_dates
    
    for analysis_date in [rt_dates.DATES["oct2024"], rt_dates.DATES["nov2024"]]:
        
        start = datetime.datetime.now()

        segment_type = "modeled_rt_stop_times"
        SPEEDS_FILE = GTFS_DATA_DICT[segment_type].speeds
        AGGREGATED_SPEEDS_FILE = GTFS_DATA_DICT[segment_type].segment_timeofday
        
        df = explode_rt_stop_times_speeds(
            analysis_date,
            segment_type
        ).pipe(
            attach_operator_natural_identifiers, 
            analysis_date, 
            segment_type
        )
    
        df.to_parquet(
            f"{SEGMENT_GCS}{SPEEDS_FILE}_{analysis_date}.parquet"
        )
        
        del df
        
        time1 = datetime.datetime.now()
        logger.info(
            f"{segment_type}  {analysis_date}: speeds wide to long {time1 - start}"
        )
        
        gdf = pd.read_parquet(
            f"{SEGMENT_GCS}{SPEEDS_FILE}_{analysis_date}.parquet"
        ).pipe(
            aggregate_speeds, 
            group_cols = ["schedule_gtfs_dataset_key", 
                        "route_id", "direction_id",
                        "stop_pair", "time_of_day"]
        ).pipe(
            merge_in_stop_segment_geom
        )
 
        utils.geoparquet_gcs_export(
            gdf,
            SEGMENT_GCS,
            f"{AGGREGATED_SPEEDS_FILE}_{analysis_date}"
        )
        
        end = datetime.datetime.now()
        logger.info(
            f"{segment_type}  {analysis_date}: aggregated stop segment speeds: {end - time1}"
        )           