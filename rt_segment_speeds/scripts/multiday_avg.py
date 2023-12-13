"""
Calculate speed columns (p20, p50, p80) across
multiple days worth of 
speeds by stop segments parquets.
"""
import datetime
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from segment_speed_utils import helpers, gtfs_schedule_wrangling, sched_rt_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, RT_SCHED_GCS, CONFIG_PATH
from shared_utils import schedule_rt_utils
from avg_speeds_by_segment import calculate_avg_speeds


def crosswalk_shape_key_to_shape_id(
    date_list: list
) -> pd.DataFrame:
    """
    Attach gtfs_dataset_key and shape_id to shape_array_key
    so that we can aggregate across days (in case shape_array_key 
    changes, but shape_id doesn't).
    """
    shape_crosswalk = pd.concat([
        helpers.import_scheduled_trips(
            d,
            columns = ["gtfs_dataset_key", 
                       "shape_array_key", "shape_id", 
                       "route_id", "direction_id"
                      ],
            get_pandas = True
        ) for d in date_list], 
        axis=0, ignore_index=True
    ).drop_duplicates().reset_index(drop=True)
        
    crosswalk = schedule_rt_utils.sample_gtfs_dataset_key_to_organization_crosswalk(
        shape_crosswalk.rename(columns = {
            "schedule_gtfs_dataset_key": "gtfs_dataset_key"}),
        date_list[0],
        quartet_data = "schedule",
        dim_gtfs_dataset_cols = [
            "key",
            "base64_url",
        ],
        dim_organization_cols = ["source_record_id", "name"]
    )

    shape_crosswalk_with_org = pd.merge(
        shape_crosswalk,
        crosswalk,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    )
    
    return shape_crosswalk_with_org


def trip_crosswalk_for_daytype_and_peak_category(
    date_list: list
) -> pd.DataFrame:
    """
    For a trip_instance_key, add peak_offpeak and
    weekday_weekend columns.
    """
    df = pd.concat([
        sched_rt_utils.get_trip_time_buckets(d).assign(
            day_name = pd.to_datetime(d).day_name()
        ) for d in date_list],
        axis=0, ignore_index=True
    ).pipe(gtfs_schedule_wrangling.add_peak_offpeak_column)
    
    df = df.assign(
        weekday_weekend = df.apply(
            lambda x: "weekend" if x.day_name in ["Saturday", "Sunday"] 
            else "weekday", axis=1)
    )
    
    keep_cols = ["trip_instance_key", "peak_offpeak", "weekday_weekend"]
    
    return df[keep_cols].drop_duplicates().reset_index(drop=True)


def stop_pairs_crosswalk(date_list: list) -> pd.DataFrame:
    """
    From the stop_times_direction table, grab for every trip-stop,
    the stop_pair (which is prior_stop_id combind with current_stop_id).
    This will help us aggregate by route-direction to ensure
    that the segments are actually referring to the portion between
    the same 2 stops.
    """
    stop_pairs_crosswalk = pd.concat(
        [pd.read_parquet(
            f"{RT_SCHED_GCS}stop_times_direction_{d}.parquet",
            columns = ["trip_instance_key", "stop_sequence", "stop_pair"]
            ) for d in date_list
        ], axis=0, ignore_index = True
    )
    return stop_pairs_crosswalk
  

def concatenate_speeds(date_list: list) -> pd.DataFrame:
    """
    Concatenate speeds by stop segments across multiple days.
    """
    dfs = [
        pd.read_parquet(
            f"{SEGMENT_GCS}speeds_stop_segments_{d}.parquet",
            columns = ["trip_instance_key", "shape_array_key", "stop_sequence",
                      "stop_id", "speed_mph"]
        ) for d in date_list
    ]
    
    df = pd.concat(dfs, axis=0, ignore_index=True)
    
    crosswalk = crosswalk_shape_key_to_shape_id(date_list)
    peak_weekday_df = trip_crosswalk_for_daytype_and_peak_category(date_list)
    stop_pairs_df = stop_pairs_crosswalk(date_list)
    
    df_concat = pd.merge(
        df,
        crosswalk,
        on = "shape_array_key",
        how = "inner"
    ).merge(
        peak_weekday_df,
        on = "trip_instance_key",
        how = "inner"
    ).merge(
        stop_pairs_df,
        on = ["trip_instance_key", "stop_sequence"],
        how = "inner"
    )
        
    return df_concat
    

def time_span_labeling(date_list: list) -> tuple[str]: 
    """
    If we grab a week's worth of trips, we'll
    use this week's average to stand-in for the entire month.
    Label with month and year.
    """
    time_span_str = list(set(
        [datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%b%Y").lower() 
         for d in date_list]
    ))
    
    time_span_num = list(set(
        [datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%m_%Y").lower() 
         for d in date_list]
    ))    
    
    if len(time_span_str) == 1:
        return time_span_str[0], time_span_num[0]

    else:
        print(f"multiple months: {time_span_str}")
        return time_span_str, time_span_num

    
def add_time_span_columns(
    df: pd.DataFrame, 
    time_span_num: str
) -> pd.DataFrame:
    
    month = int(time_span_num.split('_')[0])
    year = int(time_span_num.split('_')[1])
    
    # Downgrade some dtypes for public bucket
    df = df.assign(
        month = month, 
        year = year,   
    ).astype({
        "month": "int16", 
        "year": "int16",
        "n_trips": "int16"
    })
    
    return df


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    MAX_SPEED = STOP_SEG_DICT["max_speed"]
    SPEEDS_BY_SHAPE_EXPORT = STOP_SEG_DICT["shape_rollup"]
    SPEEDS_BY_ROUTE_DIR_EXPORT = STOP_SEG_DICT["route_direction_rollup"]
    
    df = delayed(concatenate_speeds)(analysis_date_list).persist()
    
    time_span_str, time_span_num = time_span_labeling(analysis_date_list)
    
    operator_cols = [
        "schedule_gtfs_dataset_key", 
        "base64_url", "organization_source_record_id", "organization_name"
    ]
    shape_stop_cols = ["shape_id", "stop_sequence", "stop_id"]
    route_dir_cols = ["route_id", "direction_id", "stop_id", "stop_pair"]
    time_cols = ["peak_offpeak", "weekday_weekend"]
    
    time1 = datetime.datetime.now()

    daytype_peak_stop_seg_speeds = delayed(calculate_avg_speeds)(
        df[df.speed_mph <= MAX_SPEED], 
        operator_cols + shape_stop_cols + time_cols
    )
    
    daytype_peak_stop_seg_speeds = compute(daytype_peak_stop_seg_speeds)[0]
    
    daytype_peak_stop_seg_speeds = add_time_span_columns(
        daytype_peak_stop_seg_speeds, 
        time_span_num
    )
    
    # Export with labeling of the month-year this avg speed is calculated for
    daytype_peak_stop_seg_speeds.to_parquet(
        f"{SEGMENT_GCS}rollup/{SPEEDS_BY_SHAPE_EXPORT}_{time_span_str}.parquet"
    )
    
    time2 = datetime.datetime.now()
    print(f"compute shapes average: {time2 - time1}")
    
    del daytype_peak_stop_seg_speeds
    
    daytype_peak_route_dir_speeds = delayed(calculate_avg_speeds)(
        df[df.speed_mph <= MAX_SPEED],
        operator_cols + route_dir_cols + time_cols
    )
    
    daytype_peak_route_dir_speeds = compute(daytype_peak_route_dir_speeds)[0]
    
    daytype_peak_route_dir_speeds = add_time_span_columns(
        daytype_peak_route_dir_speeds, 
        time_span_num
    )    
    
    daytype_peak_route_dir_speeds.to_parquet(
        f"{SEGMENT_GCS}rollup/{SPEEDS_BY_ROUTE_DIR_EXPORT}_{time_span_str}.parquet"
    )  
    
    end = datetime.datetime.now()
    print(f"compute route-dir average: {end - time2}")
    logger.info(f"roll up speeds for: {analysis_date_list}: {end - start}")