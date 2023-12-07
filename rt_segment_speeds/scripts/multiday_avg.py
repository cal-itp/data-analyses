import datetime
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from segment_speed_utils import helpers, gtfs_schedule_wrangling, sched_rt_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH
from avg_speeds_by_segment import calculate_avg_speeds


def crosswalk_shape_key_to_shape_id(
    date_list: list
) -> pd.DataFrame:
    """
    Attach gtfs_dataset_key and shape_id to shape_array_key
    so that we can aggregate across days (in case shape_array_key 
    changes, but shape_id doesn't).
    """
    crosswalk = pd.concat([
        helpers.import_scheduled_trips(
            d,
            columns = ["gtfs_dataset_key", 
                       "shape_array_key", "shape_id"],
            get_pandas = True
        ) for d in date_list], 
        axis=0, ignore_index=True
    ).drop_duplicates().reset_index(drop=True)
    
    return crosswalk


def trip_crosswalk_for_daytype_and_peak_category(
    date_list: list
) -> pd.DataFrame:
    """
    For a trip_instance_key, add peak_offpeak and
    weekday_weekend columns.
    """
    df = pd.concat([
        sched_rt_utils.get_trip_time_buckets(d).assign(
            service_date = pd.to_datetime(d)
        ) for d in date_list],
        axis=0, ignore_index=True
    ).pipe(gtfs_schedule_wrangling.add_peak_offpeak_column)

    df = df.assign(
        day_name = df.service_date.dt.day_name()
    )
    
    df = df.assign(
        weekday_weekend = df.apply(
            lambda x: "weekend" if x.day_name in ["Saturday", "Sunday"] 
            else "weekday", axis=1)
    )
    
    keep_cols = ["trip_instance_key", "peak_offpeak", "weekday_weekend"]
    
    return df[keep_cols].drop_duplicates().reset_index(drop=True)


def concatenate_speeds(date_list: list):
    """
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
   
    df_concat = pd.merge(
        df,
        crosswalk,
        on = "shape_array_key",
        how = "inner"
    ).merge(
        peak_weekday_df,
        on = "trip_instance_key",
        how = "inner"
    )
    
    return df_concat


def check_time_span(date_list: list): 
    time_span = list(set(
        [datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%b%Y").lower() 
         for d in date_list]
    ))
    
    if len(time_span) == 1:
        return time_span[0]

    else:
        print(f"multiple months: {time_span}")
        return time_span


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
    EXPORT_FILE = STOP_SEG_DICT["stage8"]
    
    df = delayed(concatenate_speeds)(analysis_date_list)

    group_cols = [
        "schedule_gtfs_dataset_key", "shape_id", 
        "stop_sequence", "stop_id", 
        "peak_offpeak", "weekday_weekend"
    ]

    daytype_peak_speeds = delayed(calculate_avg_speeds)(
        df[df.speed_mph <= MAX_SPEED], 
        group_cols
    )
    
    results = compute(daytype_peak_speeds)[0]
    
    time_span = check_time_span(analysis_date_list)
    
    results.to_parquet(
        f"{SEGMENT_GCS}rollup/{EXPORT_FILE}_{time_span}.parquet"
    )
    
    end = datetime.datetime.now()
    logger.info(f"roll up speeds for: {analysis_date_list}: {end - start}")

