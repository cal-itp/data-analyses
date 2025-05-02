import datetime
import pandas as pd
import geopandas as gpd

from segment_speed_utils import helpers, gtfs_schedule_wrangling, segment_calcs
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS

from calitp_data_analysis import utils
from shared_utils import rt_dates, rt_utils

def concatenate_batched_speeds_and_explode(analysis_date):
    SPEEDS_FILE = GTFS_DATA_DICT.modeled_vp.speeds_by_stop_segments
    STOP_TIMES_FILE = GTFS_DATA_DICT.modeled_vp.stop_times_on_vp_path
    
    speeds = pd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDS_FILE}_{analysis_date}.parquet"
    )
    
    speeds = speeds.assign(
        stop_sequence = speeds.apply(lambda x: list(x.stop_sequence)[:-1], axis=1),
        # skip last stop because there's no subseq stop
    )
    
    stop_times_cutoffs = pd.read_parquet(
        f"{SEGMENT_GCS}{STOP_TIMES_FILE}_{analysis_date}.parquet",
        columns = ["trip_instance_key", "stop_sequence", "stop_meters"]
    )
    
    stop_times = stop_times_cutoffs.explode(
        ["stop_sequence", "stop_meters"]).reset_index(drop=True)
    
    speeds_long = speeds.explode(
        ["meters_elapsed", "seconds_elapsed", "speed_mph", "stop_sequence"]
    ).reset_index(drop=True)
    
    df = pd.merge(
        stop_times,
        speeds_long,
        on = ["trip_instance_key", "stop_sequence"], # only last stop doesn't merge? is this right?
        how = "inner",
    )
    
    return df

def merge_in_scheduled_info(df, analysis_date):
    stop_time_info = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", "schedule_gtfs_dataset_key", "stop_sequence", "stop_pair", "stop_pair_name"],
        with_direction=True,
        get_pandas = True,
    )
    
    trip_info = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "route_id", "direction_id"],
        get_pandas = True
    )

    time_buckets = gtfs_schedule_wrangling.get_trip_time_buckets(analysis_date)
    
    df2 = pd.merge(
        df,
        stop_time_info,
        on = ["trip_instance_key", "stop_sequence"],
        how = "inner"
    ).merge(
        trip_info, 
        on = "trip_instance_key",
        how = "inner"
    ).merge(
        time_buckets,
        on = "trip_instance_key",
        how = "inner"
    )

    return df2


def modify_aggregation(df, group_cols):
    MAX_SPEED = 80
    df2 = df[df.speed_mph <= MAX_SPEED].dropna(subset="speed_mph").pipe(
        segment_calcs.calculate_avg_speeds,
        group_cols
    )
    
    return df2


def aggregate_existing_stage4(analysis_date):
    dict_inputs = GTFS_DATA_DICT.rt_stop_times

    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    EXPORT_FILE = dict_inputs["segment_timeofday"]

    SEGMENT_COLS = [*dict_inputs["segment_cols"]]
    SEGMENT_COLS_NO_GEOM = [i for i in SEGMENT_COLS if i != "geometry"]

    OPERATOR_COLS = ["schedule_gtfs_dataset_key"]
    
    stage4_speeds = pd.read_parquet(
        f"{SEGMENT_GCS}{SPEED_FILE}_{analysis_date}.parquet",
    )

    group_cols = OPERATOR_COLS + SEGMENT_COLS_NO_GEOM + [
        "stop_pair_name", "time_of_day"]
    existing_oct = modify_aggregation(stage4_speeds, group_cols)
    
    return existing_oct


def merge_in_segments(df, stop_segments):
    operators = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "name"],
        get_pandas = True,
    )
    
    gdf = pd.merge(
        stop_segments,
        df,
        on = ["schedule_gtfs_dataset_key", "route_id", "direction_id", "stop_pair"],
        how = "inner"
    ).merge(
        operators,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    )
    
    gdf = gdf.assign(
        geometry = gdf.apply(lambda x: rt_utils.try_parallel(x.geometry), axis=1)
    )
    
    return gdf


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    analysis_date = rt_dates.DATES["oct2024"]
    
    SEGMENTS_FILE = GTFS_DATA_DICT.rt_stop_times.segments_file

    stop_segments = gpd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENTS_FILE}_{analysis_date}.parquet",
        columns = ["schedule_gtfs_dataset_key", 
                   "route_id", "direction_id", 
                   "stop_pair", "geometry"],
    ).drop_duplicates(
        subset=["schedule_gtfs_dataset_key", "route_id", 
                "direction_id", "stop_pair"] # just in case geometry differs
    )
    
    existing_oct = aggregate_existing_stage4(analysis_date)
    existing_oct_speeds = merge_in_segments(existing_oct, stop_segments)
    
    utils.geoparquet_gcs_export(
        existing_oct_speeds,
        SEGMENT_GCS,
        f"resampled/existing_aggregated_speeds_{analysis_date}"
    )
    
    del existing_oct_speeds, existing_oct
    
    time1 = datetime.datetime.now()
    print(f"existing speeds aggregation: {time1 - start}")
    
    df = concatenate_batched_speeds_and_explode(analysis_date)
    df2 = merge_in_scheduled_info(df, analysis_date)
    group_cols = [
        "schedule_gtfs_dataset_key", 
        "route_id", "direction_id", "stop_pair",
        "stop_pair_name", "time_of_day"
    ]
    new_oct = modify_aggregation(df2, group_cols)
    new_oct_speeds = merge_in_segments(new_oct, stop_segments)
    
    del df, df2
    
    utils.geoparquet_gcs_export(
        new_oct_speeds,
        SEGMENT_GCS,
        f"resampled/new_aggregated_speeds_{analysis_date}"
    )    

    time2 = datetime.datetime.now()
    print(f"new speeds aggregation: {time2 - time1}")
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")