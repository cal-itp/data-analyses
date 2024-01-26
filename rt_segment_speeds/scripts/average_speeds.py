"""
Quick aggregation for speed metrics.
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from pathlib import Path
from typing import Literal

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import (gtfs_schedule_wrangling, helpers, 
                                 segment_calcs, time_helpers)
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH


OPERATOR_COLS = [
    "schedule_gtfs_dataset_key", 
    #"name", # from schedule
    #"organization_source_record_id", "organization_name", # from dim_organizations
    #"base64_url", "caltrans_district", 
]

SHAPE_STOP_COLS = [
    "shape_array_key", "shape_id", "stop_sequence",
]

STOP_PAIR_COLS = ["stop_pair"] 

ROUTE_DIR_COLS = [
    "route_id", "direction_id"
]

def merge_operator_identifiers(
    df: pd.DataFrame, 
    analysis_date_list: list
) -> pd.DataFrame:
    """
    Carrying a lot of these operator identifiers is not 
    inconsequential, esp when we need to run a week's segment speeds
    in one go.
    Instead, we'll just merge it back on before we export.
    """
    crosswalk = pd.concat([
        helpers.import_schedule_gtfs_key_organization_crosswalk(
            analysis_date,
        ).drop(columns = "itp_id") 
        for analysis_date in analysis_date_list],
        axis=0, ignore_index=True
    ).drop_duplicates()
    
    df = pd.merge(
        df,
        crosswalk,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    )
    
    return df


def import_segments(
    analysis_date: str,
    segment_type: Literal["stop_segments", "rt_stop_times"]
) -> gpd.GeoDataFrame:
    """
    Import the segments to merge.
    For stop_segments, import only 1 trip per shape for segments, and merge
    on shape_array_key and stop_pair.
    For rt_stop_times, import all trips with their segments, and merge on 
    trip_instance_key and stop_pair.
    """
    SEGMENT_FILE = f"segment_options/stop_segments_{analysis_date}.parquet"
    
    keep_cols = [
        "shape_array_key", "stop_pair", 
        "schedule_gtfs_dataset_key", "route_id", "direction_id",
        "geometry"
    ]
    
    if segment_type == "stop_segments":
        subset_trips = pd.read_parquet(
            f"{SEGMENT_GCS}segment_options/"
            f"shape_stop_segments_{analysis_date}.parquet",
            columns = ["st_trip_instance_key"]
        ).st_trip_instance_key.unique()
        
        gdf = gpd.read_parquet(
            f"{SEGMENT_GCS}{SEGMENT_FILE}",
            columns = keep_cols,
            filters = [[("trip_instance_key", "in", subset_trips)]],
        ).to_crs(WGS84) 
        
        
    elif segment_type == "rt_stop_times":
        gdf = gpd.read_parquet(
            f"{SEGMENT_GCS}{SEGMENT_FILE}",
            columns = keep_cols,
        ).to_crs(WGS84)

    return gdf


def concatenate_trip_segment_speeds(
    analysis_date_list: list,
    dict_inputs: dict
) -> pd.DataFrame:
    """
    Concatenate the speed-trip parquets together, 
    whether it's for single day or multi-day averages.
    Add columns for peak_offpeak, weekday_weekend based 
    on day of week and time-of-day.
    """
    SPEED_FILE = dict_inputs["stage4"]
    MAX_SPEED = dict_inputs["max_speed"]
    
    df = pd.concat([
        pd.read_parquet(
            f"{SEGMENT_GCS}{SPEED_FILE}_{analysis_date}.parquet", 
            columns = (OPERATOR_COLS + SHAPE_STOP_COLS + 
                       STOP_PAIR_COLS + ROUTE_DIR_COLS + [
                           "trip_instance_key", "speed_mph", 
                           "meters_elapsed", "sec_elapsed", 
                           "time_of_day"]),
            filters = [[("speed_mph", "<=", MAX_SPEED)]]
        ).assign(
            service_date = pd.to_datetime(analysis_date)
        ) for analysis_date in analysis_date_list], 
        axis=0, ignore_index = True
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    ).pipe(
        gtfs_schedule_wrangling.add_weekday_weekend_column
    )
    
    return df
    
    
def single_day_averages(analysis_date: str, dict_inputs: dict):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    """
    SHAPE_SEG_FILE = Path(dict_inputs["shape_stop_single_segment"])
    ROUTE_SEG_FILE = Path(dict_inputs["route_dir_single_segment"])
    TRIP_FILE = Path(dict_inputs["trip_speeds_single_summary"])
    ROUTE_DIR_FILE = Path(dict_inputs["route_dir_single_summary"])
    
    start = datetime.datetime.now()
    
    df = concatenate_trip_segment_speeds([analysis_date], dict_inputs)
    print("concatenated files")   
    
    t0 = datetime.datetime.now()
    shape_stop_segments = segment_calcs.concatenate_peak_offpeak_allday_averages(
        df, 
        OPERATOR_COLS + SHAPE_STOP_COLS + STOP_PAIR_COLS
    ).pipe(
        merge_operator_identifiers, [analysis_date]
    )
    
    col_order = [c for c in shape_stop_segments.columns]
    
    segment_geom = import_segments(analysis_date, "stop_segments")
    
    shape_stop_segments = pd.merge(
        segment_geom,
        shape_stop_segments,
        on = OPERATOR_COLS + ["shape_array_key"] + STOP_PAIR_COLS, 
    ).reset_index(drop=True).reindex(
        columns = col_order + ["geometry"]
    )
    
    utils.geoparquet_gcs_export(
        shape_stop_segments,
        f"{SEGMENT_GCS}{str(SHAPE_SEG_FILE.parent)}/",
        f"{SHAPE_SEG_FILE.stem}_{analysis_date}"
    )
    
    del shape_stop_segments, segment_geom
    
    t1 = datetime.datetime.now()
    logger.info(f"shape seg avg {t1 - t0}")
    
    route_dir_segments = segment_calcs.concatenate_peak_offpeak_allday_averages(
        df, 
        OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS
    ).pipe(
        merge_operator_identifiers, [analysis_date]
    )
    
    col_order = [c for c in route_dir_segments.columns]
    
    segment_geom = (import_segments(analysis_date, "stop_segments")
                    .drop(columns = "shape_array_key")
                    .drop_duplicates()
                   )

    route_dir_segments = pd.merge(
        segment_geom,
        route_dir_segments,
        on = OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS, 
    ).reset_index(drop=True).reindex(
        columns = col_order + ["geometry"]
    )
    
    utils.geoparquet_gcs_export(
        route_dir_segments,
        f"{SEGMENT_GCS}{str(ROUTE_SEG_FILE.parent)}/",
        f"{ROUTE_SEG_FILE.stem}_{analysis_date}"
    )
    
    del route_dir_segments, segment_geom
    
    t2 = datetime.datetime.now()
    logger.info(f"route dir seg avg {t2 - t1}")
    
    trip_avg = segment_calcs.weighted_average_speeds_across_segments(
        df,
        OPERATOR_COLS + ROUTE_DIR_COLS + [
            "trip_instance_key", 
            "shape_array_key", "shape_id", "time_of_day"]
    ).pipe(
        merge_operator_identifiers, [analysis_date]
    ).reset_index(drop=True)
    
    trip_avg.to_parquet(
        f"{SEGMENT_GCS}{TRIP_FILE}_{analysis_date}.parquet"
    )
    del trip_avg
    
    t3 = datetime.datetime.now()
    logger.info(f"trip avg {t3 - t2}")
    
    route_dir_avg = segment_calcs.weighted_average_speeds_across_segments(
        df,
        OPERATOR_COLS + ROUTE_DIR_COLS
    ).pipe(
        merge_operator_identifiers, [analysis_date]
    )
    
    col_order = [c for c in route_dir_avg.columns]
    
    common_shape_geom = gtfs_schedule_wrangling.most_common_shape_by_route_direction(
        analysis_date).to_crs(WGS84)
    
    route_dir_avg = pd.merge(
        common_shape_geom,
        route_dir_avg,
        on = OPERATOR_COLS + ROUTE_DIR_COLS,
        how = "inner"
    ).reset_index(drop=True).reindex(
        columns = col_order + ["geometry"]
    )
    
    utils.geoparquet_gcs_export(
        route_dir_avg,
        f"{SEGMENT_GCS}{str(ROUTE_DIR_FILE.parent)}/",
        f"{ROUTE_DIR_FILE.stem}_{analysis_date}"
    )
    
    del route_dir_avg, common_shape_geom
    
    t4 = datetime.datetime.now()
    logger.info(f"route dir avg: {t4 - t3}")
    
    return


def multi_day_averages(analysis_date_list: list, dict_inputs: dict):
    """
    Main function for calculating average speeds.
    Start from single day segment-trip speeds and 
    aggregate by peak_offpeak, weekday_weekend.
    The main difference from a single day average is that
    the seven days is concatenated first before averaging,
    so that we get weighted averages.
    """
    ROUTE_SEG_FILE = dict_inputs["route_dir_multi_summary"]
    ROUTE_DIR_FILE = dict_inputs["route_dir_multi_segment"]
        
    df = delayed(concatenate_trip_segment_speeds)(analysis_date_list, dict_inputs)
    print("concatenated files")   
    
    time_span_str, time_span_num = time_helpers.time_span_labeling(analysis_date_list)
    
    t0 = datetime.datetime.now()
        
    route_dir_segments = delayed(
        segment_calcs.concatenate_peak_offpeak_allday_averages)(
        df, 
        OPERATOR_COLS + ROUTE_DIR_COLS + STOP_PAIR_COLS + ["weekday_weekend"]
    )
    
    route_dir_segments = compute(route_dir_segments)[0]
    
    route_dir_segments = time_helpers.add_time_span_columns(
        route_dir_segments, time_span_num
    ).pipe(
        merge_operator_identifiers, analysis_date_list
    )
    
    route_dir_segments.to_parquet(
        f"{SEGMENT_GCS}{ROUTE_SEG_FILE}_{time_span_str}.parquet"
    )
    del route_dir_segments   
    
    t1 = datetime.datetime.now()
    logger.info(f"route seg avg {t1 - t0}")
    
    route_dir_avg = delayed(
        segment_calcs.weighted_average_speeds_across_segments)(
        df,
        OPERATOR_COLS + ROUTE_DIR_COLS + ["weekday_weekend"]
    )
    
    route_dir_avg = compute(route_dir_avg)[0]
    route_dir_avg = time_helpers.add_time_span_columns(
        route_dir_avg, time_span_num
    ).pipe(
        merge_operator_identifiers, analysis_date_list
    )

    route_dir_avg.to_parquet(
        f"{SEGMENT_GCS}{ROUTE_DIR_FILE}_{time_span_str}.parquet"
    )
    del route_dir_avg

    t2 = datetime.datetime.now()
    logger.info(f"route dir avg {t2 - t1}")
        
    return


def stage_open_data_exports(analysis_date: str, dict_inputs: dict):
    """
    For the datasets we publish to Geoportal, 
    export them to a stable GCS URL so we can always 
    read it in open_data/catalog.yml.
    """
    datasets = [
        dict_inputs["route_dir_single_segment"],
        dict_inputs["route_dir_single_summary"]
    ]

    for d in datasets:
        gdf = gpd.read_parquet(
            f"{SEGMENT_GCS}{d}_{analysis_date}.parquet"
        )
        
        utils.geoparquet_gcs_export(
            gdf,
            f"{SEGMENT_GCS}export/",
            f"{Path(d).stem}"
        )
        del gdf
    
    print(f"overwrite {datasets}")
    
    return
        

if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date#analysis_date_list
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    
    for analysis_date in [analysis_date]:
        
        start = datetime.datetime.now()
        
        single_day_averages(analysis_date, STOP_SEG_DICT)
        stage_open_data_exports(analysis_date, STOP_SEG_DICT)
        
        end = datetime.datetime.now()
        
        logger.info(f"average rollups for {analysis_date}: {end - start}")
    
    '''
    start = datetime.datetime.now()
    multi_day_averages(analysis_date_list, STOP_SEG_DICT)
    end = datetime.datetime.now()
    
    logger.info(f"average rollups for {analysis_date_list}: {end - start}")
    '''
    