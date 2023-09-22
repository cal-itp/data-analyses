"""
Quick aggregation for speed metrics by segment
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)
from shared_utils import utils, geography_utils


def calculate_avg_speeds(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Calculate the median, 20th, and 80th percentile speeds 
    by groups.
    """
    # Take the average after dropping unusually high speeds
    grouped_df = df.groupby(group_cols, observed=True, group_keys=False)
    
    
    avg = (grouped_df
          .agg({
            "speed_mph": "median",
            "trip_instance_key": "nunique"})
          .reset_index()
    )
    
    p20 = (grouped_df
           .agg({"speed_mph": lambda x: x.quantile(0.2)})
           .reset_index()  
          )
    
    p80 = (grouped_df
           .agg({"speed_mph": lambda x: x.quantile(0.8)})
           .reset_index()  
          )
    
    stats = pd.merge(
        avg.rename(columns = {"speed_mph": "p50_mph", 
                              "trip_instance_key": "n_trips"}),
        p20.rename(columns = {"speed_mph": "p20_mph"}),
        on = group_cols,
        how = "left"
    ).merge(
        p80.rename(columns = {"speed_mph": "p80_mph"}),
        on = group_cols,
        how = "left"
    )
    
    # Clean up for map
    speed_cols = [c for c in stats.columns if "_mph" in c]
    stats[speed_cols] = stats[speed_cols].round(2)
    
    return stats
    
def speeds_with_segment_geom(
    analysis_date: str, 
    max_speed_cutoff: int = 70,
    dict_inputs: dict = {},
) -> gpd.GeoDataFrame: 
    """
    Import the segment-trip table. 
    Average the speed_mph across all trips present in the segment.
    By default, filter out rows where meters_elapsed covers less than 40% of segment length
    """
    SEGMENT_FILE = dict_inputs["segments_file"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    SPEEDS_FILE = dict_inputs["stage4"]
    
    # Load in segment geometry
    segment_cols_to_keep = SEGMENT_IDENTIFIER_COLS + [
        "schedule_gtfs_dataset_key", 
        "stop_id",
        "loop_or_inlining",
        "geometry", 
        "district_name"
    ]
    
    segments = helpers.import_segments(
        SEGMENT_GCS,
        f"{SEGMENT_FILE}_{analysis_date}",
        columns = segment_cols_to_keep
    )
        
    # Read in speeds
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDS_FILE}_{analysis_date}.parquet", 
        filters = [[
            ("speed_mph", "<=", max_speed_cutoff),
            ("meters_elapsed", ">", 0),
            ("sec_elapsed", ">", 0)
    ]])
    
    # Do a merge with segments
    df2 = pd.merge(
        segments, 
        df, 
        on = SEGMENT_IDENTIFIER_COLS, 
        how = "inner"
    )
    
    # Keep only segments that have RT data. 
    unique_segments = (df2[segment_cols_to_keep]
                       .drop_duplicates()
                       .reset_index(drop = True)
                      ).to_crs(geography_utils.WGS84)
    
    time_of_day_df = sched_rt_utils.get_trip_time_buckets(analysis_date)

    df3 = pd.merge(
        df2, 
        time_of_day_df, 
        on = "trip_instance_key", 
        how = "inner"
    )
    
    all_day = calculate_avg_speeds(
        df3, 
        SEGMENT_IDENTIFIER_COLS
    )
    peak = calculate_avg_speeds(
        df3[df3.time_of_day.isin(["AM Peak", "PM Peak"])], 
        SEGMENT_IDENTIFIER_COLS
    )
    
    stats = pd.concat([
        all_day.assign(time_of_day = "all_day"),
        peak.assign(time_of_day = "peak")
    ], axis=0)
    
    
    # Merge in segment geometry    
    gdf = pd.merge(
        unique_segments,
        stats,
        on = SEGMENT_IDENTIFIER_COLS,
        how = "left"
    ).sort_values(SEGMENT_IDENTIFIER_COLS + ["time_of_day"]).reset_index(drop=True)
    
    return gdf


if __name__ == "__main__":
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    EXPORT_FILE = f'{STOP_SEG_DICT["stage5"]}_{analysis_date}'
    
    MAX_SPEED = 70
    
    # Average the speeds for segment for entire day
    # Drop speeds above our max cutoff
    stop_segment_speeds = speeds_with_segment_geom(
        analysis_date, 
        max_speed_cutoff = MAX_SPEED,
        dict_inputs = STOP_SEG_DICT,
    )
        
    utils.geoparquet_gcs_export(
        stop_segment_speeds,
        SEGMENT_GCS,
        EXPORT_FILE
    )
    
    logger.info(f"execution time: {datetime.datetime.now() - start}")