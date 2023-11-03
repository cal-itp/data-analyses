"""
Quick aggregation for speed metrics by segment
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH
from calitp_data_analysis import utils, geography_utils


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
    max_speed_cutoff: int = 80,
    dict_inputs: dict = {},
): 
    """
    Import the segment-trip table. 
    Average the speed_mph across all trips present in the segment.
    """
    start = datetime.datetime.now()
    
    SEGMENT_FILE = f'{dict_inputs["segments_file"]}_{analysis_date}'
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    SPEEDS_FILE = f'{dict_inputs["stage4"]}_{analysis_date}'
    EXPORT_FILE = f'{dict_inputs["stage5"]}_{analysis_date}'
    
    # Read in speeds and attach time-of-day
    df = dd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDS_FILE}.parquet", 
        filters = [[("speed_mph", "<=", max_speed_cutoff)]]
    )
    
    time_of_day_df = sched_rt_utils.get_trip_time_buckets(analysis_date)

    ddf = dd.merge(
        df,
        time_of_day_df,
        on = "trip_instance_key",
        how = "inner"
    ).repartition(npartitions=5)
    
    subset_shape_keys = df2.shape_array_key.unique().compute().tolist()
    
    # Load in segment geometry, keep shapes present in speeds  
    segments = gpd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENT_FILE}",
        columns = SEGMENT_IDENTIFIER_COLS + [
            "schedule_gtfs_dataset_key", 
            "stop_id",
            "loop_or_inlining",
            "geometry", 
            "district_name"        
        ],
        filters = [[("shape_array_key", "in", subset_shape_keys)]]
    ).to_crs(geography_utils.WGS84)

    
    existing_cols_dtypes = ddf[SEGMENT_IDENTIFIER_COLS].dtypes.to_dict()
    
    speed_cols_dtypes = {
        "p50_mph": "float64",
        "n_trips": "int64",
        "p20_mph": "float64",
        "p80_mph": "float64"    
    }
    
    all_day = ddf.map_partitions(
        calculate_avg_speeds,
        SEGMENT_IDENTIFIER_COLS,
        meta = {
            **existing_cols_dtypes,
            **speed_cols_dtypes
            },
        align_dataframes = False
    ).compute()
                              
    peak_ddf = ddf[ddf.time_of_day.isin(["AM Peak", "PM Peak"])]
    
    peak = peak_ddf.map_partitions(
        calculate_avg_speeds,
        SEGMENT_IDENTIFIER_COLS,
        meta = {
            **existing_cols_dtypes,
            **speed_cols_dtypes
            },
        align_dataframes = False,
    ).compute()
    

    stats = pd.concat([
        all_day.assign(time_of_day = "all_day"),
        peak.assign(time_of_day = "peak")
    ], axis=0)
    
    
    # Merge in segment geometry    
    gdf = pd.merge(
        segments,
        stats,
        on = SEGMENT_IDENTIFIER_COLS,
        how = "left"
    ).sort_values(SEGMENT_IDENTIFIER_COLS + ["time_of_day"]).reset_index(drop=True)
    
    utils.geoparquet_gcs_export(
        gdf,
        SEGMENT_GCS,
        EXPORT_FILE
    )
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")

    return


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/avg_speeds.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    MAX_SPEED = 80
    
    for analysis_date in analysis_date_list:
        logger.info(f"Analysis date: {analysis_date}")
        
        speeds_with_segment_geom(
            analysis_date, 
            MAX_SPEED,
            STOP_SEG_DICT
        )
    
