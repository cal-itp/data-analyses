"""
Interpolate stop arrival.
"""
import dask.dataframe as dd
import datetime
import numpy as np
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from typing import Literal

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS, 
                                              PROJECT_CRS, CONFIG_PATH)


def attach_vp_shape_meters_with_timestamp(
    analysis_date: str, 
    segment_type: Literal["stop_segments", "road_segments"],
) -> pd.DataFrame:
    """
    Merge vp_usable (which has timestamp) with the projected vp
    (shape_meters).
    """
    if segment_type == "stop_segments":
        VP_PROJ_FILE = f"vp_projected_{analysis_date}"
    
    elif segment_type == "road_segments":
        VP_PROJ_FILE = f"vp_projected_roads_{analysis_date}"
    
    vp_projected = pd.read_parquet(
        f"{SEGMENT_GCS}projection/{VP_PROJ_FILE}.parquet"
    )    
    
    # location_timestamp_local is here, and needs to be converted to seconds
    vp_usable = delayed(pd.read_parquet)(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}/",
        columns = ["vp_idx",  "location_timestamp_local"],
    )

    vp_info = delayed(pd.merge)(
        vp_usable,
        vp_projected,
        on = "vp_idx",
        how = "inner"
    )
    
    vp_info = compute(vp_info)[0]
    
    return vp_info


def get_stop_arrivals(
    df: pd.DataFrame,
    segment_type: Literal["stop_segments", "road_segments"]
) -> pd.DataFrame:
    """
    Apply np.interp to df.
    df must be set up so that a given stop is populated with its
    own stop_meters, as well as columns for nearest and subseq 
    shape_meters / location_timestamp_local.
    """
    x_col = "shape_meters"
    y_col = "location_timestamp_local"
    
    if segment_type == "stop_segments":
        stop_meters_col = "stop_meters"
        sort_cols = ["trip_instance_key", 
                     "shape_array_key", "stop_sequence"]
    
    elif segment_type == "road_segments":
        stop_meters_col = "road_meters"
        sort_cols = ["trip_instance_key", 
                     "linearid", "mtfcc", "segment_sequence"]
    
    stop_arrival_series = []
    for row in df.itertuples():

        xp = np.asarray([
            getattr(row, f"nearest_{x_col}"), 
            getattr(row, f"subseq_{x_col}")
        ])

        yp = np.asarray([
            getattr(row, f"nearest_{y_col}"), 
            getattr(row, f"subseq_{y_col}")
        ]).astype("datetime64[s]").astype("float64")

        stop_position = getattr(row, stop_meters_col)
        interpolated_arrival = np.interp(stop_position, xp, yp)
        stop_arrival_series.append(interpolated_arrival)
        
    df = df.assign(
        arrival_time = stop_arrival_series,
    ).astype(
        {"arrival_time": "datetime64[s]"}
    ).sort_values(
        sort_cols
    ).reset_index(drop=True)
    
    return df


def rename_vp_columns(
    vp: pd.DataFrame, 
    prefix: str, 
    col_list = ["vp_idx", "location_timestamp_local", "shape_meters"]
) -> pd.DataFrame:
    """
    Rename vp columns on-the-fly with prefixes like nearest_ or
    subseq_.
    On-the-fly, since we don't want to overwrite the columns,
    we just want merge to work.
    """
    new_col_list = [f"{prefix}{c}" for c in col_list]
    
    return vp.rename(columns = {k: v for k, v in zip(col_list, new_col_list)})


def add_nearest_subseq_info(
    nearest_vp_result: pd.DataFrame, 
    vp_info: pd.DataFrame,
    segment_type: Literal["stop_segments", "road_segments"]
) -> pd.DataFrame:
    """
    vp might be projected against shape or road, so how
    we merge in the nearest_ and subseq_ will be different.
    
    For road segments, we need to make sure we merge with the
    road-trip columns too.
    """
    if segment_type == "stop_segments":
        nearest_merge_cols = "nearest_vp_idx"
        subseq_merge_cols = "subseq_vp_idx"
        
    elif segment_type == "road_segments":
        road_cols = ["linearid", "mtfcc", "trip_instance_key"]
        nearest_merge_cols = road_cols + ["nearest_vp_idx"]
        subseq_merge_cols = road_cols + ["subseq_vp_idx"]
    
    vp_with_nearest_info = pd.merge(
        nearest_vp_result,
        rename_vp_columns(vp_info, "nearest_"),
        on = nearest_merge_cols,
        how = "inner"
    )

    df = pd.merge(
        vp_with_nearest_info,
        rename_vp_columns(vp_info, "subseq_"),
        on = subseq_merge_cols,
        how = "inner"
    )
        
    return df


def main(
    analysis_date: str, 
    dict_inputs: dict,
    segment_type: Literal["stop_segments", "road_segments"]
):
    NEAREST_VP = f"{dict_inputs['stage2']}_{analysis_date}"
    STOP_ARRIVALS_FILE = f"{dict_inputs['stage3']}_{analysis_date}"
    
    start = datetime.datetime.now()
    
    vp_pared = pd.read_parquet(
        f"{SEGMENT_GCS}projection/{NEAREST_VP}.parquet",
    )    

    # If we filter down pd.read_parquet, we can use np arrays
    # If we filter down dd.read_parquet, we have to use lists
    vp_info = attach_vp_shape_meters_with_timestamp(
        analysis_date,
        segment_type,
    )    
  
    df = add_nearest_subseq_info(vp_pared, vp_info, segment_type)
    
    time1 = datetime.datetime.now()
    logger.info(f"{segment_type}: set up df with nearest / subseq vp info: "
                f"{time1 - start}")
    
    stop_arrivals_df = get_stop_arrivals(df, segment_type)
    
    time2 = datetime.datetime.now()
    logger.info(f"interpolate stop arrival: {time2 - time1}")    
    
    stop_arrivals_df.to_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}.parquet")
    
    end = datetime.datetime.now()
    logger.info(f"execution time for {segment_type}: {end - start}")    

    return


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/interpolate_stop_arrival.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    #ROAD_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "road_segments")
    
    for analysis_date in analysis_date_list:
        logger.info(f"Analysis date: {analysis_date}")
        
        main(analysis_date, STOP_SEG_DICT, "stop_segments")
        #main(analysis_date, ROAD_SEG_DICT, "road_segments")

        