"""
Interpolate stop arrival.
"""
import dask.dataframe as dd
import datetime
import numpy as np
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS, 
                                              PROJECT_CRS, CONFIG_PATH)


def attach_vp_shape_meters_with_timestamp(
    analysis_date: str, **kwargs
) -> pd.DataFrame:
    """
    """
    # shape_meters is here
    vp_projected = pd.read_parquet(
        f"{SEGMENT_GCS}projection/vp_projected_{analysis_date}.parquet",
        **kwargs
    )
    
    # location_timestamp_local is here, and needs to be converted to seconds
    vp_usable = pd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}/",
        columns = ["vp_idx",  "location_timestamp_local"],
        **kwargs,
    )

    vp_info = pd.merge(
        vp_projected,
        vp_usable,
        on = "vp_idx",
        how = "inner"
    )
    
    return vp_info


def get_stop_arrivals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply np.interp to df.
    df must be set up so that a given stop is populated with its
    own stop_meters, as well as columns for nearest and subseq 
    shape_meters / location_timestamp_local_sec.
    """
    x_col = "shape_meters"
    y_col = "location_timestamp_local"
    
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

        stop_position = getattr(row, "stop_meters")
        interpolated_arrival = np.interp(stop_position, xp, yp)
        stop_arrival_series.append(interpolated_arrival)
        
    df = df.assign(
        arrival_time = stop_arrival_series,
    ).astype(
        {"arrival_time": "datetime64[s]"}
    ).sort_values(
        ["trip_instance_key", 
        "shape_array_key", "stop_sequence"]
    ).reset_index(drop=True)
    
    return df


def main(
    analysis_date: str, 
    dict_inputs: dict
):
    NEAREST_VP = f"{dict_inputs['stage2']}_{analysis_date}"
    STOP_ARRIVALS_FILE = f"{dict_inputs['stage3']}_{analysis_date}"
    
    start = datetime.datetime.now()
    
    vp_pared = pd.read_parquet(
        f"{SEGMENT_GCS}projection/{NEAREST_VP}.parquet",
    )    

    # If we filter down pd.read_parquet, we can use np arrays
    # If we filter down dd.read_parquet, we have to use lists
    subset_vp = np.union1d(
        vp_pared.nearest_vp_idx.unique(), 
        vp_pared.subseq_vp_idx.unique()
    )
    
    vp_info = attach_vp_shape_meters_with_timestamp(
        analysis_date, 
        filters = [[("vp_idx", "in", subset_vp)]]
    )
    
    vp_with_nearest_info = pd.merge(
        vp_pared,
        vp_info.add_prefix("nearest_"),
        on = "nearest_vp_idx",
        how = "inner"
    )
    
    df = pd.merge(
        vp_with_nearest_info,
        vp_info.add_prefix("subseq_"),
        on = "subseq_vp_idx",
        how = "inner"
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"set up df with nearest / subseq vp info: {time1 - start}")
    
    stop_arrivals_df = get_stop_arrivals(df)
    
    time2 = datetime.datetime.now()
    logger.info(f"interpolate stop arrival: {time2 - time1}")    
    
    stop_arrivals_df.to_parquet(
        f"{SEGMENT_GCS}{STOP_ARRIVALS_FILE}.parquet")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")    

    return


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    LOG_FILE = "../logs/interpolate_stop_arrival.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    for analysis_date in analysis_date_list:
        logger.info(f"Analysis date: {analysis_date}")
        main(analysis_date, STOP_SEG_DICT)
