"""
Tag rows where we know errors are occurring when 
we find the nearest vp.

Error 1: nearest_vp_idx = subseq_vp_idx.
We cannot interpolate if the 2 endpoints are the same points.

Error 2: change from the prior vp_idx value is negative. 
We expect monotonically increasing vp_idx across stops, 
which then translate to arrival times that are increasing.

For loop/inlining shapes, esp near origin 
(which is also destination), we can select a vp that's happening
at the end of the trip and attach to the origin stop.
This results in an arrival time for stop 1 that occurs after arrival time
for stop 2.
This happens because vp_primary_direction might be Unknown

"""
import dask.dataframe as dd
import numpy as np
import pandas as pd

from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS, PROJECT_CRS
from shared_utils import rt_dates

def tag_common_errors(df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    
    df = df.assign(
        # Find where we select same points and try to interpolate in between
        error_same_endpoints = df.apply(
            lambda x: 1 if x.nearest_vp_idx == x.subseq_vp_idx
            else 0, axis=1
        ),
        # Find the difference in nearest_vp_idx by comparing
        # against the previous row and subsequent row
        diff_prior = (df.groupby("trip_instance_key", 
                                 observed=True, group_keys=False)
                   .nearest_vp_idx
                   .apply(lambda x: x - x.shift(1))
                  ),
    )
    
    df = df.assign(
        error_arrival_order = df.apply(
            lambda x: 1 if x.diff_prior <= 0
            else 0, axis=1
        )
    )
    
    return df


if __name__ == "__main__":
    
    analysis_date = rt_dates.DATES["sep2023"]
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")

    NEAREST_VP = f"{STOP_SEG_DICT['stage2']}_{analysis_date}"
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}projection/{NEAREST_VP}.parquet"
    ).pipe(tag_common_errors)
    
    df.to_parquet(
        f"{SEGMENT_GCS}projection/nearest_vp_error_{analysis_date}.parquet")
