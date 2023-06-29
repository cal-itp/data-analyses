import os
os.environ['USE_PYGEOS'] = '0'

import dask.dataframe as dd
import datetime
import numpy as np
import pandas as pd

from segment_speed_utils import helpers#, sched_rt_utils
from segment_speed_utils.project_vars import (SEGMENT_GCS, #analysis_date,
                                              CONFIG_PATH, PROJECT_CRS
                                             )
from A2_valid_vehicle_positions import merge_usable_vp_with_sjoin_vpidx

analysis_date = "2023-05-17"

def triangulate_vp(
    ddf: dd.DataFrame, 
    group_cols: list = [
        "gtfs_dataset_key", "trip_id"]
) -> np.ndarray:
    """
    Get 3 vp for each trip.
    These vp already sjoined onto the shape.
    Pick the min, rounded mean, and max vp_idx for simplicity.
    """
    
    def integrify(df: dd.DataFrame) -> dd.DataFrame:
        """
        Make sure vp_idx returns as an integer and get rid of any NaNs. 
        """
        df2 = df.dropna().astype("int64")
        
        # persist now because it's keeping 1 row per trip.
        df2 = df2.persist() 
        return df2

    
    first_vp = (ddf.groupby(group_cols, observed=True, group_keys=False)
                .vp_idx
                .min()
                .pipe(integrify)
               ).compute().to_numpy()

    last_vp = (ddf.groupby(group_cols, observed=True, group_keys=False)
               .vp_idx
               .max()
               .pipe(integrify)
              ).compute().to_numpy()
    
    middle_vp = (ddf.groupby(group_cols, observed=True, group_keys=False)
                 .vp_idx
                 .mean()
                 .round(0)
                 .pipe(integrify)
                ).compute().to_numpy()
    
    # Here, row_stacking it results in 3 arrays, so flatten it to be 1d
    # or, equivalently, use np.concatenate
    stacked_results = np.concatenate([first_vp, middle_vp, last_vp])
    
    return stacked_results


def subset_usable_vp(dict_inputs: dict) -> np.ndarray:
    """
    Subset all the usable vp and keep the first, middle, and last 
    vp per trip.
    """
    SEGMENT_FILE = f'{dict_inputs["segments_file"]}_{analysis_date}'
    SJOIN_FILE = f'{dict_inputs["stage2"]}_{analysis_date}'
    USABLE_FILE = f'{dict_inputs["stage1"]}_{analysis_date}'
    GROUPING_COL = dict_inputs["grouping_col"]
    
    all_shapes = pd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENT_FILE}.parquet",
        columns = ["shape_array_key"]
    ).shape_array_key.unique().tolist()
    
    ddf = merge_usable_vp_with_sjoin_vpidx(
        all_shapes,
        USABLE_FILE,
        SJOIN_FILE,
        GROUPING_COL,
        columns = ["gtfs_dataset_key", "trip_id", "vp_idx"]
    )
        
    results = triangulate_vp(
        ddf, 
        ["gtfs_dataset_key", "trip_id"]
    )
    
    return results
    


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    results = subset_usable_vp(STOP_SEG_DICT)
        
    time1 = datetime.datetime.now()
    print(f"compute results: {time1 - start}")
    
    # Use these vp_idx and filter the vp with all the columns
    vp_idx_list = results.tolist()
    
    USABLE_FILE = f'{STOP_SEG_DICT["stage1"]}_{analysis_date}'

    vp_results = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        USABLE_FILE,
        file_type = "df",
        partitioned = True,
        columns = ["gtfs_dataset_key", "_gtfs_dataset_name", "trip_id",
                   "location_timestamp_local",
                   "x", "y", "vp_idx"
                  ],
        filters = [[("vp_idx", "in", vp_idx_list)]]
    ).compute()

    vp_results.to_parquet(
        f"{SEGMENT_GCS}trip_summary/vp_subset_{analysis_date}.parquet",
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
