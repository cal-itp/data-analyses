"""
Concatenate pared down vp
from normal and special cases 
and save as one file to use.
"""
import dask.dataframe as dd
import datetime

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH)

if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")

    VP_FULL_INFO = STOP_SEG_DICT["stage1"]
    INPUT_FILE = STOP_SEG_DICT["stage3"]
    SEGMENT_IDENTIFIER_COLS = STOP_SEG_DICT["segment_identifier_cols"]
    TIMESTAMP_COL = STOP_SEG_DICT["timestamp_col"]
    cases = ["normal", "special"]
    
    dfs = [
        dd.read_parquet(
            f"{SEGMENT_GCS}vp_pare_down/{INPUT_FILE}_{c}_{analysis_date}",
            columns = ["vp_idx"] + SEGMENT_IDENTIFIER_COLS
        ) for c in cases
    ]
    
    pared_down_vp = dd.multi.concat(dfs, axis=0).reset_index(
        drop=True).set_index("vp_idx", sorted=False)
    
    vp_full_info = dd.read_parquet(
        f"{SEGMENT_GCS}{VP_FULL_INFO}_{analysis_date}"
    ).set_index("vp_idx", sorted=False)
    
    df = dd.merge(
        vp_full_info,
        pared_down_vp,
        left_index = True,
        right_index = True,
        how = "inner"
    ).reset_index()
    
    df = df.repartition(npartitions = 2)
    df.to_parquet(f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}", 
                  overwrite=True)
    
    end = datetime.datetime.now()
    print(f"execution time: {end-start}")