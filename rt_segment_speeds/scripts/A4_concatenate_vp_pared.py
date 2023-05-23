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

    INPUT_FILE = STOP_SEG_DICT["stage3"]
    SEGMENT_IDENTIFIER_COLS = STOP_SEG_DICT["segment_identifier_cols"]
    TIMESTAMP_COL = STOP_SEG_DICT["timestamp_col"]
    cases = ["normal", "special"]
    
    dfs = [
        dd.read_parquet(
            f"{SEGMENT_GCS}{INPUT_FILE}_{c}_{analysis_date}"
        ) for c in cases
    ]
    
    df = dd.multi.concat(dfs, axis=0).reset_index(drop=True)
    df = df.repartition(npartitions = 5)
    df.to_parquet(f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}", 
                  overwrite=True)
    
    end = datetime.datetime.now()
    print(f"execution time: {end-start}")