"""
Remove staged files.

Do this in a separate script in case we don't want to run.
"""
from segment_speed_utils import helpers
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS

if __name__ == "__main__":
    
    from update_vars import analysis_date_list
    
    for analysis_date in analysis_date_list:
        
        INPUT_FILE = GTFS_DATA_DICT.speeds_tables.usable_vp
        
        helpers.if_exists_then_delete(
            f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}_stage"
        )       
        helpers.if_exists_then_delete(
            f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet"
        )
    