"""
Remove staged files.

Do this in a separate script in case we don't want to run.
"""
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS

if __name__ == "__main__":
    
    from update_vars import analysis_date_list, CONFIG_DICT
    
    for analysis_date in analysis_date_list:
        
        INPUT_FILE = CONFIG_DICT["usable_vp_file"]
        helpers.if_exists_then_delete(
            f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}_stage"
        )       
        helpers.if_exists_then_delete(
            f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet"
        )
    