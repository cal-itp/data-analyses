"""
Remove staged files.

Do this in a separate script in case we don't want to run.
"""
import gcsfs

fs = gcsfs.GCSFileSystem()

def if_exists_then_delete(filepath):
    if fs.exists(filepath):
        if fs.isdir(filepath):
            fs.rm(filepath, recursive=True)
        else:
            fs.rm(filepath)
    
    return


if __name__ == "__main__":
    
    from update_vars import analysis_date_list, CONFIG_DICT
    
    for analysis_date in analysis_date_list:
        
        INPUT_FILE = CONFIG_DICT["usable_vp_file"]
        if_exists_then_delete(f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}_stage")       
        if_exists_then_delete(f"{SEGMENT_GCS}vp_direction_{analysis_date}.parquet")
    