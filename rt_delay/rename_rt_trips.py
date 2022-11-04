"""
Append year and get date into isoformat in 
rt_delay/rt_trips/.

Filenames in this folder need to match naming convention in other folders.
"""
import pandas as pd
import gcsfs

fs = gcsfs.GCSFileSystem()
GCS = "gs://calitp-analytics-data/data-analyses/rt_delay/rt_trips/"


def change_file_name(gcs_file: str):
    file_name = gcs_file.split('rt_trips/')[1]
    itp_id = file_name.split('_')[0]
    month_day = file_name.split(f"{itp_id}_")[1].split('.parquet')[0]
    
    new_date = ('2022_' + month_day).replace('_', '-')
    
    new_file_name = f"{GCS}{itp_id}_{new_date}.parquet"
    
    df = pd.read_parquet(f"{GCS}{file_name}")
    df.to_parquet(new_file_name)
    print(f"Saved {new_file_name}")

          
if __name__=="__main__":
    fs_list = fs.ls(GCS)
    
    files_to_change = [i for i in fs_list if '.parquet' in i and 
                       'all_operators' not in i]
    
    for f in files_to_change:
        change_file_name(f)
        fs.rm(f)
        print(f"Remove: {f}")

