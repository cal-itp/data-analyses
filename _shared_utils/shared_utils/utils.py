import os
import pandas as pd

from calitp.storage import get_fs
fs = get_fs()

def import_csv_export_parquet(DATASET_NAME, OUTPUT_FILE_NAME, GCS_FILE_PATH, GCS=True): 
    """
    DATASET_NAME: str. Name of csv dataset.
    OUTPUT_FILE_NAME: str. Name of output parquet dataset.
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
 
    """
    df = pd.read_csv(f"{DATASET_NAME}.csv")    
    
    if GCS is True:
        df.to_parquet(f"{GCS_FILE_PATH}{OUTPUT_FILE_NAME}.parquet")
    else:
        df.to_parquet(f"./{OUTPUT_FILE_NAME}.parquet")
        

def geoparquet_gcs_export(gdf, GCS_FILE_PATH, FILE_NAME):
    '''
    Save geodataframe as parquet locally, 
    then move to GCS bucket and delete local file.
    
    gdf: geopandas.GeoDataFrame
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str. Filename.
    '''
    gdf.to_parquet(f"./{FILE_NAME}.parquet")
    fs.put(f"./{FILE_NAME}.parquet", f"{GCS_FILE_PATH}{FILE_NAME}.parquet")
    os.remove(f"./{FILE_NAME}.parquet")
