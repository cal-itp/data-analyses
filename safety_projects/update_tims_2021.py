# Aggregate updated TIMS data through 2021
## Based on .csv data received from Henry 

import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?

import pandas as pd
import geopandas as gpd
from siuba import *

import shared_utils

from gcsfs import GCSFileSystem
from calitp_data_analysis import utils
from calitp_data_analysis import get_fs
fs = get_fs()

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/safety_projects/"

# Initialize GCSFileSystem
gcs = GCSFileSystem()

def tims_combine():

    # Get a list of all CSV files in the GCS bucket folder
    csv_files = [file for file in gcs.ls(f'{GCS_FILE_PATH}TIMS_by_county') if file.endswith('.csv')]

    # Read each CSV file into a DataFrame and concatenate them
    dfs = []
    for file in csv_files:
        with gcs.open(file, 'rb') as f:
            df = pd.read_csv(f)
        dfs.append(df)

    # Concatenate all DataFrames into one
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Clean the data: Convert object columns to string
    object_columns = combined_df.select_dtypes(include=['object']).columns
    combined_df[object_columns] = combined_df[object_columns].astype(str)

    # Convert the DataFrame to a GeoDataFrame
    # Assuming you have latitude and longitude columns named 'LATITUDE' and 'LONGITUDE'
    gdf = gpd.GeoDataFrame(combined_df, geometry=gpd.points_from_xy(combined_df['POINT_X'], combined_df['POINT_Y']))
    
    #dropping for conversion erro
    #gdf = (gdf >> select(-_.JURIS))
    
    return gdf

if __name__ == "__main__":
    tims = tims_combine()
   
    #print some info in the terminal to verify
    tims.info()
    
    # Save the GeoDataFrame as a GeoParquet file
    utils.geoparquet_gcs_export(tims, GCS_FILE_PATH, "TIMS_Data_to_2021")