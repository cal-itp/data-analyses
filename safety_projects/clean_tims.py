# Clean TIMS data
## Filter down TIMS export to FSI crashes, project and save as geoparquet/geojson

import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?

import pandas as pd
import geopandas as gpd
from siuba import *

import shared_utils

import gcsfs

from calitp_data.storage import get_fs
fs = get_fs()

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/safety_projects/"

def tims_etl():
    tims = pd.read_parquet(f'{GCS_FILE_PATH}TIMS_Data.parquet')
    # keep only small subset of columns
    tims_small = (tims 
                  >> select(_.CASE_ID,_.ACCIDENT_YEAR,_.COLLISION_DATE,_.COLLISION_TIME,_.COLLISION_SEVERITY,
                                     _.PCF_VIOL_CATEGORY,_.TYPE_OF_COLLISION,_.MVIW,
                                     _.NUMBER_KILLED,_.NUMBER_INJURED,_.PEDESTRIAN_ACCIDENT,_.BICYCLE_ACCIDENT,
                                     _.LATITUDE,_.LONGITUDE,_.POINT_X,_.POINT_Y
                                )
                  #>> filter(_.COLLISION_SEVERITY<=2) # fatality or severe injury
                   )

    # make geodataframe w/ relevant columns 
    tims_gdf = ((gpd.GeoDataFrame(
        tims_small, geometry=gpd.points_from_xy(tims_small.POINT_X, tims_small.POINT_Y))
               ) >> filter(-_.geometry.is_empty)
               )

    # set a CRS: assume WGS 84? 
    tims_gdf = tims_gdf.set_crs('4326')

    # project to match project data
    tims_gdf = tims_gdf.to_crs(shared_utils.geography_utils.CA_NAD83Albers)
    
    return tims_gdf

if __name__ == "__main__":
    tims = tims_etl()
   
    #print some info in the terminal to verify
    tims.info()
    
    #filter to FSI
    tims_fsi = (tims >> filter(_.COLLISION_SEVERITY<=2)) # fatality or severe injury
    
    # save geoparquet
    shared_utils.utils.geoparquet_gcs_export(tims_fsi, GCS_FILE_PATH, "tims_fsi")
   
    # also save a geojson for use in ArcGIS Pro
    shared_utils.utils.geojson_gcs_export(tims_fsi, GCS_FILE_PATH, "tims_fsi") 
    
    # save geoparquet with all severities
    shared_utils.utils.geoparquet_gcs_export(tims, GCS_FILE_PATH, "tims_all_severity")