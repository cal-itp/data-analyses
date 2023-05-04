# header
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000) ## 800GB?

import shared_utils

import pandas as pd
import geopandas as gpd

pd.set_option('display.max_columns', None) 

import gcsfs

from calitp_data.storage import get_fs
fs = get_fs()

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/safety_projects/"

def join_crash_encamp():
    # load aggregated crashes
    crashes = gpd.read_parquet(f'{GCS_FILE_PATH}pedbikecrashes_agg.parquet')

    # load aggregated encampments
    encampments = gpd.read_parquet(f'{GCS_FILE_PATH}encampments_agg.parquet')

    # full join on bridge, name, fac - there are some slight differences in geometry
    crashes_encampments = crashes.merge(encampments, on=['BRIDGE_left','NAME_left','FAC_left', 'DIST_left', 'CO_left'], how='outer')

    # ~1200 bridge areas where geometry does not fully match. Set geometry from crash data
    crashes_encampments_gdf = gpd.GeoDataFrame(crashes_encampments, geometry="geometry_x", crs="3310")

    # fix district - make string
    crashes_encampments_gdf['DIST_left'] = crashes_encampments_gdf['DIST_left'].apply(str)
    
    return crashes_encampments_gdf

if __name__ == "__main__":
    # save out as geoparquet 
    crashes_encampments_agg = join_crash_encamp()
    #print some info in the terminal to verify
    crashes_encampments_agg.info()
    # export geojson for ArcGIS Pro
    shared_utils.utils.geojson_gcs_export(crashes_encampments_agg.drop("geometry_y", axis=1), GCS_FILE_PATH, "analytical_file_joined")
    # export parquet for overall stats
    shared_utils.utils.geoparquet_gcs_export(crashes_encampments_agg, GCS_FILE_PATH, "analytical_file_joined")