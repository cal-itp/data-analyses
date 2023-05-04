## Aggregate Crashes to Bridge Areas ##
# Load, filter, and aggregate SWITRS pedestrian crashes to buffered bridge areas

# header
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000) ## 800GB?

import shared_utils

import pandas as pd
import geopandas as gpd
import numpy as np

pd.set_option('display.max_columns', None) 

import gcsfs

from calitp_data.storage import get_fs
fs = get_fs()

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/safety_projects/"

def aggregate_crashes():
    # load TIMS crashes from project priority bucket
    tims = gpd.read_parquet(f'{GCS_FILE_PATH}tims_fsi.parquet')
    
    #filter to ped/bike crashes 
    tims_pedbike = tims[(tims['PEDESTRIAN_ACCIDENT']=='Y') | (tims['BICYCLE_ACCIDENT']=='Y')]
    
    # load bridge buffer areas
    bridge_areas = gpd.read_parquet(f'{GCS_FILE_PATH}bridgeareas_clean.parquet')
    
    # spatial join and aggregate crashes to bridge areas
    bridges_crashes = gpd.sjoin(bridge_areas, tims_pedbike, how="left")
    
    # need to create dummies for ped and bike - non-"Y" showing up as non-null
    bridges_crashes = bridges_crashes.assign(ped_crash = np.where(bridges_crashes['PEDESTRIAN_ACCIDENT']=="Y",1,0),
                       bike_crash = np.where(bridges_crashes['BICYCLE_ACCIDENT']=="Y",1,0)
                      )
    
    bridges_crashes['pedbike_crash'] = bridges_crashes[["ped_crash", "bike_crash"]].max(axis=1)
    
    # dissolve, grouping by bridge caracteristics, aggregating counts of encampment work orders 
    bridges_crashes_agg = bridges_crashes.dissolve(
        by = ["BRIDGE_left","NAME_left","FAC_left","DIST_left","CO_left"],
        aggfunc = {'NUMBER_KILLED' : 'sum',
                   'NUMBER_INJURED' : 'sum',
                   'ped_crash' : 'sum',
                   'bike_crash' : 'sum',
                   'pedbike_crash' : 'sum'
                  }
    ).reset_index()
    
    return bridges_crashes_agg

if __name__ == "__main__":
    # save out as geoparquet 
    bridges_crashes_agg = aggregate_crashes()
    #print some info in the terminal to verify
    bridges_crashes_agg.info()
    shared_utils.utils.geoparquet_gcs_export(bridges_crashes_agg, GCS_FILE_PATH, "pedbikecrashes_agg")