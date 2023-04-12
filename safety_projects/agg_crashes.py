## Aggregate Crashes to Bridge Areas ##
# Load, filter, and aggregate SWITRS pedestrian crashes to buffered bridge areas

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

def aggregate_crashes():
    # load SWITRS crashes from project priority bucket
    switrs = gpd.read_parquet('gs://calitp-analytics-data/data-analyses/project_prioritization/SWITRS_clean.parquet')
    
    #filter to ped crashes 
    switrs_ped = switrs[switrs['pedestrian_accident']=='Y']
    
    # load bridge buffer areas
    bridge_areas = gpd.read_parquet(f'{GCS_FILE_PATH}bridgeareas_clean.parquet')
    
    # spatial join and aggregate crashes to bridge areas
    bridges_crashes = gpd.sjoin(bridge_areas, switrs_ped, how="left")
    
    # dissolve, grouping by bridge caracteristics, aggregating counts of encampment work orders 
    bridges_crashes_agg = bridges_crashes.dissolve(
        by = ["BRIDGE_left","NAME_left","FAC_left", "DIST_left", "CO_left"],
        aggfunc = {'number_killed' : 'sum',
                   'number_injured' : 'sum',
                   'pedestrian_accident' : 'count'
                  }
    ).reset_index()
    
    return bridges_crashes_agg

if __name__ == "__main__":
    # save out as geoparquet 
    bridges_crashes_agg = aggregate_crashes()
    #print some info in the terminal to verify
    bridges_crashes_agg.info()
    shared_utils.utils.geoparquet_gcs_export(bridges_crashes_agg, GCS_FILE_PATH, "pedcrashes_agg")