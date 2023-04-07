## Clean and Aggregate Encampment Work Orders to Bridge Areas ##
# Load, filter, and aggregate IMMS encampment cleanup work orders to buffered bridge areas

# header
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000) ## 800GB?

import shared_utils

import pandas as pd
import geopandas as gpd
from siuba import *
import re

pd.set_option('display.max_columns', None) 

import gcsfs

from calitp_data.storage import get_fs
fs = get_fs()

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/safety_projects/"

#list of encampment words
encampment_words = ["encampment", "encampments", "homeless", "unsheltered", "unhoused", "tent", "tents", "peh", "evict", "eviction", "vacate", "camp", 
                    "campsite", "cleaning level", "removal level", "outreach", "housing", "personal"]

def clean_encampments():
    # load encampments - these were exported from ArcGIS Pro to get around the 2000 record limit on the open data portal. These are already projected. 
    with get_fs().open(f'{GCS_FILE_PATH}Encampments_Projected.geojson') as f:
        encampments = gpd.read_file(f)

    # filter based on `wo_comments` - there are non-homelessness work orders mixed in or empty records
    wo = (encampments >> select(_.FISCAL_YEAR,_.WONO,_.RESP_DIST,_.WO_COMMENTS))
    
    # tag encampment words
    pattern = '|'.join(encampment_words)
    wo['encampment_flag'] = wo['WO_COMMENTS'].str.contains(pattern, case=False)
    
    # filter encampment gdf to only those with encampment comments
    wo_filtered = (wo
                   >> filter(_.encampment_flag==True)
                   >> select(_.WONO)
                  )
    
    encampments_filtered = encampments.merge(wo_filtered, how='inner')
    
    return encampments_filtered

def aggregate_encampments(encampments):
    # load bridge buffer areas
    bridge_areas = gpd.read_parquet(f'{GCS_FILE_PATH}bridgeareas_clean.parquet')
    # spatial join and aggregate encampments to bridge areas
    bridges_encampments = gpd.sjoin(bridge_areas, encampments, how="left")
    
    # dissolve, grouping by bridge caracteristics, aggregating counts of encampment work orders 
    bridges_encampments_agg = bridges_encampments.dissolve(
        by = ["BRIDGE_left","NAME_left","FAC_left"],
        aggfunc = {'WONO' : 'nunique'}
    )
    
    # create derived variables: dummy variable, density, ranking
    bridges_encampments_agg = bridges_encampments_agg.assign(
        WO_density = bridges_encampments_agg['WONO']/bridges_encampments_agg['geometry'].area,
        WO_dummy = (bridges_encampments_agg['WONO'] > 0).astype(int)
        #WO_density_pctile = bridges_encampments_agg['WO_density'].rank(pct=True)
        ).reset_index()
    
    bridges_encampments_agg = bridges_encampments_agg.assign(
        WO_density_pctile = bridges_encampments_agg['WO_density'].rank(pct=True)
        )

    return bridges_encampments_agg

if __name__ == "__main__":
    encampments = clean_encampments()
    bridges_encampments_agg = aggregate_encampments(encampments)
    #print info to verify
    bridges_encampments_agg.info()
    # save out as geoparquet 
    shared_utils.utils.geoparquet_gcs_export(bridges_encampments_agg, GCS_FILE_PATH, "encampments_agg")