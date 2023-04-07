## Process Bridge Data ##
# Load and consolidate SHN bridge data points with small buffer + dissolve, then apply larger analysis buffer

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

# load bridges
# In terminal, from same folder:
'''
pip install esridump
esri2geojson https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHhighway/State_Highway_Bridges/FeatureServer/0 bridges.geojson
'''

def buffer_bridges():
    bridges = gpd.read_file("./bridges.geojson")
    #shared_utils.utils.geoparquet_gcs_export(bridges, GCS_FILE_PATH, "shn_bridges")
    bridges_buffer = bridges.copy()
    bridges_buffer = bridges_buffer.to_crs(3310)
    
    # buffer 100 meters (~freeway width) to consolidate nearby bridges
    bridges_buffer.geometry = bridges_buffer.buffer(100)
    
    # keep only certain columns
    bridges_buffer_select = bridges_buffer[['geometry','DIST', 'CO', 'BRIDGE', 'NAME', 'FAC', 'INTERSEC', 'AADT', 'PM', 'RTE']]

    # combine overlapping shapes it by sjoining it to itself: https://stackoverflow.com/questions/73566774/group-by-and-combine-intersecting-overlapping-geometries-in-geopandas
    bridges_intersect = bridges_buffer.sjoin(bridges_buffer, how="left", predicate="intersects")

    # dissolve intersections on right bridge ID using the minimum value
    bridges_intersect_dissolve = bridges_intersect.dissolve("BRIDGE_right", aggfunc="min")

    # dissolve again on left bridge ID using minimum
    bridges_intersect_dissolve = bridges_intersect_dissolve.reset_index().dissolve("BRIDGE_left", aggfunc="min")

    # buffer another 100m for analysis of crashes/encampments
    bridges_intersect_dissolve.geometry = bridges_intersect_dissolve.buffer(100)

    # drop extra vars
    bridges_intersect_dissolve.drop(bridges_intersect_dissolve.loc[:, 'index_right':'RTE_right'], axis = 1, inplace = True)
    
    return bridges_intersect_dissolve

if __name__ == "__main__":
    # save out as geoparquet 
    bridge_areas = buffer_bridges()
    shared_utils.utils.geoparquet_gcs_export(bridge_areas, GCS_FILE_PATH, "bridge_areas")