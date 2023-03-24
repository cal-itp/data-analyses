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

# load projects - these were exported from ArcGIS Pro to get around the 2000 record limit on the open data portal. These are already projected. 
with get_fs().open(f'{GCS_FILE_PATH}State_Highway_Bridges_Projec.geojson') as f:
    bridges = gpd.read_file(f)

# buffer 100 meters (~freeway width) to consolidate nearby bridges
bridges_buffer.geometry = bridges_buffer.buffer(100)

# combine overlapping shapes it by sjoining it to itself: https://stackoverflow.com/questions/73566774/group-by-and-combine-intersecting-overlapping-geometries-in-geopandas
bridges_intersect = bridges_buffer.sjoin(bridges_buffer, how="left", predicate="intersects")

# dissolve intersections on right bridge ID using the minimum value
bridges_intersect_dissolve = bridges_intersect.dissolve("BRIDGE_right", aggfunc="min")

# dissolve again on left bridge ID using minimum
bridges_intersect_dissolve = bridges_intersect_dissolve.reset_index().dissolve("BRIDGE_left", aggfunc="min")

# buffer another 100m for analysis of crashes/encampments
bridges_intersect_dissolve.geometry = bridges_intersect_dissolve.buffer(100)

# save out as geoparquet 
shared_utils.utils.geoparquet_gcs_export(bridges_intersect_dissolve, GCS_FILE_PATH, "bridgeareas_clean")