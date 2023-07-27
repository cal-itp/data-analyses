"""
This file contains functions for converting geodata into the form needed for CSIS analyses. It also contains functions to push the new geodata into corresponding folders in GCS. 
"""

import pandas as pd
import geopandas as gpd
import json
import geojson

from shared_utils import utils

local_path = "/home/jovyan/data-analyses/project_prioritization/accessibility/"
GCS_PATH = "gs://calitp-analytics-data/data-analyses/project_prioritization/"

"""
For this function you will need to specify the name of the geojson file you want to read in before. example:
file = "proejct_location_bike.geojson"
"""

def read_and_create_shpfiles(geojson_file, zip_name):
    location = gpd.read_file(geojson_file)
    location_zipped = utils.make_zipped_shapefile(location,
                             zip_name
                           )