"""
This file contains functions for converting geodata into the form needed for CSIS analyses. It also contains functions to push the new geodata into corresponding folders in GCS. 
"""

import pandas as pd
import geopandas as gpd
import json
# import geojson

import gcsfs
import os

import re
import ast

from calitp_data_analysis import get_fs, utils

fs = gcsfs.GCSFileSystem()

# from calitp_data_analysis import utils

#local_folder = "zipped_locations/"
GCS_PATH = "gs://calitp-analytics-data/data-analyses/project_prioritization/zipped_shpfiles/"

"""
For this function you will need to specify the name of the geojson file you want to read in before. example:
file = "project_location_bike.geojson"
"""

def read_and_create_shpfiles(geojson_file, zip_name):
   
    location = gpd.read_file(geojson_file)
    location_zipped = utils.make_zipped_shapefile(location, local_path = zip_name, gcs_folder = GCS_PATH)
    
    ##remove local version
    os.remove(f"{zip_name}.zip")
    

## Function takes a json, creates a string and then modifies it so that it is in a geojson format that works for us. 
## use for conveyal json file outputs
def manipulate_json(json_file, geojson_file_name):
    ## read in file
    input_file = json.load(open(json_file, "r", encoding="utf-8"))
    
    string_file = json.dumps(input_file)
    
    ## subset string to just have the coordinates
    m = re.search('lineStrings": (.+?), "walkTimeFactor', string_file)
    if m:
        string_coords = m.group(1)
      
    ## save the start and end text that we want in the geojson
    start_text = ('{"type": "FeatureCollection", "features": [{"geometry": { "type": "MultiLineString", "coordinates":')
    end_text = ('}, "type": "Feature", }] }')
    
    new_file = start_text + string_coords + end_text
    
    ## get string into a dictionary
    d = ast.literal_eval(new_file)
    
    ##  export to geojson 
    with open(f"{geojson_file_name}.geojson", "w") as f:
        json.dump(d, f)

        
## function puts together the two previous functions, to get from json file to shp file.
def json_to_shpfile(json_file, new_file_name):
    '''
    use this function to convert jsons to a zipped shp file
    1. upload json file to folder in jupyter notebook
    2. open notebook project_location_to_shapefiles.ipynb
    3. add information for the uploaded json file, name for geojson and then the name of the zip file output 
    '''
    ## run through json manipulation
    manipulate_json(json_file, new_file_name)
    
    ## run through function to get shpfiles
    read_and_create_shpfiles(f"{new_file_name}.geojson", new_file_name)   
    
    ##remove local geojson
    os.remove(f"{new_file_name}.geojson")
    