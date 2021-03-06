"""
Functions to create routes and stops dataset 
for open data.

Query gtfs_schedule tables, assemble,
export as geoparquet in GCS, and 
export as zipped shapefile or geojson
"""
import geopandas as gpd
import os

from datetime import datetime

import prep_data
import create_routes_data
import create_stops_data
from shared_utils import utils


if __name__ == "__main__":
    assert os.getcwd().endswith("traffic_ops"), "this script must be run from traffic_ops directory!"

    time0 = datetime.now()
    
    # Create local parquets
    prep_data.create_local_parquets(prep_data.SELECTED_DATE) 
    print("Local parquets created")
    
    routes = create_routes_data.make_routes_shapefile()    
    stops = create_stops_data.make_stops_shapefile()
    
    # Export geoparquets to GCS
    utils.geoparquet_gcs_export(routes, prep_data.GCS_FILE_PATH, "ca_transit_routes")
    utils.geoparquet_gcs_export(stops, prep_data.GCS_FILE_PATH, "ca_transit_stops")
    
    print("Geoparquets exported to GCS")
    
    # Delete local parquets
    #prep_data.delete_local_parquets()
    print("Local parquets deleted")
    
    time1 = datetime.now()
    print(f"Total run time for routes/stops script: {time1-time0}")
