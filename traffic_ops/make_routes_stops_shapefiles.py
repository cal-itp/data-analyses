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

def export_to_subfolder(analysis_date: str):
    """
    We always overwrite the same geoparquets each month, and point our
    shared_utils/shared_data_catalog.yml to the latest file.
    
    But, save historical exports just in case.
    """
    files = ["ca_transit_routes", "ca_transit_stops", "ca_transit_routes_feed"]
        
    for f in files:
        gdf = gpd.read_parquet(f"{prep_data.TRAFFIC_OPS_GCS}{f}.parquet")
        
        utils.geoparquet_gcs_export(
            gdf, 
            f"{prep_data.TRAFFIC_OPS_GCS}export/", 
            f"{f}_{analysis_date}"
        )
        
    print("All 3 files written to export subfolder")
    

if __name__ == "__main__":
    assert os.getcwd().endswith("traffic_ops"), "this script must be run from traffic_ops directory!"
    
    time0 = datetime.now()
    
    # Create local parquets
    prep_data.create_local_parquets(prep_data.ANALYSIS_DATE) 
    print("Local parquets created")
    
    # Make an operator level file (this is published)
    operator_level_cols = ["calitp_itp_id", "shape_id"]
    
    routes = create_routes_data.create_routes_file_for_export(
        prep_data.ANALYSIS_DATE, operator_level_cols)  
    
    utils.geoparquet_gcs_export(
        routes, 
        prep_data.TRAFFIC_OPS_GCS, 
        "ca_transit_routes"
    )
    
    # Make a feed level file (not published externally, publish to GCS for internal use)
    feed_level_cols = ["calitp_itp_id", "calitp_url_number", "shape_id"]
    
    feed_routes = create_routes_data.create_routes_file_for_export(
        prep_data.ANALYSIS_DATE, feed_level_cols
    )
    
    utils.geoparquet_gcs_export(
        feed_routes, 
        prep_data.TRAFFIC_OPS_GCS, 
        "ca_transit_routes_feed"
    )
    
    # Make stops file
    stops = create_stops_data.make_stops_shapefile()  
    
    utils.geoparquet_gcs_export(
        stops, 
        prep_data.TRAFFIC_OPS_GCS, 
        "ca_transit_stops"
    )
    
    print("Geoparquets exported to GCS")
    
    # Export all to its subfolder too
    export_to_subfolder(prep_data.ANALYSIS_DATE)
    
    time1 = datetime.now()
    print(f"Total run time for routes/stops script: {time1-time0}")
    