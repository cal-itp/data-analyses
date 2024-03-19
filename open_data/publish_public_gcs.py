"""
Write some open data ouputs into public GCS 
as zipped shapefiles
"""
import pandas as pd
import gcsfs
import geopandas as gpd

from pathlib import Path

from calitp_data_analysis import utils
from segment_speed_utils.project_vars import PUBLIC_GCS
from update_vars import HQTA_GCS, TRAFFIC_OPS_GCS, SEGMENT_GCS
from gcs_to_esri import remove_zipped_shapefiles

fs = gcsfs.GCSFileSystem()

HQTA_DATA = ["ca_hq_transit_stops", "ca_hq_transit_areas"]
GTFS_DATA = ["ca_transit_routes", "ca_transit_stops"]
SPEED_DATA = ["speeds_by_stop_segments", "speeds_by_route_timeofday"]

def construct_data_path(
    filename: str, 
    date: str
) -> str:
    """
    Create gdf's filepath of the open data from its path in GCS bucket
    """
    if filename in HQTA_DATA:
        path = f"{HQTA_GCS}export/{date}/{filename}"
    
    elif filename in GTFS_DATA:
        path = f"{TRAFFIC_OPS_GCS}export/{filename}"
    
    elif filename in SPEED_DATA:
        
        if filename == "speeds_by_stop_segments":
            path = f"{SEGMENT_GCS}rollup_singleday/speeds_route_dir_segments_{date}"
        
        elif filename == "speeds_by_route_timeofday":
            path = f"{SEGMENT_GCS}rollup_singleday/speeds_route_dir_{date}"
    
    return f"{path}.parquet"


def write_to_public_gcs(
    filename: str, 
    date: str
) -> str:
    """
    Import geoparquet, write out zipped shapefile to 
    local Hub, and upload to public bucket.
    """
    original_data_path = construct_data_path(filename, date)

    # Get a path obj so we can parse for stem and suffix
    # Don't save it because when we import GCS, we need str as path
    original_path_obj = Path(original_data_path)
    public_filename = (
        f"{original_path_obj.stem}_"
        f"{date}.zip" 
    )
    public_path = f"{PUBLIC_GCS}open_data/{public_filename}"
    
    gdf = gpd.read_parquet(original_data_path)
    
    # Don't write directly to GCS bucket, because our path is not 
    # exactly the same. In public bucket, we want to write it to a sub-directory
    utils.make_zipped_shapefile(
        gdf, 
        local_path = public_filename,
        gcs_folder = None
    )
    
    # Upload to GCS
    fs.put(
        public_filename,
        public_path
    )
    
    print(f"Uploaded {public_path}")
        
    return
    

if __name__ == "__main__":
    
    from shared_utils import rt_dates
    
    dates = [
        rt_dates.DATES["mar2024"]
    ]
    
    for d in dates:
        write_to_public_gcs("ca_hq_transit_stops", d)
    
    remove_zipped_shapefiles()