import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers, neighbor
from segment_speed_utils.project_vars import SEGMENT_GCS


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date
    
    LOG_FILE = "../logs/nearest_neighbors.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()

    # Unknown directions can be done separately for origin stop
    # we will use other methods to pare down the vp_idx 
    # maybe based on min(vp_idx) to use as the max bound
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}condensed/vp_nearest_neighbor_{analysis_date}.parquet",
        filters = [[("vp_primary_direction", "!=", "Unknown")]]
    )
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", 
                   "stop_sequence", "stop_primary_direction",
                   "geometry"],
        with_direction = True,
        get_pandas = True,
        crs = WGS84
    )
        
    gdf = neighbor.merge_stop_vp_for_nearest_neighbor(stop_times, vp)
    
    del vp, stop_times
    
    nearest_vp_idx = np.vectorize(neighbor.add_nearest_vp_idx)(
        gdf.geometry, gdf.stop_geometry, gdf.vp_idx)
        
    results = gdf[["trip_instance_key", "stop_sequence"]]
    results = results.assign(
        nearest_vp_idx = nearest_vp_idx
    )
    
    results.to_parquet(
        f"{SEGMENT_GCS}nearest/"
        f"nearest_rt_stop_times_{analysis_date}.parquet",
    )
    
    end = datetime.datetime.now()
    logger.info(f"nearest points: {end - start}")
                               