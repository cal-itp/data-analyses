import geopandas as gpd
import gtfs_segments
import pandas as pd

from segment_speed_utils import gtfs_schedule_wrangling     
from segment_speed_utils.project_vars import SEGMENT_GCS

def merge_stops_with_vp_find_nearest_neighbor(
    stop_times: gpd.GeoDataFrame, 
    vp_condensed: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    """
    gdf = pd.merge(
        stop_times.rename(columns = {"geometry": "start"}).set_geometry("start"),
        vp_condensed.rename(columns = {
            "vp_primary_direction": "stop_primary_direction"}),
        on = ["trip_instance_key", "stop_primary_direction"],
        how = "inner"
    )
    
    gdf = gdf.assign(
        trip_id = gdf.trip_instance_key + "__" + gdf.stop_primary_direction
    )

    results = gtfs_segments.geom_utils.nearest_points(gdf, k_neighbors=2).drop(
        columns = ["geometry", "start"]
    )
    
    return results