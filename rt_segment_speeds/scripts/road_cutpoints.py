"""
Some prep work related to where road segments 
begin and end...this is analogous to transit
"stops" along a shape.
"""
import geopandas as gpd
import pandas as pd

import cut_road_segments
from segment_speed_utils.project_vars import SEGMENT_GCS, analysis_date
from calitp_data_analysis import utils

if __name__ == "__main__":
    
    road_id_cols = ["linearid", "mtfcc"]
    segment_identifier_cols = road_id_cols + ["segment_sequence"]
    
    road_segments = gpd.read_parquet(
        f"{SEGMENT_GCS}road_segments_{analysis_date}",
        filters = [[("mtfcc", "in", ["S1100", "S1200"])]],
        columns = segment_identifier_cols + [
            #"primary_direction",
            "destination", "geometry"],
    ).set_geometry("destination")
    '''
    full_roads = road_segments[
        road_id_cols + [
            #"primary_direction", 
            "geometry"]
    ].set_geometry("geometry").dissolve(
        by = road_id_cols# + ["primary_direction"]
    ).reset_index()
    '''
    full_roads = cut_road_segments.load_roads(
        filtering = [("MTFCC", "in", ["S1100", "S1200"])]
    ).drop(columns = "road_length")

    # Merge cut road segments with full road geometry
    gdf = pd.merge(
        full_roads,
        road_segments.drop(columns = "geometry"),
        on = road_id_cols, #+ ["primary_direction"],
        how = "inner"
    )
    
    # Project each segment destination against full road geometry
    # note: we didn't add road's start at 0
    gdf = gdf.assign(
        road_meters = gdf.geometry.project(gdf.destination)
    ).drop(columns = ["destination"])
    
    utils.geoparquet_gcs_export(
        gdf,
        f"{SEGMENT_GCS}segments_staging/",
        f"roads_with_cutpoints_long_{analysis_date}"
    )
    
    # For each linearid, create an array tracking the cutpoints where
    # segments are created, which can act as "stops"
    road_info = (gdf.groupby(road_id_cols,#+ ["primary_direction"],
                 observed=True, group_keys=False)
        .agg({
            "segment_sequence": lambda x: list(x),
            "road_meters": lambda x: list(x),
        }).reset_index()
        .rename(columns = {
            "segment_sequence": "segment_sequence_arr",
            "road_meters": "road_meters_arr",
        })
    )
    
    road_info_with_geom = pd.merge(
        full_roads,
        road_info,
        on = road_id_cols,# + ["primary_direction"],
        how = "inner"
    )

    utils.geoparquet_gcs_export(
        road_info_with_geom,
        f"{SEGMENT_GCS}segments_staging/",
        f"roads_with_cutpoints_wide_{analysis_date}"
    )