"""
Some prep work related to where road segments 
begin and end...this is analogous to transit
"stops" along a shape.
"""
import datetime
import geopandas as gpd
import pandas as pd

import cut_road_segments
from segment_speed_utils.project_vars import SEGMENT_GCS, analysis_date
from calitp_data_analysis import utils

if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    road_id_cols = ["linearid", "mtfcc"]
    segment_identifier_cols = road_id_cols + ["segment_sequence"]
    road_types = ["S1100", "S1200", "S1400"]
    
    road_segments = pd.read_parquet(
        f"{SEGMENT_GCS}road_segments_{analysis_date}",
        filters = [[("mtfcc", "in", road_types)]],
        columns = segment_identifier_cols + ["road_meters", "primary_direction"]
    )

    full_roads = cut_road_segments.load_roads(
        filtering = [("MTFCC", "in", road_types)]
    ).drop(columns = "road_length")

    # Merge cut road segments with full road geometry
    gdf = pd.merge(
        full_roads,
        road_segments,
        on = road_id_cols, 
        how = "inner"
    )
    
    # For each linearid, create an array tracking the cutpoints where
    # segments are created, which can act as "stops"
    road_info = (gdf.groupby(road_id_cols,
                 observed=True, group_keys=False)
        .agg({
            "segment_sequence": lambda x: list(x),
            "road_meters": lambda x: list(x),
            "primary_direction": lambda x: list(x),
        }).reset_index()
        .rename(columns = {
            "segment_sequence": "segment_sequence_arr",
            "road_meters": "road_meters_arr",
            "primary_direction": "road_direction_arr",
        })
    )
    
    road_info_with_geom = pd.merge(
        full_roads[road_id_cols + ["fullname", "geometry"]],
        road_info,
        on = road_id_cols,
        how = "inner"
    )

    utils.geoparquet_gcs_export(
        road_info_with_geom,
        f"{SEGMENT_GCS}segments_staging/",
        f"roads_with_cutpoints_wide_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")