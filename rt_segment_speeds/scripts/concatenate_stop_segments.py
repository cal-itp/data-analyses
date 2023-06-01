"""
Concatenate the two stop segment files and arrowize segments
"""
import geopandas as gpd
import pandas as pd

from shared_utils import utils
from segment_speed_utils import helpers, sched_rt_utils, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)


def finalize_stop_segments(
    stop_segments: gpd.GeoDataFrame, 
):
    """    
    Add gtfs_dataset_key.
    """    
    if "gtfs_dataset_key" in stop_segments.columns:
        stop_segments = stop_segments.drop(columns = "gtfs_dataset_key")
        
    # Add gtfs_dataset_key to this segment data (from schedule)
    stop_segments_with_rt_key = sched_rt_utils.add_rt_keys_to_segments(
        stop_segments, 
        analysis_date, 
        ["feed_key", "shape_array_key"]
    ).reset_index(drop=True)

    stop_segments_with_rt_key = stop_segments_with_rt_key.assign(
        seg_idx = stop_segments_with_rt_key.index
    )
    
    return stop_segments_with_rt_key


if __name__ == "__main__":
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    SEGMENT_IDENTIFIER_COLS = STOP_SEG_DICT["segment_identifier_cols"]
    EXPORT_FILE = STOP_SEG_DICT["segments_file"]
    
    INPUT_FILES = [
        f"stop_segments_normal_{analysis_date}",
        f"stop_segments_special_{analysis_date}",
    ]
        
    gdf = gpd.GeoDataFrame()
    
    for file in INPUT_FILES:
        part_gdf = gpd.read_parquet(f"{SEGMENT_GCS}{file}.parquet")
        gdf = pd.concat([gdf, part_gdf], axis=0)
    
    gdf = (gdf.rename(columns = {"stop_segment_geometry": "geometry"})
           .sort_values(SEGMENT_IDENTIFIER_COLS)
           .reset_index(drop=True)
           .set_geometry("geometry")
          )
    
    gdf = finalize_stop_segments(gdf)
    
    arrowized_segments = wrangle_shapes.add_arrowized_geometry(
        gdf)
    
    utils.geoparquet_gcs_export(
        arrowized_segments, 
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )