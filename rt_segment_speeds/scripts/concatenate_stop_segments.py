"""
Concatenate the two stop segment files and arrowize segments
"""
import geopandas as gpd
import pandas as pd

from shared_utils import utils
from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)


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
    
    arrowized_segments = wrangle_shapes.add_arrowized_geometry(
        gdf)
    
    utils.geoparquet_gcs_export(
        arrowized_segments, 
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )