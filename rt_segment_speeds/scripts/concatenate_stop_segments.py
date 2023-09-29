"""
Concatenate the two stop segment files,
spatial join to district,
and arrowize segments
"""
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from calitp_data_analysis.geography_utils import WGS84
from shared_utils import portfolio_utils
from segment_speed_utils import helpers, sched_rt_utils, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)


def finalize_stop_segments(
    stop_segments: gpd.GeoDataFrame, 
):
    """    
    Add segment_idx.
    """    
    stop_segments = stop_segments.assign(
        seg_idx = stop_segments.index
    )
    
    return stop_segments


def spatial_join_to_caltrans_districts(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame: 
    """
    Spatial join any gdf to Caltrans districts (open data portal).
    Use centroid for simpler sjoin. 
    For segments, we're taking the centroid of a linestring.
    """
    gdf = gdf.assign(
        centroid = gdf.geometry.centroid,
    )
    
    caltrans_districts = gpd.read_parquet(
        "gs://calitp-analytics-data/data-analyses/shared_data/"
        "caltrans_districts.parquet"
    ).to_crs(WGS84)
    
    caltrans_districts = caltrans_districts.assign(
        district_name = caltrans_districts.DISTRICT.map(
            portfolio_utils.district_name_dict)
    ).rename(columns = {"DISTRICT": "district"})
    
    # Spatial join to Caltrans district
    sjoin_results = gpd.sjoin(
        gdf[["seg_idx", "centroid"]].set_geometry("centroid"), 
        caltrans_districts.to_crs(gdf.crs),
        how = "inner",
        predicate = "intersects"
    ).drop(columns = "index_right")[["seg_idx", "district", "district_name"]]
    
    gdf3 = pd.merge(
        gdf,
        sjoin_results,
        on = "seg_idx",
        how = "inner"
    ).set_geometry("geometry").drop(columns = "centroid")
    
    return gdf3


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
        part_gdf = gpd.read_parquet(f"{SEGMENT_GCS}segments_staging/{file}.parquet")
        gdf = pd.concat([gdf, part_gdf], axis=0)
    
    gdf = (gdf.rename(columns = {"stop_segment_geometry": "geometry"})
           .sort_values(SEGMENT_IDENTIFIER_COLS)
           .reset_index(drop=True)
           .set_geometry("geometry")
          )
    
    gdf = finalize_stop_segments(gdf)
    
    gdf2 = spatial_join_to_caltrans_districts(gdf)
    
    arrowized_segments = wrangle_shapes.add_arrowized_geometry(
        gdf2)
    
    utils.geoparquet_gcs_export(
        arrowized_segments, 
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )