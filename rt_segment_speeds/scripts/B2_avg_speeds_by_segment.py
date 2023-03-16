"""
Quick aggregation for avg speeds by segment
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)
from shared_utils import utils, portfolio_utils

def avg_speeds_with_segment_geom(
    analysis_date: str, 
    max_speed_cutoff: int = 70,
    dict_inputs: dict = {}
) -> gpd.GeoDataFrame: 
    """
    Import the segment-trip table. 
    Average the speed_mph across all trips present in the segment.
    """
    SEGMENT_FILE = dict_inputs["segments_file"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    SPEEDS_FILE = dict_inputs["stage4"]
    
    df = dd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDS_FILE}_{analysis_date}/")
    
    # Take the average after dropping unusually high speeds
    segment_cols = [
        "gtfs_dataset_key"
    ]+ SEGMENT_IDENTIFIER_COLS
    
    avg_speeds = (df[(df.speed_mph <= max_speed_cutoff)].compute()
        .groupby(segment_cols)
        .agg({
            "speed_mph": "mean",
            "trip_id": "nunique"
        }).reset_index()
    )
    
    # Clean up for map
    avg_speeds = avg_speeds.assign(
        speed_mph = avg_speeds.speed_mph.round(2),
    ).rename(columns = {"trip_id": "n_trips"})
    
    # Merge in segment geometry
    segments = helpers.import_segments(
        SEGMENT_GCS,
        f"{SEGMENT_FILE}_{analysis_date}",
        columns = segment_cols + ["geometry", "geometry_arrowized"]
    ).set_geometry("geometry_arrowized").drop(columns = "geometry").compute()
    
    gdf = pd.merge(
        segments[~segments.geometry_arrowized.is_empty], 
        avg_speeds,
        on = segment_cols,
        how = "inner"
    )
    
    gdf_with_district = spatial_join_to_caltrans_districts(gdf)
    
    return gdf_with_district


def spatial_join_to_caltrans_districts(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame: 
    """
    Spatial join any gdf to Caltrans districts (open data portal)
    """
    URL = ("https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHboundary/"
           "District_Tiger_Lines/FeatureServer/0/query?"
           "outFields=*&where=1%3D1&f=geojson"
          )
    
    caltrans_districts = gpd.read_file(URL)[["DISTRICT", "geometry"]]
    
    caltrans_districts = caltrans_districts.assign(
        district_name = caltrans_districts.DISTRICT.map(
            portfolio_utils.district_name_dict)
    ).rename(columns = {"DISTRICT": "district"})
    
    # Spatial join to Caltrans district
    gdf2 = gpd.sjoin(
        gdf, 
        caltrans_districts.to_crs(gdf.crs),
        how = "inner",
        predicate = "intersects"
    ).drop(columns = "index_right")
    
    return gdf2
    

if __name__ == "__main__":
    
    ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    MAX_SPEED = 70
    
    # Average the speeds for segment for entire day
    # Drop speeds above our max cutoff
    route_segment_speeds = avg_speeds_with_segment_geom(
        analysis_date, 
        max_speed_cutoff = MAX_SPEED,
        dict_inputs = ROUTE_SEG_DICT
    )
        
    utils.geoparquet_gcs_export(
        route_segment_speeds,
        SEGMENT_GCS,
        f"{ROUTE_SEG_DICT['stage5']}_{analysis_date}"
    )
    
    stop_segment_speeds = avg_speeds_with_segment_geom(
        analysis_date, 
        max_speed_cutoff = MAX_SPEED,
        dict_inputs = STOP_SEG_DICT
    )
        
    utils.geoparquet_gcs_export(
        stop_segment_speeds,
        SEGMENT_GCS,
        f"{STOP_SEG_DICT['stage5']}_{analysis_date}"
    )
