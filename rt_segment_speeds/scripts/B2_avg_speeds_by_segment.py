"""
Quick aggregation for avg speeds by segment
"""
import geopandas as gpd
import pandas as pd

from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)
from shared_utils import utils, portfolio_utils


def calculate_avg_speeds(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Calculate the mean, 20th, and 80th percentile speeds 
    by groups.
    """
    # Take the average after dropping unusually high speeds
    avg = (df.groupby(group_cols)
          .agg({
            "speed_mph": "mean",
            "trip_id": "nunique"})
          .reset_index()
    )
    
    p20 = (df.groupby(group_cols)
           .agg({"speed_mph": lambda x: x.quantile(0.2)})
           .reset_index()  
          )
    
    p80 = (df.groupby(group_cols)
           .agg({"speed_mph": lambda x: x.quantile(0.8)})
           .reset_index()  
          )
    
    stats = pd.merge(
        avg.rename(columns = {"speed_mph": "avg_speed_mph", 
                              "trip_id": "n_trips"}),
        p20.rename(columns = {"speed_mph": "p20_speed_mph"}),
        on = group_cols,
        how = "left"
    ).merge(
        p80.rename(columns = {"speed_mph": "p80_speed_mph"}),
        on = group_cols,
        how = "left"
    )
    
    # Clean up for map
    speed_cols = [c for c in stats.columns if "_speed_mph" in c]
    stats[speed_cols] = stats[speed_cols].round(2)
    
    return stats
    
    

def speeds_with_segment_geom(
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
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDS_FILE}_{analysis_date}", 
        filters = [[("speed_mph", "<=", max_speed_cutoff)]]
    )
    
    time_of_day_df = sched_rt_utils.get_trip_time_buckets(analysis_date)
    
    df2 = pd.merge(
        df, 
        time_of_day_df, 
        on = ["gtfs_dataset_key", "trip_id"], 
        how = "inner"
    )
    
    all_day = calculate_avg_speeds(
        df2, 
        SEGMENT_IDENTIFIER_COLS
    )
    
    peak = calculate_avg_speeds(
        df2[df2.time_of_day.isin(["AM Peak", "PM Peak"])], 
        SEGMENT_IDENTIFIER_COLS
    )
    
    stats = pd.concat([
        all_day.assign(time_of_day = "all_day"),
        peak.assign(time_of_day = "peak")
    ], axis=0)
    
    
    # Merge in segment geometry
    segments = helpers.import_segments(
        SEGMENT_GCS,
        f"{SEGMENT_FILE}_{analysis_date}",
        columns = SEGMENT_IDENTIFIER_COLS + [
            "gtfs_dataset_key", 
            "stop_id",
            "loop_or_inlining",
            "geometry", "geometry_arrowized"]
    )#.set_geometry("geometry_arrowized").drop(columns = "geometry")
    
    gdf = pd.merge(
        segments,#[~segments.geometry_arrowized.is_empty], 
        stats,
        on = SEGMENT_IDENTIFIER_COLS,
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
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    MAX_SPEED = 70
    
    # Average the speeds for segment for entire day
    # Drop speeds above our max cutoff
    stop_segment_speeds = speeds_with_segment_geom(
        analysis_date, 
        max_speed_cutoff = MAX_SPEED,
        dict_inputs = STOP_SEG_DICT
    )
        
    utils.geoparquet_gcs_export(
        stop_segment_speeds,
        SEGMENT_GCS,
        f"{STOP_SEG_DICT['stage5']}_{analysis_date}"
    )
