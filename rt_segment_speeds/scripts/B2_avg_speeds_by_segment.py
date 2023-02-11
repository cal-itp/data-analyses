"""
Quick aggregation for avg speeds by segment
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from shared_utils import utils, portfolio_utils
from update_vars import SEGMENT_GCS, analysis_date

def avg_speeds_with_segment_geom(
    analysis_date: str, 
    max_speed_cutoff: int = 70
) -> gpd.GeoDataFrame: 
    """
    Import the segment-trip table. 
    Average the speed_mph across all trips present in the segment.
    """
    df = dd.read_parquet(
        f"{SEGMENT_GCS}speeds_{analysis_date}/")
    
    # Take the average after dropping unusually high speeds
    segment_cols = ["calitp_itp_id", "route_dir_identifier", 
                    "segment_sequence"]
    
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
    segments = gpd.read_parquet(
        f"{SEGMENT_GCS}longest_shape_segments.parquet",
        columns = segment_cols + ["geometry", "geometry_arrowized"]
    ).drop_duplicates().reset_index(drop=True)
    
    segments2 = segments.set_geometry("geometry_arrowized").drop(
        columns = "geometry")
    segments2.crs = segments.crs
    
    gdf = pd.merge(
        segments2[~segments2.geometry_arrowized.is_empty], 
        avg_speeds,
        on = segment_cols,
        how = "inner"
    )
    
    return gdf



if __name__ == "__main__":
    URL = ("https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHboundary/"
           "District_Tiger_Lines/FeatureServer/0/query?"
           "outFields=*&where=1%3D1&f=geojson"
          )
    
    caltrans_districts = gpd.read_file(URL)[["DISTRICT", "geometry"]]
    
    caltrans_districts = caltrans_districts.assign(
        district_name = caltrans_districts.DISTRICT.map(
            portfolio_utils.district_name_dict)
    ).rename(columns = {"DISTRICT": "district"})
    
    # Average the speeds for segment for entire day
    # Drop speeds above our max cutoff
    gdf = avg_speeds_with_segment_geom(
        analysis_date, 
        max_speed_cutoff = 70
    )
    
    # Spatial join to Caltrans district
    gdf2 = gpd.sjoin(
        gdf, 
        caltrans_districts.to_crs(gdf.crs),
        how = "inner",
        predicate = "intersects"
    ).drop(columns = "index_right")

    utils.geoparquet_gcs_export(
        gdf2,
        SEGMENT_GCS,
        f"avg_speeds_{analysis_date}"
    )
