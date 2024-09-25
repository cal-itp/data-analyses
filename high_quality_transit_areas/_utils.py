"""
Shared utility functions for HQTA
"""
import geopandas as gpd
import intake
import pandas as pd

catalog = intake.open_catalog("catalog.yml")

def add_hqta_details(row) -> str:
    """
    Add HQTA details of why nulls are present 
    based on feedback from open data users.
    """    
    if row.hqta_type == "major_stop_bus":
        if row.schedule_gtfs_dataset_key_primary != row.schedule_gtfs_dataset_key_secondary:
            return "intersection_2_bus_routes_different_operators"
        else:
            return "intersection_2_bus_routes_same_operator"  
    
    elif row.hqta_type == "hq_corridor_bus":
        if row.peak_trips >= 4:
            return "corridor_frequent_stop"
        else:
            return "corridor_other_stop"
    
    elif row.hqta_type in ["major_stop_ferry", 
                           "major_stop_brt", "major_stop_rail"]:
        return row.hqta_type + "_single_operator"

def primary_rename(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns = {"schedule_gtfs_dataset_key": "schedule_gtfs_dataset_key_primary"})

def clip_to_ca(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Clip to CA boundaries. 
    """    
    ca = catalog.ca_boundary.read().to_crs(gdf.crs)

    gdf2 = gdf.clip(ca, keep_geom_type = False).reset_index(drop=True)

    return gdf2