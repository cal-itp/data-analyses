import geopandas as gpd
import pandas as pd

STANDARDIZED_COLUMNS_DICT = {
    "caltrans_district": "district_name",
    "organization_source_record_id": "org_id",
    "organization_name": "agency",
    "agency_name_primary": "agency_primary",
    "agency_name_secondary": "agency_secondary"
}


# Rename columns when shapefile truncates
RENAME_HQTA = {
    "agency_pri": "agency_primary",
    "agency_sec": "agency_secondary",
    "hqta_detai": "hqta_details",
    "base64_url": "base64_url_primary",
    "base64_u_1": "base64_url_secondary",  
    "org_id_pri": "org_id_primary",
    "org_id_sec": "org_id_secondary",
}

RENAME_SPEED = {
    "stop_seque": "stop_sequence",
    "time_of_da": "time_of_day",
    "time_perio": "time_period",
    "district_n": "district_name",
    "direction_": "direction_id",
    "common_sha": "common_shape_id",
    "avg_sched_": "avg_sched_trip_min", 
    "avg_rt_tri": "avg_rt_trip_min",
    "caltrans_d": "district_name",
    "organization_source_record_id": "org_id",
    "organization_name": "agency",
    "stop_pair_": "stop_pair_name"
}

def standardize_column_names(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Standardize how agency is referred to.
    """
    return df.rename(columns = STANDARDIZED_COLUMNS_DICT)


def remove_internal_keys(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Remove columns used in our internal data modeling.
    Leave only natural identifiers (route_id, shape_id).
    Remove shape_array_key, gtfs_dataset_key, etc.
    """
    exclude_list = [
        "sec_elapsed", "meters_elapsed", 
        "name", "schedule_gtfs_dataset_key"
    ]
    cols = [c for c in df.columns]
    
    internal_cols = [c for c in cols if "_key" in c or c in exclude_list] 
    
    print(f"drop: {internal_cols}")
    
    return df.drop(columns = internal_cols)