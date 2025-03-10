"""
Import trips, shapes, stops, stop_times files
and get it ready for GTFS schedule routes / stops datasets.
"""
import geopandas as gpd
import intake
import pandas as pd

from calitp_data_analysis import geography_utils
from shared_utils import gtfs_utils_v2, schedule_rt_utils
from update_vars import TRAFFIC_OPS_GCS, analysis_date, GTFS_DATA_DICT, SCHED_GCS

catalog = intake.open_catalog(
    "../_shared_utils/shared_utils/shared_data_catalog.yml")
    
    
def standardize_operator_info_for_exports(
    df: pd.DataFrame, 
    date: str
) -> pd.DataFrame:
    """
    Use our crosswalk file created in gtfs_funnel
    and add in the organization columns we want to 
    publish on.
    """
    CROSSWALK_FILE = GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk

    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()

    crosswalk = pd.read_parquet(
        f"{SCHED_GCS}{CROSSWALK_FILE}_{date}.parquet",
        columns = [
            "schedule_gtfs_dataset_key", "name", "base64_url", 
            "organization_source_record_id", "organization_name",
            "caltrans_district",
        ],
        filters = [[("schedule_gtfs_dataset_key", "in", public_feeds)]]
    )
    
    # Checked whether we need a left merge to keep stops outside of CA
    # that may not have caltrans_district
    # and inner merge is fine. All operators are assigned a caltrans_district
    # so Amtrak / FlixBus stops have values populated
    df2 = pd.merge(
        df,
        crosswalk,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    )
    
    return df2
    
    
def clip_to_usa(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Rarely, a stray stop might fall in Canada, and not along the border.
    For stops, let's clip to US boundary and then do a check for
    points that are way too far from the border.
    
    Don't do anything for routes, since an sjoin will drop the SF ferry related routes.
    """
    usa = catalog.us_states.read()[["NAME", "geometry"]]
    not_continental = [
        "Alaska", "Hawaii", 
        "U.S. Virgin Islands", "Puerto Rico"
    ]
    usa = (usa[~usa.NAME.isin(not_continental)]
           [["geometry"]]
           .dissolve()
           .reset_index()
           .simplify(tolerance = 0.001)
          )
    
    # Check if gdf contains points or polygons
    # https://shapely.readthedocs.io/en/stable/reference/shapely.get_type_id.html
    one_geom_value = gdf.geometry.iloc[0]
    geom_type = geography_utils.find_geometry_type(one_geom_value)
    
    if geom_type == "point":
        
        # Clip to the US and find the points that are near border
        gdf2 = gdf.clip(usa)

        merge_cols = [c for c in gdf.columns if c != "geometry"]

        # Select the points that are not in gdf2 (left_only)
        # Of these that are eligible to be dropped, there's
        # a couple that we do want to keep that are outside US, 
        # but we don't want to keep ones that are way too far, which are obviously wrong
        dropped = pd.merge(
            gdf,
            gdf2.drop(columns = "geometry"),
            on = merge_cols,
            how = "left",
            indicator = True
        ).query("_merge == 'left_only'").drop(columns = "_merge")

        # If it's more than 2 decimal degrees away, it's probably too far.
        # We do want to keep some points in Canada and Mexico because
        # Amtrak and Greyhound service those cities
        dropped = dropped.assign(
            distance_dec_degrees = dropped.distance(usa[0])
        ).query("distance_dec_degrees > 2")
        
        # Keep left_only, since those are the points we want to keep
        # Both refers to points that show up in the dropped list
        gdf2 = (pd.merge(
                gdf,
                dropped[merge_cols],
                how = "left",
                indicator = True
            ).query("_merge == 'left_only'")
            .drop(columns = "_merge")
            .reset_index(drop=True)
        )
        
    else:
        gdf2 = gdf.copy()
        
    return gdf2
    

STANDARDIZED_COLUMNS_DICT = {
    "caltrans_district": "district_name",
    "organization_source_record_id": "org_id",
    "organization_name": "agency",
    "agency_name_primary": "agency_primary",
    "agency_name_secondary": "agency_secondary",
    "route_name_used": "route_name",
    "route_types_served": "routetypes",
    "n_hours_in": "n_hours_in_service",
    "route_ids_": "route_ids_served",
    "meters_to_shn": "meters_to_ca_state_highway"
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