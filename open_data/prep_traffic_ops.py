"""
Import trips, shapes, stops, stop_times files
and get it ready for GTFS schedule routes / stops datasets.
"""
import geopandas as gpd
import intake
import pandas as pd

from calitp_data_analysis import utils, geography_utils
from shared_utils import schedule_rt_utils
from update_vars import TRAFFIC_OPS_GCS, analysis_date

catalog = intake.open_catalog(
    "../_shared_utils/shared_utils/shared_data_catalog.yml")

keep_trip_cols = [
    "feed_key", "name", 
    "trip_id", 
    "route_id", "route_type", 
    "shape_id", "shape_array_key",
    "route_long_name", "route_short_name", "route_desc"
]

keep_shape_cols = [
    "shape_array_key",
    "n_trips", "geometry"
]
  
keep_stop_cols = [
    "feed_key",
    "stop_id", "stop_name", 
    "geometry"
] 

keep_stop_time_cols = [
    "feed_key", "trip_id", "stop_id"
]    
    

def standardize_operator_info_for_exports(
    df: pd.DataFrame, 
    date: str
) -> pd.DataFrame:
    
    crosswalk = schedule_rt_utils.sample_schedule_feed_key_to_organization_crosswalk(
        df, 
        date,
        quartet_data = "schedule", 
        dim_gtfs_dataset_cols = [
            "key", "regional_feed_type",
            "base64_url"],
        dim_organization_cols = [
            "source_record_id", "name", "caltrans_district"]
    )
    
    df2 = pd.merge(
        df,
        crosswalk,
        on = "feed_key",
        how = "inner",
        validate = "m:1"
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
    
    
def export_to_subfolder(file_name: str, date: str):
    """
    We always overwrite the same geoparquets each month, and point our
    shared_utils/shared_data_catalog.yml to the latest file.
    
    But, save historical exports just in case.
    """
    file_name_sanitized = utils.sanitize_file_path(file_name)
    
    gdf = gpd.read_parquet(
        f"{TRAFFIC_OPS_GCS}{file_name_sanitized}.parquet")
        
    utils.geoparquet_gcs_export(
        gdf, 
        f"{TRAFFIC_OPS_GCS}export/", 
        f"{file_name_sanitized}_{date}"
    )
        
        
# Define column names, must fit ESRI 10 character limits
RENAME_COLS = {
    "organization_name": "agency",
    "organization_source_record_id": "org_id",
    "route_name_used": "route_name",
}