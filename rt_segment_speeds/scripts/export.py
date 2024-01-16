"""
Attach columns needed for publishing to open data portal.
Suppress certain rows and columns too.
"""
import datetime
import geopandas as gpd
import pandas as pd

from shared_utils import schedule_rt_utils, utils
from calitp_data_analysis import utils, geography_utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, CONFIG_PATH



def finalize_df_for_export(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Sorting, suppressing columns not needed in export.
    """

    RENAME_DICT = {
        "organization_source_record_id": "org_id",
        "organization_name": "agency",
    }
    
    gdf2 = (gdf.sort_values(["organization_name", 
                         "shape_id", "stop_sequence"])
           .reset_index(drop=True)
           .rename(columns = RENAME_DICT) 
          )
    
    return gdf2


def export_average_speeds(
    analysis_date: str, 
    dict_inputs: dict
):
    start = datetime.datetime.now()
    INPUT_FILE = f'{dict_inputs["stage5"]}_{analysis_date}'
    
    gdf = gpd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}.parquet"
    )
    
    operator_identifiers = get_operator_natural_identifiers(gdf, analysis_date)
     
    time1 = datetime.datetime.now()
    print(f"get natural identifiers: {time1 - start}")
    
    gdf2 = pd.merge(
        gdf,
        operator_identifiers,
        on = ["schedule_gtfs_dataset_key", "shape_array_key"],
        how = "inner"
    )
            
    final_gdf = finalize_df_for_export(gdf2)
        
    time2 = datetime.datetime.now()
    print(f"finalize: {time2 - time1}")
    
    keep_cols = [
        'org_id', 'agency',
        'shape_id', 'stop_sequence', 'stop_id', 
        'geometry',
        'p50_mph', 'p20_mph', 
        'p80_mph', 'n_trips', 
        'time_of_day', 
        'base64_url',
        'district_name'
    ]
    
    utils.geoparquet_gcs_export(
        final_gdf[keep_cols],
        f"{SEGMENT_GCS}export/",
        INPUT_FILE
    )
    
    # Keep a tabular version (geom is big to save) for us to compare what's published
    # and contains columns we use for internal modeling 
    # (shape_array_key, gtfs_dataset_key, etc)
    final_gdf.drop(columns = "geometry").to_parquet(
        f"{SEGMENT_GCS}export/{INPUT_FILE}_tabular.parquet"
    )
    
    utils.geoparquet_gcs_export(
        final_gdf[keep_cols],
        f"{SEGMENT_GCS}export/",
        "speeds_by_stop_segments"
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
    
    return


def average_route_speeds_for_export(
    analysis_date: str,
    dict_inputs: dict
) -> gpd.GeoDataFrame: 
    """
    Aggregate trip speeds to route-direction.
    Attach shape geometry to most common shape_id.
    """
    SPEEDS_FILE = f'{dict_inputs["stage6"]}_{analysis_date}'
    EXPORT_FILE = f'{dict_inputs["stage7"]}_{analysis_date}'
    MAX_SPEED = dict_inputs["max_speed"]
    
    df = pd.read_parquet(
        f"{SEGMENT_GCS}trip_summary/{SPEEDS_FILE}.parquet",
        filters = [[("speed_mph", "<=", MAX_SPEED)]]
    )
    
    # Aggregate by route-direction
    route_cols = [
        "schedule_gtfs_dataset_key", "time_of_day",
        "route_id", "direction_id",
        "route_name_used",
        "common_shape_id", "shape_array_key"
    ]
    
    df2 = (df.groupby(route_cols, 
                      observed = True, group_keys = False)
           .agg({
               "service_minutes": "mean",
               "rt_trip_min": "mean",
               "speed_mph": "mean",
               "trip_instance_key": "count"
           }).reset_index()
          )

    df3 = df2.assign(
        rt_trip_min = df2.rt_trip_min.round(1),
        service_minutes = df2.service_minutes.round(1),
        speed_mph = df2.speed_mph.round(1)
    ).rename(columns = {
        "service_minutes": "avg_sched_trip_min",
        "rt_trip_min": "avg_rt_trip_min",
        "trip_instance_key": "n_trips",
        "route_name_used": "route_name",
        "schedule_gtfs_dataset_key": "gtfs_dataset_key"
    })
    
    # Attach org name and source_record_id
    org_crosswalk = (
        schedule_rt_utils.sample_gtfs_dataset_key_to_organization_crosswalk(
            df3,
            analysis_date,
            quartet_data = "schedule",
            dim_gtfs_dataset_cols = ["key", "base64_url"],
            dim_organization_cols = ["source_record_id", 
                                     "name", "caltrans_district"])
    )
    
    df_with_org = pd.merge(
        df3,
        org_crosswalk.rename(columns = {
            "schedule_gtfs_dataset_key": "gtfs_dataset_key"}),
        on = "gtfs_dataset_key",
        how = "inner"
    )
    
    # Attach shape geometry and make sure it's in WGS84
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        get_pandas = True,
        crs = geography_utils.WGS84
    )
    
    df_with_shape = pd.merge(
        shapes,
        df_with_org,
        on = "shape_array_key", # once merged, can drop shape_array_key
        how = "inner"
    )
    
    agency_cols = ['organization_source_record_id', 'organization_name']
    route_cols = ['route_id', 'route_name', 
                  'direction_id', 'common_shape_id']

    col_order = agency_cols + route_cols + [
        'time_of_day',
        'speed_mph', 'n_trips', 
        'avg_sched_trip_min', 'avg_rt_trip_min', 
        'base64_url', 'caltrans_district',
        'geometry'
    ]
    
    final_df = df_with_shape.reindex(columns = col_order).rename(
        columns = {"organization_source_record_id": "org_id",
                   "organization_name": "agency", 
                   "caltrans_district": "district_name"
                  })

    
    utils.geoparquet_gcs_export(
        final_df,
        f"{SEGMENT_GCS}trip_summary/",
        EXPORT_FILE
    )
    
    utils.geoparquet_gcs_export(
        final_df,
        f"{SEGMENT_GCS}export/",
        "speeds_by_route_time_of_day"
    )
    
    return 


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    for analysis_date in analysis_date_list:
        export_average_speeds(analysis_date, STOP_SEG_DICT)
        average_route_speeds_for_export(analysis_date, STOP_SEG_DICT)
        
        
