"""
Attach columns needed for publishing to open data portal.
Suppress certain rows and columns too.
"""
import os
os.environ['USE_PYGEOS'] = '0'

import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd

from shared_utils import schedule_rt_utils, utils
from shared_utils.geography_utils import WGS84
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH)

def get_operator_natural_identifiers(
    df: pd.DataFrame, 
    analysis_date: str
) -> pd.DataFrame:
    """
    For each gtfs_dataset_key-shape_array_key combination,
    re-attach the natural identifiers and organizational identifiers.
    Return a df that should be merged against speeds_df.
    """
    operator_shape_df = (df[["gtfs_dataset_key", "shape_array_key"]]
                         .drop_duplicates()
                         .reset_index(drop=True)
                        )
    
    # Get shape_id back
    shape_identifiers = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["shape_array_key", "shape_id"],
        get_pandas = True
    )
    
    df_with_shape = pd.merge(
        operator_shape_df,
        shape_identifiers,
        on = "shape_array_key",
        how = "inner"
    )
    
    # Get base64_url, uri, organization_source_record_id and organization_name
    crosswalk = schedule_rt_utils.sample_gtfs_dataset_key_to_organization_crosswalk(
        df_with_shape,
        analysis_date,
        quartet_data = "vehicle_positions",
        dim_gtfs_dataset_cols = [
            "key",
            "base64_url",
            "uri",
        ],
        dim_organization_cols = ["source_record_id", "name"]
    )

    df_with_org = pd.merge(
        df_with_shape,
        crosswalk.rename(
            columns = {"vehicle_positions_gtfs_dataset_key": "gtfs_dataset_key"}),
        on = "gtfs_dataset_key",
        how = "inner"
    )
    
    return df_with_org


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

if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    INPUT_FILE = f'{STOP_SEG_DICT["stage5"]}_{analysis_date}'
    
    gdf = gpd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}.parquet"
    )
    
    operator_identifiers = get_operator_natural_identifiers(gdf, analysis_date)
     
    time1 = datetime.datetime.now()
    print(f"get natural identifiers: {time1 - start}")
    
    gdf2 = pd.merge(
        gdf,
        operator_identifiers,
        on = ["gtfs_dataset_key", "shape_array_key"],
        how = "inner"
    )
            
    gdf3 = finalize_df_for_export(gdf2)
    
    final_gdf = suppress_bad_data(gdf3)
    
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
    print(f"export: {end - time2}")
    print(f"execution time: {end - start}")