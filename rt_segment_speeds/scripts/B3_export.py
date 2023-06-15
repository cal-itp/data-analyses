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

from shared_utils import gtfs_utils_v2, portfolio_utils, utils
from shared_utils.geography_utils import WGS84
from segment_speed_utils import helpers, segment_calcs
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
    ).drop_duplicates()
    
    df_with_shape = pd.merge(
        operator_shape_df,
        shape_identifiers,
        on = "shape_array_key",
        how = "inner"
    )
    
    # Get base64_url and uri
    dim_gtfs_datasets = gtfs_utils_v2.get_transit_organizations_gtfs_dataset_keys(
        keep_cols=None, get_df=True)

    dim_gtfs_datasets = segment_calcs.localize_vp_timestamp(
        dim_gtfs_datasets, 
        ["_valid_from", "_valid_to"]
    )
    
    current_feeds = (
        dim_gtfs_datasets[
            (dim_gtfs_datasets.data_quality_pipeline == True)
            #& (dim_gtfs_datasets._is_current == True)
            & (dim_gtfs_datasets._valid_from_local <= pd.to_datetime(analysis_date))
            & (dim_gtfs_datasets._valid_to_local >= pd.to_datetime(analysis_date))
        ].drop_duplicates("gtfs_dataset_key")
        [["gtfs_dataset_key", "uri", "base64_url"]]
    )
    
    df_with_url = pd.merge(
        df_with_shape,
        current_feeds,
        on = "gtfs_dataset_key",
        how = "inner"
    )
    
    # Get organization_name and organization_source_record_id
    df_with_url = df_with_url.rename(columns = {
        "gtfs_dataset_key": "vehicle_positions_gtfs_dataset_key",
    })
    
    merge_cols = ["vehicle_positions_gtfs_dataset_key"]
    
    df_with_org = portfolio_utils.get_organization_name(
        df_with_url,
        analysis_date,
        merge_cols
    )
    
    return df_with_org


def finalize_df_for_export(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Sorting, suppressing columns not needed in export.
    """
    keep_cols = [
        'organization_source_record_id', 'organization_name',
        'shape_id', 'stop_sequence', 'stop_id', 
        #'loop_or_inlining', 
        'geometry',
        'median_speed_mph', 'p20_speed_mph', 
        'p80_speed_mph', 'n_trips', 
        'time_of_day', 
        'uri', 'base64_url',
        'regional_feed_type', 
        'district', 'district_name'
    ]
    
    gdf2 = (gdf.reindex(columns = keep_cols)
           .sort_values(["organization_name", 
                         "shape_id", "stop_sequence"])
           .reset_index(drop=True)
          )
    
    return gdf2


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    INPUT_FILE = f'{STOP_SEG_DICT["stage5"]}_{analysis_date}'
    
    gdf = gpd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}.parquet"
    ).drop(columns = "geometry_arrowized")
    
    operator_identifiers = get_operator_natural_identifiers(gdf, analysis_date)
     
    time1 = datetime.datetime.now()
    print(f"get natural identifiers: {time1 - start}")
    
    gdf = gdf.rename(
        columns = {"gtfs_dataset_key": "vehicle_positions_gtfs_dataset_key"})

    gdf2 = pd.merge(
        gdf,
        operator_identifiers,
        on = ["vehicle_positions_gtfs_dataset_key", "shape_array_key"],
        how = "inner"
    )
            
    final_gdf = finalize_df_for_export(gdf2)
    
    time2 = datetime.datetime.now()
    print(f"finalize: {time2 - time1}")
    
    utils.geoparquet_gcs_export(
        final_gdf,
        f"{SEGMENT_GCS}export/",
        INPUT_FILE
    )
    
    end = datetime.datetime.now()
    print(f"export: {end - time2}")
    print(f"execution time: {end - start}")