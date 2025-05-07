"""
Resample every 5 seconds and 
interpolate the distances with the resampled timestamps.
"""
import datetime
import pandas as pd
import geopandas as gpd
import sys

from loguru import logger

from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT
from shared_utils import  publish_utils
import model_utils

def determine_batches(analysis_date: str) -> dict:
    """
    Break out 2 batches, LA Metro + Bay Area in one batch
    and all other operators in another batch.
    """
    VP_FILE = GTFS_DATA_DICT.modeled_vp.raw_vp

    vp = pd.read_parquet(
        f"{SEGMENT_GCS}{VP_FILE}_{analysis_date}.parquet",
        columns = ["gtfs_dataset_name"],
    ).drop_duplicates()
    
    large_operators = [
        "LA Metro Bus",
        "LA Metro Rail",
        "Bay Area 511"
    ]
  
    # If any of the large operator name substring is 
    # found in our list of names, grab those
    # be flexible bc "Vehicle Positions" and "VehiclePositions" present
    matching1 = [i for i in vp.gtfs_dataset_name 
                if any(name in i for name in large_operators)]
    
    remaining = [i for i in vp.gtfs_dataset_name if 
                 i not in matching1]
    
    # Batch large operators together and run remaining in 2nd query
    batch_dict = {}
    
    batch_dict[0] = matching1
    batch_dict[1] = remaining
    
    return batch_dict


def get_trips_by_batches(
    operator_list: list
) -> list:
    """
    Return list of trip_instance_keys for each batch.
    """
    VP_FILE = GTFS_DATA_DICT.modeled_vp.raw_vp
    
    subset_trips = pd.read_parquet(
        f"{SEGMENT_GCS}{VP_FILE}_{analysis_date}.parquet",
        filters = [["gtfs_dataset_name", "in", operator_list]],
        columns = ["trip_instance_key"],
    ).trip_instance_key.unique().tolist()
    
    return subset_trips


def get_resampled_vp_points(
    gdf: gpd.GeoDataFrame,
    resampling_seconds_interval: int
) -> gpd.GeoDataFrame:
    """
    Resample vp timestamps every 5 seconds to start,
    then interpolate the distances and fill in between.
    
    Return a gdf with 
    - resampled timestamps (array of timestamps coerced to seconds, then floats)
    - interpolated distances (array, meters along shape geometry)
    """
    # Get timestamps that are datetime, change to display up to seconds, then convert to floats
    # This will be easier to use when we're visually inspecting
    timestamps_series = gdf.apply(
        lambda x: 
        x.location_timestamp_local.astype("datetime64[s]").astype("float64"), 
        axis=1
    )
    
    vp_meters_series = gdf.vp_meters
    vp_line_geometry = gdf.vp_geometry
    
    resampled_timestamps = [
        model_utils.resample_timestamps(t, resampling_seconds_interval) 
        for t in timestamps_series
    ]
    
    interpolated_vp_meters = [
        model_utils.interpolate_distances_for_resampled_timestamps(
            new_timestamps, orig_timestamps, vp_meters) 
            for new_timestamps, orig_timestamps, vp_meters 
            in zip(resampled_timestamps, timestamps_series, vp_meters_series)
    ]

    
    gdf2 = gdf.assign(
        resampled_timestamps = resampled_timestamps,
        interpolated_distances = interpolated_vp_meters,
    )[["trip_instance_key", "resampled_timestamps", "interpolated_distances"]]
    
    return gdf2


def export_concatenated_file(
    list_of_files: list, 
    export_file: str
):
    """
    Concatenate the batched files and export.
    """
    full_results = pd.concat([
        pd.read_parquet(f) for f in list_of_files
    ], axis=0, ignore_index=True)

    full_results.to_parquet(export_file)

    return


if __name__ == "__main__":
    
    LOG_FILE = "../logs/modeled_vp_preprocessing.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    from segment_speed_utils.project_vars import test_dates
    
    for analysis_date in test_dates:
    
        start = datetime.datetime.now()
        
        INPUT_FILE = GTFS_DATA_DICT.modeled_vp.vp_projected
        EXPORT_FILE = GTFS_DATA_DICT.modeled_vp.resampled_vp
        
        batch_dict = determine_batches(analysis_date)

        for batch_number, subset_operators in batch_dict.items():

            subset_trips = get_trips_by_batches(subset_operators)    

            gdf = gpd.read_parquet(
                f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
                filters = [[("trip_instance_key", "in", subset_trips)]]
            )

            results = get_resampled_vp_points(gdf, resampling_seconds_interval = 5)

            results.to_parquet(
                f"{SEGMENT_GCS}{EXPORT_FILE}_batch{batch_number}_{analysis_date}.parquet"
            )


            del gdf, results

            
        # Concatenate batched files into 1 file and export 
        batched_files = [
            f"{SEGMENT_GCS}{EXPORT_FILE}_batch{batch_number}_{analysis_date}.parquet"
            for batch_number in list(batch_dict.keys())
        ]
        
        export_concatenated_file(
            batched_files, 
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet"
        )
        
        # Delete the batched files here, separate it from the exporting, in case export fails.
        for f in batched_files:
            publish_utils.if_exists_then_delete(f)  

        end = datetime.datetime.now()
        
        logger.info(
            f"{analysis_date}: resample, interpolate: {end - start}"
        )