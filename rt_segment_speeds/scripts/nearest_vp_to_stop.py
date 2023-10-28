"""
Handle normal vs loopy shapes separately.

For normal shapes, find the nearest vp_idx before a stop,
and the vp_idx after.
"""
import dask.dataframe as dd
import datetime
import numpy as np
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS, PROJECT_CRS
from shared_utils import rt_dates


def rt_trips_to_shape(analysis_date: str) -> pd.DataFrame:
    """
    Filter down trip_instance_keys from schedule to 
    trips present in vp.
    Provide shape_array_key associated with trip_instance_key.
    """
    # Get RT trips
    rt_trips = pd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns = ["trip_instance_key"]
    ).drop_duplicates()

    # Find the shape_array_key for RT trips
    trip_to_shape = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key"],
        get_pandas = True
    ).merge(
        rt_trips,
        on = "trip_instance_key",
        how = "inner"
    )

    # Find whether it's loop or inlining
    shapes_loop_inlining = pd.read_parquet(
        f"{SEGMENT_GCS}stops_projected_{analysis_date}.parquet",
        columns = [
            "shape_array_key", "loop_or_inlining", 
        ]
    ).drop_duplicates().merge(
        trip_to_shape,
        on = "shape_array_key",
        how = "inner"
    )
    
    return shapes_loop_inlining


def vp_with_shape_meters(
    vp_file_name: str, 
    **kwargs
) -> dd.DataFrame:
    """
    Subset vp_usable down based on list of trip_instance_keys.
    For these trips, attach the projected shape meters.
    """
    vp = dd.read_parquet(
        f"{SEGMENT_GCS}{vp_file_name}",
        **kwargs,
        columns = ["trip_instance_key", "vp_idx", 
                   "vp_primary_direction", 
                  ]
    )
    
    projected_shape_meters = pd.read_parquet(
        f"{SEGMENT_GCS}projection/vp_projected_{analysis_date}.parquet",
    )

    vp_with_projection = dd.merge(
        vp,
        projected_shape_meters,
        on = "vp_idx",
        how = "inner"
    )
    
    return vp_with_projection


def transform_vp(vp: dd.DataFrame) -> dd.DataFrame:
    """
    For each trip, transform vp from long to wide,
    so each row is one trip.
    Store vp_idx and shape_meters as lists.
    """
    trip_shape_cols = ["trip_instance_key", "shape_array_key"]
    
    trip_info = (
        vp
        .groupby(trip_shape_cols, 
                  observed=True, group_keys=False)
        .agg({
            "vp_idx": lambda x: list(x),
            "shape_meters": lambda x: list(x),
            "vp_primary_direction": lambda x: list(x),
        })
        .reset_index()
        .rename(columns = {
            "vp_idx": "vp_idx_arr",
            "shape_meters": "shape_meters_arr",
            "vp_primary_direction": "vp_dir_arr"
        })
    )
    
    return trip_info


def find_vp_nearest_stop_position(
    df: dd.DataFrame, 
) -> dd.DataFrame:
    """
    Once we've attached where each shape has stop cutpoints (stop_meters),
    for each trip_instance_key, we want to find where the nearest
    vp_idx is to that particular stop.
    
    We have array of vp_idx and vp_shape_meters.
    Go through each row and find the nearest vp_shape_meters is
    to stop_meters, and save that vp_idx value.
    """
    trip_shape_cols = ["trip_instance_key", "shape_array_key"]
           
    nearest_vp_idx = []
    subseq_vp_idx = []
    
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/rt_analysis/rt_parser.py#L270-L271
    # Don't forget to subtract 1 for proper index  
    # Use this as a mask to hide the values that are not valid
    # https://stackoverflow.com/questions/16094563/numpy-get-index-where-value-is-true

    for row in df.itertuples():
        
        this_stop_direction = getattr(row, "stop_primary_direction")
        opposite_to_stop_direction = wrangle_shapes.OPPOSITE_DIRECTIONS[
            this_stop_direction]
        
        # Start with the array of all the vp primary direction
        vp_dir_array = np.asarray(getattr(row, "vp_dir_arr"))
        
        # Hide the array indices where it's the opposite direction
        # Keep the ones running in all other directions
        valid_indices = (vp_dir_array != opposite_to_stop_direction).nonzero()

        shape_meters_array = getattr(row, "shape_meters_arr")
        vp_idx_array = getattr(row, "vp_idx_arr")

        # Make sure these are arrays so we can search within the valid values
        valid_shape_meters_array = np.asarray(shape_meters_array)[valid_indices]
        valid_vp_idx_array = np.asarray(vp_idx_array)[valid_indices]
        

        this_stop_meters = getattr(row, "stop_meters")
        
        idx = np.searchsorted(
            valid_shape_meters_array,
            getattr(row, "stop_meters"),
            side="right" 
            # want our stop_meters value to be < vp_shape_meters,
            # side = "left" would be stop_meters <= vp_shape_meters
        )

        # For the next value, if there's nothing to index into, 
        # just set it to the same position
        # if we set subseq_value = getattr(row, )[idx], we might not get a consecutive vp
        nearest_value = valid_vp_idx_array[idx-1]
        subseq_value = nearest_value + 1

        nearest_vp_idx.append(nearest_value)
        subseq_vp_idx.append(subseq_value)
    
   
    result = df[trip_shape_cols + ["stop_sequence", "stop_id", "stop_meters"]]
    
    # Now assign the nearest vp for each trip that's nearest to
    # a given stop
    # Need to find the one after the stop later
    result = result.assign(
        nearest_vp_idx = nearest_vp_idx,
        subseq_vp_idx = subseq_vp_idx,
    )

    return result


def fix_out_of_bound_results(
    df: pd.DataFrame, 
    vp_file_name: str
) -> pd.DataFrame:
    
    # Merge in usable bounds
    usable_bounds = dd.read_parquet(
        f"{SEGMENT_GCS}{vp_file_name}"
    ).pipe(segment_calcs.get_usable_vp_bounds_by_trip)
    
    results_with_bounds = pd.merge(
        df,
        usable_bounds,
        on = "trip_instance_key",
        how = "inner"
    )
    
    correct_results = results_with_bounds.query('subseq_vp_idx <= max_vp_idx')
    incorrect_results = results_with_bounds.query('subseq_vp_idx > max_vp_idx')
    incorrect_results = incorrect_results.assign(
        subseq_vp_idx = incorrect_results.nearest_vp_idx
    )
    
    fixed_results = pd.concat(
        [correct_results, incorrect_results], 
        axis=0
    ).drop(columns = ["min_vp_idx", "max_vp_idx"]).sort_index()
    
    return fixed_results


if __name__ == "__main__":
    
    LOG_FILE = "../logs/nearest_vp.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    analysis_date = rt_dates.DATES["sep2023"]
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    VP_FILE = "vp_usable"
    VP_FILE_NAME = f"vp_usable_{analysis_date}"
    
    shape_trip_crosswalk = rt_trips_to_shape(analysis_date)
    shape_keys = shape_trip_crosswalk.shape_array_key.unique().tolist()
    
    vp = vp_with_shape_meters(
        VP_FILE_NAME, 
    ).merge(
        shape_trip_crosswalk,
        on = "trip_instance_key",
        how = "inner"
    )
    
    vp_wide = vp.map_partitions(
        transform_vp,
        meta = {"trip_instance_key": "object",
                "shape_array_key": "object",
                "vp_idx_arr": "object",
                "shape_meters_arr": "object",
                "vp_dir_arr": "object"
               },
        align_dataframes = False
    ).persist()
    
    time1 = datetime.datetime.now()
    logger.info(f"map partitions to transform vp: {time1 - start}")
    
    stops_projected = pd.read_parquet(
        f"{SEGMENT_GCS}stops_projected_{analysis_date}.parquet",
        filters = [[("shape_array_key", "in", shape_keys)]],
        columns = ["shape_array_key", "stop_sequence", "stop_id", 
                   "shape_meters", "stop_primary_direction"]
    ).rename(columns = {"shape_meters": "stop_meters"})
    
    existing_stop_cols = stops_projected[
        ["shape_array_key", "stop_sequence", "stop_id", "stop_meters"]].dtypes.to_dict()
    existing_vp_cols = vp_wide[["trip_instance_key"]].dtypes.to_dict()
    
    vp_to_stop = dd.merge(
        vp_wide,
        stops_projected,
        on = "shape_array_key",
        how = "inner"
    )
        
    result = vp_to_stop.map_partitions(
        find_vp_nearest_stop_position,
        meta = {
            **existing_vp_cols,
            **existing_stop_cols,
            "nearest_vp_idx": "int64",
            "subseq_vp_idx": "int64",
        },
        align_dataframes = False,
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"map partitions to find nearest vp to stop: {time2 - time1}")
    
    result = result.compute()
    
    fixed_results = fix_out_of_bound_results(result, VP_FILE_NAME)
    
    fixed_results.to_parquet(
        f"{SEGMENT_GCS}projection/nearest_vp_all_{analysis_date}.parquet")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
    
    # https://stackoverflow.com/questions/10226551/whats-the-most-pythonic-way-to-calculate-percentage-changes-on-a-list-of-numbers

