"""
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from pathlib import Path
from typing import Literal, Optional

from segment_speed_utils import helpers, wrangle_shapes
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils.project_vars import SEGMENT_TYPES, PROJECT_CRS

def stops_projected_against_shape(
    input_file: str,
    analysis_date: str, 
    trip_stop_cols: list,
) -> pd.DataFrame:
    """
    From nearest 10 vp points, project the stop geometry
    onto shape geometry and get 
    stop_meters.
    """
    stop_position = gpd.read_parquet(
        f"{SEGMENT_GCS}{input_file}_{analysis_date}.parquet",
        columns = trip_stop_cols + [
            "shape_array_key", "stop_geometry"],
    ).to_crs(PROJECT_CRS)
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = True
    )
    
    gdf = pd.merge(
        stop_position,
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    )
    
    gdf = gdf.assign(
        stop_meters = gdf.shape_geometry.project(gdf.stop_geometry),
    )[trip_stop_cols + ["stop_meters"]]
    
    return gdf


def explode_vp_nearest(
    input_file: str,
    analysis_date: str, 
    trip_stop_cols: list,
) -> pd.DataFrame:
    """
    Take nearest 10 vp, which holds vp_idx as an array,
    and explode it so it becomes long.
    """
    vp_nearest = pd.read_parquet(
        f"{SEGMENT_GCS}{input_file}_{analysis_date}.parquet",
        columns = trip_stop_cols + [
            "shape_array_key",
            "nearest_vp_arr"],
    ).explode(
        "nearest_vp_arr"
    ).drop_duplicates().reset_index(
        drop=True
    ).rename(
        columns = {"nearest_vp_arr": "vp_idx"}
    ).astype({"vp_idx": "int64"})
    
    return vp_nearest


def get_vp_projected_against_shape(
    input_file: str,
    analysis_date: str, 
    **kwargs
) -> pd.DataFrame:
    """
    Put in subset of vp_idx (using the kwargs)
    and turn the x, y into vp point geometry.
    Merge in shapes and project the vp position
    against shape geometry, and save out
    shape_meters.
    """    
    # Get crosswalk of trip to shapes
    trips_to_shapes = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key"],
        get_pandas = True
    )
    
    # Get shapes
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = True
    )
    
    # Subset usable vp with only the ones present in exploded vp
    # and turn those into vp geometry
    vp = pd.read_parquet(
        f"{SEGMENT_GCS}{input_file}_{analysis_date}",
        columns = ["trip_instance_key", "vp_idx", "x", "y",  
               "location_timestamp_local", 
               "moving_timestamp_local"],
        **kwargs
    ).pipe(wrangle_shapes.vp_as_gdf, crs = PROJECT_CRS)
    
    # Merge all together so we can project vp point goem
    # against shape line geom
    gdf = pd.merge(
        vp.rename(columns = {"geometry": "vp_geometry"}),
        trips_to_shapes,
        on = "trip_instance_key",
        how = "inner"
    ).merge(
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    ).set_geometry("vp_geometry")

    keep_cols = [
        "vp_idx", 
        "location_timestamp_local", "moving_timestamp_local", 
        "shape_meters"
    ]
    
    gdf = gdf.assign(
        shape_meters = gdf.shape_geometry.project(gdf.vp_geometry),
    )[keep_cols]
        
    return gdf


def find_two_closest_vp(
    df: pd.DataFrame, 
    group_cols: list
) -> pd.DataFrame:
    """
    Based on the distances calculated between vp and stop, 
    keep the 2 observations that are closest. Find the smallest
    positive distance and negative distance.
    
    This filters down the nearest 10 into nearest 2.
    """
    positive_distances_df = df.loc[df.stop_vp_distance_meters >= 0]
    negative_distances_df = df.loc[df.stop_vp_distance_meters < 0]
    
    #https://github.com/pandas-dev/pandas/issues/45089
    # add dropna=False or else too many combos are lost
    min_pos_distance = (
        positive_distances_df
        .groupby(group_cols, 
                 observed=True, group_keys=False, dropna=False)
        .agg({"stop_vp_distance_meters": "min"})
        .reset_index()
    )
    
    min_neg_distance = (
        negative_distances_df
        .groupby(group_cols, 
                 observed=True, group_keys=False, dropna=False)
        .agg({"stop_vp_distance_meters": "max"})
        .reset_index()
    )

    two_vp = pd.concat(
        [min_pos_distance, min_neg_distance], 
        axis=0, ignore_index=True
    )
    
    df2 = pd.merge(
        df,
        two_vp,
        on = group_cols + ["stop_vp_distance_meters"],
        how = "inner"
    )
    
    # since shape_meters actually might be decreasing as time progresses,
    # (bus moving back towards origin of shape)
    # we don't actually know that the smaller shape_meters is the first timestamp
    # nor the larger shape_meters is the second timestamp.
    # all we know is that stop_meters (stop) falls between these 2 shape_meters.
    # sort by timestamp, and set the order to be 0, 1    
    
    return df2


def filter_to_nearest_two_vp(
    analysis_date: str,
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional[Path] = GTFS_DATA_DICT
):
    dict_inputs = config_path[segment_type]
    trip_stop_cols = [*dict_inputs["trip_stop_cols"]]
    USABLE_VP_FILE = dict_inputs["stage1"]
    INPUT_FILE = dict_inputs["stage2"]
    EXPORT_FILE = dict_inputs["stage2b"]

    start = datetime.datetime.now()
    
    stop_meters_df = delayed(stops_projected_against_shape)(
        INPUT_FILE, analysis_date, trip_stop_cols)
    
    vp_nearest = delayed(explode_vp_nearest)(
        INPUT_FILE, analysis_date, trip_stop_cols)

    subset_vp = vp_nearest.vp_idx.unique()
        
    vp_meters_df = delayed(get_vp_projected_against_shape)(
        USABLE_VP_FILE,
        analysis_date, 
        filters = [[("vp_idx", "in", subset_vp)]]
    )
    
    gdf = delayed(pd.merge)(
        vp_nearest,
        stop_meters_df,
        on = trip_stop_cols,
        how = "inner"
    ).merge(
        vp_meters_df,
        on = "vp_idx",
        how = "inner"
    )
    
    
    # Calculate the distance between the stop and vp position
    # This is used to find the minimum positive and minimum negative
    # distance (get at vp before and after stop)
    gdf = gdf.assign(
        stop_meters = gdf.stop_meters.round(3),
        shape_meters = gdf.shape_meters.round(3),
        stop_vp_distance_meters = (gdf.stop_meters - gdf.shape_meters).round(2)
    )
    
    gdf2 = delayed(find_two_closest_vp)(gdf, trip_stop_cols)
    gdf2 = compute(gdf2)[0]
    
    gdf2.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet",
    )
    
    end = datetime.datetime.now()
    logger.info(f"nearest 2 vp for {segment_type} "
                f"{analysis_date}: {end - start}")
    

if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    for analysis_date in analysis_date_list:
        filter_to_nearest_two_vp(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        ) 