"""
Filter the nearest 10 neighbors down to the 
nearest 2 neighbors for each stop position.
Attach the projected stop position against shape,
projected vp position against shape, and timestamps.
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
    
    del shapes, stop_position
    
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
        columns = ["trip_instance_key", "vp_idx", "x", "y"],
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

    del trips_to_shapes, shapes, vp
    
    gdf = gdf.assign(
        shape_meters = gdf.shape_geometry.project(gdf.vp_geometry),
    )[["vp_idx", "shape_meters"]]
            
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
    
    return two_vp

    
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
    
    gdf_filtered = delayed(find_two_closest_vp)(gdf, trip_stop_cols)
    
    gdf2 = delayed(pd.merge)(
        gdf,
        gdf_filtered,
        on = trip_stop_cols + ["stop_vp_distance_meters"],
        how = "inner"
    )
    
    gdf2 = compute(gdf2)[0]
    
    del gdf, gdf_filtered, vp_nearest, stop_meters_df, vp_meters_df
        
    gdf2.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet",
    )
        
    end = datetime.datetime.now()
    logger.info(f"nearest 2 vp for {segment_type} "
                f"{analysis_date}: {end - start}")
    
    del gdf2
    
    return
    
'''
if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    delayed_dfs = [
        delayed(filter_to_nearest_two_vp)(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        ) for analysis_date in analysis_date_list
    ]

    [compute(i)[0] for i in delayed_dfs]
'''