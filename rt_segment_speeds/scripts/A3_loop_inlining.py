"""
Handle complex shapes and pare down 
vehicle position points by first checking the dot product
to make sure we are keeping vehicle positions 
running in the same direction as the segment.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import sys

from loguru import logger

from segment_speed_utils import helpers, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH, PROJECT_CRS)


def calculate_mean_time(
    df: dd.DataFrame, 
    group_cols: list,
) -> dd.DataFrame:
    
    timestamp_col = "location_timestamp_local"
    seconds_col = f"{timestamp_col}_sec"
    
    df = segment_calcs.convert_timestamp_to_seconds(
        df, timestamp_col)
    
    mean_time = (df.groupby(group_cols)
                 .agg({seconds_col: "mean"})
                 .reset_index()
                 .rename(columns = {
                     seconds_col: "mean_time"})
                )
    
    df2 = dd.merge(
        df,
        mean_time,
        on = group_cols,
    )
    
    df2 = df2.assign(
        group = df2.apply(
            lambda x: 0 if x[seconds_col] <= x.mean_time 
            else 1, axis=1, meta=("group", "int8"))
    ).drop(columns = "mean_time")
    
    return df2
    
    
def get_first_last_position_in_group(
    df: dd.DataFrame, 
    group_cols: list
) -> dd.DataFrame:
    """
    """
    time_col = "location_timestamp_local_sec"
    trip_group_cols = group_cols + ["group"]
    
    first = (df.groupby(trip_group_cols)
             .agg({time_col: "min"})
             .reset_index()
            )
    
    last = (df.groupby(trip_group_cols)
             .agg({time_col: "max"})
             .reset_index()
            )
    
    keep_cols = trip_group_cols + [time_col, "lat", "lon"]
    
    pared_down = (dd.multi.concat([first, last], axis=0)
                  [trip_group_cols + [time_col]]
                  .drop_duplicates()
                  .reset_index(drop=True)
                 )
    
    # get rid of the groups with only 1 obs 
    # if it has only 1 point (cannot calculate direction vector), 
    # which means it'll get excluded down the line
    more_than_2 = (pared_down
                   .groupby(trip_group_cols)
                   [time_col].size()
                   .loc[lambda x: x > 1]
                   .reset_index()
                   .drop(columns = time_col)
                  )
    
    pared_down2 = dd.merge(
        pared_down,
        more_than_2,
        on = trip_group_cols
    )
    
    df2 = dd.merge(
        df[keep_cols],
        pared_down2,
        on = trip_group_cols + [time_col]
    )
    
    df2 = df2.assign(
        obs = (df2.sort_values(trip_group_cols + [time_col])
               .groupby(trip_group_cols)[time_col]
               .cumcount() + 1
              ).astype("int8")
    )
    
    return df2


def get_stop_segments_direction_vector(
    stop_segments: gpd.GeoDataFrame
) -> pd.DataFrame:
    """
    Grab the first and last coordinate points in the stop segment
    and turn that into a normalized vector.
    """
    # Take the stop segment geometry and turn it into an array of coords
    shape_array = [np.array(shapely.LineString(i).coords) 
               for i in stop_segments.geometry]
    
    # Grab the first and last items in the array, 
    # and turn it back to shapely
    subset_shape_array = [
        np.array(
            [shapely.geometry.Point(i[0]), 
             shapely.geometry.Point(i[-1])]
        ).flatten() for i in shape_array
    ]
    
    # Get the shape's direction vector and normalize it
    direction_vector = [
        wrangle_shapes.distill_array_into_direction_vector(i) 
        for i in subset_shape_array
    ]
    
    shape_vec = [wrangle_shapes.get_normalized_vector(i) 
                 for i in direction_vector]
    
    # Assign this vector as a column, drop geometry, since we can
    # bring it back for full df later
    stop_segments2 = stop_segments.assign(
        segments_vector = shape_vec
    ).drop(columns = "geometry")
    
    return stop_segments2


def find_vp_direction_vector(
    df: dd.DataFrame, 
    group_cols: list,
    crs: str = PROJECT_CRS
):

    trip_group_cols = group_cols + ["group", "segments_vector"]
    keep_cols = trip_group_cols + ["lat", "lon"]
    
    first_position = df[df.obs == 1][keep_cols]
    last_position = df[df.obs==2][keep_cols]
    
    # Set this up to be wide so we can compare positions and 
    # get a vector
    df_wide = dd.merge(
        first_position,
        last_position,
        on = trip_group_cols,
        suffixes = ('_start', '_end')
    ).sort_values(trip_group_cols).reset_index(drop=True)
    
    first_vp = dg.points_from_xy(
        df_wide, "lon_start", "lat_start", 
        crs=crs
    ) 
           
    last_vp = dg.points_from_xy(
        df_wide, "lon_end", "lat_end", 
        crs=crs
    )
    
    first_series = first_vp.compute()
    last_series = last_vp.compute()
    
    direction_vector = [
        wrangle_shapes.get_direction_vector(start, end) 
        for start, end in zip(first_series, last_series)
    ]
    
    vector_normalized = [wrangle_shapes.get_normalized_vector(i) 
                     for i in direction_vector]
    
    results = df_wide[trip_group_cols].compute()
    results = results.assign(
        vp_vector = vector_normalized
    )
    
    dot_result = [wrangle_shapes.dot_product(vec1, vec2) for vec1, vec2 in 
                  zip(results.segments_vector, results.vp_vector)]
    
    results = results.assign(
        dot_product = dot_result
    )
    
    return results


def find_errors_in_segment_groups(
    vp_sjoin: dd.DataFrame, 
    segments: gpd.GeoDataFrame,
    group_cols: list
) -> dd.DataFrame:
    segments = get_stop_segments_direction_vector(
        segments).drop(columns = "geometry")
    
    vp_grouped = calculate_mean_time(vp_sjoin, group_cols)
    
    vp_pared_by_group = get_first_last_position_in_group(
        vp_grouped, group_cols)

    vp_with_segment_vec = dd.merge(
        segments,
        vp_pared_by_group,
        on = SEGMENT_IDENTIFIER_COLS,
    )

    vp_dot_prod = find_vp_direction_vector(
        vp_with_segment_vec, group_cols)
    
    # Only keep if vehicle positions are running in the same
    # direction as the segment
    vp_same_direction = (vp_dot_prod[vp_dot_prod.dot_product > 0]
                             [group_cols + ["group"]]
                             .drop_duplicates()
                             .reset_index(drop=True)
                            )
    
    vp_to_keep = dd.merge(
        vp_grouped,
        vp_same_direction,
        on = group_cols + ["group"],
        how = "inner",
    ).drop(columns = ["location_timestamp_local_sec", "group"])

    return vp_to_keep
        



def pare_down_vp_for_special_cases(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    """
    """
    INPUT_FILE_PREFIX = dict_inputs["stage2"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    GROUPING_COL = dict_inputs["grouping_col"]
    TIMESTAMP_COL = dict_inputs["timestamp_col"]
    EXPORT_FILE = dict_inputs["stage3"]

    
    # Handle stop segments and the normal/special cases separately
    if GROUPING_COL == "shape_array_key":
        special_shapes = identify_stop_segment_cases(
            analysis_date, GROUPING_COL, 1)
        
            
        # https://docs.dask.org/en/stable/delayed-collections.html
        vp_joined_to_segments = helpers.import_vehicle_positions(
            f"{SEGMENT_GCS}vp_sjoin/",
            f"{INPUT_FILE_PREFIX}_{analysis_date}",
            file_type = "df",
            filters = [[(GROUPING_COL, "in", special_cases)]],
            partitioned=True
        )

        segments = helpers.import_segments(
            file_name = f"{SEGMENTS_FILE}_{analysis_date}",
            filters = [[(GROUPING_COL, "in", special_cases)]],
            columns = SEGMENT_IDENTIFIER_COLS + ["geometry"],
            partitioned = False
        )
        
        vp_pared_special = find_errors_in_segment_groups(
            vp_joined_to_segments, segments, 
            SEGMENT_IDENTIFIER_COLS + ["trip_id"])
        
        vp_pared_special.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_special_{analysis_date}")
    
    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/valid_vehicle_positions.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    #ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
   
    time1 = datetime.datetime.now()
    '''
    pare_down_vp_by_segment(
        analysis_date,
        dict_inputs = ROUTE_SEG_DICT
    )
    '''
    time2 = datetime.datetime.now()
    logger.info(f"pare down vp by route segments {time2 - time1}")
    
    pare_down_vp_for_special_cases(
        analysis_date,
        dict_inputs = STOP_SEG_DICT
    )
    
    time3 = datetime.datetime.now()
    logger.info(f"pare down vp by stop segments {time3 - time2}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")