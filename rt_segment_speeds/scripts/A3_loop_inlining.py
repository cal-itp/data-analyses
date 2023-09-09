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

from shared_utils.geography_utils import WGS84
from segment_speed_utils import helpers, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH, PROJECT_CRS)
from A2_valid_vehicle_positions import (identify_stop_segment_cases, 
                                        merge_usable_vp_with_sjoin_vpidx)

def calculate_mean_time(
    df: dd.DataFrame, 
    group_cols: list,
    timestamp_col: str = "location_timestamp_local"
) -> dd.DataFrame:
    """
    Find the mean timestamp (in seconds) for a segment-trip.
    Use this to separate whether there are inbound vs 
    outbound vp attached to the same segment.
    """
    seconds_col = f"{timestamp_col}_sec"
    
    df = segment_calcs.convert_timestamp_to_seconds(
        df, [timestamp_col])
    
    mean_time = (df.groupby(group_cols, observed=True, group_keys=False)
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
    group_cols: list,
) -> pd.DataFrame:
    """
    For each grouping of vp (separated by the mean timestamp)
    for a segment-trip, get the first and last vp.
    Find the direction each pair of points.
    """
    time_col = "location_timestamp_local_sec"
    trip_group_cols = group_cols + ["group"]
    
    grouped_df = df.groupby(trip_group_cols, observed=True, group_keys=False)
    first = (grouped_df
             .agg({time_col: "min"})
             .reset_index()
            )
    
    last = (grouped_df
             .agg({time_col: "max"})
             .reset_index()
            )
    
    keep_cols = trip_group_cols + [time_col, "x", "y"]
    
    pared_down = (dd.multi.concat([first, last], axis=0)
                  [trip_group_cols + [time_col]]
                  .drop_duplicates()
                  .reset_index(drop=True)
                 )
    
    # get rid of the groups with only 1 obs 
    # if it has only 1 point (cannot calculate direction vector), 
    # which means it'll get excluded down the line
    more_than_2 = (pared_down
                   .groupby(trip_group_cols, observed=True, group_keys=False)
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
    
    # Do subset first, because dask doesn't like subsetting on-the-fly
    df2 = df[keep_cols]
    df3 = dd.merge(
        df2,
        pared_down2,
        on = trip_group_cols + [time_col]
    ).compute() # compute so we can sort by multiple columns
    
    # Sorting right before the groupby causes errors
    df3 = df3.sort_values(
        trip_group_cols + [time_col]
    ).reset_index(drop=True)
    
    df3 = df3.assign(
        obs = (df3.groupby(trip_group_cols, observed=True, 
                           group_keys=False)[time_col]
               .cumcount() + 1
              ).astype("int8")
    )
    
    return df3


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
    df: pd.DataFrame, 
    group_cols: list,
    crs: str = PROJECT_CRS
) -> pd.DataFrame:
    """
    Get direction vector for first and last vp within segment.
    """
    trip_group_cols = group_cols + ["group", "segments_vector"]
    keep_cols = trip_group_cols + ["x", "y"]
    
    first_position = df[df.obs == 1][keep_cols]
    last_position = df[df.obs==2][keep_cols]
    
    # Set this up to be wide so we can compare positions and 
    # get a vector
    df_wide = pd.merge(
        first_position,
        last_position,
        on = trip_group_cols,
        suffixes = ('_start', '_end')
    ).sort_values(trip_group_cols).reset_index(drop=True)
    
    # Use 2 geoseries, the first point and the last point
    first_series = gpd.points_from_xy(
        df_wide.x_start, df_wide.y_start,
        crs=WGS84
    ).to_crs(crs)
           
    last_series = gpd.points_from_xy(
        df_wide.x_end, df_wide.y_end, 
        crs=WGS84
    ).to_crs(crs)
    
    # Input 2 series to get a directon for each element-pair 
    direction_vector = [
        wrangle_shapes.get_direction_vector(start, end) 
        for start, end in zip(first_series, last_series)
    ]
    
    # Normalize vector by Pythagorean Theorem to get values between -1 and 1
    vector_normalized = [wrangle_shapes.get_normalized_vector(i) 
                     for i in direction_vector]
    
    results = df_wide[trip_group_cols]
    results = results.assign(
        vp_vector = vector_normalized
    )
    
    # Take the dot product. 
    # positive = same direction; 0 = orthogonal; negative = opposite direction
    dot_result = [wrangle_shapes.dot_product(vec1, vec2) for vec1, vec2 in 
                  zip(results.segments_vector, results.vp_vector)]
    
    results = results.assign(
        dot_product = dot_result
    )
    
    return results


def find_errors_in_segment_groups(
    vp_sjoin: dd.DataFrame, 
    segments: gpd.GeoDataFrame,
    segment_identifier_cols: list,
) -> dd.DataFrame:
    """
    For each sjoin result for each segment-trip:
    (1) find the direction the segment is running
    (2) use the mean timestamp to divide sjoin results into 2 groups
    (3) for each group, find the first/last vp
    (4) find the direction of each group of vp for segment-trip
    (5) as long as vp are running in same direction as segment (dot product > 0),
    keep those observations.
    """
    group_cols = segment_identifier_cols + ["trip_instance_key"]
    
    segments = get_stop_segments_direction_vector(
        segments)
    
    vp_grouped = calculate_mean_time(vp_sjoin, group_cols)
    
    vp_pared_by_group = get_first_last_position_in_group(
        vp_grouped, group_cols)

    vp_with_segment_vec = pd.merge(
        segments,
        vp_pared_by_group,
        on = segment_identifier_cols,
    )

    vp_dot_prod = find_vp_direction_vector(
        vp_with_segment_vec, group_cols)
    
    # Only keep if vehicle positions are running in the same
    # direction as the segment
    # TODO: should we keep NaNs? NaNs weren't able to have a vector calculated,
    # which could mean it's kind of an outlier in the segment, 
    # maybe should have been attached elsewhere
    vp_same_direction = (vp_dot_prod[~(vp_dot_prod.dot_product < 0)]
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
    For special shapes, include a direction check where each
    batch of vp have direction generated, and compare that against
    the direction the segment is running.
    """
    USABLE_VP = dict_inputs["stage1"]
    INPUT_FILE_PREFIX = dict_inputs["stage2"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    GROUPING_COL = dict_inputs["grouping_col"]
    TIMESTAMP_COL = dict_inputs["timestamp_col"]
    EXPORT_FILE = dict_inputs["stage3"]

    
    special_shapes = identify_stop_segment_cases(
        analysis_date, GROUPING_COL, 1)

    vp_joined_to_segments = merge_usable_vp_with_sjoin_vpidx(
        special_shapes,
        f"{USABLE_VP}_{analysis_date}",
        f"{INPUT_FILE_PREFIX}_{analysis_date}",
        sjoin_filtering = [[(GROUPING_COL, "in", special_shapes)]],
        columns = ["vp_idx", "trip_instance_key", TIMESTAMP_COL,
                   "x", "y"]
    )

    segments = helpers.import_segments(
        file_name = f"{SEGMENT_FILE}_{analysis_date}",
        filters = [[(GROUPING_COL, "in", special_shapes)]],
        columns = SEGMENT_IDENTIFIER_COLS + ["geometry"],
        partitioned = False
    )

    vp_pared_special = find_errors_in_segment_groups(
        vp_joined_to_segments, 
        segments, 
        SEGMENT_IDENTIFIER_COLS
    )

    special_vp_to_keep = segment_calcs.keep_min_max_timestamps_by_segment(
        vp_pared_special,       
        SEGMENT_IDENTIFIER_COLS + ["trip_instance_key"],
        TIMESTAMP_COL
    )
    
    special_vp_to_keep = special_vp_to_keep.repartition(npartitions=1)

    special_vp_to_keep.to_parquet(
        f"{SEGMENT_GCS}vp_pare_down/{EXPORT_FILE}_special_{analysis_date}", 
        overwrite = True)

    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/valid_vehicle_positions.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
   
    time1 = datetime.datetime.now()
    
    pare_down_vp_for_special_cases(
        analysis_date,
        dict_inputs = STOP_SEG_DICT
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"pare down vp by stop segments special cases {time2 - time1}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")