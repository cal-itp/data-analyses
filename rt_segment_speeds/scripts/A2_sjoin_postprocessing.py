"""
Spatial join post-processing.
Inlining causes erroneous sjoin results, and we may 
keep way too many sjoin pairs.

Instead of only focusing only loop_or_inlining shapes for 
direction check, do it for all shapes.
If there are 2 groupings of vp (non-consecutive) attached to 
a segment, check that the vp run the same direction as the segment.
Otherwise, drop.
"""
import dask.dataframe as dd
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
                                              PROJECT_CRS, CONFIG_PATH)
from A3_valid_vehicle_positions import merge_usable_vp_with_sjoin_vpidx


def find_convoluted_segments(
    ddf: dd.DataFrame, 
    segment_identifier_cols: list 
) -> pd.DataFrame:
    """
    Identify which segments (shape-stop_seq) are prone to 
    having double sjoin results due to inlining.

    These are particularly complex to resolve, because 
    even though we have vp_idx as a column,
    we cannot simply check whether it is a difference of 1
    compared to previous or subsequent vp. 
    
    Ex: if we're going westbound first, then eastbound, then
    both 2 distinct groupings of sorted vp_idx will appear on both segments.
    The westbound segment should be associated with smaller vp_idx.
    The eastbound segment should be associated with larger vp_idx.
        
    So, we need to use direction to resolve this.
    """
    segment_trip_cols = ["trip_instance_key"] + segment_identifier_cols
    
    ddf["prior_vp_idx"] = (ddf.groupby(segment_trip_cols, 
                                   observed=True, group_keys=False)
                       ["vp_idx"]
                       .shift(1, meta = ("prior_vp_idx", "Int64")) 
                      )

    ddf["subseq_vp_idx"] = (ddf.groupby(segment_trip_cols, 
                                   observed=True, group_keys=False)
                       ["vp_idx"]
                       .shift(-1, meta = ("subseq_vp_idx", "Int64")) 
                      )

    ddf = ddf.assign(
        change_from_prior = ddf.vp_idx - ddf.prior_vp_idx, 
        change_to_subseq = ddf.subseq_vp_idx - ddf.vp_idx
    )

    # The segments that are convoluted would have either
    # a max(change_from_prior) > 1 or max(change_to_subseq) > 1
    convoluted_segments = ddf.assign(
        max_change_from_prior = (
            ddf.groupby(segment_trip_cols, 
                        observed=True, group_keys=False)
            .change_from_prior
            .transform("max", 
                       meta = ("max_change_from_prior", "Int64"))
        ),
        max_change_to_subseq = (
            ddf.groupby(segment_trip_cols,
                        observed=True, group_keys=False)
            .change_to_subseq
            .transform("max", 
                       meta = ("max_change_to_subseq", "Int64"))
        )
    )[segment_trip_cols + [
        "max_change_from_prior", 
        "max_change_to_subseq"]
     ].drop_duplicates().query(
        'max_change_from_prior > 1 or max_change_to_subseq > 1'
    ).reset_index(drop=True)
    
    return (convoluted_segments[segment_trip_cols]
            .compute().reset_index(drop=True))


def split_vp_into_groups(
    df: dd.DataFrame, 
    group_cols: list,
    col_to_find_groups: str = "location_timestamp_local"
) -> dd.DataFrame:
    """
    Within each segment-trip, break up the vp into 2 groups using 
    vp_idx. Within each group, check direction.
    Only correct sjoin results are kept.
    
    Can use vp_idx, should be simpler than original use of location_timestamp_local.
    """
    if col_to_find_groups == "location_timestamp_local":
        col = f"{col_to_find_groups}_sec"

        df = segment_calcs.convert_timestamp_to_seconds(
            df, [col_to_find_groups])
    else:
        col = col_to_find_groups
    
    mean_df = (df.groupby(group_cols, observed=True, group_keys=False)
                 .agg({col: "mean"})
                 .reset_index()
                 .rename(columns = {col: "avg"})
                )
    
    df2 = dd.merge(
        df,
        mean_df,
        on = group_cols,
    )
    
    df2 = df2.assign(
        group = df2.apply(
            lambda x: 0 if x[col] <= x.avg 
            else 1, axis=1, meta=("group", "int8"))
    ).drop(columns = "avg")
    
    return df2
    
    
def get_first_last_position_in_group(
    df: dd.DataFrame, 
    group_cols: list,
    col_to_find_groups: str = "location_timestamp_local_sec"
) -> pd.DataFrame:
    """
    For each grouping of vp (separated by the mean timestamp)
    for a segment-trip, get the first and last vp.
    Find the direction each pair of points.
    """
    col = col_to_find_groups
    trip_group_cols = group_cols + ["group"]
    
    grouped_df = df.groupby(trip_group_cols, observed=True, group_keys=False)
    
    first = (grouped_df
             .agg({col: "min"})
             .reset_index()
            )
    
    last = (grouped_df
             .agg({col: "max"})
             .reset_index()
            )
    
    keep_cols = trip_group_cols + [col, "x", "y"]
    
    pared_down = (dd.multi.concat([first, last], axis=0)
                  [trip_group_cols + [col]]
                  .drop_duplicates()
                  .reset_index(drop=True)
                 )
    
    # get rid of the groups with only 1 obs 
    # if it has only 1 point (cannot calculate direction vector), 
    # which means it'll get excluded down the line
    more_than_2 = (pared_down
                   .groupby(trip_group_cols, observed=True, group_keys=False)
                   [col].size()
                   .loc[lambda x: x > 1]
                   .reset_index()
                   .drop(columns = col)
                  )
    
    pared_down2 = dd.merge(
        pared_down,
        more_than_2,
        on = trip_group_cols
    ).reset_index(drop=True)
    
    # Do subset first, because dask doesn't like subsetting on-the-fly
    df2 = df[keep_cols]
    df3 = dd.merge(
        df2,
        pared_down2,
        on = trip_group_cols + [col]
    ).compute() # compute so we can sort by multiple columns
    
    # Sorting right before the groupby causes errors
    df3 = df3.sort_values(
        trip_group_cols + [col]
    ).reset_index(drop=True)
    
    df3 = df3.assign(
        obs = (df3.groupby(trip_group_cols, observed=True, 
                           group_keys=False)[col]
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

def check_vp_direction_against_segment_direction(
    convoluted_sjoin_results: dd.DataFrame,
    convoluted_segments: pd.DataFrame,
    segment_identifier_cols: list,
    grouping_col: str
) -> dd.DataFrame:
    """
    Return vp sjoined to segment_identifier_cols that are 
    should be excluded.
    """
    segment_trip_cols = ["trip_instance_key"] + segment_identifier_cols
    
    convoluted_vp_grouped = split_vp_into_groups(
        convoluted_sjoin_results, 
        group_cols = segment_trip_cols,
        col_to_find_groups = "vp_idx"
    ).persist()

    convoluted_vp_first_last = get_first_last_position_in_group(
        convoluted_vp_grouped,
        group_cols = segment_trip_cols,
        col_to_find_groups = "vp_idx"
    )
    
    shapes_with_error = convoluted_segments[grouping_col].unique().tolist()
    
    segments_to_fix = gpd.read_parquet(
        f"{SEGMENT_GCS}stop_segments_{analysis_date}.parquet",
        columns = segment_identifier_cols + ["geometry"],
        filters = [[(grouping_col, "in", shapes_with_error)]]
    ).merge(
        convoluted_segments[segment_identifier_cols],
        on = segment_identifier_cols,
        how = "inner"
    )
    
    segments_to_fix = get_stop_segments_direction_vector(
        segments_to_fix)

    vp_with_segment_vec = pd.merge(
        segments_to_fix,
        convoluted_vp_first_last,
        on = segment_identifier_cols,
    )

    vp_dot_prod = find_vp_direction_vector(
        vp_with_segment_vec, segment_trip_cols)

    vp_to_drop = vp_dot_prod[vp_dot_prod.dot_product < 0][
        segment_trip_cols + ["group"]]
    
    vp_to_seg_drop = (convoluted_vp_grouped
                      .merge(
                          vp_to_drop,
                          on = segment_trip_cols + ["group"],
                          how = "inner"
                      )
                 )[segment_identifier_cols + ["vp_idx"]].drop_duplicates()
    
    return vp_to_seg_drop


def remove_erroneous_sjoin_results(
    analysis_date: str, 
    dict_inputs: dict
):
    """
    Split the sjoin results into segment-trips that look ok 
    and ones that look convoluted.
    Fix the convoluted sjoins by checking for direction.
    Drop the erroneous ones.
    Save over the existing sjoin results.
    """
    USABLE_VP = dict_inputs["stage1"]
    INPUT_FILE_PREFIX = dict_inputs["stage2"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    SEGMENT_TRIP_COLS = ["trip_instance_key"] + SEGMENT_IDENTIFIER_COLS
    GROUPING_COL = dict_inputs["grouping_col"]

    vp_trip_info = pd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}/",
        columns = ["vp_idx", "trip_instance_key"]
    )

    sjoin_results = pd.read_parquet(
        f"{SEGMENT_GCS}vp_sjoin/{INPUT_FILE_PREFIX}_{analysis_date}/",
    ).merge(
        vp_trip_info,
        on = "vp_idx",
        how = "inner"
    ).sort_values(SEGMENT_TRIP_COLS + ["vp_idx"]).reset_index(drop=True)

    # We can do groupby and shift with ddfs, but
    # divisions not known error addressed only with sort=False
    ddf = dd.from_pandas(sjoin_results, npartitions=80, sort=False)

    convoluted_segments = find_convoluted_segments(
        ddf, SEGMENT_IDENTIFIER_COLS)
    
    error_shapes = convoluted_segments.shape_array_key.unique().tolist()
        
    convoluted_sjoin_results = merge_usable_vp_with_sjoin_vpidx(
        f"{USABLE_VP}_{analysis_date}",
        f"{INPUT_FILE_PREFIX}_{analysis_date}",
        sjoin_filtering = [[(GROUPING_COL, "in", error_shapes)]]
    ).merge(
        convoluted_segments,
        on = SEGMENT_TRIP_COLS,
        how = "inner"
    )
    
    convoluted_sjoin_results = convoluted_sjoin_results.repartition(npartitions=5)

    convoluted_sjoin_drop = check_vp_direction_against_segment_direction(
        convoluted_sjoin_results,
        convoluted_segments,
        SEGMENT_IDENTIFIER_COLS,
        GROUPING_COL
    )
        
    all_sjoin_results = dd.read_parquet(
        f"{SEGMENT_GCS}vp_sjoin/{INPUT_FILE_PREFIX}_{analysis_date}",
    )
    
    cleaned_sjoin_results = dd.merge(
        all_sjoin_results,
        convoluted_sjoin_drop,
        on = SEGMENT_IDENTIFIER_COLS + ["vp_idx"],
        how = "left",
        indicator = True
    ).query('_merge=="left_only"').drop(columns = "_merge")
    
    cleaned_sjoin_results = (cleaned_sjoin_results.repartition(npartitions=5)
                             .reset_index(drop=True))
    
    cleaned_sjoin_results.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/{INPUT_FILE_PREFIX}_{analysis_date}"
    )
    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/sjoin_vp_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    remove_erroneous_sjoin_results(analysis_date, STOP_SEG_DICT)

    end = datetime.datetime.now()
    logger.info(f"remove erroneous sjoin results: {end-start}")
    