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
from A3_valid_vehicle_positions import (identify_stop_segment_cases, 
                                        merge_usable_vp_with_sjoin_vpidx)




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
        
    vp_grouped = split_vp_into_groups(
        vp_sjoin,
        group_cols,
        col_to_find_groups = "location_timestamp_local"
    )
    
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