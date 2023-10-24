"""
Spatial join vehicle positions to segments.

Ensure that RT trips can only join to the scheduled shape
for that scheduled trip. 
Otherwise, vp on the same road get joined to multiple segments
across shapes.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH, PROJECT_CRS)

def add_grouping_col_to_vp(
    vp_file_name: str,
    analysis_date: str,
    trip_grouping_cols: list
) -> pd.DataFrame:
    """
    Import unique trips present in vehicle positions.
    Use trip_instance_key to merge RT with schedule.
    
    Determine trip_grouping_cols, a list of columns to aggregate trip tables
    up to how segments are cut. 
    Can be ["route_id", "direction_id"] or ["shape_array_key"]
    
    """
    vp_trips = pd.read_parquet(
        f"{SEGMENT_GCS}{vp_file_name}_{analysis_date}",
        columns = ["trip_instance_key"]
    ).drop_duplicates().dropna().reset_index(drop=True)
   
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key"] + trip_grouping_cols,
        get_pandas = True
    )
    
    vp_with_crosswalk = dd.merge(
        vp_trips,
        trips,
        on = "trip_instance_key",
        how = "inner"
    )
    
    return vp_with_crosswalk
    
    
def import_segments_and_buffer(
    segment_file_name: str,
    buffer_size: int,
    segment_identifier_cols: list,
    **kwargs
) -> gpd.GeoDataFrame:
    """
    Import segments , subset certain columns, 
    and buffer by some specified amount.
    """
    if "stop_segments" in segment_file_name:
        filename = f"{SEGMENT_GCS}{segment_file_name}.parquet"
    
    elif "road_segments" in segment_file_name:
        filename = f"{SEGMENT_GCS}{segment_file_name}"
    
    segments = gpd.read_parquet(
        filename,
        columns = segment_identifier_cols + [
            "seg_idx", "stop_primary_direction", "geometry"],
        **kwargs
    ).to_crs(PROJECT_CRS)

    # Buffer the segment for vehicle positions (points) to fall in polygons
    segments = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    return segments


def get_sjoin_results(
    vp: dd.DataFrame, 
    segments: gpd.GeoDataFrame, 
    grouping_col: str,
    segment_identifier_cols: list,
) -> pd.DataFrame:
    """
    Merge all the segments for a shape for that trip,
    and check if vp is within.
    Export just vp_idx and seg_idx as our "crosswalk" of sjoin results.
    If we use dask map_partitions, this is still faster than dask.delayed.
    """
    vp_gddf = gpd.GeoDataFrame(
        vp,
        geometry = gpd.points_from_xy(vp.x, vp.y, crs=WGS84)
    ).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    vp_to_seg = gpd.sjoin(
        vp_gddf,
        segments,
        how = "inner",
        predicate = "within"
    ).query(
        f'{grouping_col}_left == {grouping_col}_right'
    ).drop(
        columns = f"{grouping_col}_right"
    ).rename(columns = {f"{grouping_col}_left": grouping_col})

    results = (vp_to_seg[["vp_idx"] + segment_identifier_cols]
               .drop_duplicates()
               .reset_index(drop=True)
              )
    
    return results


def stage_direction_results(
    vp: dd.DataFrame, 
    segments: gpd.GeoDataFrame, 
    grouping_col: str,
    segment_identifier_cols: list,
    direction: str
):
    opposite = wrangle_shapes.OPPOSITE_DIRECTIONS[direction]
    keep_vp = [d for d in wrangle_shapes.ALL_DIRECTIONS if d != opposite] + ["Unknown"]
    
    # Keep all directions of vp except the ones running in opposite direction
    # Esp since buses make turns, a northbound segment can be 
    # partially westbound and then northbound
    vp_subset = vp[vp.vp_primary_direction.isin(keep_vp)].repartition(npartitions=20)
    
    segments_subset = segments[
        segments.stop_primary_direction==direction
    ].reset_index(drop=True)
    
    seg_id_dtypes = segments[segment_identifier_cols].dtypes.to_dict()
    
    results_subset = vp_subset.map_partitions(
        get_sjoin_results,
        segments_subset,
        grouping_col,
        segment_identifier_cols,
        meta = {"vp_idx": "int64", 
               **seg_id_dtypes},
        align_dataframes = False
    )
    
    return results_subset
    
def sjoin_vp_to_segments(
    analysis_date: str,
    dict_inputs: dict = {}
):
    """
    Spatial join vehicle positions to segments.
    Subset by grouping columns.
    
    Vehicle positions can only join to the relevant segments.
    Use route_dir_identifier or shape_array_key to figure out 
    the relevant segments those vp can be joined to.
    """
    INPUT_FILE = dict_inputs["stage1"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    TRIP_GROUPING_COLS = dict_inputs["trip_grouping_cols"]
    GROUPING_COL = dict_inputs["grouping_col"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    EXPORT_FILE = dict_inputs["stage2"]
    
    BUFFER_METERS = 35
    
    time0 = datetime.datetime.now()
    
    # Get a list of trips we need to keep from vp
    vp_trips = add_grouping_col_to_vp(
        f"{INPUT_FILE}",
        analysis_date,
        TRIP_GROUPING_COLS
    )
    
    groups_present = vp_trips[GROUPING_COL].unique().tolist()

    # only import segments whose shape_array_key is associated with a vp_trip
    segments = import_segments_and_buffer(
        f"{SEGMENT_FILE}_{analysis_date}",
        BUFFER_METERS,
        SEGMENT_IDENTIFIER_COLS,
        filters = [[(GROUPING_COL, "in", groups_present)]]
    )
    
    # Import vp, keep trips that are usable
    vp = dd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}/",
        columns = [
            "trip_instance_key", "vp_idx", "x", "y", 
            "vp_primary_direction"]
    ).merge(
        vp_trips,
        on = "trip_instance_key",
        how = "inner"
    )
    
    vp = vp.repartition(npartitions=100).persist()
    
    time1 = datetime.datetime.now()
    logger.info(f"prep vp and persist: {time1 - time0}")
    
    
    results = [
        stage_direction_results(
            vp,
            segments,
            GROUPING_COL,
            SEGMENT_IDENTIFIER_COLS, 
            one_direction
        ).persist() for one_direction in wrangle_shapes.ALL_DIRECTIONS
    ]
    
    
    time2 = datetime.datetime.now()
    logger.info(f"sjoin with map_partitions: {time2 - time1}")
    
    full_results = dd.multi.concat(results, axis=0).reset_index(drop=True)
    full_results = full_results.repartition(npartitions=4)

    full_results.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/{EXPORT_FILE}_{analysis_date}",
        overwrite=True
    )
    
    time3 = datetime.datetime.now()
    logger.info(f"export partitioned results: {time3 - time2}")
    
        
if __name__ == "__main__":
    
    LOG_FILE = "../logs/sjoin_vp_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    sjoin_vp_to_segments(
        analysis_date = analysis_date,
        dict_inputs = STOP_SEG_DICT
    )
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")