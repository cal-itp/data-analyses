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

from shared_utils.geography_utils import WGS84
from segment_speed_utils import helpers
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
    segments = gpd.read_parquet(
        f"{SEGMENT_GCS}{segment_file_name}.parquet",
        columns = segment_identifier_cols + ["seg_idx", "geometry"],
        **kwargs
    ).to_crs(PROJECT_CRS)

    # Buffer the segment for vehicle positions (points) to fall in polygons
    segments = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    return segments


def get_sjoin_results(
    vp_gddf: dg.GeoDataFrame, 
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
    vp_to_seg = dd.merge(
        vp_gddf,
        segments,
        on = grouping_col,
        how = "inner"
    ).set_geometry("geometry_x")
    
    vp_in_seg = vp_to_seg.assign(
        is_within = vp_to_seg.geometry_x.within(vp_to_seg.geometry_y)
    ).query('is_within==True')
    
    results = (vp_in_seg[["vp_idx"] + segment_identifier_cols]
               .drop_duplicates()
               .reset_index(drop=True)
              )
    
    return results

    
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
    vp = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        f"{INPUT_FILE}_{analysis_date}/",
        columns = ["trip_instance_key", 
                   "vp_idx", "x", "y"],
        partitioned = True
    ).merge(
        vp_trips,
        on = "trip_instance_key",
        how = "inner"
    )
    
    vp_gddf = dg.from_dask_dataframe(
        vp,
        geometry = dg.points_from_xy(vp, x="x", y="y", crs=WGS84)
    ).set_crs(WGS84).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    
    vp_gddf = vp_gddf.repartition(npartitions=100).persist()
    
    time1 = datetime.datetime.now()
    logger.info(f"prep vp and persist: {time1 - time0}")
    
    # save dtypes as a dict to input in map_partitions
    seg_id_dtypes = segments[SEGMENT_IDENTIFIER_COLS].dtypes.to_dict()
    
    results = vp_gddf.map_partitions(
        get_sjoin_results,
        segments,
        GROUPING_COL,
        SEGMENT_IDENTIFIER_COLS,
        meta = {"vp_idx": "int64", **seg_id_dtypes},
        align_dataframes = False
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"sjoin with map_partitions: {time2 - time1}")
    
    results = results.repartition(npartitions=5)
    results.to_parquet(
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
    
    vp_stop_seg = sjoin_vp_to_segments(
        analysis_date = analysis_date,
        dict_inputs = STOP_SEG_DICT
    )
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")