"""
Spatial join vehicle positions to segments.

Route segments and stop-to-stop segments.
Use dask.delayed + loop so that vp can only join to the 
relevant segments.
Otherwise, vp will be joined to other segments that share the same road.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import gcsfs
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from dask.delayed import Delayed # type hint
from loguru import logger

from shared_utils import dask_utils
from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH, PROJECT_CRS)

fs = gcsfs.GCSFileSystem()


def add_grouping_col_to_vp(
    vp_file_name: str,
    analysis_date: str,
    trip_grouping_cols: list
) -> pd.DataFrame:
    """
    Import unique trips present in vehicle positions.
    
    Determine trip_grouping_cols, a list of columns to aggregate trip tables
    up to how segments are cut. 
    Can be ["route_id", "direction_id"] or ["shape_array_key"]
    
    Merge on crosswalk, which gives us feed_key. 
    Be able to link RT to schedule data (gtfs_dataset_key and feed_key present).
    """
    vp_trips = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        vp_file_name,
        columns = ["gtfs_dataset_key", "trip_id"],
        file_type = "df",
        partitioned=False
    ).drop_duplicates()

    crosswalk = sched_rt_utils.crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date, 
        ["feed_key", "trip_id"] + trip_grouping_cols
    )
    
    vp_with_crosswalk = dd.merge(
        vp_trips,
        crosswalk,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    ).compute().sort_values(
        ["gtfs_dataset_key"] + trip_grouping_cols
    ).reset_index(drop=True)
    
    return vp_with_crosswalk
   

def import_segments_and_buffer(
    segment_file_name: str,
    buffer_size: int,
    segment_identifier_cols: list
) -> gpd.GeoDataFrame:
    """
    Import segments , subset certain columns, 
    and buffer by some specified amount.
    """
    segments = helpers.import_segments(
        SEGMENT_GCS, 
        segment_file_name,
        columns = segment_identifier_cols + ["gtfs_dataset_key", "geometry"]
    ).to_crs(PROJECT_CRS)
    
    # Buffer the segment for vehicle positions (points) to fall in polygons
    segments = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    return segments


def vehicle_positions_to_gdf(
    vp_file_name: str,
    rt_dataset_key: str,
    crosswalk_vp_segments: pd.DataFrame
) -> dg.GeoDataFrame:
    """
    Import vp_usable (tabular) for an operator and create geometry column.
    Also merge in the crosswalk so we know which grouping it is, 
    whether shape_array_key or route_dir_identifier.
    """
    vp = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        vp_file_name,
        filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]],
        columns = ["gtfs_dataset_key", "trip_id", 
                   "vp_idx", "x", "y"],
        partitioned = True
    ).merge(
        crosswalk_vp_segments,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "inner"
    )
    
    vp["geometry"] = dg.points_from_xy(vp, "x", "y")
    
    # Set CRS after gddf created
    # https://github.com/geopandas/dask-geopandas/issues/189
    vp_gdf = dg.from_dask_dataframe(
        vp, geometry = "geometry", 
    ).drop(columns = ["x", "y"]).set_crs("EPSG:4326").to_crs(PROJECT_CRS)
    
    return vp_gdf
    
    
def loop_over_groups_by_operator(
    vp_file_name: str,
    vp_trips: pd.DataFrame,
    segments: dg.GeoDataFrame,
    rt_dataset_key: str,
    grouping_col: list,
    segment_identifier_cols: list
) -> list[Delayed]:
    """
    Do the loop within an operator. Loop by route_dir_identifier
    or shape_array_key.
    
    Only the vehicle positions for that grouping can sjoin
    to those segments.
    """
    operator_vp = delayed(vehicle_positions_to_gdf)(
        vp_file_name,
        rt_dataset_key,
        vp_trips,
    ).persist()
    
    operator_segments = segments[
        segments.gtfs_dataset_key == rt_dataset_key]
    
    groups = operator_vp[grouping_col].unique()
    groups = compute(groups)[0]
    
    operator_results = []
    
    for g in groups:
        vp_group = operator_vp[
            operator_vp[grouping_col] == g][["vp_idx", "geometry"]]
        
        segments_group = operator_segments[
            operator_segments[grouping_col] == g]
        
        sjoin_point_in_seg = delayed(dg.sjoin)(
            vp_group,
            segments_group,
            how = "inner",
            predicate = "within"
        )[["vp_idx"] + segment_identifier_cols]
        
        operator_results.append(sjoin_point_in_seg)

    return operator_results
    
    
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
    
    vp_trips = add_grouping_col_to_vp(
        f"{INPUT_FILE}_{analysis_date}",
        analysis_date,
        TRIP_GROUPING_COLS
    )
          
    segments = delayed(import_segments_and_buffer)(
        f"{SEGMENT_FILE}_{analysis_date}",
        BUFFER_METERS,
        SEGMENT_IDENTIFIER_COLS
    ).persist()
    
    RT_OPERATORS = vp_trips.gtfs_dataset_key.unique()
    
    for rt_dataset_key in RT_OPERATORS:
        start_id = datetime.datetime.now()

        operator_results = loop_over_groups_by_operator(
            f"{INPUT_FILE}_{analysis_date}/",
            vp_trips[vp_trips.gtfs_dataset_key == rt_dataset_key],
            segments,
            rt_dataset_key,
            GROUPING_COL,
            SEGMENT_IDENTIFIER_COLS
        )        
    
        if len(operator_results) > 0:
            dask_utils.compute_and_export(
                operator_results,
                gcs_folder = f"{SEGMENT_GCS}vp_sjoin/",
                file_name = f"{EXPORT_FILE}_{rt_dataset_key}_{analysis_date}",
                export_single_parquet = False
            )
            
            end_id = datetime.datetime.now()
            print(f"exported: {rt_dataset_key} {end_id - start_id}")
     
    
def compile_partitioned_parquets_for_operators(
    gcs_folder: str,
    file_name: str,
    analysis_date: str, 
    vp_trips: pd.DataFrame,
):
    """
    Once we've saved out indiv operator partitioned parquets,
    read those in and save as one partitioned parquet
    """
    
    all_files = fs.ls(f"{gcs_folder}")
    
    # The files to compile need format
    # {file_name}_{rt_dataset_key}_{analysis_date}.
    # do not include {file_name}_{analysis_date} (bc that is concat version)
    files_to_compile = [f for f in all_files 
                        if file_name in f and 
                        analysis_date in f and 
                        f"{file_name}_{analysis_date}" not in f
                       ]
    
    results = [delayed(pd.read_parquet)(f"gs://{f}") 
                 for f in files_to_compile]
    
    dask_utils.compute_and_export(
        results,
        gcs_folder = gcs_folder,
        file_name = f"{file_name}_{analysis_date}",
        export_single_parquet = False
    )

    # Remove these partitioned parquets (folders, set recursive = True)
    for f in files_to_compile:
        fs.rm(f"gs://{f}", recursive=True)
    
        
if __name__ == "__main__":
    
    LOG_FILE = "../logs/sjoin_vp_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    #ROUTE_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "route_segments")
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    '''
    vp_route_seg = sjoin_vp_to_segments(
        analysis_date = analysis_date,
        buffer_size = 50,
        dict_inputs = ROUTE_SEG_DICT,
    )
    
    compile_partitioned_parquets_for_operators(
        f"{SEGMENT_GCS}vp_sjoin/", 
        ROUTE_SEG_DICT["stage2"], 
        analysis_date
    ) 
    '''
    time1 = datetime.datetime.now()
    #logger.info(f"attach vp to route segments: {time1 - start}")
    '''
    vp_stop_seg = sjoin_vp_to_segments(
        analysis_date = analysis_date,
        dict_inputs = STOP_SEG_DICT
    )
    '''
    
    vp_trips = add_grouping_col_to_vp(
        f"{STOP_SEG_DICT['stage1']}_{analysis_date}",
        analysis_date,
        STOP_SEG_DICT["trip_grouping_cols"]
    )
    compile_partitioned_parquets_for_operators(
        f"{SEGMENT_GCS}vp_sjoin/", 
        STOP_SEG_DICT["stage2"], 
        analysis_date, 
        vp_trips
    ) 
    time2 = datetime.datetime.now()
    #logger.info(f"attach vp to stop-to-stop segments: {time2 - time1}")
    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")