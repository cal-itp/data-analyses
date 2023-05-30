"""
Spatial join vehicle positions to segments.

Stop-to-stop segments.
Use dask.delayed + loop so that vp can only join to the 
relevant segments.
Otherwise, vp will be joined to other segments that share the same road.
"""
import dask.dataframe as dd
import datetime
import gcsfs
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from dask import delayed, compute
from dask.delayed import Delayed # type hint
from loguru import logger

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
        partitioned=True
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
   

def subset_vp_for_shape(vp_df: dd.DataFrame, shape: str) -> pd.DataFrame:
    subset_df = vp_df[vp_df.shape_array_key==shape]
    return subset_df
    
    
def import_segments_and_buffer(
    segment_file_name: str,
    buffer_size: int,
    segment_identifier_cols: list
) -> gpd.GeoDataFrame:
    """
    Import segments , subset certain columns, 
    and buffer by some specified amount.
    """
    segments = delayed(gpd.read_parquet)(
        f"{SEGMENT_GCS}{segment_file_name}.parquet",
        columns = segment_identifier_cols + ["seg_idx", "geometry"],
    ).to_crs(PROJECT_CRS)

    # Buffer the segment for vehicle positions (points) to fall in polygons
    segments = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    return segments


def sjoin_vp_to_segment(
    vp: pd.DataFrame,
    segment: gpd.GeoDataFrame,
) -> np.ndarray: 
    
    vp_gdf = gpd.GeoDataFrame(
        vp, 
        geometry = gpd.points_from_xy(vp.x, vp.y, crs = "EPSG:4326")
    ).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    vp_in_seg = gpd.sjoin(
        vp_gdf,
        segment,
        how = "inner",
        predicate = "within"
    )[["vp_idx", "seg_idx"]]
    
    # Instead of saving it out as df, which seems to get stuck,
    # in dask delayed for the compute, convert to numpy matrix of n_rows x 2 cols.
    # Just need idx to merge all the other columns later.
    pairs = vp_in_seg.to_numpy()
        
    return pairs
    
    
    
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
        
        vp = helpers.import_vehicle_positions(
            SEGMENT_GCS,
            f"{INPUT_FILE}_{analysis_date}/",
            filters = [[("gtfs_dataset_key", "==", rt_dataset_key)]],
            columns = ["gtfs_dataset_key", "trip_id", 
                       "vp_idx", "x", "y"],
            partitioned = True
        ).merge(
            vp_trips,
            on = ["gtfs_dataset_key", "trip_id"],
            how = "inner"
        )
        
        vp = delayed(vp).persist()    
        
        all_shapes = vp_trips[vp_trips.gtfs_dataset_key==rt_dataset_key
                             ].shape_array_key.unique().tolist()

        vp_dfs = [
            delayed(subset_vp_for_shape)(vp, shape)
            for shape in all_shapes
        ]

        segment_dfs = [
            segments[segments.shape_array_key==shape] 
            for shape in all_shapes
        ]

        results = [
            delayed(sjoin_vp_to_segment)(shape_vp, shape_segments)
            for shape_vp, shape_segments in zip(vp_dfs, segment_dfs)
        ]

        time1 = datetime.datetime.now()
        print(f"delayed results: {time1 - start_id}")

        # Compute results, which are numpy arrays, and stack rows
        results2 = [compute(i)[0] for i in results]
        stacked_results = np.row_stack(results2)

        time2 = datetime.datetime.now()
        print(f"stack arrays: {time2 - time1}")

        # Change matrix to df
        result_pairs = pd.DataFrame(
            stacked_results, 
            columns = ["vp_idx", "seg_idx"]
        )
        
        result_pairs.to_parquet(
            f"{SEGMENT_GCS}vp_sjoin/{EXPORT_FILE}_"
            f"{rt_dataset_key}_{analysis_date}.parquet")
        
        end_id = datetime.datetime.now()
        print(f"finished {rt_dataset_key}: {end_id - start_id}")
     
    
def compile_parquets_for_operators(
    gcs_folder: str,
    file_name: str,
    analysis_date: str, 
    dict_inputs: dict = {}
):
    """
    Once we've saved out indiv operator parquets,
    read those in and save as one parquet. 
    Merge in segment identifier cols so we're not left with seg_idx only, 
    and export our sjoin results as 1 parquet for the date.
    """
    SEGMENT_FILE = dict_inputs["segments_file"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    
    all_files = fs.ls(f"{gcs_folder}")
    
    # The files to compile need format
    # {file_name}_{rt_dataset_key}_{analysis_date}.
    # do not include {file_name}_{analysis_date} (bc that is concat version)
    files_to_compile = [
        f"gs://{f}" for f in all_files 
        if (file_name in f) and (analysis_date in f) and 
        (f"{file_name}_{analysis_date}" not in f)
    ]
    
    delayed_dfs = [delayed(pd.read_parquet)(f) for f in files_to_compile]
    
    ddf = dd.from_delayed(delayed_dfs)
    
    segments = pd.read_parquet(
        f"{SEGMENT_GCS}{SEGMENT_FILE}_{analysis_date}.parquet", 
        columns = ["seg_idx"] + SEGMENT_IDENTIFIER_COLS
    )
    
    # Merge in segment_identifier_cols so that vp has the 
    # segment grouping columns attached for use in the next age
    ddf2 = dd.merge(
        ddf,
        segments,
        on = "seg_idx",
        how = "inner"
    ).drop(columns = "seg_idx")
        
    ddf2.to_parquet(f"{gcs_folder}{file_name}_{analysis_date}", 
                    overwrite=True)

    # Remove these partitioned parquets (if it's folder, set recursive = True)
    for f in files_to_compile:
        fs.rm(f)
            
        
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
    
    time1 = datetime.datetime.now()
    logger.info(f"attach vp to stop-to-stop segments: {time1 - start}")

    compile_parquets_for_operators(
        f"{SEGMENT_GCS}vp_sjoin/", 
        STOP_SEG_DICT["stage2"], 
        analysis_date, 
        STOP_SEG_DICT,
    ) 
    
    end = datetime.datetime.now()
    logger.info(f"compiled parquets: {end - time1}")
    logger.info(f"execution time: {end-start}")