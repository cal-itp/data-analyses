import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger

import A1_sjoin_vp_segments as A1
from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH, PROJECT_CRS)



def single_direction_spatial_join(
    vp: dd.DataFrame, 
    segments: gpd.GeoDataFrame, 
    segment_identifer_cols: list,
    direction: str
) -> dd.DataFrame:
    """
    Merge all the segments for a shape for that trip,
    and check if vp is within.
    Use map partitions, which treats each partition as df or gdf.
    """    
    vp_gdf = gpd.GeoDataFrame(
        vp,
        geometry = gpd.points_from_xy(vp.x, vp.y, crs=WGS84)
    ).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    vp_to_seg = gpd.sjoin(
        vp_gdf,
        segments,
        how = "inner",
        predicate = "within"
    )[["vp_idx"] + segment_identifer_cols]

    results = (vp_to_seg
               .drop_duplicates()
               .reset_index(drop=True)
              )
    
    return results


def stage_direction_results(
    vp: dd.DataFrame, 
    segments: gpd.GeoDataFrame, 
    segment_identifier_cols: list,
    direction: str
):
    keep_vp = [direction, "Unknown"]
    
    vp_subset = vp[vp.vp_primary_direction.isin(keep_vp)].repartition(npartitions=20)
    segments_subset = segments[
        segments.primary_direction==direction].reset_index(drop=True)
    
    seg_id_dtypes = segments[segment_identifier_cols].dtypes.to_dict()

    results_subset = vp_subset.map_partitions(
        single_direction_spatial_join,
        segments_subset,
        segment_identifier_cols,
        direction,
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
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    EXPORT_FILE = dict_inputs["stage2"]
    
    BUFFER_METERS = 35
    
    time0 = datetime.datetime.now()
    
    # Import vp, keep trips that are usable
    vp = dd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}/",
        columns = ["vp_idx", "x", "y", "vp_primary_direction"],
    ).repartition(npartitions=100)
    
    # Maybe work on primary/secondary segments first
    # then any vp_idx that has no sjoin result can be passed to local
    primary_secondary_segments = A1.import_segments_and_buffer(
        f"{SEGMENT_FILE}_{analysis_date}",
        BUFFER_METERS,
        SEGMENT_IDENTIFIER_COLS,
        filters = [[("mtfcc", "in", ["S1100", "S1200"])]]
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"import vp and segments: {time1 - time0}")
    
    all_directions = ["Northbound", "Southbound", "Eastbound", "Westbound"]
    
    results = [
        stage_direction_results(
            vp,
            primary_secondary_segments,
            SEGMENT_IDENTIFIER_COLS, 
            one_direction
        ) for one_direction in all_directions
    ]
    
    time2 = datetime.datetime.now()
    logger.info(f"primary/secondary sjoin with map_partitions: {time2 - time1}")
    
    full_results = dd.multi.concat(results, axis=0).reset_index(drop=True)
    full_results = full_results.repartition(npartitions=2)
    
    full_results.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/{EXPORT_FILE}_prisec_{analysis_date}",
        overwrite = True
    )
    
    present_vp = pd.read_parquet(
        f"{SEGMENT_GCS}vp_sjoin/{EXPORT_FILE}_prisec_{analysis_date}",
        columns = ["vp_idx"]
    ).vp_idx.unique().tolist()
    print(f"# vp not yet joined: {len(present_vp)}")
    
    time3 = datetime.datetime.now()
    #logger.info(f"export partitioned results: {time3 - time2}")
    
    
    vp_round2 = dd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}/",
        columns = ["vp_idx", "x", "y", "vp_primary_direction"],
        filters = [[("vp_idx", "not in", present_vp)]]
    ).repartition(npartitions=50)
    
    local_segments = A1.import_segments_and_buffer(
        f"{SEGMENT_FILE}_{analysis_date}",
        BUFFER_METERS,
        SEGMENT_IDENTIFIER_COLS,
        filters = [[("mtfcc", "==", "S1400")]]
    )
    
    results2 = [
        stage_direction_results(
            vp_round2,
            local_segments,
            SEGMENT_IDENTIFIER_COLS, 
            one_direction
        ) for one_direction in all_directions
    ]
    
    time4 = datetime.datetime.now()
    logger.info(f"local sjoin with map_partitions: {time4 - time3}")
    
    full_results2 = dd.multi.concat(results2, axis=0).reset_index(drop=True)
    full_results2 = full_results2.repartition(npartitions=2)
    
    full_results2.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/{EXPORT_FILE}_local_{analysis_date}",
        overwrite = True
    )
    
    time5 = datetime.datetime.now()
    logger.info(f"export partitioned results: {time5 - time4}")
    
    
    
if __name__ == "__main__":
    LOG_FILE = "../logs/test_sjoin_roads.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    ROAD_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "road_segments")

    sjoin_vp_to_segments(analysis_date, ROAD_SEG_DICT)