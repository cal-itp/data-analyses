"""
For each month, find the new local roads we need to segment.
Concatenate primary/secondary/local road segments to use
for each analysis date.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed
from loguru import logger

import cut_road_segments
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (analysis_date,
                                              SEGMENT_GCS,
                                              SHARED_GCS,
                                              PROJECT_CRS, 
                                              ROAD_SEGMENT_METERS
                                             )
from calitp_data_analysis import utils
from shared_utils import rt_utils

def monthly_local_linearids(
    analysis_date: str, 
    segment_length_meters: int
) -> gpd.GeoDataFrame:
    """
    Instead of re-cutting all local roads found from the last run,
    only cut the new local roads that are found. 
    
    Args:
        analysis_date: analysis_date
        segment_length_meters: how many meters to make the segment 
    """
    already_cut = dd.read_parquet(
        f"{SHARED_GCS}segmented_roads_2020_local.parquet",
        columns = ["linearid"]
    ).linearid.unique().compute().tolist()
    
    local_roads = cut_road_segments.load_roads(
        filtering = [
            ("MTFCC", "==", "S1400"), 
            ("LINEARID", "not in", already_cut)]
    )
    
    trips = helpers.import_scheduled_trips(
        analysis_date, 
        columns = ["shape_array_key"],
        filters = [[("name", "!=", "Amtrak Schedule")]],
        get_pandas = True
    )
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = False
    ).merge(
        trips,
        on = "shape_array_key",
        how = "inner"
    )
    
    local_roads_to_cut = cut_road_segments.sjoin_shapes_to_local_roads(
        shapes, local_roads)
    
    if len(local_roads_to_cut) > 0:
    
        roads_segmented = cut_road_segments.cut_segments_dask(
            local_roads_to_cut,
            ["linearid", "mtfcc", "fullname"],
            segment_length_meters
        )

        roads_segmented2 = cut_road_segments.add_segment_direction(roads_segmented)

        #roads_segmented_both_sets = cut_road_segments.append_reverse_segments(
        #    roads_segmented2)

        utils.geoparquet_gcs_export(
            roads_segmented2,
            f"{SHARED_GCS}road_segments_monthly_append/",
            f"segmented_roads_{analysis_date}"
        )
    else:
        pass 
    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/cut_road_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    logger.info(f"Analysis date: {analysis_date}")

    start = datetime.datetime.now()
    
    monthly_local_linearids(analysis_date, ROAD_SEGMENT_METERS)
    
    time1 = datetime.datetime.now()
    logger.info(f"add local linearids for this month: {time1 - start}")
    
    keep_cols = [
        "linearid", "mtfcc", "fullname",
        "segment_sequence", "primary_direction",
        "geometry"
    ]
                
    primary_secondary = delayed(gpd.read_parquet)(
        f"{SHARED_GCS}segmented_roads_2020_primarysecondary.parquet",
        columns = keep_cols
    )
    local = delayed(gpd.read_parquet)(
        f"{SHARED_GCS}segmented_roads_2020_local.parquet",
        columns = keep_cols
    )
    
    if rt_utils.check_cached(f"segmented_roads_{analysis_date}.parquet",
        SHARED_GCS, subfolder = "road_segments_monthly_append/"):
        
        local_monthly_append = delayed(gpd.read_parquet)(
            f"{SHARED_GCS}road_segments_monthly_append/"
            f"segmented_roads_{analysis_date}.parquet",
            columns = keep_cols
        )
        
        # Concatenate the various gdfs we need for this month
        road_segments = dd.from_delayed(
            [primary_secondary, local, local_monthly_append]
        )
    
    else: 
        road_segments = dd.from_delayed([primary_secondary, local])
    
    road_segments = road_segments.reset_index(drop=True).repartition(npartitions=2)
    road_segments["seg_idx"] = road_segments.index
    
    road_segments.to_parquet(
        f"{SEGMENT_GCS}road_segments_{analysis_date}", 
        overwrite = True
    )

    end = datetime.datetime.now()    
    logger.info(f"concatenate road segments: {end - time1}")
    logger.info(f"execution time: {end - start}")
