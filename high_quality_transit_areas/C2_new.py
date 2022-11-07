"""
Do clipping to find where bus corridors intersect.

With hqta_segment_id clipping, but looping across route_id, 
it takes 42 min to run.
LA Metro takes 3.5 min to run.

With route_id clipping, takes 1 hr 52 min to run: 
LA Metro takes 6 min to run, and ITP ID 4 takes 4 min to run.
Big Blue Bus takes 10 min to run.

From combine_and_visualize.ipynb
"""
#import dask.dataframe as dd
#import dask_geopandas as dg
import datetime as dt
import geopandas as gpd
import glob
import os
import pandas as pd
import sys

from loguru import logger

#import C1_prep_for_clipping as prep_clip
import C1_new as prep_clip
from shared_utils import utils
#from utilities import catalog_filepath, GCS_FILE_PATH
from update_vars import analysis_date

logger.add("./logs/C2_new.log")
logger.add(sys.stderr, 
           format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
           level="INFO")


# Input files
#PAIRWISE_FILE = catalog_filepath("pairwise_intersections")
#SUBSET_CORRIDORS = catalog_filepath("subset_corridors")
DASK_GCS = "gs://calitp-analytics-data/data-analyses/dask_test/"
GCS_FILE_PATH = DASK_GCS

PAIRWISE_FILE = f"{DASK_GCS}intermediate/pairwise.parquet"
SUBSET_CORRIDORS = f"{DASK_GCS}intermediate/subset_corridors.parquet"

def attach_geometry_to_pairs(corridors: gpd.GeoDataFrame, 
                             intersecting_pairs: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Take pairwise table and attach geometry to hqta_segment_id and 
    intersect_hqta_segment_id.
    """
    segment_cols = ["hqta_segment_id", "geometry"]
    
    rename_cols = {
        "hqta_segment_id": "intersect_hqta_segment_id", 
        "geometry": "intersect_geometry"
    }
    
    col_order = segment_cols + list(rename_cols.values())
    
    pairs_with_geom1 = pd.merge(
        corridors[segment_cols],
        intersecting_pairs, 
        on = "hqta_segment_id",
        how = "inner"
    )

    pairs_with_geom2 = pd.merge(
        (corridors[segment_cols]
         .rename(columns = rename_cols)),
        pairs_with_geom1, 
        on = "intersect_hqta_segment_id",
        how = "inner"
    )

    gdf = (pairs_with_geom2.reindex(columns = col_order)
           .sort_values(["hqta_segment_id", "intersect_hqta_segment_id"])
           .reset_index(drop=True)
          )
    
    return gdf
   
    
def find_intersections(pairs_table: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    We have pairwise table already, and now there's geometry attached to 
    both the hqta_segment_id and the intersect_hqta_segment_id.
    
    Use iterrtuples to loop through and store results.
    Convert back to gdf at the end.
    
    https://stackoverflow.com/questions/33817190/intersection-of-two-linestrings-geopandas
    https://stackoverflow.com/questions/43221208/iterate-over-pandas-dataframe-using-itertuples
    """
    results = []
    segments = []

    EPSG_CODE = pairs_table.crs.to_epsg()
    
    for row in pairs_table.itertuples():
        this_segment = getattr(row, "hqta_segment_id")
        this_segment_geom = getattr(row, 'geometry')
        intersecting_segment_geom = getattr(row, 'intersect_geometry')

        intersect_result = this_segment_geom.intersection(intersecting_segment_geom)

        results.append(intersect_result)
        segments.append(this_segment)
        
    intersect_results = (gpd.GeoDataFrame(
        segments, geometry = results,
        crs = f"EPSG: {EPSG_CODE}")
                         .rename(columns = {
                             0: "hqta_segment_id", 
                             1: "geometry"})
                        )
                         
    return intersect_results    

    
if __name__ == "__main__":
    logger.info(f"Analysis date: {analysis_date}")

    start = dt.datetime.now()
        
    intersecting_pairs = pd.read_parquet(PAIRWISE_FILE)
    corridors = gpd.read_parquet(SUBSET_CORRIDORS)
    
    pairs_table = attach_geometry_to_pairs(corridors, intersecting_pairs)
    
    time1 = dt.datetime.now()
    logger.info(f"attach geometry to pairwise table: {time1 - start}")
    
    results = find_intersections(pairs_table)
    
    time2 = dt.datetime.now()
    logger.info(f"find intersections: {time2 - time1}")
        
    utils.geoparquet_gcs_export(
        results_gdf,
        DASK_GCS,
        "all_clipped"
    )
 
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")