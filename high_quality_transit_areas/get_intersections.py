"""
Find where bus corridors intersect.

With hqta_segment_id clipping, and using iterrtuples to
find the area of intersection between 
an hqta_segment_id and its intersect_hqta_segment_id.

Takes 1.5 min to run.
- down from ranging from 1 hr 45 min - 2 hr 50 min in v2 
- down from several hours in v1 in combine_and_visualize.ipynb
"""
import datetime
import geopandas as gpd
import intake
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis import utils
from update_vars import GCS_FILE_PATH, analysis_date

import google.auth

catalog = intake.open_catalog("*.yml")

def attach_geometry_to_pairs(
    corridors: gpd.GeoDataFrame, 
    intersecting_pairs: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Take pairwise table and attach geometry to hqta_segment_id and 
    intersect_hqta_segment_id.
    """
    segment_cols = ["hqta_segment_id", "geometry"]
    
    rename_cols = {
        "hqta_segment_id": "intersect_hqta_segment_id", 
        "geometry": "intersect_geometry"
        
    }
    
    col_order = ["schedule_gtfs_dataset_key"] + segment_cols + list(rename_cols.values())
    
    pairs_with_geom1 = pd.merge(
        corridors[["schedule_gtfs_dataset_key"] + segment_cols],
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
           .sort_values(["schedule_gtfs_dataset_key", "hqta_segment_id", 
                         "intersect_hqta_segment_id"])
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
    # Find intersection with 2 GeoSeries,
    # if align=True, then it's a row-wise comparison to find the intersection
    intersect_results = pairs_table.geometry.intersection(
        pairs_table.intersect_geometry, align=True)
    
    # Turn GeoSeries to gdf
    results_df = (gpd.GeoDataFrame(intersect_results)
                  .rename(columns={0:'geometry'})
                  .set_geometry('geometry')
                 )
    
    # Concatenate and add this column to pairs_table, join by index 
    gdf = pd.concat([
        results_df,
        pairs_table[["schedule_gtfs_dataset_key", "hqta_segment_id"]], 
    ], axis=1)
    
    return gdf    

    
if __name__ == "__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    credentials, _=google.auth.default()
    logger.add("./logs/hqta_processing.log", retention = "3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
        
    intersecting_pairs = catalog.pairwise_intersections.read()
    corridors = catalog.subset_corridors(geopandas_kwargs={"storage_options": {"token": credentials}}).read()
    
    pairs_table = attach_geometry_to_pairs(corridors, intersecting_pairs)
        
    results = find_intersections(pairs_table)
            
    utils.geoparquet_gcs_export(
        results,
        GCS_FILE_PATH,
        "all_intersections",
    )
 
    end = datetime.datetime.now()
    logger.info(
        f"C2_find_intersections {analysis_date} "
        f"execution time: {end - start}"
    )
    
    #client.close()