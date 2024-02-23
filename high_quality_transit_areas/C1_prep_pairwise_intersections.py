"""
Prep components needed for finding where bus corridors intersect.
Find pairwise hqta_segment_ids / route_ids with dask_geopandas.sjoin
because sjoin is less computationally expensive than geopandas.clip

Takes 1 min to run. 
- down from 30 min in v2
- down from several hours in v1 in combine_and_visualize.ipynb
"""
import datetime
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis import utils
from update_vars import GCS_FILE_PATH, analysis_date

def prep_bus_corridors(is_hq_corr: bool) -> gpd.GeoDataFrame:
    """
    Import all hqta segments with hq_transit_corr tagged.
    
    Only keep if hq_transit_corr == True
    """
    bus_hqtc = gpd.read_parquet(
        f"{GCS_FILE_PATH}all_bus.parquet",
        filters = [[("hq_transit_corr", "==", is_hq_corr)]]
    ).reset_index(drop=True)
    
    bus_hqtc = bus_hqtc.assign(
        route_type = "3"
    )
    
    return bus_hqtc




def sjoin_against_other_operators(
    in_group_df: gpd.GeoDataFrame, 
    out_group_df: gpd.GeoDataFrame
) -> pd.DataFrame: 
    """
    Spatial join of the in group vs the out group. 
    This could be the operator vs other operators, 
    or a route vs other routes.
    
    Create a crosswalk / pairwise table showing these links.
    
    Compile all of them, because finding intersections is 
    computationally expensive,
    so we want to do it on fewer rows. 
    """
    route_cols = ["hqta_segment_id", "segment_direction"]
    
    s1 = gpd.sjoin(
        in_group_df[route_cols + ["geometry"]], 
        out_group_df[route_cols  + ["geometry"]],
        how = "inner",
        predicate = "intersects"
    ).drop(columns = ["index_right", "geometry"])
            
    route_pairs = (
        s1.rename(
            columns = {
                "hqta_segment_id_left": "hqta_segment_id",
                "hqta_segment_id_right": "intersect_hqta_segment_id",
            })
          [["hqta_segment_id", "intersect_hqta_segment_id"]]
          .drop_duplicates()
          .reset_index(drop=True)
    )    
    
    return route_pairs   


def pairwise_intersections(
    corridors_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Do pairwise comparisons of hqta segments.
    Take all the north-south segments and compare to east-west
    and vice versa.
    """
    # Route intersections across operators
    east_west = corridors_gdf[corridors_gdf.segment_direction == "east-west"]
    north_south = corridors_gdf[corridors_gdf.segment_direction == "north-south"]
    
    results = [
        sjoin_against_other_operators(north_south, east_west),
        sjoin_against_other_operators(east_west, north_south)
    ]
    
    pairs = pd.concat(results, axis=0, ignore_index=True)
    
    segments_p1 = pairs.hqta_segment_id.unique()
    segments_p2 = pairs.intersect_hqta_segment_id.unique()
    
    # Subset the hqta segments that do have hq_transit_corr == True 
    # down to the ones where routes have with sjoin intersections
    corridors2 = (
        corridors_gdf[
            (corridors_gdf.hqta_segment_id.isin(segments_p1)) | 
            (corridors_gdf.hqta_segment_id.isin(segments_p2))]
        .drop_duplicates()
        .sort_values(
            ["feed_key", "route_id", "hqta_segment_id"], 
            ascending = [True, True, True])
        .reset_index(drop=True)
    )
    
    pairs.to_parquet(
        f"{GCS_FILE_PATH}pairwise.parquet")
    
    utils.geoparquet_gcs_export(
        corridors2,
        GCS_FILE_PATH,
        "subset_corridors"
    )
    
    return
    

if __name__=="__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/hqta_processing.log", retention = "3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    start = datetime.datetime.now()

    corridors = prep_bus_corridors(is_hq_corr=True)   
    
    pairwise_intersections(corridors)    
    
    end = datetime.datetime.now()
    logger.info(f"C1_prep_pairwise_intersections {analysis_date} "
                f"execution time: {end - start}")

    #client.close()