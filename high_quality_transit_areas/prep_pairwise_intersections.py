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

import gcsfs
import google.auth
credentials, _ = google.auth.default()
fs = gcsfs.GCSFileSystem(token=credentials)

def prep_bus_corridors(is_ms_precursor: bool = False, is_hq_corr: bool = False) -> gpd.GeoDataFrame:
    """
    Import all hqta segments with ms_precursor or is_hq_corr tagged.
    Since these definitions have diverged, may be called to return either.
    Only keep if ms_precursor or hq_transit_corr == True
    """
    assert is_ms_precursor or is_hq_corr, 'must select exactly one category of segment'
    if is_ms_precursor:
        hqtc_or_ms_pre = gpd.read_parquet(
            f"{GCS_FILE_PATH}all_bus.parquet",
            filters = [[("ms_precursor", "==", is_ms_precursor)]],
            storage_options={"token": credentials.token}
        ).reset_index(drop=True)
    elif is_hq_corr:
        hqtc_or_ms_pre = gpd.read_parquet(
            f"{GCS_FILE_PATH}all_bus.parquet",
            filters = [[("hq_transit_corr", "==", is_hq_corr)]],
            storage_options={"token": credentials.token}
        ).reset_index(drop=True)
        
    hqtc_or_ms_pre = hqtc_or_ms_pre.assign(
        route_type = "3"
    )
    
    return hqtc_or_ms_pre


def sjoin_against_other_operators(
    in_group_df: gpd.GeoDataFrame, 
    out_group_df: gpd.GeoDataFrame
) -> pd.DataFrame: 
    """
    Spatial join of the in group vs the out group. 
    This could be the operator vs other operators, 
    or a route vs other routes. This is currently
    east-west vs north-south segments, which requires
    the additional step of excluding intersections
    resulting from the same route changing direction.
    
    Create a crosswalk / pairwise table showing these links.
    
    Compile all of them, because finding intersections is 
    computationally expensive,
    so we want to do it on fewer rows. 
    """
    route_cols = ["hqta_segment_id", "segment_direction", "route_key"]
    
    s1 = gpd.sjoin(
        in_group_df[route_cols + ["geometry"]], 
        out_group_df[route_cols  + ["geometry"]],
        how = "inner",
        predicate = "intersects"
    ).drop(columns = ["index_right", "geometry"])
    
    s1 = s1[s1["route_key_left"] != s1["route_key_right"]]
            
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
            ["schedule_gtfs_dataset_key", "route_id", "hqta_segment_id"], 
            ascending = [True, True, True])
        .reset_index(drop=True)
    )
    
    pairs.to_parquet(
        f"{GCS_FILE_PATH}pairwise.parquet",
        filesystem=fs
    )
    
    utils.geoparquet_gcs_export(
        corridors2,
        GCS_FILE_PATH,
        "subset_corridors",
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

    corridors = prep_bus_corridors(is_ms_precursor=True)   
    
    pairwise_intersections(corridors)    
    
    end = datetime.datetime.now()
    logger.info(
        f"C1_prep_pairwise_intersections {analysis_date} "
        f"execution time: {end - start}"
    )

    #client.close()