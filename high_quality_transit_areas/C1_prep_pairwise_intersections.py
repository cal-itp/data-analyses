"""
Prep components needed for finding where bus corridors intersect.
Find pairwise hqta_segment_ids / route_ids with dask_geopandas.sjoin
because sjoin is less computationally expensive than geopandas.clip

Takes 1 min to run. 
- down from 30 min in v2
- down from several hours in v1 in combine_and_visualize.ipynb
"""
import datetime as dt
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from calitp_data_analysis import utils
from utilities import catalog_filepath, GCS_FILE_PATH
from update_vars import analysis_date

# Input files
ALL_BUS = catalog_filepath("all_bus")

def prep_bus_corridors() -> gpd.GeoDataFrame:
    """
    Import all hqta segments with hq_transit_corr tagged.
    
    Only keep if hq_transit_corr == True
    """
    bus_hqtc = gpd.read_parquet(ALL_BUS).query(
        'hq_transit_corr==True'
    ).reset_index(drop=True)
    
    bus_hqtc = bus_hqtc.assign(
        hqta_type = "hqta_transit_corr",
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
    
    Compile all of them, because finding intersections is computationally expensive,
    so we want to do it on fewer rows. 
    """
    route_cols = ["hqta_segment_id", "route_direction"]
    
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


if __name__=="__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/hqta_processing.log", retention = "3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    logger.info(f"C1_prep_pairwise_intersections Analysis date: {analysis_date}")
    start = dt.datetime.now()

    corridors = prep_bus_corridors()   
        
    # Route intersections across operators
    east_west_corr = corridors[corridors.route_direction == "east-west"]
    north_south_corr = corridors[corridors.route_direction == "north-south"]
    
    # Part 1: take east-west routes for an operator, and compare against
    # all north-south routes for itself and other operators
    # Part 2: take north-south routes for an operator, and compare against
    # all east-west routes for itself and other operators
    # Even though this necessarily would repeat results from earlier, just swapped, 
    # like RouteA-RouteB becomes RouteB-RouteA, use this to key in easier later.
    results = [
        sjoin_against_other_operators(north_south_corr, east_west_corr),
        sjoin_against_other_operators(east_west_corr, north_south_corr)
    ]
    
    pairwise_intersections = pd.concat(results, axis=0, ignore_index=True)

    time1 = dt.datetime.now()
    logger.info(f"get pairwise table: {time1 - start}")

    
    routes_p1 = pairwise_intersections.hqta_segment_id.unique().tolist()
    routes_p2 = pairwise_intersections.intersect_hqta_segment_id.unique().tolist()
    
    # Subset the hqta segments that do have hq_transit_corr == True 
    # down to the ones where routes have with sjoin intersections
    subset_corridors = (
        corridors[
            (corridors.hqta_segment_id.isin(routes_p1)) | 
            (corridors.hqta_segment_id.isin(routes_p2))]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    
    subset_corridors = (subset_corridors.sort_values(
                    ["feed_key", "route_id", "hqta_segment_id"], 
                    ascending = [True, True, True])
                    .reset_index(drop=True)
    )

    time2 = dt.datetime.now()
    logger.info(f"compute for pairwise/subset_corridors: {time2 - time1}")
    
    pairwise_intersections.to_parquet(
        f"{GCS_FILE_PATH}pairwise.parquet")
    
    utils.geoparquet_gcs_export(
        subset_corridors,
        GCS_FILE_PATH,
        'subset_corridors'
    )
    
    end = dt.datetime.now()
    logger.info(f"C1_prep_pairwise_intersections execution time: {end-start}")

    #client.close()