"""
Prep components needed for finding where bus corridors intersect.
Find pairwise hqta_segment_ids / route_ids with dask_geopandas.sjoin
because sjoin is less computationally expensive than geopandas.clip

Takes 1 min to run. 
- down from 30 min in v2
- down from several hours in v1 in combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime as dt
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from shared_utils import utils
from utilities import catalog_filepath, GCS_FILE_PATH
from update_vars import analysis_date

# Input files
ALL_BUS = catalog_filepath("all_bus")

def prep_bus_corridors() -> dg.GeoDataFrame:
    """
    Import all hqta segments with hq_transit_corr tagged.
    
    Only keep if hq_transit_corr == True
    """
    bus_hqtc = dg.read_parquet(ALL_BUS)
    
    bus_hqtc2 = bus_hqtc[bus_hqtc.hq_transit_corr==True].reset_index(drop=True)
    bus_hqtc2 = bus_hqtc2.assign(
        hqta_type = "hqta_transit_corr",
        route_type = "3"
    )
    
    return bus_hqtc2


def sjoin_against_other_operators(
    in_group_df: dg.GeoDataFrame, 
    out_group_df: dg.GeoDataFrame) -> dd.DataFrame: 
    """
    Spatial join of the in group vs the out group. 
    This could be the operator vs other operators, 
    or a route vs other routes.
    
    Create a crosswalk / pairwise table showing these links.
    
    Compile all of them, because finding intersections is computationally expensive,
    so we want to do it on fewer rows. 
    """
    route_cols = ["hqta_segment_id", "route_direction"]
    
    s1 = dg.sjoin(
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


def compile_operator_intersections(
    gdf: dg.GeoDataFrame) -> dd.DataFrame:
    """
    Grab one operator's routes, and do sjoin against all other operators' routes.
    Look BETWEEN operators and WITHIN operators at the same time.
    
    Since only orthogonal routes are allowed to intersect,
    put an east-west df on the left, and a north-south df on the right.
    For a given operator's east-west routes, it can be compared to both
    north-south routes from other operators and itself.
    
    Concatenate all the small dask dfs into 1 dask df by the end.
    
    https://stackoverflow.com/questions/56072129/scale-and-concatenate-pandas-dataframe-into-a-dask-dataframe
    """
    results = []

    # Part 1: take east-west routes for an operator, and compare against
    # all north-south routes for itself and other operators
    all_east_west = gdf[gdf.route_direction=="east-west"]
    all_north_south = gdf[gdf.route_direction=="north-south"]

    results.append(sjoin_against_other_operators(
        all_east_west, all_north_south))
        
    # Part 2: take north-south routes for an operator, and compare against
    # all east-west routes for itself and other operators
    # Even though this necessarily would repeat results from earlier, just swapped, 
    # like RouteA-RouteB becomes RouteB-RouteA, use this to key in easier later.
    results.append(sjoin_against_other_operators(
        all_north_south, all_east_west)) 
    
    # Concatenate all the dask dfs in the list and get it into one dask df
    ddf = dd.multi.concat(results, axis=0).drop_duplicates().reset_index(drop=True)
    
    return ddf    


if __name__=="__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/C1_prep_pairwise_intersections.log", retention = "3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()

    corridors = prep_bus_corridors()   
        
    # Route intersections across operators
    intersection_pairs = compile_operator_intersections(corridors)

    time1 = dt.datetime.now()
    logger.info(f"get pairwise table: {time1 - start}")

    pairwise_intersections = intersection_pairs.compute()
    
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
    ).compute()
    
    subset_corridors = (subset_corridors.sort_values(
                    ["feed_key", "route_id", "hqta_segment_id"], 
                    ascending = [True, True, True])
                    .reset_index(drop=True)
    )

    time2 = dt.datetime.now()
    logger.info(f"compute for pairwise/subset_corridors: {time2 - time1}")
    
    pairwise_intersections.to_parquet(
        f"{GCS_FILE_PATH}pairwise.parquet")
    
    utils.geoparquet_gcs_export(subset_corridors,
                                GCS_FILE_PATH,
                                'subset_corridors'
                       )
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")

    #client.close()