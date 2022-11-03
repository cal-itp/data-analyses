"""
Prep components needed for clipping.
Find pairwise hqta_segment_ids / route_ids with dask_geopandas.sjoin
to narrow down the rows to pass through clipping.

This takes 21 min to run. 

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime as dt
import geopandas as gpd
import pandas as pd
import sys

from loguru import logger

from shared_utils import utils
#from utilities import catalog_filepath, GCS_FILE_PATH
from update_vars import analysis_date

logger.add("./logs/test_C1.log")
logger.add(sys.stderr, 
           format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
           level="INFO")

segment_cols = ["calitp_itp_id", "hqta_segment_id", "route_direction"]

intersect_segment_cols = ["intersect_calitp_itp_id", 
                        "intersect_hqta_segment_id", "intersect_route_direction"]

DASK_GCS = "gs://calitp-analytics-data/data-analyses/dask_test/"

GCS_FILE_PATH = DASK_GCS

# Input files
#ALL_BUS = catalog_filepath("all_bus")
ALL_BUS = f"{DASK_GCS}all_bus.parquet"

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


def grab_line_geom_for_hq_routes(hq_corr: dg.GeoDataFrame) -> dg.GeoDataFrame:
    """
    Use the line geom (dissolved) for routes to compile the pairwise table.
    Each route will be compared across other routes across operators or 
    within operators to see what it does intersect with.
    """
    longest_shape = gpd.read_parquet(f"{DASK_GCS}longest_shape_with_dir.parquet")

    hq_routes = list(hq_corr.route_identifier.unique())

    longest_shape_hq = longest_shape[
        longest_shape.route_identifier.isin(hq_routes)]
    
    return longest_shape_hq


def sjoin_against_other_operators(
    in_group_df: dg.GeoDataFrame, 
    out_group_df: dg.GeoDataFrame) -> dd.DataFrame: 
    """
    Spatial join of the in group vs the out group. 
    This could be the operator vs other operators, 
    or a route vs other routes.
    
    Create a crosswalk / pairwise table showing these links.
    
    Compile all of them, because clipping is computationally expensive,
    so we want to do it on fewer rows. 
    """
    route_cols = ["route_identifier", "route_direction"]
    
    s1 = dg.sjoin(
        in_group_df[[#"calitp_itp_id",
                     "geometry"] + route_cols], 
        out_group_df[route_cols  + ["geometry"]],
        how = "inner",
        predicate = "intersects"
    ).drop(columns = ["index_right", "geometry"])
    
    # Only allow orthogonal!
    s2 = s1[s1.route_direction_left != s1.route_direction_right]
    
    keep_cols = route_cols + [#"calitp_itp_id", 
                              "intersect_route_identifier", 
                              "intersect_route_direction"] 
    route_pairs = (
        s2.rename(
            columns = {
                "route_identifier_left": "route_identifier",
                "route_identifier_right": "intersect_route_identifier", 
                "route_direction_left": "route_direction",
                "route_direction_right": "intersect_route_direction",
            })
          [keep_cols]
          .drop_duplicates()
          .reset_index(drop=True)
    )    
    
    return route_pairs


def compile_across_operator_intersections(
    gdf: dg.GeoDataFrame, itp_id_list: list) -> dd.DataFrame:
    """
    Grab one operator's routes, and do sjoin against all other operators' routes.
    Look BETWEEN operators.
    
    Concatenate all the small dask dfs into 1 dask df by the end.
    
    https://stackoverflow.com/questions/56072129/scale-and-concatenate-pandas-dataframe-into-a-dask-dataframe
    """
    results = []

    for itp_id in sorted(itp_id_list):
        operator = gdf[gdf.calitp_itp_id==itp_id]
        not_operator = gdf[gdf.calitp_itp_id != itp_id]
        
        results.append(sjoin_against_other_operators(operator, not_operator)) 
    
    # Concatenate all the dask dfs in the list and get it into one dask df
    ddf = dd.multi.concat(results, axis=0).drop_duplicates()
    #df = ddf.drop_duplicates().reset_index(drop=True).compute()
    
    return ddf    
        
    
def compile_within_operator_intersections(
    gdf: dg.GeoDataFrame, itp_id_list: list) -> dd.DataFrame:
    results = []
    
    for itp_id in sorted(itp_id_list):
        operator_df = gdf[gdf.calitp_itp_id==itp_id]
        
        operator_routes = list(operator_df.route_identifier.unique())
        
        for r in operator_routes:
            this_route = operator_df[operator_df.route_identifier == r]
            other_routes = operator_df[operator_df.route_identifier != r]
        
            results.append(sjoin_against_other_operators(
                this_route, other_routes)) 
    
    # Concatenate all the dask dfs in the list and get it into one dask df
    ddf = dd.multi.concat(results, axis=0).drop_duplicates()
    
    return ddf           



if __name__=="__main__":
    logger.info(f"Analysis date: {analysis_date}")
    start = dt.datetime.now()

    corridors = prep_bus_corridors()   
    longest_shape = grab_line_geom_for_hq_routes(corridors)
    
    ITP_IDS = list(corridors.calitp_itp_id.unique())

    # Route intersections across operators
    across_operator_results = compile_across_operator_intersections(
        longest_shape, ITP_IDS)

    time1 = dt.datetime.now()
    logger.info(f"across operator intersections: {time1 - start}")

    # Route intersections within operators
    within_operator_results = compile_within_operator_intersections(
        longest_shape, ITP_IDS)
    
    time2 = dt.datetime.now()
    logger.info(f"within operator intersections: {time2 - time1}")
    
    # Concatenate and save pairwise table
    pairwise_intersections = dd.multi.concat(
        [across_operator_results, within_operator_results], axis=0).compute()
    
    routes_p1 = pairwise_intersections.route_identifier.unique().tolist()
    routes_p2 = pairwise_intersections.intersect_route_identifier.unique().tolist()
        
    # Subset the hqta segments that do have hq_transit_corr == True 
    # down to the ones where routes have with sjoin intersections
    subset_corridors = (
        corridors[
            (corridors.route_identifier.isin(routes_p1)) | 
            (corridors.route_identifier.isin(routes_p2))]
        .drop_duplicates()
        .reset_index(drop=True)
    ).compute()
    
    subset_corridors = (subset_corridors.sort_values(
                    ["calitp_itp_id", "route_id", "hqta_segment_id"], 
                    ascending = [True, True, True])
                    .reset_index(drop=True)
    )

    time3 = dt.datetime.now()
    logger.info(f"compute for pairwise/subset_corridors: {time3 - time2}")
    
    pairwise_intersections.to_parquet(
        f"{GCS_FILE_PATH}intermediate/pairwise.parquet")
    
    utils.geoparquet_gcs_export(subset_corridors,
                        f'{GCS_FILE_PATH}intermediate/',
                        'subset_corridors'
                       )
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end-start}")
