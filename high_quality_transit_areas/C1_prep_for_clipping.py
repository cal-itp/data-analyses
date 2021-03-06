"""
Prep components needed for clipping.
Find pairwise hqta_segment_ids / route_ids with dask_geopandas.sjoin
to narrow down the rows to pass through clipping.

This takes 15 min to run. 

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd
import pandas as pd

from B1_bus_corridors import TEST_GCS_FILE_PATH
from shared_utils import utils
from utilities import catalog_filepath

segment_cols = ["calitp_itp_id", "hqta_segment_id", "route_direction"]

intersect_segment_cols = ["intersect_calitp_itp_id", 
                        "intersect_hqta_segment_id", "intersect_route_direction"]


# Input files
ALL_BUS = catalog_filepath("all_bus")

def prep_bus_corridors():
    bus_hqtc = dask_geopandas.read_parquet(ALL_BUS)
    
    bus_hqtc2 = bus_hqtc[bus_hqtc.hq_transit_corr==True].reset_index(drop=True)
    bus_hqtc2 = bus_hqtc2.assign(
        hqta_type = "hqta_transit_corr",
        route_type = "3"
    )
    
    return bus_hqtc2


# Spatial join on route_id 
def sjoin_operator_not_operator(operator, not_operator):
    # Let's keep the pair of that intersection and store it
    # Need to rename not_operator columns so it's easier to distinguish
    not_operator = rename_cols(not_operator, with_intersect=True)    
    
    # Do a spatial join first to see what rows should be included
    # Compile all of them, because clipping is computationally expensive,
    # so we want to do it on fewer rows
    s1 = dask_geopandas.sjoin(
        operator, 
        not_operator,
        predicate="intersects"
    )
    
    # Once the spatial join is done, don't need to store the geometry
    intersecting_segments = (s1[segment_cols + 
                                intersect_segment_cols]
                             .drop_duplicates()
                             .reset_index(drop=True)
                            )
    
    return intersecting_segments


def find_intersection_across_and_within(gdf, itp_id):
    keep_cols = segment_cols + ["geometry"]
    
    # Create subset dfs for the "in_group"
    # to be compared with sjoin with the "out_group"
    # Keep route_id with operator to find intersections WITHIN operator
    operator = gdf[gdf.calitp_itp_id == itp_id][["route_id"] + keep_cols]    
    not_operator = gdf[gdf.calitp_itp_id != itp_id][keep_cols]
    
    # First, find intersections across operators
    intersections_across_operators = sjoin_operator_not_operator(
        operator[keep_cols], not_operator)
    
    # Set the metadata for intersections within operators
    intersections_within_operators = intersections_across_operators.head(0)
    
    # Now add in the intersections within operators
    operator_routes = list(operator.route_id.unique())
    
    for i in operator_routes:
        # Subset to particular route_id, then use same sjoin,
        # where the "in group" is one_route and the "out group" is other_routes
        one_route = operator[operator.route_id == i]
        other_routes = operator[operator.route_id != i]
        
        within_operator = sjoin_operator_not_operator(one_route, other_routes)
        
        intersections_within_operators = dd.multi.concat(
            [intersections_within_operators, within_operator], axis=0)
    
    # Concatenate the intersections found across operator and within operator,
    # but drop the geometry, because we only need the df to store this info
    keep_cols = segment_cols + intersect_segment_cols 
    
    all_intersections = (dd.multi.concat(
        [intersections_across_operators, 
         intersections_within_operators], axis=0)
        .drop_duplicates()
        .sort_values("hqta_segment_id")
        .reset_index(drop=True)
        [keep_cols]
    )
    
    return all_intersections



def compile_pairwise_intersections(corridors, ITP_ID_LIST):
    start = dt.datetime.now()

    # Having trouble initializing empty dask geodataframe
    # just subset so metadata is copied over
    intersecting_segments = corridors[corridors.calitp_itp_id==0][segment_cols]

    
    for itp_id in ITP_ID_LIST:
        time0 = dt.datetime.now()
        
        operator_shape = find_intersection_across_and_within(corridors, itp_id)
        
        time1 = dt.datetime.now()
        print(f"grab intersections for {itp_id}: {time1 - time0}")
        
        intersecting_segments = (dd.multi.concat(
            [intersecting_segments, operator_shape], axis=0)
            .drop_duplicates()
            .reset_index(drop=True)
        )

    end = dt.datetime.now()
    print(f"execution time for compiling pairwise: {end-start}")
    
    intersecting_segments = intersecting_segments.astype({
        "intersect_calitp_itp_id": int,
        "intersect_hqta_segment_id": int,
        "intersect_route_direction": str
    })
    
    return intersecting_segments


# Get pairwise one into just unique df
def unique_intersecting_segments(df):
    part1 = df[segment_cols].drop_duplicates()
    part2 = (df[intersect_segment_cols]
             .drop_duplicates()
            )
    
    part2 = rename_cols(part2, with_intersect=False)
    
    unique = (dd.multi.concat([part1, part2], axis=0)
              .drop_duplicates()
              .reset_index(drop=True)
             )
    
    return unique


def subset_corridors(gdf, intersecting_shapes):    
    shapes_needed = unique_intersecting_segments(intersecting_shapes)
    
    gdf2 = dd.merge(gdf, shapes_needed, 
                    on = segment_cols,
                    how = "inner"
                   )
    
    return gdf2


def rename_cols(df, with_intersect=False):
    if with_intersect is True:
        df.columns = [f'intersect_{col}' if col != 'geometry' 
                      else col for col in df.columns]
        
    elif with_intersect is False:
        df.columns = df.columns.str.replace('intersect_', '')
        
    return df


if __name__=="__main__":

    start = dt.datetime.now()

    corridors = prep_bus_corridors()   

    ITP_IDS = list(corridors.calitp_itp_id.unique())
    
    intersecting_shapes = compile_pairwise_intersections(corridors, ITP_IDS)
    
    keep_cols = segment_cols + ["geometry"]
    
    corridors2 = subset_corridors(corridors[keep_cols + ["route_id"]], intersecting_shapes)
    
    time1 = dt.datetime.now()
    print(f"subset corridors: {time1 - start}")
    
    # Save results locally temporarily
    # Here, already drop where the dask_geopandas.sjoin gave us intersections
    # of route directions going in the same direction
    # Only allow orthogonal ones to be used in the clip
    pairwise = intersecting_shapes[
        intersecting_shapes.route_direction != 
        intersecting_shapes.intersect_route_direction].compute()
        
    subset_corridors = corridors2.compute()

    time2 = dt.datetime.now()
    print(f"compute for pairwise/subset_corridors: {time2 - time1}")
    
    pairwise.to_parquet(f"{TEST_GCS_FILE_PATH}intermediate/pairwise.parquet")
    
    utils.geoparquet_gcs_export(subset_corridors,
                        f'{TEST_GCS_FILE_PATH}intermediate/',
                        'subset_corridors'
                       )
    
    end = dt.datetime.now()
    print(f"execution time: {end-start}")
