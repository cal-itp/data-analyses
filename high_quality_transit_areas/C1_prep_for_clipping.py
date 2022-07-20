"""
Prep components needed for clipping.
Find pairwise hqta_segment_ids with dask_geopandas.sjoin
to narrow down the rows to pass through clipping.

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd
import pandas as pd

import B1_bus_corridors as bus_corridors


def prep_bus_corridors():
    bus_hqtc = dask_geopandas.read_parquet(
        f'{bus_corridors.TEST_GCS_FILE_PATH}intermediate/all_bus.parquet')
    
    bus_hqtc2 = bus_hqtc[bus_hqtc.hq_transit_corr==True].reset_index(drop=True)
    bus_hqtc2 = bus_hqtc2.assign(
        hqta_type = "hqta_transit_corr",
        route_type = "3"
    )
    
    return bus_hqtc2


# Before, have been focusing on shape_id
# Now, change it to hqta_segment_id
def find_intersection_hqta_segments(gdf, itp_id):
    keep_cols = ["calitp_itp_id", "hqta_segment_id", "geometry"]
    
    operator = gdf[gdf.calitp_itp_id == itp_id][keep_cols]
    not_operator = gdf[gdf.calitp_itp_id != itp_id][keep_cols]
    
    # Let's keep the pair of that intersection and store it
    # Need to rename not_operator columns so it's easier to distinguish
    not_operator = not_operator.rename(columns = {
        "calitp_itp_id": "intersect_calitp_itp_id",
        "hqta_segment_id": "intersect_hqta_segment_id"})
    
    
    # Do a spatial join first to see what rows should be included
    # Compile all of them, because clipping is computationally expensive,
    # so we want to do it on fewer rows
    s1 = dask_geopandas.sjoin(
        operator, 
        not_operator,
        predicate="intersects"
    )
    
    intersecting_segments = (s1[keep_cols + 
                             ["intersect_calitp_itp_id", "intersect_hqta_segment_id"]]
                             .drop_duplicates()
                             .reset_index(drop=True)
                            )
    
    return intersecting_segments


def compile_pairwise_intersections(corridors, ITP_ID_LIST):
    start = dt.datetime.now()

    keep_cols = ["calitp_itp_id", "hqta_segment_id"]

    # Having trouble initializing empty dask geodataframe
    # just subset so metadata is copied over
    intersecting_segments = corridors[corridors.calitp_itp_id==0][keep_cols]

    for itp_id in ITP_ID_LIST:
        operator_shape = find_intersection_hqta_segments(corridors, itp_id)

        intersecting_segments = (dd.multi.concat(
            [intersecting_segments, operator_shape], axis=0)
            .drop_duplicates()
            .reset_index(drop=True)
        )

    end = dt.datetime.now()
    print(f"execution time for compiling pairwise: {end-start}")
    
    intersecting_segments = intersecting_segments.astype({
        "intersect_calitp_itp_id": int,
        "intersect_hqta_segment_id": int
    })
    
    return intersecting_segments


# Get pairwise one into just unique df
def unique_intersecting_segments(df):
    segment_cols = ["calitp_itp_id", "hqta_segment_id"]
    
    part1 = df[segment_cols].drop_duplicates()
    part2 = (df[["intersect_calitp_itp_id", "intersect_hqta_segment_id"]]
             .drop_duplicates()
            )
    
    part2 = rename_cols(part2, with_intersect=False)
    
    unique = (dd.multi.concat([part1, part2], axis=0)
              .drop_duplicates()
              .reset_index(drop=True)
             )
    
    return unique


def subset_corridors(gdf, intersecting_shapes):
    segment_cols = ["calitp_itp_id", "hqta_segment_id"]
    
    shapes_needed = unique_intersecting_segments(intersecting_shapes)
    
    gdf2 = dd.merge(gdf, shapes_needed, 
                    on = segment_cols,
                    how = "inner"
                   )
    
    return gdf2


def rename_cols(df, with_intersect=False):
    if with_intersect is True:
        df = df.add_prefix('intersect_')
    elif with_intersect is False:
        df.columns = df.columns.str.replace('intersect_', '')
        
    return df




if __name__=="__main__":

    start = dt.datetime.now()

    corridors = prep_bus_corridors()   

    ITP_IDS = list(corridors.calitp_itp_id.unique())
    
    intersecting_shapes = compile_pairwise_intersections(corridors, ITP_IDS)
    
    keep_cols = ["calitp_itp_id", "hqta_segment_id", "geometry"]
    
    corridors2 = subset_corridors(corridors[keep_cols], intersecting_shapes)
    
    time1 = dt.datetime.now()
    print(f"subset corridors: {time1 - start}")
    
    # Save results locally temporarily
    pairwise = intersecting_shapes.compute()
    subset_corridors = corridors2.compute()

    time2 = dt.datetime.now()
    print(f"compute for pairwise/subset_corridors: {time2 - time1}")
    
    pairwise.to_parquet("./data/pairwise.parquet")
    subset_corridors.to_parquet("./data/subset_corridors.parquet")
    
    end = dt.datetime.now()
    print(f"execution time: {end-start}")
