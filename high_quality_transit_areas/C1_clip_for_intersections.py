"""
Find intersections in corridors.

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
        f'{bus_corridors.TEST_GCS_FILE_PATH}intermediate/shape_dissolve.parquet')
    
    bus_hqtc2 = bus_hqtc[bus_hqtc.hq_transit_corr==True].reset_index(drop=True)
    bus_hqtc2 = bus_hqtc2.assign(
        hqta_type = "hqta_transit_corr",
        route_type = "3"
    )
    
    return bus_hqtc2


def prep_rail_ferry_brt_stops():
    hq_stops = dask_geopandas.read_parquet(
        f"{bus_corridors.TEST_GCS_FILE_PATH}rail_brt_ferry.parquet")
    
    hq_stops = hq_stops.assign(
        hqta_type = "major_transit_stop"
    )
    
    return hq_stops



def find_intersection_shape_ids(gdf, itp_id):
    keep_cols = ["calitp_itp_id", "shape_id", "geometry"]
    
    operator = gdf[gdf.calitp_itp_id == itp_id][keep_cols]
    not_operator = gdf[gdf.calitp_itp_id != itp_id][keep_cols]
    
    # Let's keep the pair of that intersection and store it
    # Need to rename not_operator columns so it's easier to distinguish
    not_operator = not_operator.rename(columns = {
        "calitp_itp_id": "intersect_calitp_itp_id",
        "shape_id": "intersect_shape_id"})
    
    
    # Do a spatial join first to see what shape_ids should be included
    # Compile all of them, then do gpd.clip, because that is computationally expensive,
    # so we want to do it on fewer rows
    s1 = dask_geopandas.sjoin(
        operator, 
        not_operator,
        predicate="intersects"
    )
    
    intersecting_shapes = (s1[keep_cols + 
                             ["intersect_calitp_itp_id", "intersect_shape_id"]]
                           .drop_duplicates()
                           .reset_index(drop=True)
                          )
    
    return intersecting_shapes


def compile_pairwise_intersections(corridors, ITP_ID_LIST):
    start = dt.datetime.now()

    #https://stackoverflow.com/questions/41087887/is-there-a-way-to-generate-the-dtypes-as-a-dictionary-in-pandas
    keep_cols = ["calitp_itp_id", "shape_id"]

    corridors_meta = corridors[keep_cols].dtypes.apply(lambda x: x.name).to_dict()

    # Having trouble initializing empty dask geodataframe
    # just subset so metadata is copied over
    intersecting_shapes = corridors[corridors.calitp_itp_id==0][keep_cols]

    for itp_id in ITP_ID_LIST:
        operator_shape = find_intersection_shape_ids(corridors, itp_id)

        intersecting_shapes = dd.multi.concat([intersecting_shapes, operator_shape], 
                                        axis=0).drop_duplicates().reset_index(drop=True)

    end = dt.datetime.now()
    print(f"execution time: {end-start}")
    
    return intersecting_shapes


# Get pairwise one into just unique df
def unique_intersecting_shapes(df):
    shape_cols = ["calitp_itp_id", "shape_id"]
    
    part1 = df[shape_cols].drop_duplicates()
    part2 = (df[["intersect_calitp_itp_id", "intersect_shape_id"]]
             .drop_duplicates()
            )
    
    part2.columns = part2.columns.str.replace('intersect_', '')
    
    unique_shape_ids = (dd.multi.concat([part1, part2], axis=0)
                        .drop_duplicates()
                        .reset_index(drop=True)
                       )
    
    return unique_shape_ids


def subset_corridors(gdf, intersecting_shapes):
    shape_cols = ["calitp_itp_id", "shape_id"]
    
    shapes_needed = unique_intersecting_shapes(intersecting_shapes)
    
    gdf2 = dd.merge(gdf, shapes_needed, 
                    on = ["calitp_itp_id", "shape_id"],
                    how = "inner"
                   )
    
    return gdf2


def rename_cols(df, with_intersect=False):
    if with_intersect is True:
        df = df.add_prefix('intersect_')
    elif with_intersect is False:
        df.columns = df.columns.str.replace('intersect_', '')
        
    return df


def clip_by_itp_id(corridors_df, intersecting_pairs, itp_id):
    start = dt.datetime.now()
    
    shape_cols = ["calitp_itp_id", "shape_id", "geometry"]
    
    operator = (corridors_df[corridors_df.calitp_itp_id == itp_id]
                [shape_cols]
               )
    
    # Get the pairwise comparison of which operator-shape_ids 
    # against which this given operator intersects 
    operator_pair_intersect = intersecting_pairs[
        intersecting_pairs.calitp_itp_id == itp_id][
        ["intersect_calitp_itp_id", "intersect_shape_id"]]
    
    operator_pair_intersect = rename_cols(operator_pair_intersect, with_intersect=False)
    
    
    # Now, merge in the operator-shape_ids that intersect with given operator,
    # so that there's fewer rows to do the clipping on
    not_operator = dd.merge(corridors_df[shape_cols], 
                            operator_pair_intersect,
                            on = ["calitp_itp_id", "shape_id"],
                            how = "inner"
                           )
    
    time1 = dt.datetime.now()
    print(f"prepare intersection dfs for {itp_id}: {time1-start}")
    
    not_operator_df = not_operator.compute()
    # This step takes 3.5 min for each operator, no matter the operator size
    # Need to speed up this step, think about it differently
    # not_operator.dissolve().reset_index(drop=True).compute() also takes 3.5 min
    
    time2 = dt.datetime.now()
    print(f"compute to make gdf for {itp_id}: {time2-time1}")
    
    intersection = dask_geopandas.clip(operator, 
                                       not_operator_df, keep_geom_type=True)
    
    time3 = dt.datetime.now()
    print(f"clipping for {itp_id}: {time3-time2}")
    
    return intersection


if __name__=="__main__":

    corridors = prep_bus_corridors()   

    ITP_IDS = list(corridors.calitp_itp_id.unique())
    
    intersecting_shapes = compile_pairwise_intersections(corridors, ITP_IDS)
    
    corridors2 = subset_corridors(corridors, intersecting_shapes)
    
    
    start = dt.datetime.now()
    
    clipped = corridors2[["calitp_itp_id", "shape_id", "geometry"]].head(0)

    for itp_id in ITP_IDS:
        intersection = clip_by_itp_id(corridors2, intersecting_shapes, itp_id)
        
        # compute takes a consistent amount of time across operators, no matter size
        # skip the compute if there's no need
        sum_clipped_area = intersection.geometry.area.sum()
        
        if sum_clipped_area > 0:
            intersection2 = intersection.compute()
            intersection2.to_parquet(f"./data/intersections/clipped_{itp_id}.parquet")
            #clipped = dd.multi.concat([clipped, intersection], axis=0)
        else:
            pass
    #clipped2 = clipped.compute()
    #clipped2.to_parquet("./all_clipped.parquet")
    
    end = dt.datetime.now()
    print(f"execution time for all clipping: {end-start}")
