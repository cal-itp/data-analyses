"""
Do clipping to find where bus corridors intersect.

This takes 6.5 min to run.

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd
import glob
import os
import pandas as pd

import B1_bus_corridors as bus_corridors
import C1_prep_for_clipping as prep_clip
from utilities import catalog_filepath

segment_cols = ["calitp_itp_id", "hqta_segment_id"]

# Input files
PAIRWISE_FILE = catalog_filepath("pairwise_intersections")
SUBSET_CORRIDORS = catalog_filepath("subset_corridors")


def clip_by_itp_id(corridors_df, intersecting_pairs, itp_id):
    start = dt.datetime.now()
    
    operator = (corridors_df[corridors_df.calitp_itp_id == itp_id]
                [segment_cols + ["geometry"]]
               )
    
    # Bring in the table for which pairs of hqta_segment_id this 
    # operator intersects with
    operator_pair_intersect = (intersecting_pairs
                               [intersecting_pairs.calitp_itp_id == itp_id]
                               [["intersect_calitp_itp_id", "intersect_hqta_segment_id"]]
                               .drop_duplicates()
                               .reset_index(drop=True)
                              )
    
    operator_pair_intersect = prep_clip.rename_cols(
        operator_pair_intersect, with_intersect=False)
    
    
    # Now, merge in the operator-hqta_segment that intersect with given operator,
    # so that there's fewer rows to do the clipping on
    not_operator = dd.merge(corridors_df[segment_cols + ["geometry"]], 
                            operator_pair_intersect,
                            on = segment_cols,
                            how = "inner"
                           )
    
    time1 = dt.datetime.now()
    print(f"prepare intersection dfs for {itp_id}: {time1-start}")
    
    not_operator_df = not_operator.compute()
    
    time2 = dt.datetime.now()
    print(f"compute to make gdf for {itp_id}: {time2-time1}")
    
    intersection = dask_geopandas.clip(operator, 
                                       not_operator_df, keep_geom_type=True)
    
    time3 = dt.datetime.now()
    print(f"clipping for {itp_id}: {time3-time2}")
    
    return intersection


def delete_local_clipped_files():
    temp_operator_files = [f for f in glob.glob("./data/intersections/clipped_*.parquet")]
    
    for f in temp_operator_files:
        os.remove(f)
    

if __name__ == "__main__":
    start = dt.datetime.now()
    
    intersecting_shapes = dd.read_parquet(PAIRWISE_FILE)
    
    corridors = dask_geopandas.read_parquet(SUBSET_CORRIDORS)
    
    # Presumably, this list of ITP_IDs is pared down 
    # because only ones with sjoin are included
    VALID_ITP_IDS = list(corridors.calitp_itp_id.unique())
    
    time1 = dt.datetime.now()
    print(f"read in data, assemble valid ITP_IDS: {time1 - start}")
    
    clipped = corridors.head(0)

    for itp_id in VALID_ITP_IDS:
        intersection = clip_by_itp_id(corridors, intersecting_shapes, itp_id)
                
        if len(intersection) > 0:
            intersection2 = intersection.compute()
            intersection2.to_parquet(f"./data/intersections/clipped_{itp_id}.parquet")
            
            clipped = dd.multi.concat([clipped, intersection], axis=0)
        else:
            continue
    
    
    clipped2 = (clipped.compute()
                .sort_values(segment_cols, ascending=[True, True])
                .reset_index(drop=True)
               )
    
    time2 = dt.datetime.now()
    print(f"compute for full clipped df: {time2 - time1}")
    
    clipped2.to_parquet("./data/all_clipped.parquet")
    
    # Delete the temporary clipped files for each operator
    delete_local_clipped_files()
    
    end = dt.datetime.now()