import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date,
                                              PROJECT_CRS)
from segment_speed_utils import wrangle_shapes


def sjoin_vp_to_roads(
    vp: dd.DataFrame, 
    roads: gpd.GeoDataFrame, 
    direction: str
) -> dd.DataFrame:
    
    opposite_direction = wrangle_shapes.OPPOSITE_DIRECTIONS[direction]
    
    vp2 = vp[vp.vp_primary_direction == direction]
    
    vp_gdf = wrangle_shapes.vp_as_gdf(vp2)
    
    keep_road_cols = ["linearid", "mtfcc", "primary_direction"]

    roads2 = roads[
        roads.primary_direction != opposite_direction
    ].drop_duplicates(subset=keep_road_cols).reset_index(drop=True)
    
    roads2 = roads2.assign(
        geometry = roads2.geometry.buffer(20)
    )
    
    results = gpd.sjoin(
        vp_gdf,
        roads2,
        how = "inner",
        predicate = "intersects"
    )[["vp_idx", "trip_instance_key"] + keep_road_cols
     ].drop_duplicates().sort_values("vp_idx")  
        
    return results
    
    
def make_wide(full_results, road_cols):
    results_wide = (full_results.groupby(["trip_instance_key"] + road_cols, 
                      observed=True, group_keys=False)
                    .agg({"vp_idx": lambda x: list(x)})
                    .reset_index()
                    .rename(columns = {"vp_idx": "vp_idx_arr"}) 
                   )
    
    return results_wide


if __name__ == "__main__":
    
    road_cols = ["linearid", "mtfcc", "primary_direction"]
    segment_identifier_cols = road_cols + ["segment_sequence"]

    road_segments = gpd.read_parquet(
        f"{SEGMENT_GCS}road_segments_{analysis_date}",
        filters = [[("mtfcc", "in", ["S1100", "S1200"])]],
        columns = segment_identifier_cols + ["geometry"]
    )

    vp = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns = ["vp_idx", "trip_instance_key", "x", "y", 
                   "vp_primary_direction"]
    )
    
    vp_cols_dtypes = vp[["vp_idx", "trip_instance_key"]].dtypes.to_dict()
    road_cols_dtypes = road_segments[road_cols].dtypes.to_dict()
    
    all_directions = wrangle_shapes.ALL_DIRECTIONS + ["Unknown"]
    
    results = [
        vp.map_partitions(
            sjoin_vp_to_roads,
            road_segments,
            direction,
            meta = {
                **vp_cols_dtypes,
                **road_cols_dtypes
            },
            align_dataframes = False
        ) for direction in all_directions
    ]
    
    full_results = dd.multi.concat(results, axis=0).reset_index(drop=True)
    full_results = full_results.repartition(npartitions=10).persist()
    full_results.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_{analysis_date}",
        overwrite=True
    )
    
    results_wide = full_results.map_partitions(
        make_wide,
        road_cols,
        meta = {
            "trip_instance_key": "object",
            **road_cols_dtypes,
            "vp_idx_arr": "object",
        },
        align_dataframes = False
    ).compute()
        
    results_wide.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_wide_{analysis_date}.parquet",
    )
    
