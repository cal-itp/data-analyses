import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd

from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date,
                                              PROJECT_CRS)
from segment_speed_utils import wrangle_shapes


def sjoin_vp_to_roads(
    vp: dd.DataFrame, 
    roads: gpd.GeoDataFrame, 
    road_grouping_cols: list,
) -> dd.DataFrame:
    
    vp_gdf = wrangle_shapes.vp_as_gdf(vp)
    
    roads = roads.assign(
        geometry = roads.geometry.buffer(25)
    )
        
    results = gpd.sjoin(
        vp_gdf,
        roads,
        how = "inner",
        predicate = "within"
    )[["vp_idx", "trip_instance_key"] + 
      road_grouping_cols].drop_duplicates().sort_values("vp_idx")  
        
    return results
    
    
def make_wide(
    full_results: pd.DataFrame, 
    road_cols: list
) -> pd.DataFrame:
    results_wide = (full_results.groupby(["trip_instance_key"] + road_cols, 
                      observed=True, group_keys=False)
                    .agg({"vp_idx": lambda x: list(x)})
                    .reset_index()
                    .rename(columns = {"vp_idx": "vp_idx_arr"}) 
                   )
    
    # No point in keeping results where there are fewer than 2 vp for entire road
    results_wide = results_wide.assign(
        n_vp = results_wide.apply(lambda x: len(x.vp_idx_arr), axis=1)
    ).query('n_vp > 1').drop(columns = "n_vp").reset_index(drop=True)
    
    return results_wide


def import_data(
    analysis_date: str, 
    direction: str,
    segment_identifier_cols: list
) -> tuple[dd.DataFrame, gpd.GeoDataFrame]:
    
    opposite_direction = wrangle_shapes.OPPOSITE_DIRECTIONS[direction]
    
    vp = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns = ["vp_idx", "trip_instance_key", "x", "y"],
        filters = [[("vp_primary_direction", "==", direction)]]
    )
    
    vp = vp.repartition(npartitions=10)
    
    road_segments = gpd.read_parquet(
        f"{SEGMENT_GCS}road_segments_{analysis_date}",
        filters = [[
            ("mtfcc", "in", ["S1100", "S1200"]), 
            ("primary_direction", "!=", opposite_direction)]],
        columns =  segment_identifier_cols + ["geometry"]
    )
    
    return vp, road_segments


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    road_cols = ["linearid", "mtfcc"]
    segment_identifier_cols = road_cols + ["segment_sequence", 
                                           "primary_direction"]

    all_directions = wrangle_shapes.ALL_DIRECTIONS
    
    dfs = [
        import_data(
            analysis_date, d, segment_identifier_cols)
        for d in all_directions
    ]
    

    vp_cols_dtypes = dfs[0][0][["vp_idx", "trip_instance_key"]].dtypes.to_dict()
    road_cols_dtypes = dfs[0][1][road_cols].dtypes.to_dict()
    
    results = [
        vp_subset.map_partitions(
            sjoin_vp_to_roads,
            road_subset,
            road_cols,
            meta = {
                **vp_cols_dtypes,
                **road_cols_dtypes
            },
            align_dataframes = False
        ) for vp_subset, road_subset in dfs
    ]
    print("map partitions")
    
    full_results = dd.multi.concat(results, axis=0).reset_index(drop=True)
    full_results = full_results.repartition(npartitions=10).persist()
    
    time1 = datetime.datetime.now()
    print(f"map partitions and persist full results: {time1 - start}")
    
    full_results.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_{analysis_date}_seg",
        overwrite=True
    )
    
    full_results = dd.read_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_{analysis_date}_seg"
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
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")