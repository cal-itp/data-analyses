import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd

from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date,
                                              PROJECT_CRS)
from segment_speed_utils import helpers, wrangle_shapes


def sjoin_vp_to_roads(
    vp: dd.DataFrame, 
    roads: gpd.GeoDataFrame, 
    road_grouping_cols: list,
) -> dd.DataFrame:
    
    vp_gdf = wrangle_shapes.vp_as_gdf(vp)
    
    roads2 = roads.dissolve(road_grouping_cols).reset_index()
    
    roads2 = roads2.assign(
        geometry = roads2.geometry.buffer(25)
    )
        
    results = gpd.sjoin(
        vp_gdf,
        roads2,
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


def import_vp(analysis_date: str):
    vp = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns = ["vp_idx", "trip_instance_key", "x", "y", 
                   "vp_primary_direction"],
    )
    
    rt_trips = vp.trip_instance_key.unique().compute().tolist()
    
    trip_to_shape = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key"],
        filters = [[("trip_instance_key", "in", rt_trips)]],
        get_pandas = True
    )
    
    vp2 = dd.merge(
        vp,
        trip_to_shape,
        on = "trip_instance_key",
        how = "inner"
    )
    
    return vp2


def import_roads(
    analysis_date: str, 
    vp: dd.DataFrame,
    road_grouping_cols: list,
) -> gpd.GeoDataFrame:
    
    keep_shapes = vp.shape_array_key.unique().compute().tolist()
    
    # Import shapes to roads crosswalk
    keep_roads = pd.read_parquet(
        f"{SEGMENT_GCS}shape_road_crosswalk_{analysis_date}.parquet",
        filters = [[("shape_array_key", "in", keep_shapes)]]
    ).linearid.unique().tolist()
        
    road_segments = dg.read_parquet(
        f"{SEGMENT_GCS}road_segments_{analysis_date}",
        filters = [[
            ("linearid", "in", keep_roads),
            #("mtfcc", "in", ["S1100", "S1200"]), 
        ]],
        columns =  road_grouping_cols + ["primary_direction", 
                                         "geometry"]
    )
              
    return road_segments


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    road_cols = ["linearid", "mtfcc"]

    all_directions = wrangle_shapes.ALL_DIRECTIONS + ["Unknown"]

    vp = import_vp(analysis_date).persist()
    roads = import_roads(analysis_date, vp, road_cols).persist()
                
    vp_dfs = [
        vp[vp.vp_primary_direction == d].repartition(npartitions=20)
        for d in all_directions
    ]
    
    road_dfs = [
        roads[roads.primary_direction != 
              wrangle_shapes.OPPOSITE_DIRECTIONS[d]
             ].repartition(npartitions=40) 
        for d in all_directions
    ]
    
    vp_cols_dtypes = vp[["vp_idx", "trip_instance_key"]].dtypes.to_dict()
    road_cols_dtypes = roads[road_cols].dtypes.to_dict()
    
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
        ) for vp_subset, road_subset in zip(vp_dfs, road_dfs)
    ]
    print("map partitions")
    
    full_results = dd.multi.concat(results, axis=0).reset_index(drop=True)
    full_results = full_results.repartition(npartitions=3).persist()
    
    time1 = datetime.datetime.now()
    print(f"map partitions and persist full results: {time1 - start}")
    
    full_results.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_{analysis_date}",
        overwrite=True
    )
    
    full_results = dd.read_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_{analysis_date}"
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