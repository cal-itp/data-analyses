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
    
    # Need to do similar thing as sjoin with shapes
    # because vp can only join to certain road segments
    # not all road segments that shape doesn't travel on
    # https://github.com/cal-itp/data-analyses/blob/64afd3b9ed1e239a0ca4da486e2fe1e857b17dde/rt_segment_speeds/scripts/A1_sjoin_vp_segments.py
    
    vp_gdf = wrangle_shapes.vp_as_gdf(vp)        
                
    results = gpd.sjoin(
        vp_gdf,
        roads,
        how = "inner",
        predicate = "within"
    ).query(
        "shape_array_key_left == shape_array_key_right"
    )[["vp_idx", "trip_instance_key"] + 
      road_grouping_cols].drop_duplicates().sort_values("vp_idx")  
        
    return results
    
    
def make_wide(
    full_results: pd.DataFrame, 
    road_cols: list
) -> pd.DataFrame:
    results_wide = (full_results.groupby(["trip_instance_key"] + road_cols, 
                      observed=True, group_keys=False)
                    .agg({
                        "vp_idx": lambda x: list(x),
                        "segment_sequence": "nunique"
                        
                    })
                    .reset_index()
                    .rename(columns = {"vp_idx": "vp_idx_arr"}) 
                   )
    
    # No point in keeping results where there are fewer than 2 vp for entire road
    results_wide = results_wide.assign(
        n_vp = results_wide.apply(lambda x: len(x.vp_idx_arr), axis=1)
    ).query(
        'n_vp > 1 & segment_sequence > 1'
    ).drop(
        columns = ["n_vp", "segment_sequence"]
    ).reset_index(drop=True)
    
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
    ).repartition(npartitions=25)
    
    return vp2


def import_roads(
    analysis_date: str, 
    road_grouping_cols: list,
) -> dg.GeoDataFrame:
        
    road_segments = dg.read_parquet(
        f"{SEGMENT_GCS}road_segments_{analysis_date}",
        columns = road_grouping_cols + ["primary_direction", "geometry"]
    )
    
    road_segments = road_segments.assign(
        geometry = road_segments.geometry.buffer(25)
    )
    
    shape_road_crosswalk = pd.read_parquet(
        f"{SEGMENT_GCS}shape_road_crosswalk_{analysis_date}.parquet",
    )
    
    road_segments2 = dd.merge(
        road_segments,
        shape_road_crosswalk,
        on = road_grouping_cols,
        how = "inner"
    ).repartition(npartitions=25)
              
    return road_segments2



if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    road_cols = ["linearid", "mtfcc"]
    segment_identifier_cols = road_cols + ["segment_sequence"]

    all_directions = wrangle_shapes.ALL_DIRECTIONS + ["Unknown"]
    
    vp = import_vp(analysis_date).persist()
    roads = import_roads(analysis_date, segment_identifier_cols).persist()
                
    vp_dfs = [
        vp[vp.vp_primary_direction == d]
        for d in all_directions
    ]
    
    road_dfs = [
        roads[roads.primary_direction != 
              wrangle_shapes.OPPOSITE_DIRECTIONS[d]
             ] for d in all_directions
    ]
    
    
    vp_cols_dtypes = vp[["vp_idx", "trip_instance_key"]].dtypes.to_dict()
    road_cols_dtypes = roads[segment_identifier_cols].dtypes.to_dict()
    
    results = [
        vp_subset.map_partitions(
            sjoin_vp_to_roads,
            road_subset,
            segment_identifier_cols,
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
    
    road_cols_dtypes2 = roads[road_cols].dtypes.to_dict()
    
    results_wide = full_results.map_partitions(
        make_wide,
        road_cols,
        meta = {
            "trip_instance_key": "object",
            **road_cols_dtypes2,
            "vp_idx_arr": "object",
        },
        align_dataframes = False
    ).compute()
        
    results_wide.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_wide_{analysis_date}.parquet",
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")