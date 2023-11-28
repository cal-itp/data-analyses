"""
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd

from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, 
                                              CONFIG_PATH, PROJECT_CRS)


def filter_long_sjoin_results(
    sjoin_long: dd.DataFrame,
    sjoin_wide: pd.DataFrame,
) -> dd.DataFrame:
    """
    Long results contain all sjoins.
    Wide is filtered down to road linearids that have at least
    2 vp_idx for that trip_instance_key on the road.
    """
    keep_cols = sjoin_long.columns.tolist()
    
    df = dd.merge(
        sjoin_long,
        sjoin_wide,
        on = ["trip_instance_key", "linearid", "mtfcc"],
        how = "inner"
    )[keep_cols].drop_duplicates().reset_index(drop=True)
    
    return df


def merge_vp_to_crosswalk(
    analysis_date: str, 
) -> dd.DataFrame:
    """
    """
    sjoin_long = dd.read_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_{analysis_date}",
    )

    sjoin_wide = pd.read_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_wide_{analysis_date}.parquet",
    )
    
    # these are the vps joined to roads that we will narrow down
    # for interpolation
    sjoin_vp_roads = filter_long_sjoin_results(sjoin_long, sjoin_wide)
    
    # Pull the vp info for ones that join to road segments
    vp_usable = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns = ["trip_instance_key", "vp_idx", "x", "y"],
    )
    
    vp_with_roads = dd.merge(
        vp_usable,
        sjoin_vp_roads,
        on = ["trip_instance_key", "vp_idx"],
        how = "inner"
    )
    
    return vp_with_roads


def expand_road_segments(
    roads: pd.DataFrame, 
    cols_to_unpack: list
) -> pd.DataFrame:
    """
    Convert roads from wide to long.
    This set up makes roads segment endpoints look like bus stops.
    Once we use roads full line geometry to project, we can 
    drop it and get it in a long format.
    """
    roads_long = (roads.drop(columns = "geometry")
                  .explode(cols_to_unpack)
                  .reset_index(drop=True)
                 )
    
    return roads_long


def make_wide(vp_with_projected: pd.DataFrame):
    
    vp_projected_wide = (vp_with_projected
                     .groupby(["trip_instance_key"] + road_id_cols)
                     .agg({
                         "vp_idx": lambda x: list(x),
                         "shape_meters": lambda x: list(x)})
                     .reset_index()
                     .rename(columns = {
                         "vp_idx": "vp_idx_arr",
                         "shape_meters": "shape_meters_arr"
                     })
                    )
    
    #https://stackoverflow.com/questions/42099024/pandas-pivot-table-rename-columns
    narrowed_down_roads = (vp_with_projected
                       .pivot_table(index = ["trip_instance_key"] + road_id_cols,
                                    aggfunc = {"shape_meters": ["min", "max"]})
                       .pipe(lambda x: 
                             x.set_axis(map('_'.join, x), axis=1) 
                             # if all column names are strings
                             # ('_'.join(map(str, c)) for c in x)
                             # if some column names are not strings
                            )     
                       .reset_index()
                    )
    
    vp_projected_wide2 = pd.merge(
        vp_projected_wide,
        narrowed_down_roads,
        on = ["trip_instance_key"] + road_id_cols,
        how = "inner"
    )
    
    return vp_projected_wide2


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date
    
    start = datetime.datetime.now()
    
    road_id_cols = ["linearid", "mtfcc"]
    segment_identifier_cols = road_id_cols + ["segment_sequence"]
    
    vp = merge_vp_to_crosswalk(analysis_date)
    vp = vp.repartition(npartitions=150)
    
    subset_roads = vp.linearid.unique().compute().tolist()
    
    roads = gpd.read_parquet(
        f"{SEGMENT_GCS}segments_staging/"
        f"roads_with_cutpoints_wide_{analysis_date}.parquet",
        filters = [[("linearid", "in", subset_roads)]]
    )
    
    road_dtypes = vp[road_id_cols].dtypes.to_dict()

    vp_projected = vp.map_partitions(
        wrangle_shapes.project_vp_onto_segment_geometry,
        roads,
        grouping_cols = road_id_cols,
        meta = {
            "vp_idx": "int",
            **road_dtypes,
            "shape_meters": "float",
        },
        align_dataframes = False
    ).persist()
    
    
    vp2 = vp[["vp_idx", "trip_instance_key"]]

    vp_with_projected = dd.merge(
        vp2,
        vp_projected,
        on = "vp_idx",
        how = "inner"
    ).compute()
    
    vp_with_projected.to_parquet(
        f"{SEGMENT_GCS}projection/vp_projected_roads_{analysis_date}.parquet"
    )
    
    vp_projected_wide2 = make_wide(vp_with_projected)
    
    road_cutpoints = expand_road_segments(
        roads, 
        ["road_meters_arr", "segment_sequence_arr", "road_direction_arr"]
    ).rename(columns = {
        "road_meters_arr": "road_meters",
        "segment_sequence_arr": "segment_sequence",
        "road_direction_arr": "primary_direction"
    })

    # Now merge road segments with each destination acting as the road's stop
    # and merge on arrays of projected vp against that road
    gdf = pd.merge(
        road_cutpoints,
        vp_projected_wide2,
        on = road_id_cols,
        how = "inner"
    ).query(
        'road_meters >= shape_meters_min & road_meters <= shape_meters_max'
    ).drop(
        columns = ["shape_meters_min", "shape_meters_max"]
    )
    
    nearest_vp_idx = []
    subseq_vp_idx = []

    for row in gdf.itertuples():

        this_stop_meters = getattr(row, "road_meters")
        valid_shape_meters_array = getattr(row, "shape_meters_arr")
        valid_vp_idx_array = np.asarray(getattr(row, "vp_idx_arr"))

        if (
            (this_stop_meters >= min(valid_shape_meters_array)) and 
            (this_stop_meters <= max(valid_shape_meters_array))
       ):
        
            idx = np.searchsorted(
                valid_shape_meters_array,
                this_stop_meters,
                side="right" 
                # want our stop_meters value to be < vp_shape_meters,
                # side = "left" would be stop_meters <= vp_shape_meters
            )

            # For the next value, if there's nothing to index into, 
            # just set it to the same position
            # if we set subseq_value = getattr(row, )[idx], 
            # we might not get a consecutive vp
            nearest_value = valid_vp_idx_array[idx-1]
            subseq_value = nearest_value + 1

        else:
            nearest_value = np.nan
            subseq_value = np.nan
            
        nearest_vp_idx.append(nearest_value)
        subseq_vp_idx.append(subseq_value)
        
    result = gdf[segment_identifier_cols + [
        "primary_direction", "road_meters", 
        "trip_instance_key"]]

    # Now assign the nearest vp for each trip that's nearest to
    # a given stop
    # Need to find the one after the stop later
    result = result.assign(
        nearest_vp_idx = nearest_vp_idx,
        subseq_vp_idx = subseq_vp_idx,
    )
    
    result.to_parquet(
        f"{SEGMENT_GCS}nearest_vp_roads_{analysis_date}.parquet"
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")