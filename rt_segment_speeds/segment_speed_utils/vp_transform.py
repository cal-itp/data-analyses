import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import wrangle_shapes

def condense_point_geom_to_line(
    df: pd.DataFrame, 
    group_cols: list,
    geom_col: str = "geometry",
    other_cols: list = []
) -> gpd.GeoDataFrame:
    """
    To apply nearest neighbors, we need to create our equivalent
    line with coords, out of which we can select some nearest 
    neighbors in `gtfs_segments.geom_utils.nearest_points`.
    
    If we want to apply it to vp, we want to group all the
    vp that occurred for a trip into a list and 
    create that as a shapely.LineString object.
    """
    valid_groups = (df.groupby(group_cols, 
                              observed=True, group_keys=False)
                    .agg({geom_col: "count"})
                    .reset_index()
                    .query(f'{geom_col} > 1')
                   )[group_cols].drop_duplicates()
    
    df2 = pd.merge(
        df,
        valid_groups,
        on = group_cols,
        how = "inner"
    )
    
    df3 = (df2.groupby(group_cols, 
                      observed=True, group_keys=False)
           .agg({
               geom_col: lambda x: shapely.LineString(list(x)),
               **{k: lambda x: list(x) for k in other_cols}
           })
           .reset_index()
          )
    
    return df3


def stack_vpidx_by_trip(df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    return (df.groupby("trip_instance_key")
            .vp_idx.apply(np.array)
            .apply(np.concatenate)
            .reset_index()
           )

def stack_linecoords_by_trip(df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    df = df.assign(
        geometry_array = df.apply(
            lambda x: 
            np.array([shapely.Point(p) for p in x.geometry.coords]), 
            axis=1)
    )
    return (df.groupby("trip_instance_key")
            .geometry_array.apply(np.array)
            .apply(np.concatenate)
            .reset_index()
           )


def sort_by_vp_idx_order(
    vp_idx_array: np.ndarray, 
    geometry_array: np.ndarray
) -> tuple[np.ndarray]:    
    
    sort_order = np.argsort(vp_idx_array, axis=0)
    
    vp_sorted = np.take_along_axis(vp_idx_array, sort_order, axis=0)
    geom_sorted = np.take_along_axis(geometry_array, sort_order, axis=0)
    
    return vp_sorted, geom_sorted


def combine_valid_vp_for_direction(
    vp_condensed: gpd.GeoDataFrame, 
    direction: str
) -> gpd.GeoDataFrame:
    # This is the df we will merge the valid vp from 
    # 3 other directions against
    vp_one_direction = vp_condensed[
        vp_condensed.vp_primary_direction == direction][
        ["trip_instance_key", "vp_primary_direction"]
    ].drop_duplicates()
    
    vp_valid = vp_condensed[vp_condensed.vp_primary_direction != 
                  wrangle_shapes.OPPOSITE_DIRECTIONS[direction]]
    
    stacked_vp_idx = stack_vpidx_by_trip(vp_valid)
    stacked_coords = stack_linecoords_by_trip(vp_valid)
    
    vp_valid2 = pd.merge(
        stacked_vp_idx,
        stacked_coords,
        on = "trip_instance_key",
        how = "inner"
    )
    
    sorted_vp = []
    sorted_geom = []
    
    for row in vp_valid2.itertuples():
        
        vp_idx_array = getattr(row, "vp_idx")
        geometry_array = getattr(row, "geometry_array")
        
        vp_sorted, geom_sorted = sort_by_vp_idx_order(
            vp_idx_array, geometry_array
        )
        sorted_vp.append(vp_sorted)
        sorted_geom.append(geom_sorted)

    # Overwrite the vp_idx and geometry_array 
    # with the sorted versions of the array
    vp_valid2 = vp_valid2.assign(
        geometry_array = sorted_geom,
        vp_idx = sorted_vp
    )
    
    vp_valid2 = vp_valid2.assign(
        geometry = gpd.GeoSeries(
            vp_valid2.apply(lambda x: shapely.LineString(
                [p for p in x.geometry_array]), axis=1), 
            crs = WGS84)
    ).drop(columns = "geometry_array")
    
    # Merge results back onto original trip info for that direction
    df = pd.merge(
        vp_one_direction,
        vp_valid2,
        on = "trip_instance_key",
        how = "inner"
    ).reset_index(drop=True)
    
    gdf = gpd.GeoDataFrame(
        df, geometry = "geometry", crs = WGS84
    )
    
    return gdf
    
    