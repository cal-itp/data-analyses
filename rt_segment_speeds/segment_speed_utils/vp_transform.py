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


def sort_by_vp_idx_order(
    vp_idx_array: np.ndarray, 
    geometry_array: np.ndarray,
    timestamp_array: np.ndarray,
) -> tuple[np.ndarray]:    
    
    sort_order = np.argsort(vp_idx_array, axis=0)
    
    vp_sorted = np.take_along_axis(vp_idx_array, sort_order, axis=0)
    geom_sorted = np.take_along_axis(geometry_array, sort_order, axis=0)
    timestamp_sorted = np.take_along_axis(timestamp_array, sort_order, axis=0)
    
    return vp_sorted, geom_sorted, timestamp_sorted


def combine_valid_vp_for_direction(
    vp_condensed: gpd.GeoDataFrame, 
    direction: str
) -> gpd.GeoDataFrame:
    
    opposite_direction = wrangle_shapes.OPPOSITE_DIRECTIONS[direction]
    
    coords_series = []
    vp_idx_series = []
    timestamp_series = []
    
    for row in vp_condensed.itertuples():
        vp_dir_arr = np.asarray(getattr(row, "vp_primary_direction"))

        # These are the valid index values where opposite direction 
        # is excluded
        valid_indices = (vp_dir_arr != opposite_direction).nonzero()
        
        # Subset all the other arrays to these indices
        vp_idx_arr = np.asarray(getattr(row, "vp_idx"))
        coords_arr = np.array(getattr(row, "geometry").coords)

        timestamp_arr = np.asarray(
            getattr(row, "location_timestamp_local"))
        
        vp_linestring = coords_arr[valid_indices]

        if len(vp_linestring) > 1:
            valid_vp_line = shapely.LineString([shapely.Point(p) 
                                                for p in vp_linestring])
        elif len(vp_linestring) == 1:
            valid_vp_line = shapely.Point([p for p in vp_linestring])
        else:
            valid_vp_line = shapely.LineString()
        
        coords_series.append(valid_vp_line)
        vp_idx_series.append(vp_idx_arr[valid_indices])
        timestamp_series.append(timestamp_arr[valid_indices])
    
    
    vp_condensed = vp_condensed.assign(
        vp_primary_direction = direction,
        geometry = coords_series,
        vp_idx = vp_idx_series,
        location_timestamp_local = timestamp_series,
    )[["trip_instance_key", "vp_primary_direction", 
       "geometry", "vp_idx", "location_timestamp_local"]].reset_index(drop=True)
    
    gdf = gpd.GeoDataFrame(
        vp_condensed, 
        geometry = "geometry", 
        crs = WGS84
    )
    
    del coords_series, vp_idx_series, timestamp_series
    del vp_condensed
    
    return gdf