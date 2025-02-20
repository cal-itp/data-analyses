import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from calitp_data_analysis.geography_utils import WGS84

ALL_DIRECTIONS = ["Northbound", "Southbound", "Eastbound", "Westbound"]

OPPOSITE_DIRECTIONS = {
    "Northbound": "Southbound",
    "Southbound": "Northbound",
    "Eastbound": "Westbound",
    "Westbound": "Eastbound",
    "Unknown": "",
}


def condense_point_geom_to_line(
    df: gpd.GeoDataFrame,
    group_cols: list,
    geom_col: str = "geometry",
    array_cols: list = [],
    sort_cols: list = []
) -> gpd.GeoDataFrame:
    """    
    To apply nearest neighbors, we need to create our equivalent
    line with coords, out of which we can select some nearest 
    neighbors in `gtfs_segments.geom_utils.nearest_points`.
    
    If we want to apply it to vp, we want to group all the
    vp that occurred for a trip into a list and 
    create that as a shapely.LineString object.
    """
    if len(sort_cols) == 0:
        sort_cols = group_cols
        
    valid_groups = (df.groupby(group_cols, group_keys=False)
                    .agg({geom_col: "count"})
                    .reset_index()
                    .query(f'{geom_col} > 1')
                   )[group_cols].drop_duplicates()
    
    '''
    # If we want to support multiple geometry columns...but how often will this happen?
    # Find which columns are geometry
    # Keep only groups that can be constructed as linestrings (have more than 1 coord)
    check_me = df[group_cols + array_cols]
    geo_cols = list(check_me.columns[check_me.dtypes == "geometry"])
    
    valid_groups = (df.groupby(group_cols)
                    .agg({
                        **{c: "count" for c in geo_cols}}
                    )
                    .reset_index()
                   )
    
    valid_groups["obs"] = valid_groups[geo_cols].sum(axis=1)
    valid_groups = valid_groups[
        valid_groups.obs >= (2 * len(geo_cols))
    ][group_cols].drop_duplicates()
    '''
    df2 = pd.merge(
        df,
        valid_groups,
        on = group_cols,
        how = "inner"
    ).sort_values(sort_cols).reset_index(drop=True)
    
    df3 = (
        df2
        .groupby(group_cols, group_keys=False, dropna=False)
        .agg({
            geom_col: lambda x: shapely.LineString(list(x)),
            **{c: lambda x: list(x) for c in array_cols},
        })
        .reset_index()
    )
    
    return df3