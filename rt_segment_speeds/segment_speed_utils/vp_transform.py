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