"""
Separate out the shapes that are ok to cut with our
original shapely.project from the ones that need our
own super_project. 

While super_project fixes some of the issues seen, it's 
still imperfect, so if there's a more clean cut way of 
cutting stop segments, we still prefer that.

Only for the shapes that actually are more complicated, do we 
want to pass through super_project, because the results from shapely.project
would be wrong.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import numpy as np
import pandas as pd

from typing import Union

def tag_shapes_with_stops_visited_twice(
    stop_times: Union[dd.DataFrame, dg.GeoDataFrame]
) -> np.ndarray:
    """
    Aggregate stop times by shape_array_key.
    For each stop_id, count how many stop_sequence values there are. 
    More than 1 means that the same stop_id is revisited on the same trip.
    
    Ex: a stop in a plaza that acts as origin and destination.
    """
    stop_visits = (stop_times.groupby(
                    ["shape_array_key", "stop_id"])
                  .agg({"stop_sequence": "count"}) 
                   #nunique doesn't work in dask
                  .reset_index()
                 )
    
    
    # If any of the trips within that shape revisits a stop, keep that shape
    loopy_shapes = (stop_visits[stop_visits.stop_sequence > 1]
                   .shape_array_key
                   .unique()
                   .compute()
                 )
    
    return loopy_shapes


def tag_shapes_with_inlining(
    stop_times: Union[dd.DataFrame, dg.GeoDataFrame]
) -> np.ndarray:
    """
    Rough estimate of inlining present in shapes. 
    Based on stops projected onto the shape_geometry, tag any 
    shapes where the stops are not monotonically increasing.
    When stops are converted from coordinates to distances (shapely.project), 
    it doesn't project neatly monotonically in the section of inlining, 
    because a stop can be projected onto several coord options on the shape.
    Any weirdness of values jumping around means we want to cut it with 
    super_project.
    """
    # Keep relevant columns, which is only the projected stop geometry
    # saved as shape_meters
    stop_times2 = stop_times[["shape_array_key", 
                              "stop_sequence", "shape_meters"]].compute()
    
    # Once we order it by stop sequence, save out shape_meters into a list
    # and make the gdf wide
    stop_times_wide = (stop_times2
                       .sort_values(["shape_array_key", "stop_sequence"])
                       .groupby("shape_array_key")
                       .agg({"shape_meters": lambda x: list(x)})
                       .reset_index()
                      )
    
    # Once it's wide, we can check whether the array in each row is 
    # monotonically increasing. If it's not, it's because the stop's projection 
    # as shape_meters is jumping wildly, which could indicate there's inlining present
    # first take the difference from prior value in the array
    # if it's monotonically increasing, the difference is always positive. any negative 
    # values indicates the value is fluctating.
    is_monotonic = [
        np.all(np.diff(shape_meters_arr) > 0) 
        for shape_meters_arr in stop_times_wide.shape_meters
    ]
    
    # About 1/6 of the 6,000 shapes gets tagged as being False
    stop_times_wide = stop_times_wide.assign(
        is_monotonic = is_monotonic
    )
    
    inlining_shapes = stop_times_wide[stop_times_wide.is_monotonic == False
                                     ].shape_array_key.unique()

    return inlining_shapes

