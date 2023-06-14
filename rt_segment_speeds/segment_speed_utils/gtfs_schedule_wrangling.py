"""
All kinds of GTFS schedule table wrangling.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from segment_speed_utils import helpers

def exclude_scheduled_operators(
    trips: pd.DataFrame, 
    exclude_me: list = ["Amtrak Schedule"]
):
    """
    Exclude certain operators by name.
    Here, we always want to exclude Amtrak Schedule because
    it runs outside of CA.
    """
    return trips[~trips.name.isin(exclude_me)].reset_index(drop=True)


def merge_shapes_to_trips(
    shapes: dg.GeoDataFrame, 
    trips: dd.DataFrame,
    merge_cols: list = ["shape_array_key"]
) -> dg.GeoDataFrame:   
    """
    Merge shapes and trips tables.
    We usually start with trip_id (from RT vehicle positions or stop_times),
    and we need to get the `shape_array_key`.
    """
    if isinstance(shapes, dg.GeoDataFrame):
        trips_with_geom = dd.merge(
            shapes,
            trips,
            on = merge_cols,
            how = "inner",
        )
    else:
        if isinstance(trips, dd.DataFrame):
            trips = trips.compute()
        trips_with_geom = pd.merge(
            shapes,
            trips,
            on = merge_cols,
            how = "inner"
        )
        
    return trips_with_geom


def get_trips_with_geom(
    analysis_date,
    trip_cols: list = ["feed_key", "name", 
                        "trip_id", "shape_array_key"]
) -> dg.GeoDataFrame:
    """
    Merge trips with shapes.
    """
    shapes = helpers.import_scheduled_shapes(analysis_date)

    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = trip_cols,
        get_pandas = True
    )
    
    trips = exclude_scheduled_operators(
        trips, 
        exclude_me = ["Amtrak Schedule"]
    )

    trips_with_geom = merge_shapes_to_trips(
        shapes, trips)
    
    return trips_with_geom


def merge_shapes_to_stop_times(
    trips_with_shape_geom: dg.GeoDataFrame,
    stop_times: dd.DataFrame,
) -> dg.GeoDataFrame:
    """
    Merge stop_times with trips (with shape_geom) attached.
    """    
    st_with_shape = dd.merge(
        trips_with_shape_geom,
        stop_times, 
        on = ["feed_key", "trip_id"],
        how = "inner",
    )
    
    if isinstance(trips_with_shape_geom, (gpd.GeoDataFrame, dg.GeoDataFrame)):
        geometry_col = trips_with_shape_geom.geometry.name
        # Sometimes, geometry is lost...need to set it so it remains dg.GeoDataFrame
        st_with_shape = st_with_shape.set_geometry(geometry_col)
    
    return st_with_shape
    

def attach_stop_geometry(
    df: pd.DataFrame, 
    stops: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Merge df with stop's point geometry, using feed_key and stop_id.
    """
    if isinstance(df, pd.DataFrame):
        df2 = pd.merge(
            stops,
            df,
            on = ["feed_key", "stop_id"],
            how = "inner"
        )
    elif isinstance(df, [dd.DataFrame, dg.GeoDataFrame]):
        # We will lose geometry by putting stops on the right
        # but, in the case where we have another dg.GeoDataFrame on left,
        # we should explicitly set geometry column after
        df2 = dd.merge(
            df,
            stops,
            on = ["feed_key", "stop_id"],
            how = "inner"
        )
    
    return df2