"""
All kinds of GTFS schedule table wrangling.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from typing import Union

from segment_speed_utils import helpers

def exclude_scheduled_operators(
    trips: pd.DataFrame, 
    exclude_me: list = ["Amtrak Schedule", "*Flex"]
):
    """
    Exclude certain operators by name.
    Here, we always want to exclude Amtrak Schedule because
    it runs outside of CA.
    """
    substrings_to_exclude = [i for i in exclude_me if "*" in i]
    
    if len(substrings_to_exclude) > 0:
        substrings = [i.replace("*", "") for i in substrings_to_exclude]
        for i in substrings:
            trips = trips[~trips.name.str.contains(i)].reset_index(drop=True)
    
    return trips[~trips.name.isin(exclude_me)].reset_index(drop=True)


def get_trips_with_geom(
    analysis_date: str,
    trip_cols: list = ["feed_key", "name", 
                       "trip_id", "shape_array_key"],
    exclude_me: list = ["Amtrak Schedule", "*Flex"],
    crs: str = "EPSG:3310"
) -> gpd.GeoDataFrame:
    """
    Merge trips with shapes. 
    Also exclude Amtrak and Flex trips.
    """
    shapes = helpers.import_scheduled_shapes(
        analysis_date, 
        columns = ["shape_array_key", "geometry"],
        get_pandas = True,
        crs = crs
    )

    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = trip_cols,
        get_pandas = True
    )
    
    trips = exclude_scheduled_operators(
        trips, 
        exclude_me
    )

    trips_with_geom = pd.merge(
        shapes,
        trips,
        on = "shape_array_key",
        how = "inner"
    ).drop_duplicates().reset_index(drop=True)
    
    return trips_with_geom


def merge_shapes_to_stop_times(
    trips_with_shape_geom: Union[dg.GeoDataFrame, gpd.GeoDataFrame],
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
    ).drop_duplicates()
    
    if isinstance(trips_with_shape_geom, (gpd.GeoDataFrame, dg.GeoDataFrame)):
        geometry_col = trips_with_shape_geom.geometry.name
        # Sometimes, geometry is lost...need to set it so it remains dg.GeoDataFrame
        st_with_shape = st_with_shape.set_geometry(geometry_col)
        st_with_shape = st_with_shape.set_crs(trips_with_shape_geom.crs)
    
    return st_with_shape