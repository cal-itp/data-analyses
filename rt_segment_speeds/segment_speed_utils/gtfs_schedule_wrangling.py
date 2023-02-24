"""
All kinds of GTFS schedule table wrangling.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd


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
    trips: dd.DataFrame
) -> dg.GeoDataFrame:   
    """
    Merge shapes and trips tables.
    We usually start with trip_id (from RT vehicle positions or stop_times),
    and we need to get the `shape_array_key`.
    """
    trips_with_geom = dd.merge(
        shapes,
        trips,
        on = "shape_array_key",
        how = "inner",
    )
        
    return trips_with_geom



def merge_shapes_to_stop_times(
    stop_times: dd.DataFrame,
    trips_with_shape_geom: dg.GeoDataFrame
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
    
    # Sometimes, geometry is lost...need to set it so it remains dg.GeoDataFrame
    if "geometry" in st_with_shape.columns:
        st_with_shape = st_with_shape.set_geometry("geometry")
    
    return st_with_shape
    

def attach_stop_geometry(
    df: pd.DataFrame, 
    stops: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    
    if isinstance(df, pd.DataFrame):
        df = dd.from_pandas(df, npartitions=1)
    elif isinstance(df, gpd.GeoDataFrame):
        df = dg.from_geopandas(df, npartitions=1)
        
    df2 = dd.merge(
        df,
        stops,
        on = ["feed_key", "stop_id"],
        how = "inner"
    )
    
    return df2
    
    
    
def merge_speeds_to_segment_geom(
    speeds_df: dd.DataFrame,
    segments_df: gpd.GeoDataFrame,
) -> dg.GeoDataFrame:
    """
    Merge in stop segment geometry
    """
    stop_segments = gpd.read_parquet(
        f"{SEGMENT_GCS}stop_segments_{analysis_date}.parquet",
        columns = ["gtfs_dataset_key", 
                   "shape_array_key", "stop_sequence", 
                   "stop_name",
                   "geometry"]
    )

    stop_segments_with_speed = dd.merge(
        stop_segments,
        speeds_df,
        on = ["gtfs_dataset_key", "shape_array_key", "stop_sequence"],
        how = "inner",
    )
    
    return stop_segments_with_speed