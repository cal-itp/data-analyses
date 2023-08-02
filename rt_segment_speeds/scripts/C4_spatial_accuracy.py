"""
Calculate a trip-level metric of spatial accuracy.

Newmark's GTFS RT spatial accuracy metric is simply
how many vehicle positions correctly join onto a buffered
shape.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd

from dask import delayed, compute

from segment_speed_utils.project_vars import (COMPILED_CACHED_VIEWS, SEGMENT_GCS,
                                              analysis_date, PROJECT_CRS
                                             )

def grab_shape_keys_in_vp(analysis_date: str) -> pd.DataFrame:
    """
    Subset raw vp and find unique trip_instance_keys.
    Create crosswalk to link trip_instance_key to shape_array_key.
    """
    vp_trips = (pd.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}.parquet",
        columns=["trip_instance_key"])
        .drop_duplicates()
        .trip_instance_key
        .tolist()
    )
    
    trips_with_shape = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet",
        columns=["trip_instance_key", "shape_array_key"],
        filters=[[("trip_instance_key", "in", vp_trips)]],
    )

    return trips_with_shape


def buffer_shapes(
    analysis_date: str,
    buffer_meters: int = 35,
    **kwargs
) -> gpd.GeoDataFrame:
    """
    Filter scheduled shapes down to the shapes that appear in vp.
    Buffer these.
    """
    shapes = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet",
        columns = ["shape_array_key", "geometry"],
        **kwargs
    ).to_crs(PROJECT_CRS)
    
    # to_crs takes awhile, so do a filtering on only shapes we need
    shapes = shapes.assign(
        geometry = shapes.geometry.buffer(buffer_meters)
    )
    
    return shapes


def merge_vp_with_shape(
    analysis_date: str,
    trips_with_shape: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Merge vp with crosswalk and buffered shapes.
    """ 
    subset_trips = trips_with_shape.trip_instance_key.unique().tolist()
    
    vp = dg.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}.parquet",
        columns=["trip_instance_key", "location_timestamp_local", 
                 "geometry"],
    ).to_crs(PROJECT_CRS)
    
    vp2 = dd.merge(
        vp,
        trips_with_shape,
        on = "trip_instance_key",
        how = "inner"
    )
    
    return vp2


def subset_vp(vp: dg.GeoDataFrame, one_shape: str) -> dg.GeoDataFrame:
    vp2 = vp[vp.shape_array_key==one_shape].reset_index(drop=True)
    return vp2


def total_vp_counts_by_trip(vp: dg.GeoDataFrame) -> dd.DataFrame:
    # Get a count of vp for each trip, whether or not those fall 
    # within buffered shape or not
    count_vp = (
        vp.groupby("trip_instance_key", 
                   observed=True, group_keys=False)
        .agg({"location_timestamp_local": "count"})
        .reset_index()
        .rename(columns={"location_timestamp_local": "total_vp"})
    )
    
    return count_vp
    
    
def vp_in_shape_by_trip(
    vp: dg.GeoDataFrame, 
    one_shape: gpd.GeoDataFrame,
) -> dd.DataFrame:
    """
    Find if vp point intersects with our buffered shape.
    Get counts by trip.
    """
    vp2 = dg.sjoin(
        vp,
        one_shape,
        how = "inner",
        predicate = "intersects"
    )[["trip_instance_key", "location_timestamp_local"]].drop_duplicates()
    
    count_in_shape = (
        vp2.groupby("trip_instance_key", 
                    observed=True, group_keys=False)
        .agg({"location_timestamp_local": "count"})
        .reset_index()
        .rename(columns={"location_timestamp_local": "vp_in_shape"})
    )
    
    return count_in_shape


if __name__=="__main__":    
    
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    print(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    trips_with_shape = grab_shape_keys_in_vp(analysis_date)
    shapes_in_vp = trips_with_shape.shape_array_key.unique().tolist()
    print(shapes_in_vp)
    
    shapes = delayed(buffer_shapes)(
        analysis_date, 
        buffer_meters = 35, 
        filters = [[("shape_array_key", "in", shapes_in_vp)]]
    )
    
    vp_with_shape = delayed(merge_vp_with_shape)(
        analysis_date, trips_with_shape)
    
    vp_dfs = [
        delayed(subset_vp)(vp_with_shape, shape)
        for shape in shapes_in_vp
    ]
    
    shape_dfs = [
        delayed(shapes[shapes.shape_array_key==s])
        for s in shapes_in_vp
    ]
    
    results = [
        delayed(vp_in_shape_by_trip)(one_vp_df, one_shape_df).persist() 
        for one_vp_df, one_shape_df in zip(vp_dfs, shape_dfs)
    ]
    
    results2 = [compute(i)[0] for i in results]
    
    vp_trip_in_shape_totals = dd.multi.concat(
        results2, axis=0).set_index("trip_instance_key")
    
    print(vp_trip_in_shape_totals.dtypes)

    time1 = datetime.datetime.now()
    print(f"get trip counts in shape: {time1 - start}")
    
    vp_trip_totals = total_vp_counts_by_trip(
        vp_with_shape).set_index("trip_instance_key")

    time2 = datetime.datetime.now()
    print(f"get trip total counts: {time2 - time1}")
    
    
    # Merge our total counts by trip with the vp that fall within shape
    results_df = dd.merge(
        vp_trip_totals,
        vp_trip_in_shape_totals,
        left_index = True, 
        right_index = True,
        how = "left"
    )
    
    results_df.to_parquet(
        f"{SEGMENT_GCS}trip_summary/vp_spatial_accuracy_{analysis_date}.parquet",
        #overwrite = True,
    )
    
    end = datetime.datetime.now()
    print(f"export: {end - time1}")
    print(f"execution time: {end - start}")
    
    #client.close()