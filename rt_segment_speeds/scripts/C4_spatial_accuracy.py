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
import numpy as np
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
    
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}vp_{analysis_date}.parquet",
        columns=["trip_instance_key", "location_timestamp_local", 
                 "geometry"],
    ).to_crs(PROJECT_CRS)
    
    vp2 = pd.merge(
        vp,
        trips_with_shape,
        on = "trip_instance_key",
        how = "inner"
    )
    
    return vp2


def subset_vp(vp: dg.GeoDataFrame, one_shape: str) -> dg.GeoDataFrame:
    vp2 = vp[vp.shape_array_key==one_shape].reset_index(drop=True)
    return vp2


def total_vp_counts_by_trip(vp: dg.GeoDataFrame) -> pd.DataFrame:
    # Get a count of vp for each trip, whether or not those fall 
    # within buffered shape or not
    count_vp = (
        vp.groupby("trip_instance_key", 
                   observed=True, group_keys=False)
        .agg({"location_timestamp_local": "count"})
        .reset_index()
        .rename(columns={"location_timestamp_local": "total_vp"})
    ).compute()
    
    return count_vp
    
    
def vp_in_shape_by_trip(
    vp: dg.GeoDataFrame,
    analysis_date: str,
    one_shape: str,
) -> dd.DataFrame:
    """
    Find if vp point intersects with our buffered shape.
    Get counts by trip.
    """
    one_shape = buffer_shapes(
        analysis_date, 
        buffer_meters = 35, 
        filters = [[("shape_array_key", "==", one_shape)]]
    )
    
    vp2 = gpd.sjoin(
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
    
    count_in_shape = count_in_shape.to_numpy()
    
    return count_in_shape


def find_vp_in_buffered_shapes(
    analysis_date: str,
    shape_keys_list: list,
    trips_with_shape: pd.DataFrame,
    batch: int
):
    """
    """
    time0 = datetime.datetime.now()
    
    vp_with_shape = delayed(merge_vp_with_shape)(
        analysis_date, 
        trips_with_shape[trips_with_shape.shape_array_key.isin(shape_keys_list)]
    )
    
    vp_dfs = [
        delayed(subset_vp)(vp_with_shape, shape)
        for shape in shapes_in_vp
    ]
    
    print("set up lists of delayed inputs")

    results = [
        delayed(vp_in_shape_by_trip)(
            one_vp_df, analysis_date, one_shape)
        for one_vp_df, one_shape in zip(vp_dfs, shape_keys_list)
    ]
    
    time1 = datetime.datetime.now()
    print(f"delayed list of results: {time1 - time0}")
    
        
    results2 = [compute(i)[0] for i in results]
    
    vp_trip_in_shape_totals_arr = np.row_stack(results2)
    
    vp_trip_in_shape_totals = pd.DataFrame(
        vp_trip_in_shape_totals_arr, 
        columns = ["trip_instance_key", "vp_in_shape"]
    )
  
    time2 = datetime.datetime.now()
    print(f"delayed list of results: {time2 - time1}")
    
    vp_trip_in_shape_totals.to_parquet(
        f"{SEGMENT_GCS}trip_summary/accuracy_staging/"
        f"vp_spatial_accuracy_batch{batch}_{analysis_date}.parquet"
    )
    
    print(f"exported batch: {batch}  {datetime.datetime.now()-time0}")
    


def compile_parquets_for_operator(
    analysis_date: str, 
    file_name: str = "vp_spatial_accuracy_batch"
):
    all_files = fs.ls(f"{SEGMENT_GCS}trip_summary/accuracy_staging/")
    
    # The files to compile need format
    # {file_name}_{batch}_{analysis_date}.
    files_to_compile = [
        f"gs://{f}" for f in all_files 
        if (file_name in f) and (analysis_date in f) and 
        (f"{file_name}_{analysis_date}" not in f)
    ]
    
    delayed_dfs = [delayed(pd.read_parquet)(f) for f in files_to_compile]
    
    ddf = dd.from_delayed(delayed_dfs)
    
    return ddf

    
if __name__=="__main__":    
    
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    print(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    trips_with_shape = grab_shape_keys_in_vp(analysis_date)
    shapes_in_vp = trips_with_shape.shape_array_key.unique().tolist()

    '''
    shapes = delayed(buffer_shapes)(
        analysis_date, 
        buffer_meters = 35, 
        filters = [[("shape_array_key", "in", shapes_in_vp)]]
    )
    '''
    n_in_list = 500 # number of elements in list
    chunked_shapes = [shapes_in_vp[i:i + n_in_list] 
                      for i in range(0, len(shapes_in_vp), n_in_list)]

    time1 = datetime.datetime.now()
    
    for i, sub_list in enumerate(chunked_shapes):
        find_vp_in_buffered_shapes(
            analysis_date,
            sub_list,
            trips_with_shape[trips_with_shape.shape_array_key.isin(sub_list)],
            i
        )
    
    
    time2 = datetime.datetime.now()
    print(f"get trip counts in shape by batch: {time2 - time1}")
    '''
    
    vp_in_shape_totals = compile_parquets_for_operator(
    `analysis_date, file_name = "vp_spatial_accuracy_batch")
    
    vp_trip_totals = total_vp_counts_by_trip(vp_with_shape)

    time3 = datetime.datetime.now()
    print(f"get trip total counts: {time3 - time2}")
    
    # Merge our total counts by trip with the vp that fall within shape
    results_df = dd.merge(
        vp_trip_in_shape_totals,
        vp_trip_totals,
        on = "trip_instance_key", 
        how = "left"
    )
    
    results_df.to_parquet(
        f"{SEGMENT_GCS}trip_summary/vp_spatial_accuracy_{analysis_date}.parquet",
    )
    '''
    end = datetime.datetime.now()
    #print(f"export: {end - time3}")
    print(f"execution time: {end - start}")
    
    #client.close()