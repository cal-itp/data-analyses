import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from dask import delayed, compute

from calitp_data_analysis import utils
from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import rt_dates

import new_vp_around_stops as nvp

analysis_date = rt_dates.DATES["jul2024"]

def get_vp_geom(analysis_date: str):
    vp = pd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}",
        #filters = [[("schedule_gtfs_dataset_key", "==", one_operator)]],
        columns = ["schedule_gtfs_dataset_key", 
                   "trip_instance_key", "vp_idx", "x", "y",  
               "location_timestamp_local", 
               "moving_timestamp_local"]
    ).pipe(wrangle_shapes.vp_as_gdf, crs = PROJECT_CRS)
    
    return vp

def filter_and_merge(
    vp_nearest,
    vp,
    shapes,
    one_operator,
    trip_stop_cols: list
):
    
    vp = vp[vp.schedule_gtfs_dataset_key==one_operator]
    
    gdf = pd.merge(
        vp_nearest,
        vp.rename(columns = {"geometry": "vp_geometry"}),
        on = ["vp_idx", "trip_instance_key"],
        how = "inner"
    ).merge(
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    )    
    
    gdf2 = nvp.get_projected_results(gdf)
    
    return gdf2

def set_up_df(
    analysis_date: str,
    one_operator: str,
    trip_stop_cols: list
):
    
    vp_with_dwell = delayed(pd.read_parquet)(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}",
        filters = [[("schedule_gtfs_dataset_key", "==", one_operator)]],
        columns = ["trip_instance_key", "vp_idx", "x", "y",  
               "location_timestamp_local", 
               "moving_timestamp_local"]
    ).pipe(wrangle_shapes.vp_as_gdf, crs = PROJECT_CRS)
    
    subset_trips = vp_with_dwell.trip_instance_key.unique()
    
    vp_nearest = delayed(gpd.read_parquet)(
        f"{SEGMENT_GCS}{NEAREST_VP}_{analysis_date}.parquet",
        filters = [[("trip_instance_key", "in", subset_trips)]],
        columns = trip_stop_cols + [
            "shape_array_key", "stop_geometry",
            "nearest_vp_arr"],
    ).explode(
        "nearest_vp_arr"
    ).drop_duplicates().reset_index(
        drop=True
    ).rename(
        columns = {"nearest_vp_arr": "vp_idx"}
    ).astype({"vp_idx": "int64"})
    '''
    subset_shapes = vp_nearest.shape_array_key.unique()
    
    shapes = delayed(helpers.import_scheduled_shapes)(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        filters = [[("shape_array_key", "in", subset_shapes)]],
        crs = PROJECT_CRS,
        get_pandas = True
    )
    '''
    gdf = delayed(pd.merge)(
        vp_nearest,
        vp_with_dwell.rename(columns = {"geometry": "vp_geometry"}),
        on = ["vp_idx", "trip_instance_key"],
        how = "inner"
    ).merge(
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    )
    
    gdf2 = delayed(nvp.get_projected_results)(gdf)
    #gdf3 = delayed(nvp.find_two_closest_vp)(gdf2, trip_stop_cols).persist()
    #gdf4 = delayed(nvp.consolidate_surrounding_vp)(gdf3, trip_stop_cols)
    
    return gdf2



if __name__ == "__main__":
    
    dict_inputs = GTFS_DATA_DICT.stop_segments
    USABLE_VP = dict_inputs["stage1"]
    NEAREST_VP = dict_inputs["stage2"]
    trip_stop_cols = [*dict_inputs["trip_stop_cols"]]
    EXPORT_FILE = dict_inputs["stage2b"]
    
    start = datetime.datetime.now()
    
    trips_present = pd.read_parquet(
        f"{SEGMENT_GCS}{NEAREST_VP}_{analysis_date}.parquet",
        columns = ["trip_instance_key"]
    ).drop_duplicates()

    operators = pd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}",
        columns = ["schedule_gtfs_dataset_key", "trip_instance_key"]
    ).merge(
        trips_present,
        on = "trip_instance_key",
        how = "inner"
    ).schedule_gtfs_dataset_key.unique()
    '''
    vp_nearest = delayed(gpd.read_parquet)(
        f"{SEGMENT_GCS}{NEAREST_VP}_{analysis_date}.parquet",
        columns = trip_stop_cols + [
            "shape_array_key", "stop_geometry",
            "nearest_vp_arr"],
    ).explode(
        "nearest_vp_arr"
    ).drop_duplicates().reset_index(
        drop=True
    ).rename(
        columns = {"nearest_vp_arr": "vp_idx"}
    ).astype({"vp_idx": "int64"})
    
    vp = delayed(get_vp_geom)(analysis_date)
    
    shapes = delayed(helpers.import_scheduled_shapes)(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = True
    )
    '''
    
    #result_dfs = [
    #    delayed(filter_and_merge)(vp_nearest, vp, shapes, one_operator, trip_stop_cols) 
    #    for one_operator in operators
    #]
    result_dfs = [
        delayed(set_up_df)(analysis_date, one_operator, trip_stop_cols) 
        for one_operator in operators
    ]
    
    closest_dfs = [
        delayed(nvp.find_two_closest_vp)(df, trip_stop_cols)
        for df in result_dfs
    ]
    
    time1 = datetime.datetime.now()
    print(f"delayed stuff: {time1 - start}")
    
    closest_ddf = dd.from_delayed(closest_dfs)
       
    time2 = datetime.datetime.now()
    print(f"from delayed: {time2 - time1}")
    
    closest_ddf = closest_ddf.repartition(npartitions=10)
    closest_ddf = closest_ddf.compute()
    
    time3 = datetime.datetime.now()
    print(f"compute: {time3 - time2}")    
    
    
    consolidated_dfs = [
        delayed(nvp.consolidate_surrounding_vp)(df, trip_stop_cols) 
        for df in closest_dfs
    ]

    final_df = dd.from_delayed(consolidated_dfs)
    final_df = final_df.repartition(npartitions=5)
    
    final_df.compute().to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet",
    )
    
    end = datetime.datetime.now()
    print(f"export: {end - time3}")  
    print(f"execution time: {end - start}")