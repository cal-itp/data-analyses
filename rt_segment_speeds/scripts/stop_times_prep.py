import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import numpy as np
import pandas as pd
import geopandas as gpd

from segment_speed_utils import helpers, vp_transform
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS

from shared_utils import rt_dates
from resample import project_point_onto_shape

analysis_date = rt_dates.DATES["oct2024"]


def is_monotonically_increasing(my_array: list):
    """
    Somehow store whether projecting stop position onto vp path is increasing or not.
    results that are True, we can calculate speeds for, otherwise, we shouldn't.
    these results are better than stop_meters calculated off of shape.
    """
    my_array2 = np.array(my_array)
    boolean_results = np.diff(my_array2) > 0
    # add first observation, which is true because the first distance is compared to 0?
    return np.array(boolean_results) #np.array([True] + boolean_results) 


def merge_stop_times_and_vp(analysis_date: str):
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", "stop_sequence", "geometry"],
        with_direction = True,
        get_pandas = False,
    )
    
    vp_path =gpd.read_parquet(
        f"{SEGMENT_GCS}vp_condensed/vp_projected_{analysis_date}.parquet",
        columns = ["trip_instance_key", "vp_geometry"]
    ).drop_duplicates()
    
    stop_times_vp_geom = dd.merge(
        stop_times,
        vp_path,
        on = "trip_instance_key",
        how = "inner"
    ).sort_values(["trip_instance_key", "stop_sequence"]).reset_index(drop=True)
    
    stop_times_vp_geom = stop_times_vp_geom.repartition(npartitions=20)
    
    return stop_times_vp_geom

def project_stop_onto_vp_geom_and_condense(gddf):
    
    orig_dtypes = gddf.dtypes.to_dict()

    gdf2 = gddf.map_partitions(
        project_point_onto_shape,
            line_geom = "vp_geometry", 
            point_geom = "geometry",
        meta = {
            **orig_dtypes, 
            "projected_meters": "float"
        },
        align_dataframes = True
    )

    gdf2 = gdf2.rename(
        columns = {"projected_meters": "stop_meters"}
    ).drop(
        columns = ["vp_geometry", "geometry"]
    ).persist()
        
    
    return gdf2




if __name__ == "__main__":
    
    
    start = datetime.datetime.now()
    
    stop_times_vp_geom = merge_stop_times_and_vp(analysis_date)
    
    gdf = project_stop_onto_vp_geom_and_condense(stop_times_vp_geom).compute()
    
    results = (gdf
        .groupby("trip_instance_key", group_keys=False)
        .agg({
            "stop_sequence": lambda x: list(x),
            "stop_meters": lambda x: list(x)
        })
        .reset_index()
       )

    results = results.assign(
        stop_meters_increasing = results.apply(
            lambda x: is_monotonically_increasing(x.stop_meters), axis=1
        )
    )
     
    results.to_parquet(
        f"{SEGMENT_GCS}stop_times_projected_{analysis_date}.parquet"
    )

    
    end = datetime.datetime.now()
    print(f"stop times prep: {end - start}")