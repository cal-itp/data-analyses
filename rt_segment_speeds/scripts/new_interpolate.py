import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import rt_dates
import interpolate_stop_arrival

analysis_date = rt_dates.DATES["apr2024"]

def add_arrival_time(analysis_date: str):    
    gdf = gpd.read_parquet(
        f"{SEGMENT_GCS}test_nearest2_vp_to_stop_{analysis_date}.parquet"
    )
    
    arrival_time_series = []
    
    for row in gdf.itertuples():
        
        stop_position = getattr(row, "stop_meters")
        
        projected_points = np.asarray([
            getattr(row, "prior_shape_meters"), 
            getattr(row, "subseq_shape_meters")
        ])
        
        timestamp_arr = np.asarray([
            getattr(row, "prior_vp_timestamp_local"),
            getattr(row, "subseq_vp_timestamp_local"),
        ])
        
        
        interpolated_arrival = wrangle_shapes.interpolate_stop_arrival_time(
            stop_position, projected_points, timestamp_arr)
        
        arrival_time_series.append(interpolated_arrival)
        
    gdf["arrival_time"] = arrival_time_series
    
    drop_cols = [i for i in gdf.columns if 
                 ("prior_" in i) or ("subseq_" in i)]
    
    gdf2 = gdf.drop(columns = drop_cols + ["stop_geometry"])
    
    return gdf2

if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    trip_stop_cols = [
        "trip_instance_key", "stop_sequence", 
    "stop_sequence1"] + ["shape_array_key", "stop_pair", "stop_meters"]   
    #trip_stop_cols = [*dict_inputs["trip_stop_cols"]]

    results = add_arrival_time(analysis_date)
    
    results = interpolate_stop_arrival.enforce_monotonicity_and_interpolate_across_stops(
        results, trip_stop_cols)
        
    
    results.to_parquet(
        f"{SEGMENT_GCS}test_arrivals_{analysis_date}.parquet"
    )
    
    end = datetime.datetime.now()
    print(f"test arrivals (BBB): {end - start}")
