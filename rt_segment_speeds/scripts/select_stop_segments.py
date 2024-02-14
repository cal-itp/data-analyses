"""
Select 1 shape to use for stop segments by shape 
for each route-direction.

We want to aggregate the speeds across stop segments,
so the segments need to be the same, in order to 
vertically stack them.
This is simply adjusting where the segments are cut, and 
we are still looking for the nearest neighbors to 
these cutpoints.
"""
import datetime
import geopandas as gpd
import pandas as pd

#from calitp_data_analysis import utils
from shared_utils import utils_to_add
from shared_utils import rt_dates
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS


def select_one_trip_per_shape(analysis_date: str):
    """
    From stop segments across all trips, select 1 trip per shape.
    Select the trip that's the longest length.
    Column called st_trip_instance_key will help us find
    which trip we chose for that shape_array_key.
    """
   
    stop_segments = gpd.read_parquet(
        f"{SEGMENT_GCS}segment_options/stop_segments_{analysis_date}.parquet"
    )
    
    stop_segments = stop_segments.assign(
        segment_meters = stop_segments.geometry.length
    )
    
    selected_trip = (stop_segments
                     .groupby(["trip_instance_key", "shape_array_key"],
                              observed=True, group_keys=False)
                     .agg({"segment_meters": "sum"})
                     .reset_index()
                     .sort_values(["shape_array_key",
                         "segment_meters", "trip_instance_key"])
                     .drop_duplicates(subset="shape_array_key")
                     .drop(columns = "segment_meters")
                     .rename(columns = {
                         "trip_instance_key": "st_trip_instance_key"})
                    )
    
    stop_segments_subset = pd.merge(
        stop_segments.drop(columns = "segment_meters"),
        selected_trip,
        on = "shape_array_key",
        how = "inner"
    )
    
    return stop_segments_subset


if __name__ == "__main__":
    from segment_speed_utils.project_vars import analysis_date_list, CONFIG_PATH
    
    # This is a different stop segments file (a subset)
    # of the RT stop times one that cuts segments for all trips
    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
    
    for analysis_date in analysis_date_list:
        start = datetime.datetime.now()
        
        SEGMENT_FILE = STOP_SEG_DICT["segments_file"]        
        
        segments = select_one_trip_per_shape(analysis_date)
        
        utils_to_add.geoparquet_gcs_export(
            segments,
            SEGMENT_GCS,
            f"{SEGMENT_FILE}_{analysis_date}"
        )    
        
        end = datetime.datetime.now()
        print(f"execution time: {end - start}")