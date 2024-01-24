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

from calitp_data_analysis import utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS


def select_one_trip_per_shape(analysis_date: str):
    """
    """
    stop_segments = gpd.read_parquet(
        f"{SEGMENT_GCS}stop_segments_{analysis_date}.parquet"
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
                     .rename(columns = {"trip_instance_key": "st_trip_instance_key"})
                    )
    
    stop_segments_subset = pd.merge(
        stop_segments.drop(columns = "segment_meters"),
        selected_trip,
        on = "shape_array_key",
        how = "inner"
    )
    
    return stop_segments_subset


if __name__ == "__main__":
    from segment_speed_utils.project_vars import analysis_date_list
    
    for analysis_date in analysis_date_list[:1]:
        start = datetime.datetime.now()
        
        stop_segments = select_one_trip_per_shape(analysis_date)

        utils.geoparquet_gcs_export(
            stop_segments,
            f"{SEGMENT_GCS}segment_options/",
            f"shape_stop_segments_{analysis_date}"
        )
        
        end = datetime.datetime.now()
        print(f"execution time: {end - start}")