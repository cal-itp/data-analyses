"""
Merge segment speed datasets across dates to create time-series data
for GTFS digest.
Grain is operator-service_date-route-direction-segment(stop_pair)-time_period.
The segment is defined by stop_pair (stop1__stop2).


Memory considerations:
segment speeds time-series with geom: ~500 MB, running close to memory limit
    
segment speeds time-series tabular: 50-60 MB
segment geom only: 80 MB
    
Attaching geometry is expensive, especially when it's happening over 4.3M rows.
Let's keep it apart for now.
"""
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils

from segment_speed_utils import time_series_utils
from shared_utils import gtfs_utils_v2, publish_utils
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS, RT_SCHED_GCS
import merge_data

route_time_cols = merge_data.route_time_cols
sort_cols = merge_data.sort_cols

def concatenate_segment_speeds_by_route_direction(
    date_list: list
) -> gpd.GeoDataFrame:
    """
    Concatenate segment speeds (from rt_segment_speeds)
    for route-direction-time_period grain
    for all the dates we have.
    """
    FILE = GTFS_DATA_DICT.stop_segments.route_dir_single_segment
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        FILE,
        date_list,
        data_type = "df",
        columns = route_time_cols + [
            "stop_pair", "stop_pair_name",
            "p20_mph", "p50_mph", 
            "p80_mph"],
    ).sort_values(sort_cols).reset_index(drop=True)
    
    return df


def concatenate_segment_geometry(
    date_list: list
) -> gpd.GeoDataFrame:
    """
    Concatenate segment geometry (from rt_segment_speeds)
    for all the dates we have 
    and get it to route-direction-segment grain
    """
    FILE = GTFS_DATA_DICT.stop_segments.route_dir_single_segment
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        FILE,
        date_list,
        data_type = "gdf",
        columns = route_time_cols + [
            "stop_pair", "stop_pair_name",
            "geometry"],
        filters = [[("time_period", "==", "all_day")]]
    ).sort_values(sort_cols).drop(
        columns = ["time_period", "service_date"]
    ).drop_duplicates().reset_index(drop=True)
    
    return df

if __name__ == "__main__":
    
    from shared_utils import rt_dates
    
    analysis_date_list = (
        rt_dates.y2024_dates + rt_dates.y2023_dates
    )
    
    DIGEST_SEGMENT_SPEEDS = GTFS_DATA_DICT.digest_tables.route_segment_speeds
    DIGEST_SEGMENT_GEOMETRY = GTFS_DATA_DICT.digest_tables.route_segment_geometry
    
    # These are public schedule_gtfs_dataset_keys
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    segment_speeds = concatenate_segment_speeds_by_route_direction(
        analysis_date_list
    ).pipe(
        publish_utils.exclude_private_datasets, 
        public_gtfs_dataset_keys = public_feeds
    ).pipe(
        merge_data.merge_in_standardized_route_names, 
    )
    
    # Get primary route type
    primary_typology = merge_data.concatenate_schedule_by_route_direction(
        analysis_date_list
    ).pipe(merge_data.set_primary_typology)
    
    segment_speeds2 = pd.merge(
        segment_speeds,
        primary_typology,
        on = route_time_cols,
        how = "left"
    )
    
    segment_speeds2.to_parquet(
        f"{RT_SCHED_GCS}{DIGEST_SEGMENT_SPEEDS}.parquet"
    )
        
    segment_geom = concatenate_segment_geometry(analysis_date_list)
    
    utils.geoparquet_gcs_export(
        segment_geom,
        RT_SCHED_GCS,
        DIGEST_SEGMENT_GEOMETRY
    )
    
    print("Saved Digest segment speeds")
    