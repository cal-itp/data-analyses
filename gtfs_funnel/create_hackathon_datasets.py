"""
Grab a set of Cal-ITP tables for Posit hackathon in Feb 2025.

Grab GTFS schedule and vp tables from GCS bucket and 
subset to the operators. Concatenate across dates.

Our GTFS analytics data catalog: 
https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/gtfs_analytics_data.yml
"""
import geopandas as gpd
import glob
import pandas as pd
import os
import shutil

from segment_speed_utils import time_series_utils
from update_vars import GTFS_DATA_DICT
from shared_utils import rt_dates

OUTPUT_FOLDER = "./hackathon/"
CRS = "EPSG:4326"

def merge_crosswalk(
    df: pd.DataFrame, 
    crosswalk: pd.DataFrame
) -> pd.DataFrame:
    """
    Drop feed_key after merging in schedule_gtfs_dataset_key.
    Use this identifier when merging in vp.
    """
    df2 = pd.merge(
        df,
        crosswalk,
        on = "feed_key",
        how = "inner"
    ).drop(columns = "feed_key") 
    
    return df2


def export_gtfs_schedule_tables(
    operator_list: list,
    date_list: list
):
    """
    Export subset of downloaded schedule tables (minimally processed in our warehouse),
    by inputting a list of operator names.
    """
    schedule_tables = GTFS_DATA_DICT.schedule_downloads
    
    trips = time_series_utils.concatenate_datasets_across_dates(
        gcs_bucket = schedule_tables.dir,
        dataset_name = schedule_tables.trips,
        date_list = date_list,
        data_type = "df",
        get_pandas = True,
        filters = [[("name", "in", operator_list)]], 
        columns = ["feed_key", "gtfs_dataset_key", "name",
                   "service_date", "trip_id", "trip_instance_key",
                   "route_key", "route_id", "route_type",
                   "route_short_name", "route_long_name", "route_desc",
                   "direction_id", 
                   "shape_array_key", "shape_id", 
                   "trip_first_departure_datetime_pacific", 
                   "trip_last_arrival_datetime_pacific",
                   "service_hours"
                  ]
    ).rename(
        columns = {
            "gtfs_dataset_key": "schedule_gtfs_dataset_key",
    })
    
    subset_feeds = trips.feed_key.unique().tolist()
    crosswalk = trips[["feed_key", "schedule_gtfs_dataset_key"]].drop_duplicates()
    
    shapes = time_series_utils.concatenate_datasets_across_dates(
        gcs_bucket = schedule_tables.dir,
        dataset_name = schedule_tables.shapes,
        date_list = date_list,
        data_type = "gdf",
        get_pandas = True,
        filters = [[("feed_key", "in", subset_feeds)]], 
        columns = ["feed_key",
                   "shape_array_key", "shape_id", 
                   "n_trips", "geometry"
                  ]
    ).pipe(
        merge_crosswalk, crosswalk
    ).to_crs(CRS)  
    
    stops = time_series_utils.concatenate_datasets_across_dates(
        gcs_bucket = schedule_tables.dir,
        dataset_name = schedule_tables.stops,
        date_list = date_list,
        data_type = "gdf",
        get_pandas = True,
        filters = [[("feed_key", "in", subset_feeds)]], 
        columns = [
            "feed_key", "service_date",
            "stop_id", "stop_key", "stop_name", 
            "geometry"
        ]           
    ).pipe(
        merge_crosswalk, crosswalk
    ).to_crs(CRS) 
    
    stop_times = time_series_utils.concatenate_datasets_across_dates(
        gcs_bucket = schedule_tables.dir,
        dataset_name = schedule_tables.stop_times,
        date_list = date_list,
        data_type = "df",
        get_pandas = True,
        filters = [[("feed_key", "in", subset_feeds)]], 
        columns = ["feed_key", 
                   "trip_id", "stop_id",
                   "stop_sequence", "arrival_sec", 
                   "timepoint"
                  ]     
    ).pipe(merge_crosswalk, crosswalk)  
    
    trips.to_parquet(f"{OUTPUT_FOLDER}trips.parquet")
    shapes.to_parquet(f"{OUTPUT_FOLDER}shapes.parquet")
    stops.to_parquet(f"{OUTPUT_FOLDER}stops.parquet")
    stop_times.to_parquet(f"{OUTPUT_FOLDER}stop_times.parquet")

    return 


def export_gtfs_vp_table(
    schedule_gtfs_dataset_key_list: list,
    date_list: list
):
    """
    Export subset of downloaded vp (deduped in our warehouse),
    by inputting a list of schedule_gtfs_dataset_keys.
    """
    vp_tables = GTFS_DATA_DICT.speeds_tables
    
    vp = time_series_utils.concatenate_datasets_across_dates(
        gcs_bucket = vp_tables.dir,
        dataset_name = vp_tables.raw_vp,
        date_list = analysis_date_list,
        data_type = "gdf",
        get_pandas = True,
        filters = [[("schedule_gtfs_dataset_key", "in", schedule_gtfs_dataset_key_list)]], 
        columns = [
            "gtfs_dataset_name", "gtfs_dataset_key", "schedule_gtfs_dataset_key", 
            "trip_id", "trip_instance_key",
            "location_timestamp_local",
            "geometry"]
    ).rename(
        columns = {"gtfs_dataset_key": "vp_gtfs_dataset_key"}
    ).to_crs(CRS)
    
    vp.to_parquet(f"{OUTPUT_FOLDER}vp.parquet")
    
    return

    
def export_segments_table(
    schedule_gtfs_dataset_key_list: list,
    date_list: list
):
    """
    Export subset of cut segments from gtfs_segments.create_segments,
    by inputting a list of schedule_gtfs_dataset_keys.
    """
    vp_tables = GTFS_DATA_DICT.rt_stop_times
    
    segments = time_series_utils.concatenate_datasets_across_dates(
        gcs_bucket = vp_tables.dir,
        dataset_name = vp_tables.segments_file,
        date_list = analysis_date_list,
        data_type = "gdf",
        get_pandas = True,
        filters = [[("schedule_gtfs_dataset_key", "in", schedule_gtfs_dataset_key_list)]],
        columns = ["trip_instance_key", 
                   "schedule_gtfs_dataset_key", "route_id", "direction_id",
                   "stop_pair", "stop_id1", "stop_id2", 
                   "stop_sequence", "geometry"
                  ]
    ).to_crs(CRS)
    
    segments.to_parquet(f"{OUTPUT_FOLDER}segments.parquet")

    return


def export_roads_for_district(district: int = 7):
    """
    Export subset roads in District 7, which is where
    all the operators are.
    """
    SHARED_GCS = GTFS_DATA_DICT.gcs_paths.SHARED_GCS
    
    roads = gpd.read_parquet(
        f"{SHARED_GCS}all_roads_2020_state06.parquet"
    ).to_crs(CRS)
    
    caltrans_districts = gpd.read_parquet(
        f"{SHARED_GCS}caltrans_districts.parquet",
        filters = [[("DISTRICT", "==", district)]]
    ).to_crs(CRS)
    
    roads_in_district = gpd.sjoin(
        roads, 
        caltrans_districts,
        how = "inner",
        predicate = "within"
    ).drop(columns = ["DISTRICT", "index_right"])
    
    roads_in_district.to_parquet(f"{OUTPUT_FOLDER}district7_roads.parquet")
    
    return
    
    

if __name__ == "__main__":
    
    analysis_date_list = [
        rt_dates.DATES[f"{m}2024"] for m in ["oct", "nov"]
    ]
    
    operators = [
        "Big Blue Bus Schedule",
        "Culver City Schedule",
        "BruinBus Schedule",
        "Santa Clarita Schedule", 
        "LA DOT Schedule",
    ]
    
    # Schedule tables - trips, shapes, stops, stop_times
    export_gtfs_schedule_tables(
        operators, analysis_date_list
    )
    
    schedule_keys = pd.read_parquet(
        f"{OUTPUT_FOLDER}trips.parquet"
    ).schedule_gtfs_dataset_key.unique().tolist()
           
    # vehicle positions table
    export_gtfs_vp_table(
        schedule_keys, analysis_date_list
    )
    
    # segments for mapping
    export_segments_table(
        schedule_keys, analysis_date_list
    )    
    
    # roads for D7, which is where these operators are
    export_roads_for_district(district = 7)
    
    # Zip up folder of parquets within OUTPUT_FOLDER -- download this and send
    # do not check into GitHub
    shutil.make_archive("gtfs_assembled_hackathon", "zip", OUTPUT_FOLDER)
    
    # Delete local parquets, just keep zipped folder to download and send
    for f in glob.glob(f"{OUTPUT_FOLDER}*.parquet"):
        os.remove(f)
    
    