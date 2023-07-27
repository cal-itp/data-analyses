import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from shared_utils import rt_dates, utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS

# for prep_stop_segments
def update_stops_projected(analysis_date):
    df = dg.read_parquet(f"{SEGMENT_GCS}stops_projected_{analysis_date}")
    
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "gtfs_dataset_key", 
                   "trip_id", "trip_instance_key"],
        get_pandas = True
    ).rename(columns = {"trip_id": "st_trip_id"})
    
    df2 = dd.merge(
        df,
        trips,
        on = ["feed_key", "st_trip_id"],
        how = "inner"
    ).drop(columns = ["feed_key"])
    
    # persist to shrink task graph
    # can't read and overwrite same parquet in task graph
    df2 = df2.repartition(npartitions=10).persist()
    
    print(df2.dtypes)
    
    df2.to_parquet(
        f"{SEGMENT_GCS}stops_projected_{analysis_date}", overwrite=True
    )
    
    print("updated stops projected")
    
# for cut_normal_stop_segments, cut_special_stop_segments, concatenate_stop_segments    
# for stop_segments_normal, stop_segments_special, stop_segments
def switch_feed_key_out(
    file_name: str, analysis_date: str
):
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["feed_key", "gtfs_dataset_key"],
        get_pandas = True
    ).drop_duplicates()
    
    print(f"file: {file_name}_{analysis_date}")
    df = gpd.read_parquet(
        f"{SEGMENT_GCS}{file_name}_{analysis_date}.parquet")
    
    df2 = pd.merge(
        df,
        trips,
        on = "feed_key",
        how = "inner"
    ).drop(columns = "feed_key")
    
    print(df2.dtypes)
    
    utils.geoparquet_gcs_export(
        df2,
        SEGMENT_GCS,
        f"{file_name}_{analysis_date}"
    )
    

# for B1_speeds_by_segment_trip
def update_speeds_by_trip(analysis_date: str):

    vp_trip_ids = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}", 
        columns = ["gtfs_dataset_key", "trip_id", 
                   "trip_instance_key",
                   "schedule_gtfs_dataset_key"]
    ).drop_duplicates().reset_index(drop=True).compute()
    
    speeds = dd.read_parquet(
        f"{SEGMENT_GCS}speeds_stop_segments_{analysis_date}"
    )
    
    speeds2 = dd.merge(
        speeds,
        vp_trip_ids,
        on = ["gtfs_dataset_key", "trip_id"],
        how = "left"
    ).rename(columns = {"_gtfs_dataset_name": "gtfs_dataset_name"})
    
    print(speeds2.dtypes)
    
    speeds2 = speeds2.persist()
    
    speeds2.to_parquet(
        f"{SEGMENT_GCS}speeds_stop_segments_{analysis_date}", 
        overwrite=True)
    
if __name__ == "__main__":
    
    dates_list = ["mar2023", "apr2023", "may2023", "jun2023", "jul2023"]
    dates = [rt_dates.DATES[d] for d in dates_list]
    
    for analysis_date in dates:
        update_stops_projected(analysis_date)
        
        for file in [
            "stop_segments_normal", 
            "stop_segments_special",
            "stop_segments"
        ]:
            print(file)
            switch_feed_key_out(file, analysis_date)
        
    update_speeds_by_trip(analysis_date)