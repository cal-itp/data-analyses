# Simple script to test dask clutser
import dask.dataframe as dd
import gcsfs
import os
import pandas as pd

from calitp_data_analysis import get_fs
fs = get_fs()

RT_GCS = 'gs://calitp-analytics-data/data-analyses/rt_delay/compiled_cached_views/'
DASK_GCS = 'gs://calitp-analytics-data/data-analyses/dask_test/'

analysis_date = "2022-10-12"

def categorize_time_of_day(value: int ) -> str:
    if isinstance(value, int):
        hour = value
    if hour < 4:
        return "Owl"
    elif hour < 7:
        return "Early AM"
    elif hour < 10:
        return "AM Peak"
    elif hour < 15:
        return "Midday"
    elif hour < 20:
        return "PM Peak"
    else:
        return "Evening"
    
    
def merge_stop_times_to_trips(stop_times: dd.DataFrame, 
                          trips: dd.DataFrame) -> dd.DataFrame:   
    shape_id_cols = ["calitp_itp_id", "shape_id"]

    merged = dd.merge(
        stop_times,
        trips[shape_id_cols + ["trip_id"]].drop_duplicates(),
        on = ["calitp_itp_id", "trip_id"]
    )
    
    return merged



def aggregation_function(df: dd.DataFrame) -> dd.DataFrame:
    shape_id_cols = ["calitp_itp_id", "shape_id"]

    # Map to time-of-day
    stop_times_binned = df.assign(
        time_of_day=df.apply(
            lambda x: categorize_time_of_day(x.departure_hour), axis=1, 
            #meta=('time_of_day', 'str')
        )
    )
    
    # Calculate the number of arrivals by time-of-day
    arrivals = (stop_times_binned.groupby(shape_id_cols + ["time_of_day"])
          .agg({"stop_id": "count"})
            .reset_index()
         )
    
    return arrivals


if __name__=="__main__":
    from dask.distributed import Client

    client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    stop_times = dd.read_parquet(f"{RT_GCS}st_{analysis_date}.parquet")
    trips = dd.read_parquet(f"{RT_GCS}trips_{analysis_date}.parquet")
        
    merged = merge_stop_times_to_trips(stop_times, trips)    
    merged = merged.repartition(npartitions=3)
    print("partitioned")
    
    # Save to parquet
    merged.to_parquet(
        f"{DASK_GCS}test", 
        storage_options={'token': fs.credentials.credentials}
    )
    print("save to GCS as partitioned")

    df = dd.read_parquet(f"{DASK_GCS}test")
    print("read from GCS as partitioned")
    
    df = df.map_partitions(aggregation_function, 
                           meta = {
                               "calitp_itp_id": "int", 
                               "shape_id": "str",
                               "time_of_day": "str",
                               "stop_id": "int",
                               }) # Be sure not to '.compute' here
    print("aggregation function")

    df.compute().to_parquet(f'{DASK_GCS}preprocesed.parquet', 
                            storage_options={'token': fs.credentials.credentials}
                           )
    print("saved preprocessed")

    client.close()