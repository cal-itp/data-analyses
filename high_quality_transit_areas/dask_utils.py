import dask.dataframe as dd
import numpy as np
import pandas as pd

from shared_utils import rt_utils


def stop_times_aggregation(itp_id, analysis_date):
    date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)
    
    ddf = dd.read_parquet(f"{rt_utils.GCS_FILE_PATH}"
                                 f"cached_views/st_{itp_id}_{date_str}.parquet")

    # Some fixing, transformation, aggregation with dask
    # Grab departure hour
    #https://stackoverflow.com/questions/45428292/how-to-convert-pandas-str-split-call-to-to-dask
    ddf = ddf.assign(
        **{"departure_hour": ddf.departure_time.str.partition(":")[0].astype(int)}
    )
    
    # Since hours past 24 are allowed for overnight trips
    # coerce these to fall between 0-23
    #https://stackoverflow.com/questions/54955833/apply-a-lambda-function-to-a-dask-dataframe
    ddf["departure_hour"] = ddf.departure_hour.map(lambda x: x-24 if x >=24 else x)

    stop_cols = ["calitp_itp_id", "stop_id"]
    # Aggregate how many trips are made at that stop by departure hour
    trips_per_hour = (ddf.groupby(stop_cols + ["departure_hour"])
                      .agg({'trip_id': 'count'})
                      .reset_index()
                      .rename(columns = {"trip_id": "n_trips"})
                     )        
        
    # Flexible AM peak - find max trips at the stop before noon
    am_max = (trips_per_hour[trips_per_hour.departure_hour < 12]
              .groupby(stop_cols)
              .agg({"n_trips": np.max})
              .reset_index()
              .rename(columns = {"n_trips": "am_max_trips"})
             ).compute() # compute is what makes it a pandas df, rather than dask df
    
    # Flexible PM peak - find max trips at the stop after noon
    pm_max = (trips_per_hour[trips_per_hour.departure_hour >= 12]
              .groupby(stop_cols)
              .agg({"n_trips": np.max})
              .reset_index()
              .rename(columns = {"n_trips": "pm_max_trips"})
             ).compute()
    
    df = pd.merge(
        am_max,
        pm_max,
        on = stop_cols,
    )
    
    return df
