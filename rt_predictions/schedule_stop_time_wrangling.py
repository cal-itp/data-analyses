import dask.dataframe as dd
import pandas as pd

from segment_speed_utils import helpers, sched_rt_utils
from segment_speed_utils.project_vars import (PREDICTIONS_GCS, 
                                              analysis_date)

trip_cols = ["gtfs_dataset_key", "service_date", "trip_id"]
stop_cols = trip_cols + ["stop_id"] 

def prep_scheduled_stop_times(analysis_date: str) -> dd.DataFrame: 
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "trip_id",
                   "stop_id", "stop_sequence", 
                   "arrival_sec"],
    )
    
    stop_times = stop_times.assign(
        service_date = pd.to_datetime(analysis_date).date(),
        arrival_datetime = dd.to_datetime(
            stop_times.arrival_sec, unit="s").dt.time.astype(str)
    )
    
    stop_times = stop_times.assign(
        scheduled_arrival = dd.to_datetime(
            stop_times.service_date.astype(str) + " " + 
            stop_times.arrival_datetime
        )
    ).drop(columns = ["arrival_sec", "arrival_datetime"])
    
    return stop_times


def scheduled_stop_times_with_rt_dataset_key(
    analysis_date: str,
) -> dd.DataFrame:
    """
    Get scheduled stop_times and attach gtfs_dataset_key.
    """
    stop_times = prep_scheduled_stop_times(analysis_date)
    
    trips = helpers.import_scheduled_trips(
        analysis_date, 
        columns = ["feed_key", "trip_id", "shape_id", "route_id"]
    ).compute()
    
    stop_times2 = dd.merge(
        stop_times,
        trips,
        on = ["feed_key", "trip_id"],
        how = "inner"
    )
    
    trip_start = (stop_times2.groupby(["feed_key", "trip_id"])
                  .agg({"scheduled_arrival": "min"})
                  .reset_index()
                  .rename(columns = {
                      "scheduled_arrival": "scheduled_trip_start"})
                  .compute()
                 )
    
    stop_times_with_trip_start = dd.merge(
        stop_times2,
        trip_start,
        on = ["feed_key", "trip_id"], 
        how = "inner"
    )
    
    crosswalk = sched_rt_utils.crosswalk_scheduled_trip_grouping_with_rt_key(
        analysis_date,
        ["feed_key", "trip_id"],
        # keep shape_id because some of the aggregations may have to 
        # take place at shape-level first, before route-level
        feed_types = ["trip_updates"]
    )

    stop_times_with_rt_key = dd.merge(
        stop_times_with_trip_start,
        crosswalk, 
        on = ["feed_key", "trip_id"],
        how = "inner"
    )
    
    return stop_times_with_rt_key


def derive_schedule_relationship(
    stop_time_updates: pd.DataFrame, 
    scheduled_stop_times: dd.DataFrame,
) -> dd.DataFrame:
    """
    Can we infer that all missing values for schedule_relationship 
    should be the default of SCHEDULED?
    Or, we can look in scheduled stop_times and see if that 
    trip_id exists.
    """
    trips_in_rt = stop_time_updates[trip_cols].drop_duplicates()
    trips_in_schedule = scheduled_stop_times[
        trip_cols + ["scheduled_trip_start",
                     "shape_id", "route_id"]].drop_duplicates()
    
    if isinstance(trips_in_schedule, dd.DataFrame):
        trips_in_schedule = trips_in_schedule.compute()
        
    if isinstance(trips_in_rt, dd.DataFrame):
        trips_in_rt = trip_in_rt.compute()
        
    trip_df = pd.merge(
        trips_in_rt,
        trips_in_schedule,
        on = trip_cols,
        how = "left",
        indicator=True
    )
    
    trip_df = trip_df.assign(
        derived_schedule_relationship = trip_df.apply(
            lambda x: "SCHEDULED" if x._merge=="both" 
            else "ADDED", axis=1)
    ).drop(columns = "_merge")
    
    st_with_start = pd.merge(
        stop_time_updates,
        trip_df,
        on=trip_cols, 
        how = "inner"
    )
    
    st_with_start = st_with_start.assign(
        schedule_relationship = st_with_start.schedule_relationship.fillna(
            st_with_start.derived_schedule_relationship),
    ).drop(columns = ["derived_schedule_relationship"])
    
    # Now merge in scheduled arrival time by stop 
    # since the df is back at stop-level
    st_with_schedule = dd.merge(
        st_with_start,
        scheduled_stop_times[stop_cols + ["stop_sequence", 
                                          "scheduled_arrival"]], 
        on = stop_cols + ["stop_sequence"],
        how = "left",
    )
    
    return st_with_schedule

    
