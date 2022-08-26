"""
Create dataset, at route-level.

Aggregate stop times df by time-of-day.
Add in competitive_route_variability df (created in E5_make_stripplot_data),
which is at the trip-level, and only keep the route-level stats.

Merge these so dataset has these route-level stats:
   num_stop_arrivals_*time_of_day
   num_trips_*time_of_day
   % and # trips competitive
   route_group, route_name
   
Output: bus_routes_aggregated_stats
"""
import dask.dataframe as dd
import intake
import geopandas as gpd
import pandas as pd

from calitp.sql import to_snakecase

from shared_utils import (geography_utils, gtfs_utils, 
                          rt_dates, rt_utils
                         )
from utils import GCS_FILE_PATH

ANALYSIS_DATE = rt_dates.DATES["may2022"]
catalog = intake.open_catalog("*.yml")
TRAFFIC_OPS_GCS = 'gs://calitp-analytics-data/data-analyses/traffic_ops/'


def subset_trips_and_stop_times(trips: dd.DataFrame, 
                                stop_times: dd.DataFrame,
                                itp_id_list: list, 
                                route_list: list) -> dd.DataFrame:
    """
    Subset trips with smaller list of ITP IDs and route_ids.
    Then subset stop times.
    
    Merge trips with stop times so stop_times also comes with route_ids.
    
    Add in time-of-day column.
    """
    subset_trips = trips[
        (trips.calitp_itp_id.isin(itp_id_list) & 
         (trips.route_id.isin(route_list)))
    ]

    keep_trip_keys = subset_trips.trip_key.unique().compute().tolist()
    
    # Get route_info
    with_route_id = subset_trips[
        ["calitp_itp_id", "trip_key", "route_id"]
    ].drop_duplicates()
    
    # Subset stop times, first keep only trip_keys found in trips
    # Trips give the routes we're interested in
    subset_stop_times = stop_times[stop_times.trip_key.isin(keep_trip_keys)]
    
    subset_stop_times2 = dd.merge(
        subset_stop_times,
        with_route_id,
        on = ["calitp_itp_id", "trip_key"],
        how = "inner"
    )
    
    stop_times_with_hr = gtfs_utils.fix_departure_time(subset_stop_times2)
    
    stop_times_binned = stop_times_with_hr.assign(
        time_of_day=stop_times_with_hr.apply(
            lambda x: rt_utils.categorize_time_of_day(x.departure_hour), axis=1, 
            meta=('time_of_day', 'str'))
    )
    
    return stop_times_binned


def aggregate_by_time_of_day(stop_times: dd.DataFrame, 
                             group_cols: list) -> pd.DataFrame:
    """
    Aggregate given different group_cols.
    Count each individual stop time (a measure of 'saturation' along a hwy corridor)
    or just count number of trips that occurred?
    """    
    # nunique seems to not work in groupby.agg
    # even though https://github.com/dask/dask/pull/8479 seems to resolve it?
    # alternative is to use nunique as series
    nunique_trips = (stop_times.groupby(group_cols)["trip_id"].nunique()
                     .reset_index()
                     .rename(columns = {"trip_id": "num_trips"})
                    )
    
    count_stop_arrivals = (stop_times.groupby(group_cols)
           .agg({"departure_hour": "count",})
           .reset_index()
           .rename(columns = {
               "departure_hour": "num_stop_arrivals",
           })
    )
    
    ddf = dd.merge(
        nunique_trips,
        count_stop_arrivals,
        on = group_cols,
        how = "inner"
    ).compute()
    
    return ddf



if __name__=="__main__":
    # Read in bus routes that run on highways to use for filtering in dask df
    bus_routes = catalog.bus_routes_on_hwys.read()
    
    keep_itp_ids = bus_routes.itp_id.unique().tolist()
    keep_routes = bus_routes.route_id.unique().tolist()
    
    '''
    gtfs_utils.all_trips_or_stoptimes_with_cached(
        dataset="st",
        analysis_date = ANALYSIS_DATE,
        itp_id_list = keep_itp_ids,
        export_path = GCS_FILE_PATH
    )
    '''
    
    stop_times = dd.read_parquet(f"{GCS_FILE_PATH}st_{ANALYSIS_DATE}.parquet")
    trips = dd.read_parquet(f"{TRAFFIC_OPS_GCS}trips_{ANALYSIS_DATE}.parquet")
    
    # Subset stop times and merge with trips
    stop_times_with_hr = subset_trips_and_stop_times(
        trips, stop_times, 
        itp_id_list = keep_itp_ids, 
        route_list = keep_routes
    )
    
    route_level_cols = [
        "calitp_itp_id", "service_date", 
        "route_id", "time_of_day",
    ]

    #stop_level_cols = route_level_cols + ["stop_id"]
    
    # Aggregate to stop-level (should we use stop point geom later?)
    #by_stop_and_time_of_day = aggregate_by_time_of_day(
    #    stop_times_with_hr, stop_level_cols
    #)
    
    # Aggregate to route-level
    by_route_and_time_of_day = aggregate_by_time_of_day(
        stop_times_with_hr, route_level_cols
    )