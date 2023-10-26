"""
GTFS schedule data is downloaded in gtfs_funnel/.

Pull those parquets and combine / aggregate.
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from calitp_data_analysis.tables import tbls
from siuba import *

from calitp_data_analysis import utils, geography_utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling, sched_rt_utils


def calculate_trip_run_time(selected_date: str) -> pd.DataFrame:
    """
    For a given date, read in cached trips table.
    Attach time-of-day, departure_hour, and get service_minutes.
    Filter out rows that do not have complete info.
    """
    trips = helpers.import_scheduled_trips(
        selected_date,
        columns = ["gtfs_dataset_key", "feed_key", 
                   "trip_instance_key", "trip_id", 
                   "shape_id",
                   "route_id",
                   "service_date",
                  ],
        get_pandas = True
    )
    
    time_of_day = sched_rt_utils.get_trip_time_buckets(selected_date)
    
    trips2 = pd.merge(
        trips,
        time_of_day,
        on = "trip_instance_key",
        how = "inner"
    )
    
    trips3 = trips2[
        (trips2.trip_first_departure_datetime_pacific.notna()) & 
        (trips2.service_minutes.notna())
    ].reset_index(drop=True)
    
    
    trips3 = trips3.assign(
        departure_hour = pd.to_datetime(
            trips3.trip_first_departure_datetime_pacific).dt.hour,
        day_name = pd.to_datetime(
            trips3.trip_first_departure_datetime_pacific).dt.day_name(),
    )
    
    trips3.to_parquet(f"{DATA_PATH}trip_run_times_{selected_date}.parquet")
    

def aggregate_stop_times_add_stop_geometry(selected_date: str) -> pd.DataFrame:
    """
    For a given date, read in cached stop_times table.
    Aggregate it to the number of arrivals per stop
    and attach stop point geometry.
    """
    stop_cols = ["schedule_gtfs_dataset_key", "stop_id"]
    
    stop_times = helpers.import_assembled_stop_times_with_direction(
        selected_date,
        columns = stop_cols + ["stop_sequence", "geometry"],
        get_pandas = True,
        crs = geography_utils.WGS84
    )
    
    # Aggregate to count daily stop times
    # Count the number of times a bus arrives at that stop daily    
    stop_arrivals = gtfs_schedule_wrangling.stop_arrivals_per_stop(
        stop_times,
        group_cols = stop_cols,
        count_col = "stop_sequence"
    )
    
    aggregated_stops_with_geom = pd.merge(
        stop_times[stop_cols + ["geometry"]],
        stop_arrivals,
        on = stop_cols,
        how = "inner"
    )
    
    utils.geoparquet_gcs_export(
        aggregated_stops_with_geom,
        DATA_PATH,
        f"aggregated_stops_with_geom_{selected_date}"
    )

    
def funding_table(is_current_status: bool):
    # no dollar amounts though, and there doesn't appear to
    # be any table in warehouse with dollar amounts or even NTD operating expenses
    funding = (
        tbls.mart.transit_database.bridge_organizations_x_funding_programs 
        >> filter(_.is_current == is_current_stats)
        >> select(_.organization_name, _.funding_program_name,)
        >> collect()
    )
    
    organizations = (
        tbls.mart.transit_database.dim_organizations
        >> select(_.source_record_id, _.ntd_id, _.name)
        >> rename(organization_name = _.name)
        >> collect()
    )
    
    df = pd.merge(
        funding, 
        organizations,
        on = "organization_name",
        how = "inner"
    )
    
    return df

    
if __name__ == "__main__":
    from service_increase_vars import dates, DATA_PATH
        
    # (1) Get existing service 
    for analysis_date in dates.values():
        calculate_trip_run_time(analysis_date)
    
    # (2) Get daily bus stop arrivals with geometry
    # Only do it for a weekday
    aggregate_stop_times_add_stop_geometry(dates["wed"])
    