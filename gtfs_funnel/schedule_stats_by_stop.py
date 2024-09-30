"""
Add some GTFS schedule derived metrics
by stop (arrivals, number of trips/routes served,
service hours).

This is stop grain version of schedule_stats_by_route_direction.
Grain: schedule_gtfs_dataset_key-stop_id
"""
import datetime
import geopandas as gpd
import pandas as pd

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import helpers

def stats_for_stop(
    df: pd.DataFrame, 
    group_cols: list
) -> pd.DataFrame:
    """
    List the stats we'd like to calculate for each stop.
    """
    df2 = (
        df
        .groupby(group_cols, group_keys=False)
        .agg({
            "trip_id": "nunique", 
            "route_id": "nunique",
            "route_type": lambda x: list(sorted(set(x))),
            "departure_sec": "count",
            "departure_hour": "nunique"
        }).reset_index()
        .rename(columns = {
            "departure_sec": "n_arrivals",
            "departure_hour": "n_hours_in_service",
            "trip_id": "n_trips",
            "route_id": "n_routes",
            "route_type": "route_types_served"
        })
    )
    
    # Instead of producing list, we want to show values like 0, 3 instead of [0, 3]
    # portal users can see combinations more quickly
    # and access particular rows using str.contains
    df2 = df2.assign(
        route_types_served = df2.route_types_served.str.join(", ")
    )
    
    return df2


def schedule_stats_by_stop(
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Import stop_times, trips, and stops.
    Merge and aggregate for stop-level schedule stats.
    
    Calculate some extra stats from other schedule tables,
    such as how many route_ids and route_types the
    stop shares.
    """
    # departure hour nunique values can let us know span of service
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "stop_id", "trip_id", 
                   "departure_sec", "departure_hour"],
        with_direction = False,
        get_pandas = True
    )

    # include route info so we know how many trips, routes,
    # route_types that the stop serves
    # stop can serve 1 light rail + 5 bus routes vs 6 bus routes
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "feed_key", 
                   "trip_id", 
                   "route_id", "route_type"],
        get_pandas = True,
    )

    stops = helpers.import_scheduled_stops(
        analysis_date,
        columns = ["feed_key", "stop_id", "stop_name", "geometry"],
        get_pandas = True,
        crs = WGS84
    )
    
    stop_df = pd.merge(
        stop_times,
        trips,
        on = ["feed_key", "trip_id"],
        how = "inner"
    ).pipe(
        stats_for_stop, 
        group_cols = ["schedule_gtfs_dataset_key", "feed_key", "stop_id"]
    )
    
    
    stop_gdf = pd.merge(
        stops,
        stop_df,
        on = ["feed_key", "stop_id"],
        how = "inner"
    ).drop(columns = "feed_key")

    # Fix order of columns
    col_order = [
        c for c in stop_gdf.columns 
        if c not in ["schedule_gtfs_dataset_key", "geometry"]
    ]

    stop_gdf = stop_gdf.reindex(
        columns = ["schedule_gtfs_dataset_key", *col_order, "geometry"] 
    )
    
    return stop_gdf


if __name__ == "__main__":

    from update_vars import analysis_date_list, RT_SCHED_GCS, GTFS_DATA_DICT
    
    EXPORT_FILE = GTFS_DATA_DICT.rt_vs_schedule_tables.sched_stop_metrics
    
    for analysis_date in analysis_date_list:
        start = datetime.datetime.now()
        
        gdf = schedule_stats_by_stop(analysis_date)
                
        utils.geoparquet_gcs_export(
            gdf,
            RT_SCHED_GCS,
            f"{EXPORT_FILE}_{analysis_date}"
        )
        
        end = datetime.datetime.now()
        print(f"schedule stop stats for {analysis_date}: {end - start}")