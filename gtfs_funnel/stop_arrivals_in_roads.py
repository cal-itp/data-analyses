"""
Select a date and look at peak arrivals
for road segments.
"""
import datetime
import geopandas as gpd
from itertools import product
import pandas as pd

from segment_speed_utils import (helpers, 
                                 gtfs_schedule_wrangling, 
                                )
from segment_speed_utils.project_vars import PROJECT_CRS   
from shared_utils import rt_dates, time_helpers
from update_vars import SHARED_GCS, SCHED_GCS

road_cols = ["linearid", "mtfcc", "fullname"]
road_segment_cols = road_cols + ["segment_sequence"]

import google.auth
credentials, _ = google.auth.default()

def buffer_roads(road_file: str, buffer_meters: int) -> gpd.GeoDataFrame:
    """
    Buffer 2 mile road segments
    """
    df = gpd.read_parquet(
        f"{SHARED_GCS}"
        f"{road_file}.parquet",
        columns = road_segment_cols + ["geometry"],
        storage_options={"token": credentials.token}
    ).to_crs(PROJECT_CRS)
    
    df = df.assign(
        road_meters = df.geometry.length,
        geometry = df.geometry.buffer(buffer_meters)
    )
    
    return df


def peak_stop_arrivals(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Get a stop gdf with peak arrivals for a given day.
    """
    operator_cols = ["schedule_gtfs_dataset_key", "feed_key",]
    stop_cols = operator_cols + ["trip_instance_key", "stop_id"]
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        with_direction=True,
        get_pandas = True,
        columns = stop_cols
    )

    sched_time_of_day = gtfs_schedule_wrangling.get_trip_time_buckets(
        analysis_date
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    )[["trip_instance_key", "peak_offpeak"]]

    stop_times2 = pd.merge(
        stop_times, 
        sched_time_of_day, 
        on = "trip_instance_key"
    ).query('peak_offpeak == "peak"')
    
    stop_arrivals = gtfs_schedule_wrangling.stop_arrivals_per_stop(
        stop_times2,
        group_cols = operator_cols + ["peak_offpeak", "stop_id"],
        count_col = "trip_instance_key"
    )

    stop_geom = helpers.import_scheduled_stops(
        analysis_date,
        columns = ["feed_key", "stop_id", "stop_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = True
    )

    stops_with_arrivals = pd.merge(
        stop_geom,
        stop_arrivals,
        on = ["feed_key", "stop_id"],
        how = "inner"
    )    
    
    return stops_with_arrivals

def spatial_join_stop_arrivals_to_roads(
    road_file: str,
    analysis_date: str,
    buffer_meters: int
):
    roads = buffer_roads(road_file, buffer_meters)
    arrivals = peak_stop_arrivals(analysis_date)
    
    # Spatial join - find peak arrivals in road segments
    road_arrivals = gpd.sjoin(
        roads,
        arrivals,
        how = "inner",
        predicate = "intersects"
    ).drop(columns = ["index_right"])
    
    road_arrivals = road_arrivals.assign(
        road_meters = road_arrivals.geometry.length
    )
    
    # Count total arrivals across all stops for a road segment
    total_arrivals = (road_arrivals
                      .groupby(road_segment_cols,
                               observed=True, group_keys = False)
                      .agg({
                          "n_arrivals": "sum",
                          "stop_key": "nunique",
                          "road_meters": "mean"
                      }).reset_index()
                      .rename(columns = {"stop_key": "n_stops"})
                     )
    
    # Convert arrivals into frequency 
    peak_hours = sum(v for k, v in time_helpers.HOURS_BY_TIME_OF_DAY.items() 
             if k in time_helpers.PEAK_PERIODS) 

    METERS_PER_MILE = 1609.34
    
    total_arrivals = total_arrivals.assign(
        frequency = (total_arrivals.n_arrivals/ peak_hours).round(2),
        stops_per_mi = total_arrivals.n_stops.divide(
            total_arrivals.road_meters) * METERS_PER_MILE
    )
    
    total_arrivals.to_parquet(
        f"{SCHED_GCS}corridor_frequency/"
        f"arrivals_by_road_segment_{analysis_date}.parquet"
    )
    
    return


def corridor_frequency_by_year(
    year: int
) -> pd.DataFrame:
    """
    Average the road segment's n_arrivals, n_stops, etc
    across multiple dates.
    """
    files_in_folder = fs.ls(
        f"{SCHED_GCS}corridor_frequency/"
    )
    
    files_to_use = [
        f for f in files_in_folder if str(year) in f
    ]
    
    # Read in all the frequency for that year and concatenate
    df = pd.concat([
        pd.read_parquet(
            f"gs://{f}") 
        for f in files_to_use
    ], axis=0, ignore_index=True)
    
    # Take the mean across dates
    df2 = (df.groupby(road_segment_cols, 
                 observed=True, group_keys=False)
        .agg({
             "n_arrivals": "mean",
             "n_stops": "mean",
             "frequency": "mean",
            "stops_per_mi": "mean",
        }).reset_index()
    )
    
    # No decimals - round up to nearest integer
    round_and_integrify = ["n_arrivals", "n_stops"]
    round_cols = ["frequency", "stops_per_mi"]

    df2[round_and_integrify] = df2[round_and_integrify].round(0).astype(int)
    df2[round_cols] = df2[round_cols].round(2)
    
    return df2


if __name__ == "__main__":
    
    from update_vars import GTFS_DATA_DICT
    
    # Set the months we want quarterly data for
    months = ["jan", "apr", "jul", "oct"]
        
    # Set the date combinations, like Jan2023, Apr2023, Jul2023, etc
    analysis_date_list = [
        rt_dates.DATES[f"{m}{y}"] for y, m in product(rt_dates.years_available, months)
        if f"{m}{y}" in rt_dates.DATES.keys()
    ]

    # Only run this script for a new quarterly date when needed
    for analysis_date in analysis_date_list:
        start = datetime.datetime.now()

        print(f"Analysis Date: {analysis_date}")

        ROAD_BUFFER_METERS = 50
        ROAD_FILE = GTFS_DATA_DICT.shared_data.road_segments_twomile

        spatial_join_stop_arrivals_to_roads(
            ROAD_FILE, analysis_date, 
            ROAD_BUFFER_METERS
        )

        end = datetime.datetime.now()
        print(f"attach peak stop arrivals to roads: {end - start}")

    
    for year in rt_dates.current_year:
        
        start = datetime.datetime.now()
        
        road_stats = corridor_frequency_by_year(year)
        road_stats.to_parquet(f"{SCHED_GCS}arrivals_by_road_segment_{year}.parquet")
        
        end = datetime.datetime.now()
        print(f"road stats for year {year}: {end - start}")