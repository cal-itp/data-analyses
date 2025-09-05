"""
Stop summary script
"""
import pandas as pd
import geopandas as gpd
import google.auth
import gcsfs
import os
import fsspec

from shared_utils import time_helpers 

GCS_FILE_PATH  = 'gs://calitp-analytics-data/data-analyses/ahsc_grant'
analysis_date = "06_02_2025"
credentials, project = google.auth.default()
fs = gcsfs.GCSFileSystem()


def export_stop_times_as_parquet(analysis_date: str):
    """
    get stoptimes as a parquet (drop the interval BQ columns and index)
    _interval columns are used in BQ, more for storing data in specific time intervals, 
    but not useful in a parquet
    data types can be set upon reading in a csv (since csvs get confused)
    """
    pd.read_csv(
        f"{GCS_FILE_PATH}/stoptimes_{analysis_date}.csv",
        dtype = {
            "trip_id": "str",
            "stop_id": "str"
        }
    ).drop(
        columns = [
            "Unnamed: 0", "key", "_gtfs_key", 
            "arrival_time_interval", "departure_time_interval"
        ]
    ).to_parquet(f"{GCS_FILE_PATH}/stoptimes_{analysis_date}.parquet")
    
    return


def prep_trips(analysis_date: str) -> pd.DataFrame:
    """
    Prep the trips table
    """
    # Can read only specific columns for parquets
    trips = pd.read_parquet(
        f"{GCS_FILE_PATH}/trips_{analysis_date}.parquet",
        columns = ["feed_key", "name", "trip_id", 
                "trip_instance_key", "route_id", "direction_id", 
                "trip_first_departure_datetime_pacific"]
    )

    # instead of gtfs_schedule_wrangling.get_trip_time_buckets, 
    # which relies on helpers function and looks for a filepath in different folder,
    # just used the important portion, which is taking a trip_first_departure and 
    # categorizing it into time_of_day.
    trips = trips.assign(
        time_of_day = trips.apply(
            lambda x: time_helpers.categorize_time_of_day(
                x.trip_first_departure_datetime_pacific), axis=1), 
    )

    return trips


def prep_stops(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Prep the stops table and keep only stops within California county boundaries.
    """
    stops = gpd.read_parquet(
        f"{GCS_FILE_PATH}/stop_locations_{analysis_date}.parquet",
        columns = ["feed_key", "stop_id", "stop_name", "geometry"],
        storage_options={'token': credentials.token}
    )

    # Load California county boundaries GeoJSON from ArcGIS REST service
    CA_URL = "https://services1.arcgis.com/jUJYIo9tSA7EHvfZ/arcgis/rest/services/California_County_Boundaries/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    CA_county = gpd.read_file(CA_URL)

    # Reproject CA_county to match CRS of stops
    CA_county = CA_county.to_crs(stops.crs)

    # Spatial join to keep only stops within CA counties
    stops = gpd.sjoin(stops, CA_county, how="inner", predicate="within")

    # Keep only original stop columns (drop join columns)
    stops = stops[["feed_key", "stop_id", "stop_name", "geometry"]]

    # Add combined column if needed
    stops = stops.assign(
        stop_combo_col = stops.stop_id + stops.stop_name
    )

    return stops



def stop_summary_stats(
    df: pd.DataFrame,
    stops: gpd.GeoDataFrame,
    group_cols: list
) -> gpd.GeoDataFrame:
    """
    Take stop times and start counting frequency based on 
    how many scheduled arrivals it's supposed to make (# rows, so count of a column works).

    Store how many routes / route-direction it serves, but those will not be used for 
    frequency or headway.
    
    Returns a stop summary table with stop's point geometry
    """
    df2 = (
        df.groupby(group_cols)
        .agg({
            "trip_instance_key": "nunique",
            "stop_sequence": "count", # preferred
            "route_id": "nunique"
        }).reset_index()
        .rename(columns = {
            "trip_instance_key": "number_of_trips",
            "stop_sequence": "number_of_arrivals",
            "route_id": "number_of_routes",
        })
    )
    
    # when time-of-day is a column, just get the number of hours in each bin
    if "time_of_day" in df2.columns:
        df2["duration"] = df2.time_of_day.map(
            time_helpers.HOURS_BY_TIME_OF_DAY
        )
    # when time-of-day is not a column, hours are set to 24    
    else:
        df2["duration"] = 24
    
    
    df2["frequency"] = df2.number_of_arrivals.divide(df2.duration)
    df2["headway_minutes"] = 60 / df2.frequency
    
    # post groupby, each row is a stop!
    # Merge stop_geom back in.
    # After we reduced the df from a lot of rows, where each row represented a trip-stop,
    # now that every row is a stop, attach the stop's pt geometry in basically a 1:1 merge
    # stop_combo_col shows up here
    gdf = pd.merge(
        stops,
        df2,
        on = ["feed_key", "stop_id"],
        how = "inner"
    )
    
    return gdf    


# def export_gdf(gdf, filename: str):
    
#     gdf.to_parquet(f"{filename}.parquet")
    
#     fs.put(
#         f"{filename}.parquet",
#         f"{GCS_FILE_PATH}/{filename}.parquet",
#         token = credentials.token
#     )
    
#     os.remove(f"{filename}.parquet")
#     print(f"saved {GCS_FILE_PATH}/{filename}.parquet")
    
#     return

def export_gdf(gdf, filename: str):
    # Convert GeoDataFrame to GeoJSON string
    geojson_str = gdf.to_json()

    # Write GeoJSON string directly to cloud storage
    with fsspec.open(f"{GCS_FILE_PATH}/{filename}.geojson", 'w') as f_out:
        f_out.write(geojson_str)

    print(f"saved {GCS_FILE_PATH}/{filename}.geojson")

if __name__ == "__main__":
    
    
    stop_times = pd.read_parquet(
        f"{GCS_FILE_PATH}/stoptimes_{analysis_date}.parquet",
        columns = ["feed_key", "trip_id", "stop_id", "stop_sequence"]
    )

    trips = prep_trips(analysis_date)
    stops = prep_stops(analysis_date)
    
    # Merge stop_times with trips, which doesn't change the number of rows, 
    # but adds trip characteristics that are shared for all stops along the same trip
    df = pd.merge(
        stop_times,
        trips, # adding this gives me route_id, trip_instance_key, or route_type
        on = ["feed_key", "trip_id"],
        how = "left"
    )

    # All day
    daily_stats = stop_summary_stats(
        df, stops, group_cols = ["feed_key", "stop_id"])

    # Time-of-Day
    timeofday_stats = stop_summary_stats(
        df, stops, group_cols = ["feed_key", "stop_id", "time_of_day"])
    
    # TODO: export the resulting tables as geojson
    # saving a version as geoparquet for easier debugging   
    am_peak_stats = timeofday_stats[timeofday_stats['time_of_day'] == 'AM Peak']

    # Export all-day and AM Peak separately
    export_gdf(daily_stats, "stops_all_day")
    export_gdf(am_peak_stats, "stops_am_peak")