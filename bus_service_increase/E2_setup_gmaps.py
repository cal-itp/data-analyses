"""
Additional data processing to get df into 
structure that's needed to make Google Directions API requests,
with `origin`, `destination`, `departure_time`, `waypoints` args.
"""
import dask.dataframe as dd
import geopandas as gpd
import intake
import pandas as pd
import zlib

from datetime import timedelta
from calitp.storage import get_fs

from bus_service_utils import utils
from E1_setup_parallel_trips_with_stops import ANALYSIS_DATE, COMPILED_CACHED

fs = get_fs()
catalog = intake.open_catalog("./*.yml")

# Define grouping of cols that captures each trip
trip_group = ["calitp_itp_id", "route_id", "trip_id", "shape_id", "service_hours"]


def grab_first_stop_time(stop_times: dd.DataFrame) -> pd.DataFrame:
    # Only keep first stop
    first_stop = (stop_times.groupby("trip_key")["stop_sequence"].min()
            .reset_index()
    )
        
    first_stop_time = dd.merge(
        stop_times,
        first_stop,
        on = ["trip_key", "stop_sequence"],
        how = "inner"
    ).rename(columns = {"departure_ts": "trip_first_departure"}
            ).compute()
    
    df = first_stop_time.assign(
        trip_departure = pd.to_datetime(
            first_stop_time.trip_first_departure, 
            unit = "s", errors = "coerce").dt.time,
        trip_first_departure_hour = pd.to_datetime(
            first_stop_time.trip_first_departure, unit='s', 
            errors="coerce").dt.hour.astype("Int64"),
    
    )
    
    # trip first departure shows 1970 date. Reconstruct with service date 
    # and change to datetime
    df2 = df.assign(
        trip_first_departure = df.apply(
            lambda x: str(x.service_date) + " " + str(x.trip_departure), 
            axis=1),
    )

    df2 = df2.assign(
        trip_first_departure = pd.to_datetime(
            df2.trip_first_departure, errors="coerce")
    )
    
    return df2
    
    

def stop_time_for_selected_trip(stop_times: dd.DataFrame, 
                                selected_trip: pd.DataFrame
                               ) -> gpd.GeoDataFrame:
    stop_times_for_one_trip = dd.merge(
        selected_trip[["trip_key"]].drop_duplicates(),
        stop_times,
        on = "trip_key",
        how = "inner",
    )
    
    df = grab_first_stop_time(stop_times_for_one_trip)
        
    return df
    

def data_wrangling(df: gpd.GeoDataFrame) ->pd.DataFrame:
    # Turn shapely point geometry into floats, then tuples for gmaps
    # Make tuples instead of just floats
    df2 = (df.assign(
            # Typically, it's (x, y) = (lon, lat), but Google Maps takes (lat, lon)
            geom_flipped = df.apply(lambda row: [row.geometry.y, row.geometry.x], axis=1)        
        ).drop(columns = "geometry")
    )
    
    df3 = pd.DataFrame(df2)
        
    return df3


# If you have more than 25 waypoints, you have to break it into 2 requests
# (x - first_waypoint_rank) / divisible_by) + 1 = 25
# 25 waypoints in the middle, then allow origin and destination to bookend it
# origin = stop1
# destination is >= last_waypoint_rank & <= last_waypoint_rank + divisible_by
# can allow destination to be last_waypoint_rank

def last_waypoint_and_destination_rank(divisible_by: int) -> (int, int):
    first = 1
    first_waypoint = first + divisible_by
    # (x- first_waypoint) / divisible_by) + 1 = 25
    # Solve for x
    x = (divisible_by * 24) + first_waypoint
    return x, x + divisible_by


def subset_stops(df: pd.DataFrame) -> pd.DataFrame:
    # https://stackoverflow.com/questions/25055712/pandas-every-nth-row
    # df = df.iloc[::3]
    df["stop_rank"] = df.groupby(trip_group).cumcount() + 1
    df["max_stop"] = df.groupby(trip_group)["stop_rank"].transform("max")
    
    # every 3rd for shorter routes
    # every 4th, 5th for longer ones...stay under 25 waypoints
    every_third, dest3_max = last_waypoint_and_destination_rank(divisible_by=3)
    every_fourth, dest4_max = last_waypoint_and_destination_rank(divisible_by=4)
    every_fifth, dest5_max = last_waypoint_and_destination_rank(divisible_by=5)
    # Do we want to go beyond every 5th? Maybe need to investigate 
    
    def tag_waypoints(row):
        flag = 0
        if row.max_stop <= dest3_max:
            # Want remainder of 1, because if we are keeping stop 1, 
            # then 3rd stop is stop 4.
            if row.stop_rank % 3 == 1:
                flag=1
        elif (row.max_stop > dest3_max) and (row.max_stop <= dest4_max):
            if row.stop_rank % 4 == 1:
                flag = 1
        elif (row.max_stop > dest4_max) and (row.max_stop <= dest5_max):
            if row.stop_rank % 5 == 1:
                flag = 1
        return flag

    df["is_waypoint"] = df.apply(tag_waypoints, axis=1)
    df["is_od"] = df.apply(lambda x: 
                           1 if ((x.stop_rank==1) or (x.stop_rank==x.max_stop))
                           else 0, axis=1)
                           
    # we also have origin and destination
    subset = (df[(df.is_waypoint==1) | 
                (df.is_od==1) 
               ].drop(columns = ["stop_rank", "max_stop"])
              .reset_index(drop=True)
             )
    
    return subset


def select_origin_destination(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.is_od==1].reset_index(drop=True)
    
    # Wrangle it so there are columns with previous point and 
    # current point in the same row
    df = df.assign(
        previous = (df.sort_values(trip_group + ["stop_sequence"])
                        .groupby(trip_group)["geom_flipped"]
                        .apply(lambda x: x.shift(1))
                       ),
    )
    
    # Only keep the observation that has start_geom (drop the first obs for each trip grouping)
    df2 = (df[df.previous.notna()]
           [trip_group + ["trip_departure", "trip_first_departure", 
                          "trip_first_departure_hour", "geom_flipped", "previous"]]
           .assign(
               departure_in_one_year = df.apply(
                   lambda x: x.trip_first_departure + timedelta(weeks=52), axis=1)
           ).reset_index(drop=True)
           .rename(columns = {"geom_flipped": "destination", 
                              "previous": "origin"})
          )
        
    return df2


def assemble_waypoints(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df.is_waypoint==1) & (df.is_od==0)].reset_index(drop=True)
    
    # Take all the stops in between origin/destination, put tuples into a list
    #https://stackoverflow.com/questions/22219004/how-to-group-dataframe-rows-into-list-in-pandas-groupby
    waypoint_df = (
        df.sort_values(trip_group + ["stop_sequence"])
        .groupby(trip_group)
        .agg({"geom_flipped": lambda x: list(x)})
        .reset_index()
        .rename(columns = {"geom_flipped": "waypoints"})
    )
    
    return waypoint_df


if __name__ == "__main__":
    trip = catalog.trips_with_stops.read()
    
    stop_times = dd.read_parquet(
        f"{COMPILED_CACHED}st_{ANALYSIS_DATE}.parquet")

    df = stop_time_for_selected_trip(stop_times, trip)

    df2 = data_wrangling(df)

    # Subset to every 3rd
    subset = subset_stops(df2)
    
    # Grab origin/destination pair, and wrangle into separate columns
    od = select_origin_destination(subset)
    
    # Assemble waypoints as a list
    waypoints = assemble_waypoints(subset)

    final = pd.merge(
        od, 
        waypoints, 
        on = trip_group, 
        how = "inner", 
        validate = "1:1"
    ).sort_values(trip_group).reset_index(drop=True)
    
    # all the "geometry" kind of columns are tuples or lists now    
    final = final.assign(
        identifier = final.calitp_itp_id.astype(str).str.cat(
            [final.route_id, final.trip_id, final.shape_id], sep = "__"
        ),
        # this checksum hash always give same value if the same 
        # combination of strings are given
        identifier_num = final.apply(
            lambda x: zlib.crc32(
                (str(x.calitp_itp_id) + 
                 x.route_id + x.trip_id + x.shape_id)
                .encode("utf-8")), axis=1),
    )

    # These weird characters are preventing the name from being used in GCS
    final = final.assign(
        identifier = (
            final.identifier.str.replace('@', '', regex=False)
                  .str.replace('.', '', regex=False)
                  .str.replace('[', '', regex=False)
                  .str.replace(']', '', regex=False)
                  .str.replace('/', '', regex=False)
                     
                 )
    ) 
    
    
    final.to_parquet(f"{utils.GCS_FILE_PATH}gmaps_df_{ANALYSIS_DATE}.parquet")
    print("Exported to GCS")
    