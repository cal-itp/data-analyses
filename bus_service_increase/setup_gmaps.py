"""
Additional data processing to get df into 
structure that's needed to make Google Directions API requests,
with `origin`, `destination`, `departure_time`, `waypoints` args.
"""
import geopandas as gpd
import intake
import pandas as pd

from datetime import timedelta

import utils

catalog = intake.open_catalog("./*.yml")

# Define grouping of cols that captures each trip
trip_group = ["calitp_itp_id", "route_id", "trip_id", "shape_id", "service_hours"]

def data_wrangling(df):
    df["first_departure"] = pd.to_datetime(df.trip_first_departure_ts, unit='s').dt.time
    
    # If departure_time isn't available, or, when parsed, will be outside 0-23 hrs
    # use trip_first_departure_ts instead.
    def assemble_trip_departure_datetime(x):
        if (x.departure_time != None) and int(x.departure_time.split(':')[0]) < 23:
            return  str(x.date) + " " + str(x.departure_time)
        else: 
            return str(x.date) + " " + str(x.first_departure)


    df["trip_departure"] = df.apply(lambda x: assemble_trip_departure_datetime(x), axis=1)    
    
    # Turn shapely point geometry into floats, then tuples for gmaps
    # Make tuples instead of just floats
    df2 = (df.assign(
            trip_departure = pd.to_datetime(df.trip_departure, errors="coerce"),
            # Typically, it's (x, y) = (lon, lat), but Google Maps takes (lat, lon)
            geometry = df.apply(lambda row: (row.geometry.y, row.geometry.x), axis=1)        
        ).drop(columns = ["date", "is_in_service", 
                          "departure_time", "trip_first_departure_ts", 
                          "first_departure"])
    )
        
    return df2


# If you have more than 25 waypoints, you have to break it into 2 requests
# (x - first_waypoint_rank) / divisible_by) + 1 = 25
# 25 waypoints in the middle, then allow origin and destination to bookend it
# origin = stop1
# destination is >= last_waypoint_rank & <= last_waypoint_rank + divisible_by
# can allow destination to be last_waypoint_rank

def last_waypoint_and_destination_rank(divisible_by):
    first = 1
    first_waypoint = first + divisible_by
    # (x- first_waypoint) / divisible_by) + 1 = 25
    # Solve for x
    x = (divisible_by * 24) + first_waypoint
    return x, x+divisible_by


def subset_stops(df):
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
            # Want remainder of 1, because if we are keeping stop 1, then 3rd stop is stop 4.
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


def select_origin_destination(df):
    df = df[df.is_od==1].reset_index(drop=True)
    
    # Wrangle it so there are columns with previous point and current point in the same row
    df = df.assign(
        previous = (df.sort_values(trip_group + ["stop_sequence"])
                        .groupby(trip_group)["geometry"]
                        .apply(lambda x: x.shift(1))
                       ),
    )
    
    # Only keep the observation that has start_geom (drop the first obs for each trip grouping)
    df2 = (df[df.previous.notna()]
           [trip_group + ["day_name", "trip_departure", "geometry", "previous"]]
           .assign(
               departure_in_one_year = df.apply(
                   lambda x: x.trip_departure + timedelta(weeks=52), axis=1)
           ).reset_index(drop=True)
           .rename(columns = {"geometry": "destination", 
                              "previous": "origin"})
          )
    
    return df2


def assemble_waypoints(df):
    df = df[(df.is_waypoint==1) & (df.is_od==0)].reset_index(drop=True)
    
    # Take all the stops in between origin/destination, put tuples into a list
    #https://stackoverflow.com/questions/22219004/how-to-group-dataframe-rows-into-list-in-pandas-groupby
    waypoint_df = (
        df.sort_values(trip_group + ["stop_sequence"])
        .groupby(trip_group)
        .agg({"geometry": lambda x: list(x)})
        .reset_index()
        .rename(columns = {"geometry": "waypoints"})
    )
    
    return waypoint_df


def make_gmaps_df():
    df = catalog.parallel_trips_with_stops.read()
    df = data_wrangling(df)
    
    # Subset to every 3rd
    subset = subset_stops(df)
    
    # Grab origin/destination pair, and wrangle into separate columns
    od = select_origin_destination(subset)
    
    # Assemble waypoints as a list
    waypoints = assemble_waypoints(subset)

    final = pd.merge(
        od, 
        waypoints, 
        on = trip_group, 
        how = "inner", validate = "1:1"
    ).sort_values(trip_group).reset_index(drop=True)
    
    # all the "geometry" kind of columns are tuples or lists now
    final = pd.DataFrame(final)
    
    final["identifier"] = (
        final.calitp_itp_id.astype(str).str.cat(
            [final.route_id, final.trip_id, final.shape_id], sep ="__")
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
    
    final.to_parquet(f"{utils.GCS_FILE_PATH}gmaps_df.parquet")
    print("Exported to GCS")
    
if __name__ == "__main__":
    make_gmaps_df()
    