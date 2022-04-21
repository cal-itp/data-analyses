"""
Merge competitive routes info back onto all trips.

Use this df to back the stripplot
showing variability of trip service hours 
(bus_multiplier) compared to car travel.
"""
import intake
import math 
import os
import pandas as pd

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

from calitp.tables import tbl
from siuba import *

import setup_parallel_trips_with_stops
import utils

catalog = intake.open_catalog("./*.yml")

def pare_down_trips(df):
    # This dataset is trip-stop level
    # Keep unique trip
    df2 = (df.drop(columns = ["stop_id", "stop_sequence", "date", 
                              "is_in_service", "day_name", "departure_time"])
           .drop_duplicates(subset=["calitp_itp_id", "route_id", "trip_id"])
           .reset_index(drop=True)
          )
    
    return df2


def time_of_day(row):
    if (row.departure_hour <= 6) or (row.departure_hour >= 20):
        return "Owl Service"
    elif (row.departure_hour > 6) and (row.departure_hour <= 9):
        return "AM Peak"
    elif (row.departure_hour > 9) and (row.departure_hour <= 16):
        return "Midday"
    elif (row.departure_hour > 16) and (row.departure_hour <= 19):
        return "PM Peak"

    
def add_quantiles_timeofday(df):
    df = df.assign(
        departure_hour = pd.to_datetime(
            df.trip_first_departure_ts, unit='s').dt.hour,
        service_hours = df.service_hours.round(2),
    )
    
    group_cols = ["calitp_itp_id", "route_id"]
    
    df["time_of_day"] = df.apply(lambda x: time_of_day(x), axis=1)        
    
    # Sort routes within an operator a certain way - fastest trip in ascending order?
    # Just plot routes 10 at a time for readability
    df2 = (df.sort_values(group_cols + ["service_hours"], 
                          ascending=[True, True, True])
           .reset_index(drop=True)
    )
    
    # Identify the 25th, 50th, 75th percentile trips
    quantile_dict = {}
    for i in [25, 50, 75]:
        # the groupby / quantile destroys index, need to merge in separately as df
        quantile_dict[i] = (df2.groupby(group_cols)["service_hours"]
                .quantile(i/100).reset_index()
                .rename(columns = {"service_hours": f"p{i}"})
               ) 
        
    # Merge the quantile dfs in
    df3 = (df2.merge(quantile_dict[25],
                    on = group_cols, how = "left", validate = "m:1"
            ).merge(quantile_dict[50],
                    on = group_cols, how = "left", validate = "m:1"
            ).merge(quantile_dict[75],
                    on = group_cols, how = "left", validate = "m:1"
                   )
          )
    
    df3["abs_diff"] = abs(df3.service_hours - df3.p50)
    df3["min_diff"] = df3.groupby(group_cols)["abs_diff"].transform("min")
    df3 = df3.assign(
        p50_trip = df3.apply(lambda x: 1 if x.min_diff == x.abs_diff
                                else 0, axis=1)
    ).drop(columns = ["abs_diff", "min_diff"])
    
    return df3


def merge_in_competitive_routes(df):
    # Tag as competitive
    gdf = catalog.gmaps_results.read()
    
    route_cols = ["calitp_itp_id", "route_id"]
    trip_cols = route_cols + ["shape_id", "trip_id"]
    
    # Merge in the competitive trip info
    df2 = pd.merge(df, 
                   gdf[trip_cols + ["competitive"]],
                   on = trip_cols,
                   how = "left",
                   validate = "m:1",
    ).rename(columns = {"competitive": "fastest_trip"})
    
    # Merge in route-level info
    df3 = pd.merge(df2,
                   gdf[route_cols + ["car_duration_hours", "competitive"]],
                   on = route_cols,
                   how = "left",
                   validate = "m:1"
    ).rename(columns = {"competitive": "competitive_route"})
    
    df3 = df3.assign(
        competitive_route = df3.competitive_route.fillna(0).astype(int),
        fastest_trip = df3.fastest_trip.fillna(0).astype(int),
        bus_multiplier = df3.service_hours.divide(df3.car_duration_hours).round(2),
        bus_difference = (df3.service_hours - df3.car_duration_hours).round(2),
    )
    
    
    # Calculate % of trips below threshold
    df4 = df3.assign(
        num_trips = df3.groupby(route_cols)["trip_id"].transform("nunique"),
        is_competitive = df3.apply(lambda x: 1 if x.bus_multiplier <= 2 
                                   else 0, axis=1)    
    )
    
    df4["num_competitive"] = df4.groupby(route_cols)["is_competitive"].transform("sum")
    
    df4 = df4.assign(
        pct_trips_competitive = df4.num_competitive.divide(df4.num_trips).round(3)
    ).drop(columns = ["is_competitive"])
    
    return df4


def designate_plot_group(df):
    # Add plot group, since stripplot can get crowded depending 
    # on how many routes plotted
    # But, there's also some where bus_multiplier can't be derived, if Google API didn't return results
    # Use 1 trip to designate
    for c in ["bus_multiplier", "bus_difference"]:
        df = df.assign(
            minimum = df.groupby(["calitp_itp_id", "route_id"])[c].transform("min"),
            maximum = df.groupby(["calitp_itp_id", "route_id"])[c].transform("max"),
        )
        df = df.assign(
            spread = (df.maximum - df.minimum).round(3)
        ).rename(columns = {"spread": f"{c}_spread"}).drop(columns = ["minimum", "maximum"])

    df2 = (df[df.bus_multiplier.notna()]
           [["calitp_itp_id", "route_id", "pct_trips_competitive", 
             "bus_multiplier_spread", "bus_difference_spread"]]
           .drop_duplicates()
           .sort_values(["calitp_itp_id", "pct_trips_competitive", "bus_multiplier_spread"], 
                        ascending=[True, False, False])
           .reset_index(drop=True)
          )
    
    '''
    # Find the trip closest to p50
    df2 = (df[(df.service_hours == df.p50) & (df.bus_multiplier.notna())]
           .sort_values(["calitp_itp_id", "route_id", "departure_hour"])
           .drop_duplicates(subset=["calitp_itp_id", "route_id"])
           .reset_index(drop=True)
    )
    '''
    df2["order"] = df2.groupby('calitp_itp_id')["pct_trips_competitive"].cumcount()
    # use -1 to round to nearest 10s
    # since we generated cumcount(), which orders it from 1, 2, ...n for each group
    # if we want groups of 10 per chart, can just round to nearest 10s
    df2["plot_group"] = df2.apply(lambda x: math.floor(x.order / 10.0), axis=1)
    
    # Merge plot_group in for all routes
    df3 = pd.merge(
        df, 
        df2[["calitp_itp_id", "route_id", "plot_group"]], 
        on = ["calitp_itp_id", "route_id"],
        how = "left",
        validate = "m:1",
    )
    
    return df3


def merge_in_airtable_name_district(df):
    airtable_organizations = (
        tbl.airtable.california_transit_organizations()
        >> select(_.itp_id, _.name, _.caltrans_district
                  , _.drmt_organization_name)
        >> collect()
        >> filter(_.itp_id.notna())
    ).sort_values(["itp_id", "name"]).drop_duplicates(
        subset="itp_id").reset_index(drop=True)
                            
    # Airtable gives us fewer duplicates than doing tbl.gtfs_schedule.agency()
    df2 = pd.merge(
        df,
        airtable_organizations.rename(columns = {"itp_id": "calitp_itp_id"}),
        on = "calitp_itp_id",
        how = "left",
        validate = "m:1",
    )
    
    return df2


if __name__ == "__main__":
    '''
    DATA_PATH = f"{utils.GCS_FILE_PATH}2022_Jan/"

    # Read in intermediate parquet for trips on selected date
    trips = pd.read_parquet(f"{DATA_PATH}trips_joined_thurs.parquet")

    SELECTED_DATE = '2022-1-6' #warehouse_queries.dates['thurs']

    # Attach service hours
    # This df is trip_id-stop_id level
    trips_with_service_hrs = setup_parallel_trips_with_stops.grab_service_hours(
        trips, SELECTED_DATE)

    trips_with_service_hrs.to_parquet("./data/trips_with_service_hours.parquet")
    '''
    
    df = pd.read_parquet(
        "./data/trips_with_service_hours.parquet")
    df2 = pare_down_trips(df)
    df3 = add_quantiles_timeofday(df2)
    df4 = merge_in_competitive_routes(df3)
    df5 = designate_plot_group(df4)
    df6 = merge_in_airtable_name_district(df5)
    
    df6.to_parquet("./data/stripplot_trips.parquet")