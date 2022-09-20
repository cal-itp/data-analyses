"""
Merge competitive routes info back onto all trips.
Use this df to back the stripplot
showing variability of trip service hours 
(bus_multiplier) compared to car travel.
"""
import dask.dataframe as dd
import geopandas as gpd
import intake
import os
import pandas as pd

from calitp.tables import tbl
from siuba import *

import shared_utils
import D2_setup_gmaps as setup_gmaps
from bus_service_utils import utils
from D1_setup_parallel_trips_with_stops import (ANALYSIS_DATE, COMPILED_CACHED,
                                                merge_trips_with_service_hours)

catalog = intake.open_catalog("./*.yml")
route_cols = ["calitp_itp_id", "route_id"]

def add_trip_time_of_day(trips: pd.DataFrame) -> pd.DataFrame:
    """
    Take trips table that has service hours,
    find the first departure time for trip (in stop_times)
    and add in time_of_day.
    """
    # Grab 
    stop_times = dd.read_parquet(
        f"{COMPILED_CACHED}st_{ANALYSIS_DATE}.parquet")
    
    stop_times2 = setup_gmaps.grab_first_stop_time(stop_times)
    
    keep_cols = ["trip_key", "trip_first_departure",
                 "trip_departure", "trip_first_departure_hour"]
    
    # Somehow, stop_times2 is not unique
    # There are multiple route_keys, feed_keys, but departure hour is the same
    # but the departure time is slightly different - keep the later departure time
    stop_times3 = (stop_times2[keep_cols]
                   .sort_values(["trip_key", "trip_first_departure"], 
                               ascending=[True, False])
                   .drop_duplicates(subset=["trip_key"])
                   .reset_index(drop=True)
                  )
    
    df = pd.merge(
        trips, 
        stop_times3,
        on = "trip_key",
        how = "inner",
        # many on left because trip_key / trip_id can be shared across 
        # multiple route_names
        validate = "m:1"
    )
    
    # Add time-of-day
    df = df.assign(
        time_of_day = df.apply(
            lambda x: shared_utils.rt_utils.categorize_time_of_day(
                x.trip_first_departure), 
            axis=1)
    )
    
    return df
    
    
def add_quantiles(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        service_hours = df.service_hours.round(2),
    )
    
    group_cols = ["calitp_itp_id", "route_id"]
        
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
    

def merge_in_competitive_routes(df: pd.DataFrame, threshold: float) -> gpd.GeoDataFrame:
    # Tag as competitive
    gmaps_results = catalog.gmaps_results.read()

    # Merge in the competitive trip info
    gdf = pd.merge(gmaps_results[route_cols + ["car_duration_hours"]],
                   df, 
                   on = route_cols,
                   how = "outer",
                   validate = "1:m",
                   indicator=True
    )

    gdf = gdf.assign(
        bus_multiplier = gdf.service_hours.divide(gdf.car_duration_hours).round(2),
        # difference (in minutes) between car and bus
        bus_difference = ((gdf.service_hours - gdf.car_duration_hours) * 60).round(1),
        num_trips = gdf.groupby(route_cols)["trip_id"].transform("nunique")
    )

    gdf = gdf.assign(
        competitive = gdf.apply(lambda x: 1 if x.bus_multiplier <= threshold 
                               else 0, axis=1)
    )
        
    # Calculate % of trips below threshold
    gdf = gdf.assign(
        num_competitive = gdf.groupby(route_cols)["competitive"].transform("sum")
   
    )
        
    gdf = gdf.assign(
        pct_trips_competitive = gdf.num_competitive.divide(gdf.num_trips).round(3)
    )    
    
    return gdf


def designate_plot_group(df: gpd.GeoDataFrame, diff_cutoffs: dict) -> gpd.GeoDataFrame:
    # Add plot group, since stripplot can get crowded, plot 15 max?
    
    for c in ["bus_difference"]:
        df = df.assign(
            minimum = df.groupby(route_cols)[c].transform("min"),
            maximum = df.groupby(route_cols)[c].transform("max"),
        )
        df = df.assign(
            spread = (df.maximum - df.minimum) 
        ).rename(columns = {"spread": f"{c}_spread"}
                ).drop(columns = ["minimum", "maximum"])

    df2 = (df.assign(
               # Break it up into short / medium / long routes instead of plot group
               max_trip_hrs = df.groupby(route_cols)["service_hours"].transform("max"),
          )
           .reset_index(drop=True)
    )
    
    
    df2 = df2.assign(
        route_group = df2.apply(lambda x: "short" if x.max_trip_hrs <= 1.0
                               else "medium" if x.max_trip_hrs <=1.5
                               else "long", axis=1)
    )
    
    df2 = df2.assign(
        max_trip_route_group = df2.groupby(route_cols)["service_hours"].transform("max") 
    )
    
    # Merge back in
    df3 = pd.merge(
        df, 
        df2[["calitp_itp_id", "route_id", "route_group", 
             "max_trip_hrs", "max_trip_route_group"]].drop_duplicates(), 
        on = route_cols,
        how = "left",
        validate = "m:1"
    )
    
    # Add cut-off thresholds by route_group
    # Calculate a certain threshold of competitive trips within that cut-off, and
    # call those "viable"
    df4 = df3.assign(
        below_cutoff = df3.apply(lambda x: 
                                 1 if x.bus_difference <= diff_cutoffs[x.route_group]
                                 else 0, axis=1),
        num_trips = df3.groupby(route_cols)["trip_id"].transform("count")
    )
    

    df4["below_cutoff"] = df4.groupby(route_cols)["below_cutoff"].transform("sum")
    df4["pct_below_cutoff"] = df4.below_cutoff.divide(df4.num_trips)
    
    return df4


# Use agency_name from our views.gtfs_schedule.agency instead of Airtable?
def merge_in_agency_name(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    agency_names = shared_utils.portfolio_utils.add_agency_name(
        selected_date = ANALYSIS_DATE)
    
    df2 = pd.merge(
        df,
        agency_names,
        on = "calitp_itp_id",
        how = "left",
        validate = "m:1",
    )
    
    return df2
    
    
def merge_in_airtable(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Don't use name from Airtable. But, use district.
    caltrans_districts = shared_utils.portfolio_utils.add_caltrans_district()
                            
    # Airtable gives us fewer duplicates than doing tbl.gtfs_schedule.agency()
    # But naming should be done with tbl.gtfs_schedule.agency because that's what's used
    # in speedmaps already. Need standardization
    df2 = pd.merge(
        df,
        caltrans_districts,
        on = "calitp_itp_id",
        how = "left",
        validate = "m:1",
    )
    
    return df2
    

if __name__ == "__main__":    
    
    # Import all service hours associated with trip
    trip_service_hours = merge_trips_with_service_hours(ANALYSIS_DATE)

    # Grab first stop time for each trip, get departure hour
    df = add_trip_time_of_day(trip_service_hours)
    
    # Calculate p25, p50, p75 quantiles for the route
    df2 = add_quantiles(df)
    
    # Tag competitive routes based on threshold specified (service_hours/car_duration_hours)
    df3 = merge_in_competitive_routes(df2, threshold=1.2)
    
    # Break up plot groups by route travel time
    diff_cutoffs = {
        "short": 20,
        "medium": 30,
        "long": 40,
    }
    df4 = designate_plot_group(df3, diff_cutoffs)
    
    # Merge in agency name
    df5 = merge_in_agency_name(df4)
    
    # Merge in Caltrans district from Airtable
    df6 = merge_in_airtable(df5)
    
    shared_utils.utils.geoparquet_gcs_export(
        df6, 
        utils.GCS_FILE_PATH, 
        f"competitive_route_variability_{ANALYSIS_DATE}")