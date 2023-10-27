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

from calitp_data_analysis.tables import tbls
from siuba import *

import D2_setup_gmaps as setup_gmaps
import E2_aggregated_route_stats as aggregated_route_stats 
from bus_service_utils import utils as bus_utils
from calitp_data_analysis import utils
from shared_utils import portfolio_utils, rt_utils
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
            lambda x: rt_utils.categorize_time_of_day(
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
    
    # Sort so that the smallest abs_diff is kept (1 trip kept for p50_trip)
    # since multiple trips can have be 50th percentile trip
    p50_trip = (df3.sort_values(group_cols + ["abs_diff", "trip_id"], 
                          ascending=[True, True, True, True])
           .drop_duplicates(subset=group_cols)
                [group_cols + ["trip_id"]]
          )
    
    df4 = pd.merge(
        df3,
        p50_trip.assign(p50_trip = 1),
        on = group_cols + ["trip_id"],
        how = "left",
    )
    
    df4 = df4.assign(
        p50_trip = df4.p50_trip.fillna(0).astype(int)
    ).drop(columns = ["abs_diff"])
    
    return df4
    

def merge_in_competitive_routes(df: pd.DataFrame, threshold: float) -> gpd.GeoDataFrame:
    # Tag as competitive
    gmaps_results = catalog.gmaps_results.read()

    # Merge in the competitive trip info
    gdf = pd.merge(gmaps_results[route_cols + ["car_duration_hours", "geometry"]],
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
        num_trips = gdf.groupby(route_cols)["trip_id"].transform("count")
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


def add_route_group(df: gpd.GeoDataFrame, 
                    service_time_cutoff: dict = {"short": 1, "medium": 1.5}
                   ) -> gpd.GeoDataFrame:
    
    max_service_hours = (df.groupby(route_cols)["service_hours"]
                         .max().reset_index()
                        )
    
    max_service_hours["route_group"] = max_service_hours.apply(
        lambda x: "short" if x.service_hours <= service_time_cutoff["short"]
        else "medium" if ((x.service_hours > service_time_cutoff["short"]) and                  
        (x.service_hours <= service_time_cutoff["medium"]) )
        else "long", axis=1
    )
    
    df2 = pd.merge(
        df,
        max_service_hours.drop(columns = "service_hours"), # drop the max service hours
        on = route_cols,
        how = "left",
        validate = "m:1"
    )
    
    return df2


# Use agency_name from our views.gtfs_schedule.agency instead of Airtable?
def merge_in_agency_name(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    agency_names = portfolio_utils.add_agency_name(
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
    caltrans_districts = portfolio_utils.add_caltrans_district()
                            
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
    
    
def add_route_categories(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add in route categories that are flagged 
    under quarterly performance objective work.
    """
    route_categories = (gpd.read_parquet(
        f"{bus_utils.GCS_FILE_PATH}routes_categorized_{ANALYSIS_DATE}.parquet")
        .rename(columns = {"itp_id": "calitp_itp_id"})
    )
    
    gdf2 = pd.merge(
        gdf,
        route_categories[["calitp_itp_id", "route_id", "category"]],
        on = route_cols,
        how = "left",
        validate = "m:1"
    )
    
    # Clean up route_name
    route_names = portfolio_utils.add_route_name(ANALYSIS_DATE)
    
    gdf3 = pd.merge(
        gdf2,
        route_names,
        on = ["calitp_itp_id", "route_id"],
        how = "left"
    )
    
    return gdf3
    
    
def add_mean_speed_by_route(
    competitive_df: gpd.GeoDataFrame, analysis_date: str) -> gpd.GeoDataFrame:
    
    mean_speed_by_route = aggregated_route_stats.calculate_mean_speed_by_route(
        analysis_date,                                                               
        ["calitp_itp_id", "route_id"])

    gdf = pd.merge(
        competitive_df, 
        mean_speed_by_route,
        on = ["calitp_itp_id", "route_id"],
        how = "left",
        validate = "m:1",
        indicator="merge_speed"
    )
    
    print("merge: competitive to speeds; left_only means no speeds available")
    print(gdf.merge_speed.value_counts())
    
    gdf = gdf.assign(
        mean_speed_mph = gdf.mean_speed_mph.round(1),
        num_trips = gdf.num_trips.astype("Int64"),
        competitive = gdf.competitive.astype("Int64"),
        num_competitive = gdf.num_competitive.astype("Int64")         
    )
    
    return gdf
    
    
def assemble_data(analysis_date: str, threshold: float = 1.5, 
                  service_time_cutoffs: dict = {
                    "short": 1.0,
                    "medium": 1.5}
               ) -> gpd.GeoDataFrame:   
    
    # Import all service hours associated with trip
    trip_service_hours = merge_trips_with_service_hours(analysis_date)

    # Grab first stop time for each trip, get departure hour
    df = add_trip_time_of_day(trip_service_hours)
    
    # Calculate p25, p50, p75 quantiles for the route
    df2 = add_quantiles(df)
    
    # Tag competitive routes based on threshold specified (service_hours/car_duration_hours)
    df3 = merge_in_competitive_routes(df2, threshold=threshold)
    
    # Break up plot groups by route travel time
    df4 = add_route_group(df3, service_time_cutoffs)
    
    # Merge in agency name
    df5 = merge_in_agency_name(df4)
    
    # Merge in Caltrans district from Airtable
    df6 = merge_in_airtable(df5)
    
    # Merge in route categories from Quarterly Performance Objective work
    df7 = add_route_categories(df6)
    
    # Merge in avg speed by route from 100 Recs
    df8 = add_mean_speed_by_route(df7, analysis_date)
    
    return df8

    
if __name__ == "__main__":    
    SERVICE_TIME_CUTOFFS = {
        "short": 1.0,
        "medium": 1.5,
    }
    
    gdf = assemble_data(ANALYSIS_DATE, threshold = 1.5, 
                        service_time_cutoffs = SERVICE_TIME_CUTOFFS)
    
    utils.geoparquet_gcs_export(
        gdf, 
        bus_utils.GCS_FILE_PATH, 
        f"competitive_route_variability_{ANALYSIS_DATE}")