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
                          rt_utils, portfolio_utils, utils
                         )
from E0_bus_oppor_vars import GCS_FILE_PATH, ANALYSIS_DATE, COMPILED_CACHED_GCS

month = ANALYSIS_DATE.split('-')[1]
day = ANALYSIS_DATE.split('-')[2]
ANALYSIS_MONTH_DAY = f"{month}_{day}"

catalog = intake.open_catalog("*.yml")
COMPILED_CACHED_FOLDER = "compiled_cached_views/"

#--------------------------------------------------------------------#
## Trips / Stop Times for Routes on SHN
#--------------------------------------------------------------------#
def import_trips_and_stop_times(analysis_date: str) -> tuple[dd.DataFrame]:
    st_path = rt_utils.check_cached(f"st_{analysis_date}.parquet", 
                                 rt_utils.GCS_FILE_PATH,
                                 subfolder=COMPILED_CACHED_FOLDER
                                )
    if st_path:
        stop_times = dd.read_parquet(st_path)
    else:
        gtfs_utils.all_trips_or_stoptimes_with_cached(
            dataset="st",
            analysis_date = analysis_date,
            itp_id_list = keep_itp_ids,
            # export to bus_service_increase/, not rt_delay/, 
            # since this is a subset of operators
            export_path = GCS_FILE_PATH
        )
        
        stop_times = dd.read_parquet(f"{GCS_FILE_PATH}st_{analysis_date}.parquet")
    
    trip_path = rt_utils.check_cached(f"trips_{analysis_date}_all.parquet",
                                      rt_utils.GCS_FILE_PATH,
                                      subfolder=COMPILED_CACHED_FOLDER
                                     )
    
    if trip_path:
        trips = dd.read_parquet(trip_path)
    else:
        gtfs_utils.all_trips_or_stoptimes_with_cached(
            dataset="st",
            analysis_date = analysis_date,
            itp_id_list = keep_itp_ids,
            export_path = GCS_FILE_PATH
        )
        
        trips = dd.read_parquet(f"{GCS_FILE_PATH}trips_{analysis_date}.parquet")
    
    return stop_times, trips


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


#--------------------------------------------------------------------#
## General Aggregation and Wrangling Functions
#--------------------------------------------------------------------#
def aggregate_stat_by_time_of_day(df: dd.DataFrame, 
                                  group_cols: list, 
                                  stat_cols: dict = {"trip_id": "nunique", 
                                                    "departure_hour": "count",
                                                    "stop_id": "nunique"}
                                 ) -> pd.DataFrame:
    """
    Aggregate given different group_cols.
    """    
    
    def group_and_aggregate(df: dd.DataFrame, 
                            group_cols: list, 
                            agg_col: str, agg_func: str) -> pd.DataFrame:
        # nunique seems to not work in groupby.agg
        # even though https://github.com/dask/dask/pull/8479 seems to resolve it?
        # alternative is to use nunique as series
        if agg_func=="nunique":
            agg_df = (df.groupby(group_cols)[agg_col].nunique()
                    .reset_index()
                 )
        else:
            agg_df = (df.groupby(group_cols)
                      .agg({agg_col: agg_func})
                      .reset_index()
                     )
        
        # return pd.DataFrame for now, since it's not clear what the metadata should be
        # if we are inputting different things in stats_col
        return agg_df.compute()
    
    final = pd.DataFrame()
    
    for agg_col, agg_func in stat_cols.items():
        agg_df = group_and_aggregate(df, group_cols, agg_col, agg_func)
        
        # If it's empty, just add our new table of aggregations in with concat
        if final.empty:
            final = pd.concat([final, agg_df], axis=0, ignore_index=True)
        
        # If it's not empty, do a merge
        else:
            final = pd.merge(
                final, agg_df, on = group_cols, how = "left"
            )
    
    return final
        

def reshape_long_to_wide(df: pd.DataFrame, 
                         group_cols: list,
                         long_col: str = 'time_of_day',
                         value_col: str = 'trips',
                         long_col_sort_order: list = ['owl', 'early_am', 
                                                      'am_peak', 'midday', 
                                                      'pm_peak', 'evening'],
                        )-> pd.DataFrame:
    """
    To reshape from long to wide, use df.pivot.
    Args in this function correspond this way:
    
    df.pivot(index=group_cols, columns = long_col, values = value_col)
    """
    # To reshape, cannot contain duplicate entries
    # Get it down to non-duplicate form
    # For stop-level, if you're reshaping on value_col==trip, that stop contains
    # the same trip info multiple times.
    df2 = df[group_cols + [long_col, value_col]].drop_duplicates()
    
    #https://stackoverflow.com/questions/22798934/pandas-long-to-wide-reshape-by-two-variables
    reshaped = df2.pivot(
        index=group_cols, columns=long_col,
        values=value_col
    ).reset_index().pipe(to_snakecase)

    # set the order instead of list comprehension, which will just do alphabetical
    add_prefix_cols = long_col_sort_order

    # Change the column order
    reshaped = reshaped.reindex(columns=group_cols + add_prefix_cols)

    # If there are NaNs, fill it with 0, then coerce to int
    reshaped[add_prefix_cols] = reshaped[add_prefix_cols].fillna(0).astype(int)

    # Now, instead columns named am_peak, pm_peak, add a prefix 
    # to distinguish between num_trips and num_stop_arrivals
    reshaped.columns = [f"{value_col}_{c}" if c in add_prefix_cols else c
                            for c in reshaped.columns]

    return reshaped


def long_to_wide_format(df: pd.DataFrame, 
                        group_cols: list, 
                        stat_cols: list = ["trips", "stop_arrivals", "stops"]
                       ) -> pd.DataFrame:
    """
    Take the long df, which is structured where each row is 
    a route_id-time_of_day combination, and columns are 
    'trips' and 'stop_arrivals' and 'stops'.
    
    Reshape it to being wide, so each row is route_id.
    """

    df = df.astype({"time_of_day": "category"})
    
    # Do the reshape to wide format separately
    # so the renaming of columns is cleaner
    # Summing across time-of-day is problematic for stops...but not for trips
    # doing a sum of nunique across categories is not the same as counting nunique over a larger group
    time_of_day_sorted = ['peak', 'all_day']
    
    df_wide = pd.DataFrame()
    
    for c in stat_cols:
        one_stat_wide = reshape_long_to_wide(
            df, group_cols = group_cols,
            long_col = "time_of_day",
            value_col = c, long_col_sort_order = time_of_day_sorted
        )
        
        # for the first column to reshape, just concatenate it
        if df_wide.empty:
            df_wide = pd.concat([df_wide, one_stat_wide], axis=0)
        else:
            df_wide = pd.merge(
                df_wide,
                one_stat_wide, 
                on = group_cols,
                how = "left"
            )
    
    return df_wide


def compile_peak_all_day_aggregated_stats(
    stop_times_with_time_of_day: dd.DataFrame,
    group_cols: list,
    stat_cols: dict = {"trip_id": "nunique", 
                       "departure_hour": "count", 
                       "stop_id": "nunique"}) -> pd.DataFrame:
    
    rename_aggregated_cols = {
        "trip_id": "trips", 
        "departure_hour": "stop_arrivals",
        "stop_id": "stops"
    }
    
    # Peak Hours
    peak_bins = ["AM Peak", "PM Peak"]
    
    stop_times_peak = stop_times_with_time_of_day[
        stop_times_with_time_of_day.time_of_day.isin(peak_bins)
    ].assign(time_of_day="peak")
    
    peak_table = aggregate_stat_by_time_of_day(
        stop_times_peak, 
        group_cols + ["time_of_day"], 
        stat_cols = stat_cols
    ).rename(columns = rename_aggregated_cols)
        
    # All day    
    stop_times_all_day = stop_times_with_time_of_day.assign(time_of_day="all_day")

    all_day_table = aggregate_stat_by_time_of_day(
        stop_times_all_day, 
        group_cols + ["time_of_day"],
        stat_cols = stat_cols
    ).rename(columns = rename_aggregated_cols)

    df = pd.concat(
        [peak_table, all_day_table], axis=0)
    
    # Reshape from long to wide (do it for each aggregated stat separately and merge)
    aggregated_stats_cols = [c for c in df.columns if c not in group_cols and 
                             c != "time_of_day"]
    
    df_wide = long_to_wide_format(df, group_cols, 
                                  stat_cols = aggregated_stats_cols)

    return df_wide


#--------------------------------------------------------------------#
## RT speeds by trips
#--------------------------------------------------------------------#
def compile_rt_trips_all_operators(analysis_date: str):
    """
    If there's a rt_trips df for the operator, concatenate it and save in GCS.
    """
    df = pd.DataFrame()

    for itp_id in gtfs_utils.ALL_ITP_IDS:
        try:
            trip_df = pd.read_parquet(
                f"{rt_utils.GCS_FILE_PATH}rt_trips/{itp_id}_{ANALYSIS_MONTH_DAY}.parquet")

            df = pd.concat([df, trip_df], axis=0, ignore_index=True)
        except:
            continue
        
    df = (df.sort_values(["calitp_itp_id", "route_id", "trip_id"])
          .reset_index(drop=True)
         )
    
    df.to_parquet(
        f"{rt_utils.GCS_FILE_PATH}rt_trips/all_operators_{analysis_date}.parquet")

    print("Concatenated all parquets for rt_trips")        
    

def calculate_mean_speed_by_route(group_cols: list) -> pd.DataFrame:
    """
    Aggregate trip-level RT mean speed to route-level.
    Take in any set of group_cols to calculate unweighted average.
    """
    df = pd.read_parquet(
        f"{rt_utils.GCS_FILE_PATH}rt_trips/all_operators_{ANALYSIS_DATE}.parquet")
        
    # Each trip is 1 observation, just take the average (not weighted)
    # to get route-level mean_speed_mph
    mean_speed = geography_utils.aggregate_by_geography(
        df,
        group_cols = group_cols,
        mean_cols = ["mean_speed_mph"]
    )
    
    return mean_speed


#--------------------------------------------------------------------#
## Competitive Routes (from E5_make_stripplot_data)
#--------------------------------------------------------------------#
def get_competitive_routes() -> pd.DataFrame:
    """
    Trip-level data for whether the trip is competitive or not,
    with other columns that are route-level.
    
    Keep only the route-level columns.
    """
    trip_df = catalog.competitive_route_variability.read()
    
    route_level_cols = [
        "calitp_itp_id", "route_id", "route_group",
        "bus_difference_spread",
        "num_competitive", "pct_trips_competitive",
    ]

    route_df = geography_utils.aggregate_by_geography(
        trip_df,
        group_cols = route_level_cols,
        mean_cols = ["bus_multiplier", "bus_difference"],
        rename_cols = True
    )
    
    return route_df
    
    
def build_route_level_table(bus_routes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # (1) Compile route-level stats
    # (1a) Compile RT trips for all operators and aggregate to route-level
    route_cols = ["calitp_itp_id", "route_id"]

    #compile_rt_trips_all_operators(ANALYSIS_DATE)
    route_mean_speed = calculate_mean_speed_by_route(route_cols + ["route_type"])
    
    # (1b) Aggregate stop_times to route-level
    trips_by_route = compile_peak_all_day_aggregated_stats(
        stop_times_with_hr, 
        route_cols + ["service_date"], 
        stat_cols = {"trip_id": "nunique"})
    
    # (1c) Get competitive trip variability dataset at route-level
    # This contains, at the route-level, % trips competitive, num_trips competitive
    # whether it's a short/medium/long route, etc
    # Also do left merge because may not always appear in this dataset 
    # There were some that couldn't be processed in Google Maps
    competitive_stats_by_route = get_competitive_routes()
    
    # (2) Merge together route-level stats
    # Put trips_by_route on left because we may not always have mean speed
    trips_with_speed = pd.merge(
        trips_by_route,
        route_mean_speed,
        on = route_cols,
        how = "left"
    )
    
    stats_by_route = pd.merge(
        trips_with_speed,
        competitive_stats_by_route,
        on = route_cols,
        how = "left"
    )
    
    # Add in district info
    itp_id_with_district = portfolio_utils.add_caltrans_district()
    stats_by_route = stats_by_route.merge(
        itp_id_with_district,
        on = "calitp_itp_id",
        how = "left",
    )
    
    # Now merge back bus_routes geometry
    stats_by_route_with_geom = pd.merge(
        bus_routes.rename(columns = {"itp_id": "calitp_itp_id"}),
        stats_by_route,
        on = route_cols,
        how = "inner",
    )
    
    utils.geoparquet_gcs_export(
        stats_by_route_with_geom, 
        GCS_FILE_PATH,
        "bus_routes_on_hwys_aggregated_stats"
    )
    #stats_by_route2.to_parquet(
    #    f"{GCS_FILE_PATH}bus_routes_on_hwys_aggregated_stats.parquet")    

    
    
if __name__=="__main__":
    # (1) Import data
    # (1a) Read in bus routes that run on highways to use for filtering in dask df
    bus_routes = catalog.bus_routes_on_hwys.read()
    
    keep_itp_ids = bus_routes.itp_id.unique().tolist()
    keep_routes = bus_routes.route_id.unique().tolist()
    
    # (1b) Check if there's already a cached file for stop_times and trips
    # should exist for 5/4. if not, generate an export.
    stop_times, trips = import_trips_and_stop_times(ANALYSIS_DATE)
    
    # (2) Combine stop_times and trips, and filter to routes that appear in bus_routes
    # Subset stop times and merge with trips
    stop_times_with_hr = subset_trips_and_stop_times(
        trips, stop_times, 
        itp_id_list = keep_itp_ids, 
        route_list = keep_routes
    )
    
    stop_times_with_hr.compute().to_parquet(f"./data/stop_times_for_routes_on_shn.parquet")

    # (3) Assemble route-level table and export
    build_route_level_table(bus_routes)