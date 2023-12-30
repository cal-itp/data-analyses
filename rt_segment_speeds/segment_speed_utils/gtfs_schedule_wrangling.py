"""
All kinds of GTFS schedule table wrangling.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from typing import Union

from segment_speed_utils import helpers

peak_periods = ["AM Peak", "PM Peak"]

HOURS_BY_TIME_OF_DAY = {
    "Owl": 4, #[0, 3]
    "Early AM": 3,  #[4, 6]
    "AM Peak": 3,  #[7, 9]
    "Midday": 5,  #[10, 14]
    "PM Peak": 5, #[15, 19]
    "Evening": 4 #[20, 23]
}

def exclude_scheduled_operators(
    trips: pd.DataFrame, 
    exclude_me: list = ["Amtrak Schedule", "*Flex"]
):
    """
    Exclude certain operators by name.
    Here, we always want to exclude Amtrak Schedule because
    it runs outside of CA.
    """
    substrings_to_exclude = [i for i in exclude_me if "*" in i]
    
    if len(substrings_to_exclude) > 0:
        substrings = [i.replace("*", "") for i in substrings_to_exclude]
        for i in substrings:
            trips = trips[~trips.name.str.contains(i)].reset_index(drop=True)
    
    return trips[~trips.name.isin(exclude_me)].reset_index(drop=True)


def get_trips_with_geom(
    analysis_date: str,
    trip_cols: list = ["feed_key", "name", 
                       "trip_id", "shape_array_key"],
    exclude_me: list = ["Amtrak Schedule", "*Flex"],
    crs: str = "EPSG:3310"
) -> gpd.GeoDataFrame:
    """
    Merge trips with shapes. 
    Also exclude Amtrak and Flex trips.
    """
    shapes = helpers.import_scheduled_shapes(
        analysis_date, 
        columns = ["shape_array_key", "geometry"],
        get_pandas = True,
        crs = crs
    )

    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = trip_cols,
        get_pandas = True
    )
    
    trips = exclude_scheduled_operators(
        trips, 
        exclude_me
    )

    trips_with_geom = pd.merge(
        shapes,
        trips,
        on = "shape_array_key",
        how = "inner"
    ).drop_duplicates().reset_index(drop=True)
    
    return trips_with_geom


def stop_arrivals_per_stop(
    stop_times: pd.DataFrame,
    group_cols: list,
    count_col: str = "trip_id"
) -> pd.DataFrame:
    """
    Aggregate stop_times by list of grouping columns 
    and count number of stop arrivals.
    """
    arrivals_by_stop = (stop_times
                        .groupby(group_cols, 
                                 observed=True, group_keys=False)
                        .agg({count_col: 'count'})
                        .reset_index()
                        .rename(columns = {count_col: "n_arrivals"})          
                     )    
    return arrivals_by_stop
    
    
def add_peak_offpeak_column(df: pd.DataFrame):
    """
    Add a single peak_offpeak column based on the time-of-day column.
    """
    df = df.assign(
        peak_offpeak = df.apply(
            lambda x: "peak" if x.time_of_day in peak_periods
            else "offpeak", 
            axis=1)
    )
    
    return df
    
def aggregate_time_of_day_to_peak_offpeak(
    df: pd.DataFrame,
    group_cols: list,
) -> pd.DataFrame:
    """
    Aggregate time-of-day bins into peak/offpeak periods.
    Return n_trips and frequency for grouping of columns (route-direction, etc).
    """    
    peak_hours = sum(v for k, v in HOURS_BY_TIME_OF_DAY.items() 
                 if k in peak_periods) 
    
    offpeak_hours = sum(v for k, v in HOURS_BY_TIME_OF_DAY.items() 
                 if k not in peak_periods) 
    
    df = add_peak_offpeak_column(df)
    
    df2 = (df.groupby(group_cols + ["peak_offpeak"])
           .agg({"trip_instance_key": "count"})
           .reset_index()
           .rename(columns = {"trip_instance_key": "n_trips"})
          )
    
    # Add service frequency (trips per hour)
    # there are different number of hours in peak and offpeak periods
    df2 = df2.assign(
        frequency = df2.apply(
            lambda x:
            round(x.n_trips / peak_hours, 2) if x.peak_offpeak=="peak"
            else round(x.n_trips / offpeak_hours, 2), axis=1
        )
    )
    
    # Reshape from wide to long
    # get rid of multiindex column names
    df3 = df2.pivot(index=group_cols, 
          columns="peak_offpeak",
          values=["n_trips", "frequency"]
         )

    df3.columns = [f'{b}_{a}' for a, b in df3.columns]
    df3 = df3.reset_index()

    return df3


def most_recent_route_info(
    df: pd.DataFrame,
    group_cols: list,
    route_col: str
) -> pd.DataFrame:
    """
    Find the most recent value across a grouping.
    Ex: if we group by route_id, we can find the most recent 
    value for route_long_name.
    
    Needs a date column to work.
    """
    sort_order = [True for c in group_cols]
    
    most_recent = (df.sort_values(group_cols + ["service_date"], 
                                  ascending = sort_order + [False])
                   .drop_duplicates(subset = group_cols)  
                   .rename(columns = {route_col: f"recent_{route_col}"})
                  )
    
    df2 = pd.merge(
        df,
        most_recent[group_cols + [f"recent_{route_col}"]],
        on = group_cols,
        how = "left"
    )
    
    return df2


OPERATORS_USE_HYPHENS = [
    "Monterey Salinas", "LA Metro",
    "BART", # Beige - N, Beige - S
    "MVGO", # B - AM, B - PM
]

OPERATORS_USE_UNDERSCORES = [
    "Roseville", # 5_AM, 5_PM
]

def standardize_route_id(
    row, 
    gtfs_name_col: str, 
    route_col: str
) -> str:
    """
    Standardize route_id across time. 
    For certain operators, we can parse away the suffix after an
    hyphen or underscore.
    Must include a column that corresponds to `gtfs_dataset_name`.
    """
    word = row[route_col]
    
    if any(word in row[gtfs_name_col] for word in OPERATORS_USE_HYPHENS): 
        word = word.split("-")[0]
    
    if any(word in row[gtfs_name_col] for word in OPERATORS_USE_UNDERSCORES):
        word = word.split("_")[0]
    
    
    word = word.strip()
    
    return word
    