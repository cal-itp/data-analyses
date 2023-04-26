import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

import prep_stop_segments
from segment_speed_utils import gtfs_schedule_wrangling, helpers
from segment_speed_utils.project_vars import analysis_date

def grab_loop_trips(analysis_date: str) -> pd.DataFrame:
    """
    Use stop_times table to grab the trips that 
    visit the same stop_id at least twice.
    """
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date, 
        columns = [
            "feed_key", "trip_id", 
            "stop_id", "stop_sequence",
        ]
    ).drop_duplicates()
    
    stop_visits = (stop_times.groupby(
                    ["feed_key", "trip_id", "stop_id"])
                  .agg({"stop_sequence": "count"}) 
                   #nunique doesn't work in dask
                  .reset_index()
                 )
    
    loop_trips = (stop_visits[stop_visits.stop_sequence > 1]
                  [["feed_key", "trip_id"]]
                  .drop_duplicates()
                  .reset_index(drop=True)
                  .compute()
                 )
    return loop_trips


def grab_loop_shapes(analysis_date: str) -> gpd.GeoDataFrame: 
    
    loop_trips = grab_loop_trips(analysis_date)
    
    trips_with_geom = gtfs_schedule_wrangling.get_trips_with_geom(
        analysis_date).compute()
    
    loop_trips_with_geom = pd.merge(
        trips_with_geom,
        loop_trips,
        on = ["feed_key", "trip_id"],
        how = "inner"
    )
        
    return loop_trips_with_geom


def assign_visits_to_stop(df: pd.DataFrame):
    """
    Groupby shape and stop_id and count how many times it's being visited
    and which number visit it is.
    """
    df = df.assign(
        num_visits = df.groupby(["shape_array_key", "stop_id"])
                    .stop_sequence.transform("nunique")
    )
    
    return df