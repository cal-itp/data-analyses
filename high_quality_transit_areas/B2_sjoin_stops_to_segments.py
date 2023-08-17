"""
Attach stop times table to HQTA segments, 
and flag which segments are HQ transit corridors.

Takes 2.5 min to run.
- down from 1 hr in v2 (was part of B1)
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime as dt
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger
from typing import Union

from shared_utils import utils, gtfs_utils
from utilities import GCS_FILE_PATH
from update_vars import analysis_date, COMPILED_CACHED_VIEWS

def max_trips_by_group(df: dd.DataFrame, 
                       group_cols: list,
                       max_col: str = "n_trips"
                      ) -> dd.DataFrame:
    """
    Find the max trips, by stop_id or by hqta_segment_id.
    Put in a list of group_cols to find the max.
    Can also subset for AM or PM by df[df.departure_hour < 12]
    """
    df2 = (df.groupby(group_cols)
           .agg({max_col: np.max})
           .reset_index()
          )
    
    return df2 


def stop_times_aggregation_max_by_stop(stop_times: dd.DataFrame) -> dd.DataFrame:
    """
    Take the stop_times table 
    and group by stop_id-departure hour
    and count how many trips occur.
    """
    stop_cols = ["feed_key", "stop_id"]

    stop_times = stop_times.assign(
        departure_hour = dd.to_datetime(
            stop_times.departure_sec, unit="s").dt.hour
    )
            
    # Aggregate how many trips are made at that stop by departure hour
    trips_per_hour = (stop_times.groupby(stop_cols + ["departure_hour"])
                      .agg({'trip_id': 'count'})
                      .reset_index()
                      .rename(columns = {"trip_id": "n_trips"})
                     )    
    
    # Subset to departure hour before or after 12pm
    am_trips = max_trips_by_group(
        trips_per_hour[trips_per_hour.departure_hour < 12], 
        group_cols = stop_cols,
        max_col = "n_trips"
    ).rename(columns = {"n_trips": "am_max_trips"})
    
    pm_trips = max_trips_by_group(
        trips_per_hour[trips_per_hour.departure_hour >= 12], 
        group_cols = stop_cols,
        max_col = "n_trips"
    ).rename(columns = {"n_trips": "pm_max_trips"})
    
    max_trips_by_stop = dd.merge(
        am_trips, 
        pm_trips,
        on = stop_cols,
        how = "left"
    )
    
    max_trips_by_stop = max_trips_by_stop.assign(
        am_max_trips = max_trips_by_stop.am_max_trips.fillna(0).astype(int),
        pm_max_trips = max_trips_by_stop.pm_max_trips.fillna(0).astype(int),
        n_trips = (max_trips_by_stop.am_max_trips.fillna(0) + 
                   max_trips_by_stop.pm_max_trips.fillna(0))
    )
        
    return max_trips_by_stop


def hqta_segment_to_stop(hqta_segments: dg.GeoDataFrame, 
                         stops: dg.GeoDataFrame
                        ) -> dg.GeoDataFrame:    
    """
    Spatially join hqta segments to stops. 
    Which stops fall into which segments?
    """
    segment_cols = ["hqta_segment_id", "segment_sequence"]

    segment_to_stop = (dg.sjoin(
            stops[["stop_id", "geometry"]],
            hqta_segments,
            how = "inner",
            predicate = "intersects"
        ).drop(columns = ["index_right"])
    )[segment_cols + ["stop_id"]]
    
    
    # After sjoin, we don't want to keep stop's point geom
    # Merge on hqta_segment_id's polygon geom
    segment_to_stop2 = dd.merge(
        hqta_segments,
        segment_to_stop,
        on = segment_cols
    )
    
    return segment_to_stop2


def hqta_segment_keep_one_stop(
    hqta_segments: dg.GeoDataFrame, 
    stop_times: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Since multiple stops can fall into the same segment, 
    keep the stop wiht the highest trips (sum across AM and PM).
    
    Returns gdf where each segment only appears once.
    """
    stop_cols = ["feed_key", "stop_id"]
    # dd.merge between dask dataframes can be expensive
    # put pd.DataFrame on right if possible
    segment_to_stop_times = dd.merge(
            hqta_segments, 
            stop_times,
            on = stop_cols
        )
                      
    # Can't sort by multiple columns in dask,
    # so, find the max, then inner merge
    max_trips_by_segment = max_trips_by_group(
        segment_to_stop_times,
        group_cols = ["hqta_segment_id"],
        max_col = "n_trips"
    ).compute()
    
    # Merge in and keep max trips observation
    # Since there might be duplicates still, where multiple stops all 
    # share 2 trips for that segment, do a drop duplicates at the end 
    max_trip_cols = ["hqta_segment_id", "am_max_trips", "pm_max_trips"]
    
    segment_to_stop_unique = dd.merge(
        segment_to_stop_times,
        max_trips_by_segment,
        on = ["hqta_segment_id", "n_trips"],
        how = "inner"
    ).drop_duplicates(subset=max_trip_cols)
    
    # In the case of same number of trips overall, do a sort
    # with descending order for AM, then PM trips
    segment_to_stop_gdf = segment_to_stop_unique.compute()
    segment_to_stop_gdf = (segment_to_stop_gdf
                           .sort_values(max_trip_cols, 
                                        ascending=[True, False, False])
                            .drop_duplicates(subset="hqta_segment_id")
                           .reset_index(drop=True)
                          )
    
    return segment_to_stop_gdf


def sjoin_stops_and_stop_times_to_hqta_segments(
    hqta_segments: Union[gpd.GeoDataFrame, dg.GeoDataFrame], 
    stops: Union[gpd.GeoDataFrame, dg.GeoDataFrame],
    stop_times: Union[pd.DataFrame, dd.DataFrame],
    buffer_size: int = 50,
    hq_transit_threshold: int = 4,
) -> dg.GeoDataFrame:
    """
    Take HQTA segments, draw a buffer around the linestrings.
    Spatial join the stops (points) to segments (now polygons).
    If there are multiple stops in a segment, keep the stop
    with more trips.
    Tag the segment as hq_transit_corr (boolean)
    """
    # Draw 50 m buffer to capture stops around hqta segments
    hqta_segments2 = hqta_segments.assign(
        geometry = hqta_segments.geometry.buffer(buffer_size)
    )
    
    # Join hqta segment to stops
    segment_to_stop = hqta_segment_to_stop(hqta_segments2, stops)
    
    segment_to_stop_unique = hqta_segment_keep_one_stop(
        segment_to_stop, stop_times)

    # Identify hq transit corridor
    # Tag segment as being hq_transit_corr if it has at least 4 trips in AM and PM 
    # (before 12pm, after 12pm, whatever is max in each period)
    drop_cols = ["n_trips"]

    segment_hq_corr = segment_to_stop_unique.assign(
        hq_transit_corr = segment_to_stop_unique.apply(
            lambda x: True if (x.am_max_trips >= hq_transit_threshold and 
                               (x.pm_max_trips >= hq_transit_threshold))
            else False, axis=1)
    ).drop(columns = drop_cols)
    

    return segment_hq_corr


if __name__ == "__main__":
    # Connect to dask distributed client, put here so it only runs for this script
    #from dask.distributed import Client
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/B2_sjoin_stops_to_segments.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")

    start = dt.datetime.now()
    
    # (1) Aggregate stop times - by stop_id, find max trips in AM/PM peak
    # takes 1 min
    stop_times = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}st_{analysis_date}.parquet")
    max_arrivals_by_stop = stop_times_aggregation_max_by_stop(stop_times)
    
    max_arrivals_by_stop.compute().to_parquet(
        f"{GCS_FILE_PATH}max_arrivals_by_stop.parquet")
    
    ## (2) Spatial join stops and stop times to hqta segments
    # this takes < 2 min
    hqta_segments = dg.read_parquet(f"{GCS_FILE_PATH}hqta_segments.parquet")
    stops = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet")
    max_arrivals_by_stop = pd.read_parquet(
        f"{GCS_FILE_PATH}max_arrivals_by_stop.parquet") 
    
    hqta_corr = sjoin_stops_and_stop_times_to_hqta_segments(
        hqta_segments, 
        stops,
        max_arrivals_by_stop,
        buffer_size = 50, #50meters
        hq_transit_threshold = 4
    )
        
    utils.geoparquet_gcs_export(
        hqta_corr,
        GCS_FILE_PATH,
        "all_bus"
    )
    
    end = dt.datetime.now()
    logger.info(f"Execution time: {end-start}")
    
    #client.close()
