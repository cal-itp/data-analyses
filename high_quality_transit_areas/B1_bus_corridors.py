"""
Move bus_corridors.ipynb into script.
"""
#import os
#os.environ["CALITP_BQ_MAX_BYTES"] = str(900_000_000_000) ## 800GB?

import datetime as dt
import geopandas as gpd
import intake
import pandas as pd
import zlib

from calitp.tables import tbl
from siuba import *

import A1_rail_ferry_brt as rail_ferry_brt
import utilities
from shared_utils import rt_utils, geography_utils


catalog = intake.open_catalog("./*.yml")
analysis_date = rail_ferry_brt.analysis_date

itp_id = 182
date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)

# Skip writing geoparquet again for now 
# TODO: tweak rt_utils to overwrite export? 
# Overwriting while testing this is not ideal, don't want to mess it up

#routelines = gpd.read_parquet(f"{rt_utils.GCS_FILE_PATH}"
#                             f"cached_views/routelines_{itp_id}_{date_str}.parquet" 
#                            )

#routelines.to_parquet(f"./data/routelines_{itp_id}_{date_str}.parquet")

## force clear to ensure route type data present
#trips = rt_utils.get_trips(itp_id, analysis_date, force_clear=True, route_types = ['3'])
#trips.to_parquet(f"./data/trips_{itp_id}_{date_str}.parquet")

#stop_times = rt_utils.get_stop_times(itp_id, analysis_date)
#stop_times.to_parquet(f"./data/st_{itp_id}_{date_str}.parquet")

#stops = rt_utils.get_stops(itp_id, analysis_date)
#stops.to_parquet(f"./data/stops_{itp_id}_{date_str}.parquet")

def add_segment_id(df):
    df = df.reset_index(drop=True)
    
    ## compute (hopefully unique) hash of segment id that can be used 
    # across routes/operators
    df = df.assign(
        segment_sequence = df.index.astype(str))
    
    df2 = df.assign(
        hqta_segment_id = df.apply(lambda x: 
                                   # this checksum hash always give same value if 
                                   # the same combination of strings are given
                                   zlib.crc32(
                                       (str(x.calitp_itp_id) + x.shape_id + x.segment_sequence)
                                       .encode("utf-8")), 
                                       axis=1),
        ##generous buffer for street/sidewalk width? 
        # Required to spatially find stops within each segment
        geometry = df.geometry.buffer(50),
    )
    
    return df2

## Aggregate stops by departure hour
def clean_stop_times(stop_times_df):
    
    df = (stop_times_df.dropna(subset=["departure_time"])
            ## filter duplicates for top2 approach
          .drop_duplicates(subset=["trip_id", "stop_id"])
         )
    
    ## reformat GTFS time to a format datetime can ingest 
    df["departure_time"] = df.departure_time.apply(utilities.fix_arrival_time)
    df["departure_hour"] = df.departure_time.apply(
            lambda x: dt.datetime.strptime(x, rt_utils.HOUR_MIN_SEC_FMT).hour)
    
    return df


def aggregate_stops_by_hour(df):
    df2 = (df  
           >> count(_.calitp_itp_id, _.stop_id, _.departure_hour)
           >> rename(n_trips = _.n)
          )
    
    return df2
        
        
    trips_shape_sorted = (
        trips.groupby("shape_id")
        .count()
        .sort_values(by="trip_id", ascending=False)
        .index
    )
    
    trips_shape_sorted = pd.Series(trips_shape_sorted)
    
## Join HQTA segment to stop
# Find nearest stop
def hqta_segment_to_stops(hqta_segmented, stops):
    segment_to_stop = (gpd.sjoin(
            stops.drop(columns = ["stop_lon", "stop_lat"]), 
            hqta_segmented[["hqta_segment_id", "geometry"]],
            how = "inner",
            predicate = "intersects"
        ).drop(columns = "index_right")
        .drop_duplicates(subset=["calitp_itp_id", "stop_id", "hqta_segment_id"])
        .reset_index(drop=True)
    )
    
    segment_to_stop2 = pd.merge(
        hqta_segmented[["hqta_segment_id", "geometry"]],
        segment_to_stop.drop(columns = "geometry"),
        how = "inner"
    )
    
    return segment_to_stop2

def max_trips_by_stop(df, new_col):
    df2 = (df
           >> group_by(_.calitp_itp_id, _.stop_id)
           >> summarize(n_trips = _.n_trips.max())
    ).rename(columns = {"n_trips": new_col})

    return df2


def hqta_segment_with_max_trips(df):
    # Within a hqta_segment_id find
    # max am_peak trips and max pm_peak trips
    # drop stop_id info (since multiple stops can share the same max)

    df2 = (df.assign(
            am_max_trips = df.groupby("hqta_segment_id")["am_trips"].transform("max"),
            pm_max_trips = df.groupby("hqta_segment_id")["pm_trips"].transform("max"),
        )[["hqta_segment_id", "am_max_trips", "pm_max_trips", "geometry"]]
           .drop_duplicates()
           .reset_index(drop=True)
    )
    
    df2 = df2.assign(
        hq_transit_corr = df2.apply(lambda x: 
                                    x.am_max_trips > 4 and x.pm_max_trips > 4, 
                                    axis=1)
    )
    
    return df2


def single_shape_hqta2(routelines, stops, stop_times, shape_id):
    # Filter to just 1 shape_id
    single_line = routelines >> filter(_.shape_id == shape_id)

    # Turn 1 row of geometry into hqta_segments, every 1,250 m
    segmented = gpd.GeoDataFrame() ##changed to gdf?

    for segment in utilities.create_segments(single_line.geometry):
        to_append = single_line.drop(columns=["geometry"])
        to_append["geometry"] = segment
        segmented = pd.concat((segmented, to_append))

    segmented = add_segment_id(segmented)

    # Fix stop times so it can be parsed
    stop_times = clean_stop_times(stop_times)
    
    # Join hqta_segment_id to stop_id
    segment_to_stop = hqta_segment_to_stops(segmented, stops)
    
    # Aggregate stop_times to departure_hour
    stop_times_by_hour = aggregate_stops_by_hour(stop_times)

    # Calculate number of trips by AM/PM peak for each hqta_segment_id-stop_id
    am_peak = max_trips_by_stop(
        stop_times_by_hour[stop_times_by_hour.departure_hour < 12], 
        "am_trips")

    pm_peak = max_trips_by_stop(
        stop_times_by_hour[stop_times_by_hour.departure_hour >= 12], 
        "pm_trips")

    # Merge in AM/PM peak trips on hqta_segment_id-stop_id
    stop_cols = ["calitp_itp_id", "stop_id"]

    segment_with_trips = pd.merge(
        segment_to_stop, 
        am_peak,
        on = stop_cols,
    ).merge(pm_peak, on = stop_cols)

    # Only keep max AM/PM peak trip info by hqta_segment_id (no more stop_id)
    segment_peak_trips = hqta_segment_with_max_trips(segment_with_trips)
    
    return segment_peak_trips


if __name__=="__main__":
    date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)

    itp_id_list = [182]

    for itp_id in itp_id_list:
        # Import data
        routelines = gpd.read_parquet(f"./data/routelines_{itp_id}_{date_str}.parquet")
        trips = pd.read_parquet(f"./data/trips_{itp_id}_{date_str}.parquet")
        stop_times = pd.read_parquet(f"./data/st_{itp_id}_{date_str}.parquet")
        stops = gpd.read_parquet(f"./data/stops_{itp_id}_{date_str}.parquet")
        
        trips_shape_sorted = (
            trips.groupby("shape_id")
            .count()
            .sort_values(by="trip_id", ascending=False)
            .index
        )

        trips_shape_sorted = pd.Series(trips_shape_sorted)
    
    
        start = dt.datetime.now()
        #df = pd.DataFrame()

        for shape_id in trips_shape_sorted:

            hqta_for_shape = single_shape_hqta2(routelines, stops, stop_times, shape_id)
            hqta_for_shape = hqta_for_shape.assign(
                shape_id = shape_id
            )
            
            # Stage each shape_id for now
            # Then append at the end 
            hqta_for_shape.to_parquet(f"./data/bus_corridor/{itp_id}_{shape_id}.parquet")
            #df = pd.concat([df, hqta_for_shape], axis=0, ignore_index=True)

        end = dt.datetime.now()
        print(f"Execution time for {itp_id}: {end-start}")
    
        #df.to_parquet(f"./data/{itp_id}_bus2.parquet")