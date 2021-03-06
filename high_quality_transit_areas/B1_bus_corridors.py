"""
Move bus_corridors.ipynb into script.

Picking just the longest route takes 21 min to run.
The known issue related to this is that it misses layover 
and possible 1-way streets in certain directions.
but hqta segments are drawn in longer bits, closer to the 1,250 m 
as intended.

This takes 2.5 hr to run (when keeping all the shapes, dissolving).
This has problems with how points in a line are ordered,
and choppy hqta segments are not useful, because they may not attach
properly to stops.
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd
import numpy as np
import pandas as pd
from calitp.storage import get_fs
from siuba import *

import dask_utils
import operators_for_hqta
import utilities
from A1_rail_ferry_brt import analysis_date
from shared_utils import rt_utils, geography_utils, utils, gtfs_utils

date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)

fs = get_fs()

# Apparently these have to be defined in the script?
GCS_PROJECT = "cal-itp-data-infra"
BUCKET_DIR = "data-analyses/hqta_test"
TEST_GCS_FILE_PATH = f"gs://{utilities.BUCKET_NAME}/{BUCKET_DIR}/"


segment_cols = ["hqta_segment_id", "segment_sequence"]
stop_cols = ["calitp_itp_id", "stop_id"]


# merged = merge_routes_to_trips(routelines, trips) throwing error
# ValueError: You are trying to merge on object and int32 columns. If you wish to proceed you should use pd.concat 
VALUE_ERROR_IDS = [
    117, 118, 15,  
    167, 169, 16,
    186, 192, 199,
    206, 207, 213, 214, 21, 
    254, 263, 265, 273, 
    338, 33, 365, 394, 
    41, 54, 81, 91,
]

FILE_NOT_FOUND_IDS = [
    203, #FileNotFoundError: for routelines (confirmed)
    # What's in ITP_IDS but not in GCS
    481, # this is City of South SF
    485, # this is the Treasure Island ferry, it's captured by points
]

TOO_LONG_IDS = [
    13, # taking a long time, for utilities.create_segments(gdf.geometry) 
    # This is Amtrak and is excluded by Eric already
]

ERROR_IDS = VALUE_ERROR_IDS + FILE_NOT_FOUND_IDS + TOO_LONG_IDS

VALID_ITP_IDS = operators_for_hqta.get_valid_itp_ids()
ITP_IDS_IN_GCS = [x for x in VALID_ITP_IDS if x not in ERROR_IDS]


## Join HQTA segment to stop
def hqta_segment_to_stop(hqta_segments, stops):    
    
    segment_to_stop = (dask_geopandas.sjoin(
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


def hqta_segment_keep_one_stop(hqta_segments, stop_times):
    # Find stop with the highest trip count
    # If there are multiple stops within hqta_segment, only keep 1 stop
    trip_count_by_stop = dask_utils.find_stop_with_high_trip_count(stop_times)

    # Keep the stop in the segment with highest trips
    segment_to_stop = (dd.merge(
            hqta_segments, 
            trip_count_by_stop,
            on = stop_cols
        ).sort_values(["hqta_segment_id", "n_trips"], ascending=[True, False])
        .drop_duplicates(subset="hqta_segment_id")
        .reset_index(drop=True)
    )
    
    return segment_to_stop
    

def add_hqta_segment_peak_trips(df, aggregated_stop_times):
    
    def max_trips_by_segment(df, group_cols):
        df2 = (df
               .groupby(group_cols)
               .agg({"n_trips": np.max})
               .reset_index()
              )
        return df2.compute() # compute is what makes it a pandas df, rather than dask df    
    
    # Flexible AM peak - find max trips at the stop before noon
    am_max = (max_trips_by_segment(
        aggregated_stop_times[aggregated_stop_times.departure_hour < 12], 
        group_cols = stop_cols
        ).rename(columns = {"n_trips": "am_max_trips"})
    )
    
    # Flexible PM peak - find max trips at the stop before noon
    pm_max = (max_trips_by_segment(
        aggregated_stop_times[aggregated_stop_times.departure_hour >= 12],
        group_cols = stop_cols
        ).rename(columns = {"n_trips": "pm_max_trips"})
    )
    
    # This is at the stop_id level
    peak_trips_by_segment = dd.merge(
        am_max, pm_max,
        on = stop_cols,
    )
    
    # Merge at the hqta_segment_id-stop_id level to get it back to segments
    gdf = dd.merge(
        df[stop_cols + segment_cols + 
           ["calitp_url_number", "route_id", "route_direction", "geometry"]].drop_duplicates(),
        peak_trips_by_segment,
        on = stop_cols,
        how = "left"
    )
    
    gdf = gdf.assign(
        am_max_trips = gdf.am_max_trips.fillna(0).astype(int),
        pm_max_trips = gdf.pm_max_trips.fillna(0).astype(int),
    )
        
    return gdf.compute()


def identify_hq_transit_corr(df):
    df = df.assign(
        hq_transit_corr = df.apply(lambda x: 
                                   True if (x.am_max_trips > 4 and 
                                            (x.pm_max_trips > 4))
                                   else False, axis=1)
    )

    return df


def single_operator_hqta(routelines, trips, stop_times, stops):
    # Pare down all the shape_id-trip_id combos down to route_id
    # This is the dissolved shape for each route_id (no more shape_id)
    route_shapes = dask_utils.select_needed_shapes_for_route_network(routelines, trips)
    
    # Loop through each row to segment that route's line geom into hqta segments
    # Looping because dask_utils.segment_route function cuts 1 line geom into several lines
    all_routes = gpd.GeoDataFrame()
    
    for i in route_shapes.index:
        one_route = route_shapes[route_shapes.index==i]
        gdf = dask_utils.segment_route(one_route)
    
        all_routes = pd.concat([all_routes, gdf])
    
    
    # Add HQTA segment ID
    all_routes2 = dask_utils.add_segment_id(all_routes)

    ##generous buffer for street/sidewalk width? 
    # Required to spatially find stops within each segment
    all_routes3 = dask_utils.add_buffer(all_routes2, buffer_size=50)
    
    # Convert to dask gdf
    hqta_segments = dask_geopandas.from_geopandas(all_routes3, npartitions=1)
    # Join hqta segment to stops
    segment_to_stop = hqta_segment_to_stop(hqta_segments, stops)
    
    # Within hqta segment, if there are multiple stops, keep stop with highest trip count
    segment_to_stop_unique = hqta_segment_keep_one_stop(segment_to_stop, stop_times)
    
    # Get aggregated stops by departure_hour and stop_id
    trips_by_stop_hour = dask_utils.stop_times_aggregation_by_hour(stop_times)
    
    # By hqta segment, find the max trips for AM/PM peak
    segment_with_max_stops = add_hqta_segment_peak_trips(
        segment_to_stop_unique, trips_by_stop_hour)
    
    # Tag whether that row is a HQ transit corr
    hq_transit_segments = identify_hq_transit_corr(segment_with_max_stops)
    
    return hq_transit_segments
    


def import_data(itp_id, date_str):
    FILE_PATH = f"{rt_utils.GCS_FILE_PATH}cached_views/"

    routelines = dask_geopandas.read_parquet(
                f"{FILE_PATH}routelines_{itp_id}_{date_str}.parquet")
    stop_times = dd.read_parquet(f"{FILE_PATH}st_{itp_id}_{date_str}.parquet")
    stops = dask_geopandas.read_parquet(f"{FILE_PATH}stops_{itp_id}_{date_str}.parquet")
    
    # For Metrolink, trips need to have shape_id manually filled in,
    # since it shows up as NaN in the raw GTFS files
    if itp_id == 323:
        trips1 = pd.read_parquet(f"{FILE_PATH}trips_{itp_id}_{date_str}.parquet")
        trips2 = gtfs_utils.fill_in_metrolink_trips_df_with_shape_id(trips1)
        trips = dd.from_pandas(trips2, npartitions=1) 
    else:
        trips = dd.read_parquet(f"{FILE_PATH}trips_{itp_id}_{date_str}.parquet")
    
    return routelines, trips, stop_times, stops
    
    
    
if __name__=="__main__":
    date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)
    
    start_time = dt.datetime.now()
            
    for itp_id in ITP_IDS_IN_GCS:

        operator_start = dt.datetime.now()
            
        routelines, trips, stop_times, stops = import_data(itp_id, date_str)   

        print(f"read in cached files: {itp_id}")                
            
        # The files are stored in GCS regardless, so they'll always return something
        # But, only keep going if all the files have rows present
        # If any are zero, skip it, because we can't draw HQTA boundaries on it
        if ((len(routelines) > 0) and (len(trips) > 0) and 
            (len(stop_times) > 0) and (len(stops) > 0)):
            
            gdf = single_operator_hqta(routelines, trips, stop_times, stops)

            print(f"created single operator hqta: {itp_id}")

            # Export each operator to test GCS folder (separate from Eric's)        
            utils.geoparquet_gcs_export(
                gdf, f'{TEST_GCS_FILE_PATH}bus_corridors/', f'{itp_id}_bus')

            print(f"successful export: {itp_id}")

            operator_end = dt.datetime.now()
            print(f"execution time for {itp_id}: {operator_end - operator_start}")
        else:
            continue
    
    end_time = dt.datetime.now()
    print(f"total execution time: {end_time-start_time}")
        
