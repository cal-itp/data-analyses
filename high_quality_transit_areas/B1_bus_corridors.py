"""
Move bus_corridors.ipynb into script.
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd
import numpy as np
import pandas as pd

from siuba import *

#import utilities
import dask_utils
from A1_rail_ferry_brt import analysis_date
from utilities import GCS_FILE_PATH
from shared_utils import rt_utils, geography_utils


itp_id = 182
date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)

# Skip writing geoparquet again for now 
# TODO: tweak rt_utils to overwrite export? 
# Overwriting while testing this is not ideal, don't want to mess it up

#routelines = gpd.read_parquet(f"{rt_utils.GCS_FILE_PATH}"
#                             f"cached_views/routelines_{itp_id}_{date_str}.parquet" 
#                            )

## force clear to ensure route type data present
#trips = rt_utils.get_trips(itp_id, analysis_date, force_clear=True, route_types = ['3'])
#stop_times = rt_utils.get_stop_times(itp_id, analysis_date)
#stops = rt_utils.get_stops(itp_id, analysis_date)

segment_cols = ["hqta_segment_id", "segment_sequence"]

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
            on = ["calitp_itp_id", "stop_id"]
        ).sort_values(["hqta_segment_id", "n_trips"], ascending=[True, False])
        .drop_duplicates(subset="hqta_segment_id")
        .reset_index(drop=True)
    )
    
    return segment_to_stop


def max_trips_by_segment(df, group_cols):
    df2 = (df
           .groupby(group_cols)
           .agg({"n_trips": np.max})
           .reset_index()
          )
    return df2.compute() # compute is what makes it a pandas df, rather than dask df
    

def add_hqta_segment_peak_trips(df, aggregated_stop_times):
    stop_cols = ["calitp_itp_id", "stop_id"]
        
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
    peak_trips_by_segment = pd.merge(
        am_max, pm_max,
        on = stop_cols,
    )
    
    # Merge at the hqta_segment_id-stop_id level to get it back to segments
    gdf = dd.merge(
        df[stop_cols + segment_cols + 
           ["calitp_url_number", "shape_id", "geometry"]].drop_duplicates(),
        peak_trips_by_segment,
        on = stop_cols,
        how = "left"
    ).compute()
    
    gdf = gdf.assign(
        am_max_trips = gdf.am_max_trips.fillna(0).astype(int),
        pm_max_trips = gdf.pm_max_trips.fillna(0).astype(int),
    )
        
    return gdf


#TODO: must exclude trips that run only in the AM and PM. those don't count
def identify_hq_transit_corr(df):
    df = df.assign(
        hq_transit_corr = df.apply(lambda x: 
                                   True if (x.am_max_trips > 4 and 
                                            (x.pm_max_trips > 4))
                                   else False, axis=1)
    )

    return df


def single_operator_hqta(routelines, trips, stop_times, stops):
    # Pare down all the shape_id-trip_id combos down to shape_id-route_id
    # Keep the longest route_length to use to get hqta segments
    route_shapes = dask_utils.merge_routes_to_trips(routelines, trips)
    
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


if __name__=="__main__":
    date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)

    itp_id_list = [182]

    for itp_id in itp_id_list:
        # Import data
        FILE_PATH = f"{rt_utils.GCS_FILE_PATH}cached_views/"
        
        routelines = dask_geopandas.read_parquet(
            f"{FILE_PATH}routelines_{itp_id}_{date_str}.parquet")
        trips = dd.read_parquet(f"{FILE_PATH}trips_{itp_id}_{date_str}.parquet")
        stop_times = dd.read_parquet(f"{FILE_PATH}st_{itp_id}_{date_str}.parquet")
        stops = dask_geopandas.read_parquet(f"{FILE_PATH}stops_{itp_id}_{date_str}.parquet")
        
        gdf = bus_corridors.single_operator_hqta(routelines, trips, stop_times, stops)