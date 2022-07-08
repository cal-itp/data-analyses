"""
Move bus_corridors.ipynb into script.
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd
import pandas as pd

from siuba import *

import A1_rail_ferry_brt as rail_ferry_brt
import utilities
import dask_utils
from shared_utils import rt_utils, geography_utils

analysis_date = rail_ferry_brt.analysis_date

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


## Join HQTA segment to stop
# Find nearest stop
def hqta_segment_to_stop(hqta_segments, stops):    
    segment_to_stop = (dask_geopandas.sjoin(
            stops.drop(columns = ["stop_lon", "stop_lat"]),
            hqta_segments[["hqta_segment_id", "geometry"]],
            how = "inner",
            predicate = "intersects"
        ).drop(columns = ["index_right"])
        .drop_duplicates(subset=["calitp_itp_id", "stop_id", "hqta_segment_id"])
        .reset_index(drop=True)
    )

    # Dask geodataframe, even if you drop geometry col, retains df as gdf
    # Use compute() to convert back to df or gdf and merge
    segment_to_stop2 = segment_to_stop.drop(columns = "geometry").compute()
    
    segment_to_stop3 = pd.merge(
        hqta_segments[["hqta_segment_id", "geometry"]].compute(),
        segment_to_stop2,
        on = "hqta_segment_id",
        how = "inner"
    )
    
    return segment_to_stop3


def hqta_segment_with_max_trips(df, peak_trips_by_stop):
    ddf = dd.from_pandas(df[["hqta_segment_id", "calitp_itp_id", "stop_id"]], 
                         npartitions=1)

    # Within a hqta_segment_id find
    # max am_peak trips and max pm_peak trips
    # drop stop_id info (since multiple stops can share the same max)    
    peak_trips_by_stop_segment = dd.merge(
        ddf, 
        peak_trips_by_stop, 
        on = ["calitp_itp_id", "stop_id"],
        how = "inner",
    )
    
    peak_trips_by_segment = (peak_trips_by_stop_segment.groupby("hqta_segment_id")
                         .agg({"am_max_trips": "max", 
                               "pm_max_trips": "max"})
                         .reset_index()
                        ).compute()
    
    # Merge the geometry back in after aggregation (finding max trips along that segment)
    gdf = pd.merge(
        df[["hqta_segment_id", "geometry"]].drop_duplicates(),
        peak_trips_by_segment,
        on = "hqta_segment_id",
        how = "left"
    )

    gdf = gdf.assign(
        hq_transit_corr = gdf.apply(lambda x: 
                                    True if (x.am_max_trips > 4 and (x.pm_max_trips > 4))
                                    else False, axis=1)
    )
    
    return gdf


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
    
    hqta_segments = dask_geopandas.from_geopandas(all_routes3, npartitions=1)

    segment_to_stop = hqta_segment_to_stop(hqta_segments, stops)
    peak_trips_by_stop = dask_utils.stop_times_aggregation(stop_times)

    segment_with_max_stops = hqta_segment_with_max_trips(segment_to_stop, peak_trips_by_stop)
    
    return segment_with_max_stops


if __name__=="__main__":
    date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)

    itp_id_list = [182]

    for itp_id in itp_id_list:
        # Import data
        FILE_PATH = f"{rt_utils.GCS_FILE_PATH}cached_views/"
        
        routelines = gpd.read_parquet(f"{FILE_PATH}routelines_{itp_id}_{date_str}.parquet")
        trips = dd.read_parquet(f"{FILE_PATH}trips_{itp_id}_{date_str}.parquet")
        stop_times = dd.read_parquet(f"{FILE_PATH}st_{itp_id}_{date_str}.parquet")
        stops = dask_geopandas.read_parquet(f"{FILE_PATH}stops_{itp_id}_{date_str}.parquet")
        