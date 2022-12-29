import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import numpy as np
import pandas as pd
import zlib

import A1_vehicle_positions as A1
from shared_utils import geography_utils, utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"

analysis_date = "2022-10-12"

def merge_routes_to_trips(routelines: dg.GeoDataFrame, 
                          trips: dd.DataFrame) -> dg.GeoDataFrame:   
    """
    Merge routes and trips tables.
    Keep the longest shape by route_length in each direction.
    
    For LA Metro, out of ~700 unique shape_ids,
    this pares it down to ~115 route_ids.
    Use this pared down shape_ids to get hqta_segments.
    """
    shape_id_cols = ["calitp_itp_id", "shape_id"]
    route_dir_cols = ["calitp_itp_id", "route_id", "direction_id"]

    routelines_ddf = routelines.assign(
        route_length = routelines.geometry.length,
    )
        
    # Merge routes to trips with using trip_id
    # Keep route_id and shape_id, but drop trip_id by the end
    m1 = (dd.merge(
            routelines_ddf,
            # Don't merge using calitp_url_number because ITP ID 282 (SFMTA)
            # can use calitp_url_number = 1
            # Just keep calitp_url_number = 0 from routelines_ddf
            trips[shape_id_cols + ["route_id", "direction_id"]],
            on = shape_id_cols,
            how = "inner",
        ).drop_duplicates(subset = route_dir_cols + ["route_length"])
        .reset_index(drop=True)
    )
    
    # If direction_id is missing, then later code will break, because
    # we need to find the longest route_length
    # Don't really care what direction is, since we will replace it with north-south
    # Just need a value to stand-in, treat it as the same direction
    m1 = m1.assign(
        direction_id = m1.direction_id.fillna('0')
    )
    
    m1 = m1.assign(    
        # dask can only sort by 1 column
        # so, let's combine route-dir into 1 column and drop_duplicates
        route_dir_identifier = m1.apply(
            lambda x: zlib.crc32((str(x.calitp_itp_id) + 
                x.route_id + str(x.direction_id)).encode("utf-8")), 
            axis=1, meta=("route_dir_identifier", "int"))
    )
    
    # Keep the longest shape_id for each direction
    # with missing direction_id filled in
    longest_shapes = (m1.sort_values("shape_id")
                      .drop_duplicates("route_dir_identifier")
                      .rename(columns = {"shape_id": "longest_shape_id"})
                     )
        
    return longest_shapes


def get_longest_shapes(analysis_date: str):
    trips = A1.get_scheduled_trips(analysis_date)
    routelines = A1.get_routelines(analysis_date)

    longest_shapes = merge_routes_to_trips(routelines, trips)
    
    return longest_shapes


if __name__ == "__main__":
    
    longest_shapes = get_longest_shapes(analysis_date)
    
    # Cut segments
    longest_shapes_gdf = longest_shapes.compute()

    segments = geography_utils.cut_segments(
        longest_shapes_gdf,
        group_cols = ["calitp_itp_id", "calitp_url_number", 
                      "route_id", "direction_id", "longest_shape_id",
                      "route_dir_identifier", "route_length"],
        segment_distance = 1_000
    )
    
    utils.geoparquet_gcs_export(
        segments,
        DASK_TEST,
        "longest_shape_segments"
    )

