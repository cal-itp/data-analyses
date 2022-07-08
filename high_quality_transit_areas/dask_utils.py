import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import numpy as np
import pandas as pd
import zlib

import utilities
from shared_utils import rt_utils


#------------------------------------------------------#
# Stop times
#------------------------------------------------------#
## Aggregate stops by departure hour
def stop_times_aggregation(stop_times):
    # Some fixing, transformation, aggregation with dask
    # Grab departure hour
    #https://stackoverflow.com/questions/45428292/how-to-convert-pandas-str-split-call-to-to-dask
    ddf = stop_times.assign(
        departure_hour = stop_times.departure_time.str.partition(":")[0].astype(int)
    )
    
    # Since hours past 24 are allowed for overnight trips
    # coerce these to fall between 0-23
    #https://stackoverflow.com/questions/54955833/apply-a-lambda-function-to-a-dask-dataframe
    ddf["departure_hour"] = ddf.departure_hour.map(lambda x: x-24 if x >=24 else x)

    stop_cols = ["calitp_itp_id", "stop_id"]
    # Aggregate how many trips are made at that stop by departure hour
    trips_per_hour = (ddf.groupby(stop_cols + ["departure_hour"])
                      .agg({'trip_id': 'count'})
                      .reset_index()
                      .rename(columns = {"trip_id": "n_trips"})
                     )        
        
    def max_trips_at_stop(df):
        df2 = (df
               .groupby(stop_cols)
               .agg({"n_trips": np.max})
               .reset_index()
              )
        return df2.compute() # compute is what makes it a pandas df, rather than dask df
    
    # Flexible AM peak - find max trips at the stop before noon
    am_max = (max_trips_at_stop(
        trips_per_hour[trips_per_hour.departure_hour < 12])
              .rename(columns = {"n_trips": "am_max_trips"})
    )

    
    # Flexible PM peak - find max trips at the stop after noon
    pm_max = (max_trips_at_stop(
        trips_per_hour[trips_per_hour.departure_hour >= 12])
              .rename(columns = {"n_trips": "pm_max_trips"})
    )

    df = pd.merge(
        am_max,
        pm_max,
        on = stop_cols,
    )
    
    return df


#------------------------------------------------------#
# Routelines
#------------------------------------------------------#
# For LA Metro, out of ~700 unique shape_ids,
# this pares is down to ~115 route_ids
# Use the pared down shape_ids to get hqta_segments
def merge_routes_to_trips(routelines, trips):    
    routelines_ddf = routelines.assign(
        route_length = routelines.geometry.length
    )
        
    # Merge routes to trips with using trip_id
    # Keep route_id and shape_id, but drop trip_id by the end
    shape_id_cols = ["calitp_itp_id", "calitp_url_number", "shape_id"]
    
    m1 = dd.merge(
        routelines_ddf,
        trips[shape_id_cols + ["trip_id", "route_id"]],
        on = shape_id_cols,
        how = "left",
    )
    
    # Sort in descending order by route_length
    # Since there are short/long trips for a given route_id,
    # Keep the longest one for each route_id
    m2 = (m1.sort_values(["route_id", "route_length"],
                     ascending=[True, False])
      .reset_index(drop=True)
      .drop_duplicates(subset="route_id")
      .reset_index(drop=True)
      .drop(columns = "trip_id")
     )
    
    return m2.compute()


def add_buffer(gdf, buffer_size=50):

    gdf = gdf.assign(
        geometry = gdf.geometry.buffer(buffer_size)
    )
    
    return gdf


def add_segment_id(df):
    ## compute (hopefully unique) hash of segment id that can be used 
    # across routes/operators

    df2 = df.assign(
        hqta_segment_id = df.apply(lambda x: 
                                   # this checksum hash always give same value if 
                                   # the same combination of strings are given
                                   zlib.crc32(
                                       (str(x.calitp_itp_id) + x.shape_id + x.segment_sequence)
                                       .encode("utf-8")), 
                                       axis=1),
    )

    return df2


def segment_route(gdf):
    segmented = gpd.GeoDataFrame() ##changed to gdf?

    route_cols = ["calitp_itp_id", "calitp_url_number", 
                  "route_id", "shape_id"]

    # Turn 1 row of geometry into hqta_segments, every 1,250 m
    for segment in utilities.create_segments(gdf.geometry):
        to_append = gdf.drop(columns=["geometry"])
        to_append["geometry"] = segment
        segmented = pd.concat([segmented, to_append], axis=0, ignore_index=True)
    
        segmented = segmented.assign(
            temp_index = (segmented.sort_values(route_cols)
                          .reset_index(drop=True).index
                         )
        )
    
    segmented = (segmented.assign(
        segment_sequence = (segmented.groupby(route_cols)["temp_index"]
                            .transform("rank") - 1).astype(int).astype(str)
                           )
                 .sort_values(route_cols)
                 .reset_index(drop=True)
                 .drop(columns = "temp_index")
                )
    
    return segmented
        
    
