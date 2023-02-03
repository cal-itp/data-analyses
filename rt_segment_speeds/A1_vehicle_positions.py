"""
Concatenate vehicle positions for operators on a single day.

Note: vehicle positions get changed back to tabular df 
because adding a geometry column makes the file large.
This script could be changed to just do the concatenation
without making the geometry, and save the geometry making for later.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import pandas as pd
import gcsfs
import geopandas as gpd
import shapely

from shared_utils import geography_utils, utils
from update_vars import COMPILED_CACHED_VIEWS, DASK_TEST, analysis_date


def get_scheduled_trips(analysis_date: str) -> dd.DataFrame:
    """
    Get scheduled trips info (all operators) for single day, 
    and keep subset of columns.
    """
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet")
    
    keep_cols = ["feed_key", "name", 
                 "trip_id", "shape_id",
                 "route_id", "direction_id"
                ] 
    trips = trips[keep_cols]
    
    return trips


def get_routelines(
    analysis_date: str, buffer_size: int = 50
) -> dg.GeoDataFrame: 
    """
    Import routelines (shape_ids) and add route_length and buffer by 
    some specified size (50 m to start)
    """
    routelines = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}.parquet"
    ).to_crs(geography_utils.CA_NAD83Albers)
                 
    routelines = routelines.assign(
        route_length = routelines.geometry.length,
        shape_geometry_buffered = routelines.geometry.buffer(buffer_size)
    )
    
    return routelines
        

if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    # Append individual operator vehicle position parquets together
    # and cache a single vehicle positions parquet
    fs = gcsfs.GCSFileSystem()
    fs_list = fs.ls(f"{DASK_TEST}")

    vp_files = [i for i in fs_list if "vp_raw" in i 
                and f"{analysis_date}_batch" in i]
    
    df = pd.DataFrame()
    
    for f in vp_files:
        batch_df = pd.read_parquet(f"gs://{f}")
        df = pd.concat([df, batch_df], axis=0)
    
    time1 = datetime.datetime.now()
    print(f"appended all batch parquets: {time1 - start}")
    
    # Drop Nones or else shapely will error
    df = df[df.location.notna()].reset_index(drop=True)
    
    geom = [shapely.wkt.loads(x) for x in df.location]

    gdf = gpd.GeoDataFrame(
        df, geometry=geom, 
        crs="EPSG:4326").drop(columns="location")
    
    time2 = datetime.datetime.now()
    print(f"change to gdf: {time2 - time1}")
    
    utils.geoparquet_gcs_export(
        gdf, 
        DASK_TEST,
        f"vp_{analysis_date}"
    )
    
    #df.to_parquet(f"{DASK_TEST}vp_{analysis_date}.parquet")
    end = datetime.datetime.now()
    print(f"execution time: {end-start}")
