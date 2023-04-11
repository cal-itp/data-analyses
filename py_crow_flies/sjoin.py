"""
Buffer, spatial join, and calculate distance.
For each origin, draw 20 mile buffer,
spatial join against all valid destinations,
and get distance between origin and destination points.

Save out by region.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from prep import GCS_FILE_PATH


def chunk_origin_points(df: gpd.GeoDataFrame, n: int) -> list:
    """
    Subset full POI dataset into chunks by region.
    Region is more informative than general batches,
    and Mojave seems to be really fast, but SoCal is a lot slower.
    Use smaller chunk sizes for more populated areas.
    """
    # https://stackoverflow.com/questions/33367142/split-dataframe-into-relatively-even-chunks-according-to-length
    list_df = [delayed(df[i:i+n]) for i in range(0, df.shape[0], n)]

    return list_df


def buffer_origin_sjoin_calculate_distance(
    origin_gdf: dg.GeoDataFrame,
    destination_gdf: gpd.GeoDataFrame,
    buffer_miles: int = 20
) -> dd.DataFrame:
    """
    Draw 20 mi buffer around origin point.
    Note: Our CRS is in meters, need to convert 20 miles into ___ meters.
    """
    METERS_IN_MILES = 1609.34
    
    origin_gdf = dg.from_geopandas(origin_gdf, npartitions=1)
    origin_gdf = origin_gdf.repartition(partition_size="50MB")
    
    origin_buffered = origin_gdf.assign(
        geometry = origin_gdf.geometry.buffer(
            buffer_miles * METERS_IN_MILES)
    )
            
    sjoin_to_destination = dg.sjoin(
        origin_buffered, 
        destination_gdf,
        how = "inner",
        predicate = "intersects"
    )[["poi_index_left", 
       "poi_index_right", "grid_code"]].drop_duplicates()
    
    sjoin_results = (sjoin_to_destination.rename(
        columns = {
            "poi_index_left": "origin_poi_index", 
            "poi_index_right": "destination_poi_index"}
        ).reset_index(drop=True)
        .repartition(npartitions=1)
    )
    
    # Merge point geometry back in
    with_origin_point_geom = dd.merge(
        origin_gdf,
        sjoin_results,
        left_on = "poi_index",
        right_on = "origin_poi_index",
        how = "inner"
    ).drop(columns = "poi_index")
    
    with_destin_point_geom = dd.merge(
        with_origin_point_geom,
        origin_gdf,
        left_on = "destination_poi_index",
        right_on = "poi_index",
        how = "inner"
    ).drop(columns = "poi_index")
    
    with_distance = calculate_distance(
        with_destin_point_geom, 
        "geometry_x", 
        "geometry_y"
    )
    
    return with_distance


def calculate_distance(
    gdf: dg.GeoDataFrame,
    origin_col: str, 
    destination_col: str
)-> dd.DataFrame: 
    """
    Calculate distance between origin point geometry column
    and destination point geometry column.
    This distance is as the crow flies.
    """                                
    origin_geom = gdf.set_geometry(origin_col)[origin_col]
    destin_geom = gdf.set_geometry(destination_col)[destination_col]
    
    distance = origin_geom.distance(destin_geom)
    
    gdf2 = gdf.drop(columns = [origin_col, destination_col])
    gdf2 = gdf2.assign(
        dist = distance
    )
    
    return gdf2
    
    
def wrangle_region_in_chunks(
    valid_destinations: gpd.GeoDataFrame,
    region: str, 
    n_rows_per_chunk: int, 
) -> list:
    """
    For each region, take chunks of the df and do the buffering, 
    spatial join, and distance calculation.
    
    Save each chunk as dask.delayed objects.
    Compute them and append together as ddf and 
    save out as partitioned parquet.
    
    Try doing it all at once...but the final ddf is too large to save out, 
    will crash the kernel.
    """
    keep_cols = ["poi_index", "geometry"]
    
    df = gpd.read_parquet(
        f"{GCS_FILE_PATH}all_pois.parquet", 
        columns = keep_cols, 
        filters = [[("region" , "==", region)]]
    )
    
    list_df = chunk_origin_points(df, n_rows_per_chunk)
    logger.info(f"chunk n: {n}, # of chunks: {len(list_df)}")
    
    sjoin_results = []

    for chunk_df in list_df:
        chunk_sjoin = delayed(buffer_origin_sjoin_calculate_distance)(
            chunk_df, 
            valid_destinations, 
            buffer_miles = 20
        )

        sjoin_results.append(chunk_sjoin)
    
    return sjoin_results


if __name__ == "__main__":   
    
    LOG_FILE = "./logs/t2_buffer_sjoin_distance.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    keep_cols = ["poi_index", "geometry"]

    # Use persist because we use it in subsequent functions, and it saves time
    valid_destinations = delayed(gpd.read_parquet)(
        f"{GCS_FILE_PATH}all_pois.parquet",
        filters = [[("grid_code", ">", 0)]], 
        columns = keep_cols + ["grid_code"]
    ).persist()
    
    REGION_CHUNKS = {
        "CentralCal": 12_000, 
        "Mojave": 300_000,
        "NorCal": 25_000,
        "SoCal": 6_000,
    }
    
    for region, n in REGION_CHUNKS.items():
        region_start = datetime.datetime.now()

        sjoin_results = wrangle_region_in_chunks(
            valid_destinations,
            region, 
            n, 
        )
        
        time1 = datetime.datetime.now()
        logger.info(f"create list of delayed results: {time1 - region_start}")
        
        sjoin_results2 = [compute(i)[0] for i in sjoin_results]
        time2 = datetime.datetime.now()
        logger.info(f"compute list of delayed: {time2 - time1}")
        
        sjoin_pairs_ddf = dd.multi.concat(
            sjoin_results2, axis=0
        ).reset_index(drop=True)
   
        sjoin_pairs_ddf.to_parquet(
            f"{GCS_FILE_PATH}sjoin_pairs_{region}", overwrite=True)
    
        region_end = datetime.datetime.now()
        logger.info(f"compute, concat, export batch {region}: {region_end - region_start}")
    
    finish = datetime.datetime.now()
    logger.info(f"execution time: {finish-start}")
    