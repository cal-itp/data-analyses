"""
Cut road segments.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed
from loguru import logger
from typing import Literal

from calitp_data_analysis.sql import to_snakecase
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (SHARED_GCS, 
                                              PROJECT_CRS, 
                                              ROAD_SEGMENT_METERS
                                             )
from calitp_data_analysis import geography_utils, utils
from shared_utils import rt_utils

"""
TIGER
"""
def load_roads(filtering: tuple) -> gpd.GeoDataFrame:
    """
    Load roads based on what you filter for MTFCC values (road types).
    Do some basic cleaning/dissolving.

    Args:
        road_type_wanted (list): the type of roads you want.
            S1100: primary roads
            S1200: secondary roads
            S1400: local roads                    
                            
        https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2019/TGRSHP2019_TechDoc.pdf

    Returns:
        gdf. As of 4/18/23, returns 953914 nunique linearid
    """
    # https://stackoverflow.com/questions/56522977/using-predicates-to-filter-rows-from-pyarrow-parquet-parquetdataset
    df = gpd.read_parquet(
        f"{SHARED_GCS}all_roads_2020_state06.parquet",
        filters = filtering,
        columns=["LINEARID", "MTFCC", "FULLNAME", "geometry"],
    ).to_crs(PROJECT_CRS).pipe(to_snakecase)
    
    # If a road has mutliple rows but the same
    # linear ID, dissolve it so it becomes one row.    
    df = (df.sort_values(["linearid", "mtfcc", "fullname"])
          .drop_duplicates(subset=["linearid", "mtfcc", "fullname"])
          .dissolve(by=["linearid", "mtfcc"])
          .reset_index()
         )
    
    df = df.assign(
        road_length = df.geometry.length
    )
    
    # There are some multilinestrings that appear for local roads
    # This will throw error when we try to add primary cardinal direction
    # For this handful of roads, keep the longest length,
    # a cursory check for the base local roads shows that 75th percentile < 2 meters
    multi_df = df[
        df.geometry.geom_type=="MultiLineString"
    ].explode().reset_index(drop=True)
    
    multi_df = multi_df.assign(
        road_length = multi_df.geometry.length
    )
    
    multi_df2 = multi_df.assign(
        max_part = (multi_df.groupby(["linearid", "mtfcc"], 
                                     observed= True, group_keys=False)
                    .road_length
                    .transform("max")
                   )
    ).query('road_length == max_part').drop(
        columns = "max_part"
    )
    
    df2 = pd.concat([
        df[df.geometry.geom_type!="MultiLineString"], 
        multi_df2
    ], axis=0).reset_index(drop=True)
    
    df2 = df2.reindex(
        columns = ["linearid", "mtfcc", "fullname", 
                   "geometry", "road_length"]
    )
    
    return df2 
    
    
def cut_segments(
    gdf: gpd.GeoDataFrame,
    group_cols: list,
    segment_length_meters: int
) -> gpd.GeoDataFrame:
    gdf["segment_geometry"] = gdf.apply(
        lambda x: 
        geography_utils.create_segments(x.geometry, int(segment_length_meters)), 
        axis=1,
    )
    
    gdf2 = geography_utils.explode_segments(
        gdf,
        group_cols,
        segment_col = "segment_geometry",
    )[group_cols + ["segment_sequence", "geometry"]
     ].set_geometry("geometry").set_crs(PROJECT_CRS)
    
    return gdf2 


def cut_local_roads(
    roads: gpd.GeoDataFrame, 
    group_cols: list,
    segment_length_meters: int
) -> gpd.GeoDataFrame:
    
    # If roads are less than our segment length
    # keep these intact...they are freebies, already segmented for us (1 segment)
    short_roads = roads[
        roads.road_length <= segment_length_meters
    ].drop(columns = "road_length").reset_index(drop=True)

    short_roads = short_roads.assign(
        segment_sequence = 0
    ).astype({"segment_sequence": "int16"})

    long_roads = roads[
        roads.road_length > segment_length_meters
    ].drop(columns = "road_length").reset_index(drop=True)

    
    long_roads = long_roads.repartition(npartitions=50)
    
    time1 = datetime.datetime.now()

    # Concatenate the short roads and the segmented roads
    road_dtypes = long_roads[group_cols].dtypes.to_dict()
    
    segmented_long_roads = long_roads.map_partitions(
        cut_segments,
        group_cols,
        segment_length_meters,
        meta = {
            **road_dtypes,
            "segment_sequence": "int16",
            "geometry": "geometry"
        },
        align_dataframes = False
    ).persist()
    
    print("first persist")
    print(segmented_long_roads.dtypes)
        
    gdf = dd.multi.concat(
        [short_roads, segmented_long_roads],
        axis=0
    ).reset_index(drop=True)
    
    gdf = gdf.repartition(npartitions=20)
    
    gdf_dtypes = gdf.dtypes.to_dict()
    
    gdf2 = gdf.map_partitions(
        add_segment_direction,
        meta = {
            **gdf_dtypes, 
            "origin": "geometry",
            "destination": "geometry",
            "primary_direction": "object"
            }
    ).persist()
    
    time2 = datetime.datetime.now()
    print(f"map partitions to cut segments and add direction: {time2 - time1}")
    
    return gdf2


def add_segment_direction(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add primary cardinal direction for road segment.
    Since we're going to reverse the road segment to create the
    one running on other side, we need to distinguish between
    these 2 rows with same linearid-mtfcc-fullname.
    """
    df = rt_utils.add_origin_destination(df)
    
    df = df.assign(
        primary_direction = df.apply(
            lambda x: 
            rt_utils.primary_cardinal_direction(x.origin, x.destination),
            axis=1,
        )
    )

    return df
 

if __name__ == '__main__': 
    
    LOG_FILE = "../logs/cut_road_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    ROAD_SEGMENT_METERS = 1_609*2
    road_cols = ["linearid", "mtfcc", "fullname"]

    # Always cut all the primary and secondary roads
    road_type = "primarysecondary"
    road_type_values = ["S1100", "S1200"]

    roads = load_roads(filtering = [("MTFCC", "in", road_type_values)])
    primary_secondary_roads = cut_segments(
        roads, road_cols, ROAD_SEGMENT_METERS)
    primary_secondary_roads = add_segment_direction(primary_secondary_roads)
    
    utils.geoparquet_gcs_export(
        primary_secondary_roads,
        SHARED_GCS,
        f"segmented_roads_twomile_2020_{road_type}"
    )
    
    del roads, primary_secondary_roads
    time1 = datetime.datetime.now()
    logger.info(f"cut primary/secondary roads: {time1 - start}")
        
    road_type = "local"  
    road_type_values = ["S1400"]
    roads = delayed(load_roads)(filtering = [("MTFCC", "in", road_type_values)])
    
    local_gddf = dd.from_delayed(roads).repartition(npartitions=50)
    
    local_roads = cut_local_roads(
        local_gddf, road_cols, ROAD_SEGMENT_METERS
    ).compute()
    
    utils.geoparquet_gcs_export(
        local_roads,
        SHARED_GCS,
        f"segmented_roads_twomile_2020_{road_type}"
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"cut local roads: {time2 - time1}")
        
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")