"""
Cut road segments.
"""
#import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from calitp_data_analysis.sql import to_snakecase
from segment_speed_utils.project_vars import (SHARED_GCS, 
                                              PROJECT_CRS, 
                                              ROAD_SEGMENT_METERS
                                             )
from calitp_data_analysis import geography_utils, utils
from shared_utils import rt_utils

def load_roads(**kwargs) -> gpd.GeoDataFrame:
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
        columns=["LINEARID", "MTFCC", "FULLNAME", "geometry"],
        **kwargs,
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
    roads: gpd.GeoDataFrame,
    group_cols: list,
    segment_length_meters: int
) -> gpd.GeoDataFrame:
    
    short_roads = roads[
        roads.road_length <= segment_length_meters
    ].drop(columns = "road_length").assign(
        segment_sequence = 0
    ).astype({"segment_sequence": "int16"}).reset_index(drop=True)
    
    gdf = roads[
        roads.road_length > segment_length_meters
    ].drop(columns = "road_length").reset_index(drop=True)
    
    gdf["segment_geometry"] = gdf.apply(
        lambda x: 
        geography_utils.create_segments(x.geometry, int(segment_length_meters)), 
        axis=1,
    )
    
    gdf2 = geography_utils.explode_segments(
        gdf,
        group_cols,
        segment_col = "segment_geometry",
    )[
        group_cols + ["segment_sequence", "geometry"]
    ].set_geometry("geometry").set_crs(PROJECT_CRS)
    
    segmented = pd.concat(
        [short_roads, gdf2], axis=0
    ).reset_index(drop=True)
    
    segmented = gpd.GeoDataFrame(segmented, geometry="geometry", crs=PROJECT_CRS)
    
    return segmented 


def add_segment_direction(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add primary cardinal direction for road segment.
    Since we're going to reverse the road segment to create the
    one running on other side, we need to distinguish between
    these 2 rows with same linearid-mtfcc-fullname.
    """
    df = df.dropna(subset="geometry")
    df = rt_utils.add_origin_destination(df)
    
    df = df.assign(
        primary_direction = df.apply(
            lambda x: 
            rt_utils.primary_cardinal_direction(x.origin, x.destination),
            axis=1,
        )
    )

    return df


def cut_road_segments(
    group_cols: list,
    segment_length_meters: int, 
    road_segment_str: str, 
    **kwargs
):
    roads = delayed(load_roads)(**kwargs)
    
    road_segments = delayed(cut_segments)(
        roads, 
        group_cols, 
        segment_length_meters
    ).pipe(add_segment_direction)
    
    road_segments = compute(road_segments)[0]
    
    utils.geoparquet_gcs_export(
        road_segments,
        SHARED_GCS,
        f"segmented_roads_{road_segment_str}_2020"
    )   
    
    return 
    
    
if __name__ == '__main__': 
    
    LOG_FILE = "../logs/cut_road_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    road_cols = ["linearid", "mtfcc", "fullname"]

    cut_road_segments(road_cols, ROAD_SEGMENT_METERS, "onekm")
 
    '''
    TWO_MILES_IN_METERS = 1_609*2
    cut_road_segments(road_cols, TWO_MILES_IN_METERS, "twomile")
    '''
        
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")