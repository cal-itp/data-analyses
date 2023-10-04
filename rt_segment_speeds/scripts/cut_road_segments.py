"""
Cut road segments.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd
import sys

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
    
    return df2 

"""
Primary/Secondary Roads 
"""
def cut_primary_secondary_roads(
    roads: gpd.GeoDataFrame,
    segment_length_meters: int
) -> gpd.GeoDataFrame:
    """
    Cut all primary and secondary roads.
    Add a primary cardinal direction for the segment.
    """
    roads_segmented = geography_utils.cut_segments(
        roads,
        group_cols = ["linearid", "mtfcc", "fullname"],
        segment_distance = segment_length_meters,
    ).sort_values(
        ["linearid", "segment_sequence"]
    ).reset_index(drop=True)
    
    roads_segmented2 = add_segment_direction(roads_segmented)

    return roads_segmented2
    
    
"""
Local Roads (base)
"""
def explode_segments(
    gdf: gpd.GeoDataFrame,
    group_cols: list,
    segment_col: str = "segment_geometry"
) -> gpd.GeoDataFrame:
    """
    Explode the column that is used to store segments, which is a list.
    Take the list and create a row for each element in the list.
    We'll do a rough rank so we can order the segments.
    """
    gdf_exploded = gdf.explode(segment_col).reset_index(drop=True)
    
    gdf_exploded["temp_index"] = gdf_exploded.index
    
    gdf_exploded = gdf_exploded.assign(
        segment_sequence = (gdf_exploded
                        .groupby(group_cols, observed=True, group_keys=False)
                        .temp_index
                        .transform("rank") - 1
                        # there are NaNs, but since they're a single segment, just use 0
                       ).fillna(0).astype("int16") 
    )
    
    # Drop the original line geometry, use the segment geometry only
    gdf_exploded2 = (
        gdf_exploded
        .drop(columns = ["geometry", "temp_index"])
        .rename(columns = {segment_col: "geometry"})
        .set_geometry("geometry")
        .set_crs(gdf_exploded.crs)
        .sort_values(group_cols + ["segment_sequence"])
        .reset_index(drop=True)
    )
    
    return gdf_exploded2


def sjoin_shapes_to_local_roads(
    shapes: dg.GeoDataFrame,
    roads: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Using shapes instead of stops roughly doubles the linearids we pick up.
    All the ones in stops is found in shapes (as expected).
    """
    shapes = shapes.assign(
        geometry = shapes.geometry.buffer(100)
    )

    # Keep unique linearids from the sjoin
    local_roads_sjoin_shapes = dg.sjoin(
        shapes,
        roads,
        how = "inner",
        predicate = "intersects"
    ).linearid.unique()
    
    local_ids = local_roads_sjoin_shapes.compute().tolist()
    
    # Filter down our local roads to ones that showed up in the sjoin
    local_roads_cut = roads[
        roads.linearid.isin(local_ids)
    ].reset_index(drop=True)
    
    return local_roads_cut


def cut_segments_dask(
    roads_to_cut: gpd.GeoDataFrame,
    group_cols: list,
    segment_length_meters: int
) -> gpd.GeoDataFrame:
    # Use dask geopandas to create a new column that is a list
    # and contains all the shapely cut segments
    gddf = dg.from_geopandas(roads_to_cut, npartitions=20)
    
    gddf["segment_geometry"] = gddf.apply(
        lambda x: 
        geography_utils.create_segments(x.geometry, int(segment_length_meters)), 
        axis=1, meta = ("segment_geometry", "object")
    )
    
    # Exploding only works as gdf
    gdf = gddf.compute()
    
    gdf2 = explode_segments(
        gdf, 
        group_cols = group_cols,
        segment_col = "segment_geometry"
    )
    
    return gdf2


def local_roads_base(
    roads: gpd.GeoDataFrame, 
    segment_length_meters: int
) -> gpd.GeoDataFrame:
    """
    Use Sep 2023 shapes to figure out the "base" of local roads we are cutting.
    In the future, grab only the local road linearids that are not here,
    and cut any additional ones.
    """
    # If roads are less than our segment length
    # keep these intact...they are freebies, already segmented for us (1 segment)
    short_roads = roads[
        roads.road_length <= segment_length_meters
    ].reset_index(drop=True)
    
    long_roads = roads[
        roads.road_length > segment_length_meters
    ].reset_index(drop=True)
    
    # Remove Amtrak, because some shapes are erroneous and 
    # kills the kernel while buffering by 100m 
    # Since Amtrak is not traveling on roads, we'll be removing those vp anyway
    base_date = "2023-09-13"
    
    trips = helpers.import_scheduled_trips(
        base_date,
        columns = ["name", "shape_array_key"],
        get_pandas = True
    )
    
    shapes = helpers.import_scheduled_shapes(
        base_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = False
    ).merge(
        trips,
        on = "shape_array_key",
        how = "inner"
    ).query('name != "Amtrak Schedule"')
    
    local_roads_to_cut = sjoin_shapes_to_local_roads(shapes, long_roads)
    
    segmented_long_roads = cut_segments_dask(
        local_roads_to_cut,
        ["linearid", "mtfcc", "fullname"],
        segment_length_meters
    )
    
    # Concatenate the short roads and the segmented roads
    gdf = pd.concat([
        segmented_long_roads, 
        short_roads.assign(segment_sequence = 0).astype({"segment_sequence": "int16"})
    ], axis=0).reset_index(drop=True)
    
    gdf2 = add_segment_direction(gdf)
    
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
    ).drop(columns = ["origin", "destination"])

    return df
    
    
def append_reverse_segments(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Flip the geometry for road segment and append it to df.
    If we have eastbound segments, create set of westbound segments
    so we can capture both sides of street.
    """
    df_flipped = df.assign(
        geometry = df.geometry.reverse(),
    )
    
    df_flipped2 = add_segment_direction(df_flipped)
    
    both_sets_of_segments = pd.concat(
        [df, df_flipped2], axis=0
    ).sort_values(
        ["linearid", "segment_sequence", "primary_direction"]
    ).reset_index(drop=True)
    
    return both_sets_of_segments
 

if __name__ == '__main__': 
    
    LOG_FILE = "../logs/cut_road_segments.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    # Always cut all the primary and secondary roads
    road_type = "primarysecondary"
    road_type_values = ["S1100", "S1200"]

    roads = load_roads(filtering = [("MTFCC", "in", road_type_values)])
    roads_segmented = cut_primary_secondary_roads(roads, ROAD_SEGMENT_METERS)
    primary_secondary_roads = append_reverse_segments(roads_segmented).drop(
        columns = "road_length")
    
    utils.geoparquet_gcs_export(
        primary_secondary_roads,
        SHARED_GCS,
        f"segmented_roads_2020_{road_type}"
    )
    
    time1 = datetime.datetime.now()
    logger.info(f"cut primary/secondary roads: {time1 - start}")
    
    # Grab Sep 2023's shapes as base to grab majority of local roads we def need to cut
    road_type = "local"    
    roads = load_roads(filtering = [("MTFCC", "==", "S1400")])
    roads_segmented = local_roads_base(roads, ROAD_SEGMENT_METERS)
    local_roads = append_reverse_segments(roads_segmented).drop(
        columns = "road_length")
    
    utils.geoparquet_gcs_export(
        local_roads,
        SHARED_GCS,
        f"segmented_roads_2020_{road_type}"
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"cut local roads base: {time2 - time1}")
        
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
    