"""
Cut road segments.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import gcsfs
import pandas as pd

from typing import Literal

from calitp_data_analysis.sql import to_snakecase
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (analysis_date, 
                                              SEGMENT_GCS, 
                                              GCS_FILE_PATH, 
                                              PROJECT_CRS
                                             )
from calitp_data_analysis import geography_utils, utils

SHARED_GCS = f"{GCS_FILE_PATH}shared_data/"

"""
TIGER
"""
def load_roads(road_type_wanted: list) -> gpd.GeoDataFrame:
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
    df = gpd.read_parquet(
        f"{SHARED_GCS}all_roads_2020_state06.parquet",
        filters=[("MTFCC", "in", road_type_wanted)],
        columns=["LINEARID", "MTFCC", "FULLNAME", "geometry"],
    ).to_crs(PROJECT_CRS).pipe(to_snakecase)

    # If a road has mutliple rows but the same
    # linear ID, dissolve it so it becomes one row.    
    df = (df.sort_values(["linearid", "mtfcc", "fullname"])
          .drop_duplicates(subset=["linearid", "fullname"])
          .dissolve(by="linearid")
          .reset_index()
         )
    
    df = df.assign(
        road_length = df.geometry.length
    )

    return df

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
    roads: gpd.GeoDataFrame,
    shapes: gpd.GeoDataFrame
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
        on = "inner",
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
    
    shapes = helpers.import_scheduled_shapes(
        "2023-09-13",
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = False
    )
    
    local_roads_to_cut = sjoin_shapes_to_local_roads(long_roads, shapes)
    
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
    
    return gdf


def monthly_local_linearids(
    analysis_date: str, 
    segment_length_meters: int
) -> gpd.GeoDataFrame:
    """
    Instead of re-cutting all local roads found from the last run,
    only cut the new local roads that are found. 
    
    Args:
        analysis_date: analysis_date

    """
    already_cut = pd.read_parquet(
        f"{SHARED_GCS}segmented_roads_2020_state06_local_base.parquet",
        columns = ["linearid"]
    ).linearid.unique().tolist()
    
    local_roads = load_roads(["S1400"]).query("linearid not in @already_cut")
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = False
    )
    
    local_roads_to_cut = sjoin_shapes_to_local_roads(local_roads, shapes)
    
    segmented_roads = cut_segments_dask(
        local_roads_to_cut,
        ["linearid", "mtfcc", "fullname"],
        segment_length_meters
    )
    
    # Save
    utils.geoparquet_gcs_export(
        segmented_roads,
        f"{SHARED_GCS}road_segments_monthly_append/",
        f"segmented_local_{analysis_date}"
    )


"""
Segment primary/secondary roads & local roads (base)
"""
def cut_roads(
    road_type_name: Literal["primarysecondary", "local"],
    segment_length_meters: int,  
):
    """
    By default, we always want to cut primary/secondary roads, and
    have a set of local roads we have precut.
    
    For every subsequent month, we can check for additional roads we need to keep.
    """
    if road_type_name == "primarysecondary":
        road_type_values = ["S1100", "S1200"]
        roads = local_roads(road_type_values)

        roads_segmented = geography_utils.cut_segments(
            roads,
            group_cols = ["linearid", "mtfcc", "fullname"],
            segment_distance = segment_length_meters,
        ).sort_values(
            ["linearid", "segment_sequence"]
        ).reset_index(drop=True)
        
        utils.geoparquet_gcs_export(
            roads_segmented,
            SHARED_GCS,
            f"segmented_roads_2020_state06_{road_type_name}"
        )
        
    elif road_type_name == "local":
        road_type_values = ["S1400"]
        roads = load_roads(road_type_values)

        roads_segmented = local_roads_base(roads, segment_length_meters)
        
        utils.geoparquet_gcs_export(
            roads_segmented,
            SHARED_GCS,
            f"segmented_roads_2020_state06_{road_type_name}_base"
        )
        
    return
   

# CD to rt_segment_speeds -> pip install -r requirements.txt
if __name__ == '__main__': 
    
    start = datetime.datetime.now()

    ROAD_SEGMENT_METERS = 1_000
    '''
    # Always cut all the primary and secondary roads
    road_type = "primarysecondary"
    cut_roads(road_type, ROAD_SEGMENT_METERS)
    
    time1 = datetime.datetime.now()
    print(f"cut primary/secondary roads: {time1 - start}")
    
    # Grab Sep 2023's shapes as base to grab majority of local roads we def need to cut
    road_type = "local"
    cut_roads(road_type, ROAD_SEGMENT_METERS)
    
    time2 = datetime.datetime.now()
    print(f"cut local roads base: {time2 - time1}")
    '''
    time2 = datetime.datetime.now()

    monthly_local_linearids(analysis_date, ROAD_SEGMENT_METERS)
    time3 = datetime.datetime.now()
    print(f"add local linearids for this month: {time3 - time2}")
        
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
    