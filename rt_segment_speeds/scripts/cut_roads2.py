"""
Cut roads
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

def import_roads(road_type_wanted: list) -> gpd.GeoDataFrame: 
    """
    Import roads dataset and filter by MTFCC type (primary/secondary/local).
    Do some basic cleaning/dissolving.
    """
    roads = gpd.read_parquet(
        f"{SHARED_GCS}all_roads_2020_state06.parquet",
        filters=[("MTFCC", "in", road_type_wanted)],
        columns=["MTFCC", "LINEARID", "geometry", "FULLNAME"],
    ).to_crs(PROJECT_CRS).pipe(to_snakecase)
    
    
    roads2 = (roads.sort_values(["linearid", "mtfcc", "fullname"])
          .drop_duplicates(subset=["linearid", "fullname"])
          .dissolve(by="linearid")
          .reset_index()
         )
    
    roads2 = roads2.assign(
        road_length = roads2.geometry.length
    )
    
    return roads2


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
    
    
def local_roads_base(
    roads: gpd.GeoDataFrame, 
    segment_length_meters: int
) -> gpd.GeoDataFrame:
    """
    Use Sep 2023 shapes to figure out the "base" of local roads we are cutting.
    In the future, grab only the local road linearids that are not here,
    and cut any additional ones.
    """
    # If roads are less than our segment length, keep these because we get these for free
    # without putting it through segmentizing.
    short_roads = roads[roads.road_length <= segment_length_meters].reset_index(drop=True)
    long_roads = roads[roads.road_length > segment_length_meters].reset_index(drop=True)
    
    shapes = helpers.import_scheduled_shapes(
        "2023-09-13",
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = False
    )
    
    local_roads_to_cut = sjoin_shapes_to_local_roads(long_roads, shapes)
    
    # Use dask geopandas to create a new column that is a list
    # and contains all the shapely cut segments
    gddf = dg.from_geopandas(local_roads_to_cut, npartitions=20)
    
    gddf["segment_geometry"] = gddf.apply(
        lambda x: geography_utils.create_segments(x.geometry, int(segment_length_meters)), 
        axis=1, meta = ("segment_geometry", "object")
    )
    
    # Exploding only works as gdf
    gdf = gddf.compute()
    
    gdf2 = explode_segments(
        gdf, 
        group_cols = ["linearid", "mtfcc", "fullname"],
        segment_col = "segment_geometry"
    )
    
    # Concatenate the short roads and the segmented roads
    gdf3 = pd.concat([
        gdf2, 
        short_roads.assign(segment_sequence = 0).astype({"segment_sequence": "int16"})
    ], axis=0).reset_index(drop=True)
    
    return gdf3

    
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
        roads = import_roads(road_type_values)

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
        roads = import_roads(road_type_values)

        roads_segmented = local_roads_base(roads, segment_length_meters)
        
        utils.geoparquet_gcs_export(
            roads_segmented,
            SHARED_GCS,
            f"segmented_roads_2020_state06_{road_type_name}_base"
        )
        
    return


if __name__ == "__main__":
    
    start = datetime.datetime.now()

    ROAD_SEGMENT_METERS = 1_000
    
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
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
