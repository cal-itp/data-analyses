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
    segment_col: str
):
    """
    """
    gdf_exploded = gdf.explode(segment_col).reset_index(drop=True)
    
    gdf_exploded["temp_index"] = gdf_exploded.index
    
    gdf_exploded = gdf_exploded.assign(
        segment_sequence = (gdf_exploded
                        .groupby(group_cols, observed=True, group_keys=False)
                        .temp_index
                        .transform("rank") - 1
                       ).fillna(0).astype("int16")
    )
    
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
    shapes = shapes.assign(
        geometry = shapes.geometry.buffer(100)
    )
    
    local_roads_sjoin_shapes = dg.sjoin(
        shapes,
        roads,
        on = "inner",
        predicate = "intersects"
    ).linearid.unique()
    
    local_ids = local_roads_sjoin_shapes.compute().tolist()
    
    local_roads_cut = roads[
        roads.linearid.isin(local_ids)
    ].reset_index(drop=True)
    
    return local_roads_cut
    
    
def local_roads_base(segment_length_meters: int = 1_000):
    """
    """
    roads = import_roads(["S1400"])
    
    short_roads = roads[roads.road_length <= segment_length_meters].reset_index(drop=True)
    long_roads = roads[roads.road_length > segment_length_meters].reset_index(drop=True)
    
    shapes = helpers.import_scheduled_shapes(
        "2023-09-13",
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = False
    )
    
    local_roads_to_cut = sjoin_shapes_to_local_roads(long_roads, shapes)
    
    gddf = dg.from_geopandas(local_roads_to_cut, npartitions=20)
    
    gddf["segment_geometry"] = gddf.apply(
        lambda x: geography_utils.create_segments(x.geometry, int(segment_length_meters)), 
        axis=1, meta = ("segment_geometry", "object")
    )
    
    gdf = gddf.compute()
    
    gdf2 = explode_segments(
        gdf, 
        group_cols = ["linearid", "mtfcc", "fullname"],
        segment_col = "segment_geometry"
    )
    
    gdf3 = pd.concat([
        gdf2, 
        short_roads.assign(segment_sequence = 0).astype({"segment_sequence": "int16"})
    ], axis=0).reset_index(drop=True)
    
    utils.geoparquet_gcs_export(
        gdf3,
        SHARED_GCS,
        "segmented_roads_2020_state06_local_base"
    )
    
    return

    
def cut_roads(
    road_type_name: Literal["primarysecondary", "local"],
    segment_length_meters: int = 1_000,  
):
    """
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
    
    elif road_type_name == "local":
        road_type_values = ["S1400"]
        roads = import_roads(road_type_values)

        
        
    utils.geoparquet_gcs_export(
        roads_segmented,
        SHARED_GCS,
        f"segmented_roads_2020_state06_{road_type_name}"
    )


if __name__ == "__main__":
    
    start = datetime.datetime.now()

    # MTFCC values
    ROAD_TYPE_DICT = {
        "primarysecondary": ["S1100", "S1200"],
        "local": ["S1400"]
    }

    ROAD_SEGMENT_METERS = 1_000
    
    # Always cut all the primary and secondary roads
    road_type = "primarysecondary"
    cut_roads(road_type, ROAD_SEGMENT_METERS)
    
    time1 = datetime.datetime.now()
    print(f"cut primary/secondary roads: {time1 - start}")
    
    # Grab Sep 2023's shapes as base to grab majority of local roads we def need to cut
    local_roads_base()
    
    time2 = datetime.datetime.now()
    print(f"cut local roads base: {time2 - time1}")
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
