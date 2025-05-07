"""
Take road segment end points and treat as "stops".
Project these end points along vp path.

Must use wide, where each row is a trip-linearid combo, 
because long df is too large.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely
import sys

from loguru import logger

from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS, SHARED_GCS

def merge_road_endpoints_with_vp_path(
    analysis_date: str
) -> dg.GeoDataFrame:
    """
    Take vp path, merge in crosswalk to find relevant roads (linearids),
    and grab the road segment endpoints.
    """
    VP_PROJECTED_FILE = GTFS_DATA_DICT.modeled_vp.vp_projected
    VP_ROADS_CROSSWALK = GTFS_DATA_DICT.modeled_road_segments.crosswalk
    SHN_CONDENSED = GTFS_DATA_DICT.shared_data.condensed_shn
    
    roads_vp_crosswalk = dd.read_parquet(
        f"{SEGMENT_GCS}{VP_ROADS_CROSSWALK}_{analysis_date}.parquet",
    )
    
    vp_projected = dg.read_parquet(
        f"{SEGMENT_GCS}{VP_PROJECTED_FILE}_{analysis_date}.parquet",
        columns = ["trip_instance_key", "vp_geometry"],
    )
    
    shn_condensed = gpd.read_parquet(
        f"{SHARED_GCS}{SHN_CONDENSED}.parquet",
    )

    # Merge vp path with crosswalk first, then merge in relevant road end points
    gddf = dd.merge(
        vp_projected,
        roads_vp_crosswalk,
        on = "trip_instance_key",
        how = "inner"
    ).merge(
        shn_condensed,
        on = ["linearid", "fullname"],
        how = "inner"
    )

    return gddf


def get_projected_distance(
    gdf: dg.GeoDataFrame,
    group_cols: list
) -> dg.GeoDataFrame:
    """
    Project the road endpoint against vp path.
    """
    road_projected_dist_series = []

    for row in gdf.itertuples():
        road_endpoint_array = np.array(getattr(row, "geometry").coords)
        vp_geom = getattr(row, "vp_geometry")

        road_endpoint_distance = np.array(
            [vp_geom.project(shapely.Point(p)) 
             for p in road_endpoint_array]
        )
        
        road_projected_dist_series.append(road_endpoint_distance)
    
    gdf2 = gdf[group_cols].assign(
        road_projected_dist = road_projected_dist_series
    )

    return gdf2


if __name__ == "__main__":
    
    LOG_FILE = "../logs/modeled_road_speeds.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    from segment_speed_utils.project_vars import test_dates
    
    for analysis_date in test_dates:
        start = datetime.datetime.now()

        EXPORT_FILE = GTFS_DATA_DICT.modeled_road_segments.road_endpoints_projected
        
        trip_road_group_cols = [
            "trip_instance_key", 
            "linearid", "segment_sequence"
        ]
        
        gdf = merge_road_endpoints_with_vp_path(
            analysis_date
        ).repartition(npartitions=1)
        
        orig_dtypes = gdf[trip_road_group_cols].dtypes.to_dict()
        
        results = gdf.map_partitions(
            get_projected_distance,
            trip_road_group_cols,
            meta = {
                **orig_dtypes,
                "road_projected_dist": "str",
            },
            align_dataframes = False
        )
        
        results = results.compute()
        
        results.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet"
        )
        
        end = datetime.datetime.now()
        logger.info(f"project road end points: {end - start}")
        #project road end points: 0:33:23.110514 # 1 partition
        #project road end points: 0:49:16.040237 # 3 partitions