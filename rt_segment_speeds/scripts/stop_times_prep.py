"""
Condense stop times to create arrays holding stop positions.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import numpy as np
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import rt_dates
import model_utils


def merge_stop_times_and_vp(
    analysis_date: str
) -> dg.GeoDataFrame:
    """
    Merge stop_times_direction file (stop_times + stop geom) 
    with vp.
    """
    VP_PROJECTED = GTFS_DATA_DICT.modeled_vp.vp_projected
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", "stop_sequence", "geometry"],
        with_direction = True,
        get_pandas = True,
        crs = PROJECT_CRS
    ).sort_values(["trip_instance_key", "stop_sequence"]).reset_index(drop=True)

    vp_path = dg.read_parquet(
        f"{SEGMENT_GCS}{VP_PROJECTED}_{analysis_date}.parquet",
        columns = ["trip_instance_key", "vp_geometry"]
    ).repartition(npartitions=20)

    stop_times_vp_geom = dd.merge(
        vp_path,
        stop_times,
        on = "trip_instance_key",
        how = "inner"
    ).set_geometry("geometry")
        
    return stop_times_vp_geom


def project_stop_onto_vp_geom_and_condense(
    gddf: dg.GeoDataFrame
) -> dd.DataFrame:
    """
    Project stop geometry onto vp path geometry.
    """
    orig_dtypes = gddf.dtypes.to_dict()

    gdf2 = gddf.map_partitions(
        model_utils.project_point_onto_shape,
            line_geom = "vp_geometry", 
            point_geom = "geometry",
        meta = {
            **orig_dtypes, 
            "projected_meters": "float"
        },
        align_dataframes = True
    )

    gdf2 = gdf2.rename(
        columns = {"projected_meters": "stop_meters"}
    ).drop(
        columns = ["vp_geometry", "geometry"]
    ).persist()
    
    return gdf2


def condense_stop_positions(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Condense the stop positions (stop_meters) projected against vp path
    by trip. 
    The values within those arrays define the segments (stop_distance1, stop_distance2, ...).
    segment1 = between stop_distance1 and stop_distance2 
    (relevant values here for distance/time can be used for speeds)
    """
    df2 = (df
        .groupby("trip_instance_key", group_keys=False)
        .agg({
            "stop_sequence": lambda x: list(x),
            "stop_meters": lambda x: list(x)
        })
        .reset_index()
       )

    # Check whether stop meters better meets the monotonically increasing condition
    # when projected against the actual vp path of travel.
    # Still have some Falses, but we can deal with these by removing those calculations later.
    df2 = df2.assign(
        stop_meters_increasing = df2.apply(
            lambda x: 
            model_utils.check_monotonically_increasing_condition(x.stop_meters), 
            axis=1
        )
    )
    
    return df2


if __name__ == "__main__":
    
    LOG_FILE = "../logs/modeled_vp_preprocessing.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    
    analysis_date_list = [
        rt_dates.DATES["oct2024"]
    ]
    
    for analysis_date in analysis_date_list:    
    
        start = datetime.datetime.now()

        EXPORT_FILE = GTFS_DATA_DICT.modeled_rt_stop_times.stop_times_projected

        stop_times_vp_geom = merge_stop_times_and_vp(analysis_date)

        gdf = project_stop_onto_vp_geom_and_condense(stop_times_vp_geom).compute()

        results = condense_stop_positions(gdf)
        
        results.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet"
        )

        end = datetime.datetime.now()

        logger.info(
            f"{analysis_date}: condense stop times and interpolate: {end - start}"
        )