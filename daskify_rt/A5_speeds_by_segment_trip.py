"""
Do linear referencing by by segment-trip 
and derive speed.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import gcsfs
import numpy as np
import pandas as pd
import sys
import warnings

from dask import delayed
from loguru import logger
from shapely.errors import ShapelyDeprecationWarning
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

import dask_utils
from A4_valid_vehicle_positions import operators_with_data

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"

analysis_date = "2022-10-12"
fs = gcsfs.GCSFileSystem()                    

    
@delayed
def merge_in_segment_shape(
    vp: dd.DataFrame, segments: gpd.GeoDataFrame) -> dd.DataFrame:
    """
    Do linear referencing and calculate `shape_meters` for the 
    enter/exit points on the segment. 
    
    This allows us to calculate the distance_elapsed.
    
    Return dd.DataFrame because geometry makes the file huge.
    Merge in segment geometry later before plotting.
    """
    segment_cols = ["route_dir_identifier", "segment_sequence"]
    
    # https://stackoverflow.com/questions/71685387/faster-methods-to-create-geodataframe-from-a-dask-or-pandas-dataframe
    # https://github.com/geopandas/dask-geopandas/issues/197
    vp = vp.assign(
        geometry = dg.points_from_xy(vp, "lon", "lat", crs = "EPSG:3310"), 
    )
    
    # Refer to the geometry column by name
    vp_gddf = dg.from_dask_dataframe(
        vp, 
        geometry="geometry"
    )
                
    linear_ref_vp_to_shape = dd.merge(
        vp_gddf, 
        segments[segment_cols + ["geometry"]],
        on = segment_cols,
        how = "inner"
    )#.rename(
     #   columns = {"geometry_x": "geometry",
     #              "geometry_y": "shape_geometry"}
    #)

    linear_ref_vp_to_shape['shape_meters'] = linear_ref_vp_to_shape.apply(
        lambda x: x.geometry_y.project(x.geometry_x), axis=1,
        meta = ('shape_meters', 'float'))
    
    linear_ref_df = (linear_ref_vp_to_shape.drop(
                        columns = ["geometry_x", "geometry_y",
                            #"geometry", "shape_geometry", 
                            "lon", "lat"])
                     .drop_duplicates()
                     .reset_index(drop=True)
    )
    
    return linear_ref_df


def derive_speed(df: dd.DataFrame, 
    distance_cols: tuple = ("min_dist", "max_dist"), 
    time_cols: tuple = ("min_time", "max_time")) -> dd.DataFrame:
    """
    Derive meters and sec elapsed to calculate speed_mph.
    """
    min_dist, max_dist = distance_cols[0], distance_cols[1]
    min_time, max_time = time_cols[0], time_cols[1]    
    
    df = df.assign(
        meters_elapsed = df[max_dist] - df[min_dist],
        sec_elapsed = (df[max_time] - df[min_time]).divide(
                       np.timedelta64(1, 's')),
    )
    
    MPH_PER_MPS = 2.237

    df = df.assign(
        speed_mph = df.meters_elapsed.divide(df.sec_elapsed) * MPH_PER_MPS
    )
    
    return df


@delayed
def calculate_speed_by_segment_trip(
    gdf: dg.GeoDataFrame) -> dd.DataFrame:
    """
    For each segment-trip pair, calculate find the min/max timestamp
    and min/max shape_meters. Use that to derive speed column.
    """ 
    segment_cols = ["route_dir_identifier", "segment_sequence"]
    segment_trip_cols = ["calitp_itp_id", "trip_id"] + segment_cols
    
    min_time = (gdf.groupby(segment_trip_cols)
                .vehicle_timestamp.min()
                .compute()
                .reset_index(name="min_time")
    )
    
    max_time = (gdf.groupby(segment_trip_cols)
                .vehicle_timestamp.max()
                .compute()
                .reset_index(name="max_time")
               )
    
    min_dist = (gdf.groupby(segment_trip_cols)
                .shape_meters.min()
                .compute()
                .reset_index(name="min_dist")
    )
    
    max_dist = (gdf.groupby(segment_trip_cols)
                .shape_meters.max()
                .compute()
                .reset_index(name="max_dist")
               )  
    
    base_agg = gdf[segment_trip_cols].drop_duplicates().reset_index(drop=True)
    segment_trip_agg = (
        base_agg
        .merge(min_time, on = segment_trip_cols, how = "left")
        .merge(max_time, on = segment_trip_cols, how = "left")
        .merge(min_dist, on = segment_trip_cols, how = "left")
        .merge(max_dist, on = segment_trip_cols, how = "left")
    )
    
    segment_speeds = derive_speed(
        segment_trip_agg,
        distance_cols = ("min_dist", "max_dist"),
        time_cols = ("min_time", "max_time")
    )
        
    return segment_speeds


@delayed
def import_vehicle_positions(itp_id: int) -> dd.DataFrame:
    vp = dd.read_parquet(f"{DASK_TEST}vp_pared_{analysis_date}/")
    
    subset = vp[vp.calitp_itp_id == itp_id].reset_index(drop=True)
    
    return subset


@delayed
def import_segments(itp_id: int) -> gpd.GeoDataFrame:
    """
    Import segments and subset to operator segments.
    """
    segments = gpd.read_parquet(
        f"{DASK_TEST}longest_shape_segments.parquet", 
        filters = [[("calitp_itp_id", "==", itp_id)]]
    )
        
    return segments


if __name__ == "__main__": 
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/A5_speeds_by_segment_trip.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    ITP_IDS = operators_with_data(f"{DASK_TEST}vp_sjoin/")  
    
    results_linear_ref = []
    
    for itp_id in ITP_IDS:
        time0 = datetime.datetime.now()
        
        # https://docs.dask.org/en/stable/delayed-collections.html
        operator_vp = import_vehicle_positions(itp_id)
        operator_segments = import_segments(itp_id)        
        
        time1 = datetime.datetime.now()
        logger.info(f"imported data: {time1 - time0}")
        
        vp_linear_ref = merge_in_segment_shape( 
            operator_vp, operator_segments)
        
        results_linear_ref.append(vp_linear_ref)

        time2 = datetime.datetime.now()
        logger.info(f"merge in segment shapes and do linear referencing: "
                    f"{time2 - time1}")
    
    
    time3 = datetime.datetime.now()
    logger.info(f"start compute and export of results")

    dask_utils.compute_and_export(
        results_linear_ref,
        gcs_folder = f"{DASK_TEST}",
        file_name = f"vp_linear_ref_{analysis_date}",
        export_single_parquet = False
    )
    
    time4 = datetime.datetime.now()
    logger.info(f"computed and exported linear ref: {time4 - time3}")
    
    time5 = datetime.datetime.now()
        
    linear_ref_df = delayed(dd.read_parquet)(
            f"{DASK_TEST}vp_linear_ref_{analysis_date}/")

    operator_speeds = calculate_speed_by_segment_trip(
        linear_ref_df)
        
    # Save as list to use in compute_and_export
    results_speed = [operator_speeds]
        
    time6 = datetime.datetime.now()
    logger.info(f"calculate speed: {time6 - time5}")
                
    time7 = datetime.datetime.now()
    logger.info(f"start compute and export of results")
    
    dask_utils.compute_and_export(
        results_speed, 
        gcs_folder = f"{DASK_TEST}", 
        file_name = f"speeds_{analysis_date}",
        export_single_parquet = False
    )
    
    time8 = datetime.datetime.now()
    logger.info(f"exported all speeds: {time8 - time7}")
    
    # Now write out individual parquets for speeds
    speeds_df = dd.read_parquet(f"{DASK_TEST}speeds_{analysis_date}/").compute()

    for itp_id in speeds_df.calitp_itp_id.unique():
        subset = (speeds_df[speeds_df.calitp_itp_id == itp_id]
                  .reset_index(drop=True)
                 )
        subset.to_parquet(
            f"{DASK_TEST}speeds_by_operator/"
            f"speeds_{itp_id}_{analysis_date}.parquet")
    
    time9 = datetime.datetime.now()
    logger.info(f"exported operator speed parquets: {time9 - time8}")

    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    #client.close()
        