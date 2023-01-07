import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import gcsfs
import numpy as np
import pandas as pd
import sys
import warnings

from dask import delayed, compute
from dask.delayed import Delayed
from loguru import logger
from shapely.errors import ShapelyDeprecationWarning
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

"""
References

Why call compute twice on dask delayed?
https://stackoverflow.com/questions/56944723/why-sometimes-do-i-have-to-call-compute-twice-on-dask-delayed-functions

Parallelize dask aggregation
https://stackoverflow.com/questions/62352617/parallelizing-a-dask-aggregation

Dask delayed stuff
https://docs.dask.org/en/latest/delayed.htmls
https://tutorial.dask.org/03_dask.delayed.html
https://stackoverflow.com/questions/71736681/possible-overhead-on-dask-computation-over-list-of-delayed-objects
https://docs.dask.org/en/stable/delayed-collections.html
https://distributed.dask.org/en/latest/manage-computation.html
https://docs.dask.org/en/latest/delayed-best-practices.html
"""

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"
COMPILED_CACHED_VIEWS = f"{GCS_FILE_PATH}rt_delay/compiled_cached_views/"

analysis_date = "2022-10-12"
fs = gcsfs.GCSFileSystem()

segment_cols = ["route_dir_identifier", "segment_sequence"]
segment_trip_cols = ["calitp_itp_id", "trip_id"] + segment_cols
                    

def operators_with_data(gcs_folder: str = f"{DASK_TEST}vp_sjoin/") -> list:
    """
    Return a list of operators with RT vehicle positions 
    spatially joined to route segments.
    """
    all_files_in_folder = fs.ls(gcs_folder)
    
    files = [i for i in all_files_in_folder if "vp_segment_" in i]
    
    ITP_IDS = [int(i.split('vp_segment_')[1]
               .split(f'_{analysis_date}')[0]) 
           for i in files]
    
    return ITP_IDS
    

# https://stackoverflow.com/questions/58145700/using-groupby-to-store-value-counts-in-new-column-in-dask-dataframe
# https://github.com/dask/dask/pull/5327
def keep_min_max_timestamps_by_segment(
    vp_to_seg: dd.DataFrame) -> dd.DataFrame:
    """
    For each segment-trip combination, throw away excess points, just 
    keep the enter/exit points for the segment.
    """
    segment_cols = ["route_dir_identifier", "segment_sequence"]
    segment_trip_cols = ["calitp_itp_id", "trip_id"] + segment_cols
    
    # https://stackoverflow.com/questions/52552066/dask-compute-gives-attributeerror-series-object-has-no-attribute-encode    
    # comment out .compute() and just .reset_index()
    enter = (vp_to_seg.groupby(segment_trip_cols)
             .vehicle_timestamp.min()
             #.compute()
             .reset_index()
            )

    exit = (vp_to_seg.groupby(segment_trip_cols)
            .vehicle_timestamp.max()
            #.compute()
            .reset_index()
           )
    
    enter_exit = dd.multi.concat([enter, exit], axis=0)
    
    # Merge back in with original to only keep the min/max timestamps
    # dask can't sort by multiple columns to drop
    enter_exit_full_info = dd.merge(
        vp_to_seg,
        enter_exit,
        on = segment_trip_cols + ["vehicle_timestamp"],
        how = "inner"
    ).repartition(npartitions=1)
    
    return enter_exit_full_info
    
    
@delayed
def merge_in_segment_shape(
    vp: dd.DataFrame, segments: dg.GeoDataFrame) -> dd.DataFrame:
    """
    Do linear referencing and calculate `shape_meters` for the 
    enter/exit points on the segment. 
    
    This allows us to calculate the distance_elapsed.
    
    Return dd.DataFrame because geometry makes the file huge.
    Merge in segment geometry later before plotting.
    """
    segment_cols = ["route_dir_identifier", "segment_sequence"]
    segments2 = segments[segment_cols + ["geometry"]]
    
    # https://stackoverflow.com/questions/71685387/faster-methods-to-create-geodataframe-from-a-dask-or-pandas-dataframe
    vp_gddf = dg.from_dask_dataframe(
        vp, 
        geometry = dg.points_from_xy(
            vp, 
            x = "lon", y = "lat", 
            crs = "EPSG:3310")
    ).drop(columns = ["lat", "lon"])
            
    linear_ref_vp_to_shape = dd.merge(
        vp_gddf, 
        segments2,
        on = segment_cols,
        how = "inner"
    ).rename(
        columns = {"geometry_x": "geometry",
                   "geometry_y": "shape_geometry"}
    )

    linear_ref_vp_to_shape['shape_meters'] = linear_ref_vp_to_shape.apply(
        lambda x: x.shape_geometry.project(x.geometry), axis=1,
        meta = ('shape_meters', 'float'))
    
    linear_ref_df = (linear_ref_vp_to_shape.drop(
                        columns = ["geometry", "shape_geometry"])
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
        
    df = df.assign(
        meters_elapsed = df[distance_cols[1]] - df[distance_cols[0]],
        sec_elapsed = (df[time_cols[1]] - df[time_cols[0]]).divide(
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
    
    segment_trip_agg = (
        gdf[segment_trip_cols].drop_duplicates()
        .merge(min_time, on = segment_trip_cols, how = "left")
        .merge(max_time, on = segment_trip_cols, how = "left")
        .merge(min_dist, on = segment_trip_cols, how = "left")
        .merge(max_dist, on = segment_trip_cols, how = "left")
    ).reset_index(drop=True)
    
    
    segment_speeds = derive_speed(
        segment_trip_agg, 
        distance_cols = ("min_dist", "max_dist"), 
        time_cols = ("min_time", "max_time")
    )
    
    return segment_speeds


def compute_and_export(df_delayed: Delayed):
    time0 = datetime.datetime.now()
    
    df = compute(df_delayed)[0] #df_delayed.compute()
    itp_id = df.calitp_itp_id.unique().compute().iloc[0]
    
    #df = df.repartition(partition_size="75MB")
    
    df.compute().to_parquet(
        f"{DASK_TEST}vp_sjoin/speeds_{itp_id}_{analysis_date}.parquet")
    
    time1 = datetime.datetime.now()
    logger.info(f"exported: {itp_id}: {time1 - time0}")

    
@delayed    
def import_vehicle_positions(itp_id: int)-> dd.DataFrame:
    """
    Import vehicle positions spatially joind to segments for 
    each operator.
    """
    operator_vp_segments = dd.read_parquet(
            f"{DASK_TEST}vp_sjoin/vp_segment_{itp_id}_{analysis_date}.parquet")
    
    vp_pared = keep_min_max_timestamps_by_segment(operator_vp_segments)
        
    return vp_pared


@delayed
def import_segments(itp_id: int) -> dg.GeoDataFrame:
    """
    Import segments and subset to operator segments.
    """
    segments = dg.read_parquet(
        f"{DASK_TEST}longest_shape_segments.parquet", 
        filters = [[("calitp_itp_id", "==", itp_id)]]
    )
    
    segments = segments.repartition(npartitions=1)
    
    return segments


if __name__ == "__main__": 
    from dask.distributed import Client
    
    client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    logger.add("./logs/A4_speeds_by_segment_trip.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    ALL_ITP_IDS = operators_with_data(f"{DASK_TEST}vp_sjoin/")  
    
    DONE = [45, 126, 246, 310, 300, 135, 30, 188, 336, 221, 
            110, 127, 148, 159, 167, 194, 218, 226,
            235, 243, 247, 259, 269, 273, 
            282, # 282 SF muni takes 40 min!!
            284, 290, 293, 294, 295, 301, 314, 315,
            349, 350, 361, 372, 75,
            #182 LA Metro skipped because it is too big to do in 1 partition
            # delayed can handle LA Metro...but it takes 2 hrs!
           ]
    
    #ITP_IDS = [i for i in ALL_ITP_IDS if i not in DONE]
    ITP_IDS = [4]
    
    results = []
    for itp_id in ITP_IDS:
        start_id = datetime.datetime.now()

        # https://docs.dask.org/en/stable/delayed-collections.html
        operator_vp_segments = import_vehicle_positions(itp_id)
        segments_subset = import_segments(itp_id)
        
        vp_linear_ref = merge_in_segment_shape(
            operator_vp_segments, segments_subset).persist()
        
        operator_speeds = calculate_speed_by_segment_trip(vp_linear_ref).persist()
        
        compute_and_export(operator_speeds)
        
        end_id = datetime.datetime.now()
        
        logger.info(f"ITP ID: {itp_id}: {end_id-start_id}")

    
    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    client.close()
        