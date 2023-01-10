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

Map partitions has trouble computing result.
Just use partitioned df and don't use `ddf.map_partitions`.
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
@delayed
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
    ).reset_index(drop=True)
        
    return enter_exit_full_info
    
    
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


def concat_and_export(filename: str, filetype: str = "df"):
    """
    Read in a folder of partitioned parquets and export as 1 parquet.
    """
    if filetype == "df":
        ddf = dd.read_parquet(filename)
        ddf.compute().to_parquet(filename)
        # Remove the folder version, not the single parquet
        fs.rm(f"{filename}/", recursive=True)
        
    elif filetype == "gdf":
        ddf = dg.read_parquet(filename)
        

def compute_and_export(results: list, itp_id: int, file_name: str):
    time0 = datetime.datetime.now()
    
    ddf = compute(results)[0]    
    ddf2 = ddf.repartition(partition_size="85MB")
    
    ddf2.to_parquet(
        f"{DASK_TEST}vp_sjoin/{file_name}_{itp_id}_{analysis_date}.parquet"
    )
    
    '''
    custom_metadata = {
        "calitp_itp_id": "int64",
        "trip_id": "object",
        "route_dir_identifier": "int64",
        "segment_sequence": "string",
        "min_time": "timestamp[ns]",
        "max_time": "timestamp[ns]",
        "min_dist": "float",
        "max_dist": "float",
        "meters_elapsed": "float",
        "sec_elapsed": "float",
        "speed_mph": "float",
    }
    '''
    if file_name == "speeds":
        concat_and_export(
            f"{DASK_TEST}vp_sjoin/{file_name}_{itp_id}_{analysis_date}.parquet")
    
    time1 = datetime.datetime.now()
    logger.info(f"exported: {itp_id}: {time1 - time0}")

    
@delayed    
def import_vehicle_positions(itp_id: int)-> dd.DataFrame:
    """
    Import vehicle positions spatially joined to segments for 
    each operator.
    """
    vp_segments = dd.read_parquet(
            f"{DASK_TEST}vp_sjoin/vp_segment_{itp_id}_{analysis_date}.parquet")

    vp_segments = vp_segments.repartition(partition_size = "85MB")
    
    return vp_segments


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
    
    logger.add("./logs/A4_speeds_by_segment_trip.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()
    
    #ITP_IDS = operators_with_data(f"{DASK_TEST}vp_sjoin/")  
    #ITP_IDS = [i for i in ITP_IDS if i not in DONE and i not in EXCLUDE]
    
    for itp_id in [182]:
        logger.info(f"start {itp_id}")
        
        start_id = datetime.datetime.now()

        # https://docs.dask.org/en/stable/delayed-collections.html
        #operator_vp_segments = import_vehicle_positions(itp_id)
        segments_subset = import_segments(itp_id)        
        
        time1 = datetime.datetime.now()
        logger.info(f"imported data: {time1 - start_id}")
        '''
        vp_pared = keep_min_max_timestamps_by_segment(
            operator_vp_segments)
        
        compute_and_export(vp_pared, itp_id, "vp_pared")
        '''
        time2 = datetime.datetime.now()
        logger.info(f"keep enter/exit points by segment-trip: {time2 - time1}")
        
        vp_pared = delayed(dd.read_parquet)(
            f"{DASK_TEST}vp_sjoin/vp_pared_{itp_id}_{analysis_date}.parquet")
        
        vp_linear_ref = merge_in_segment_shape( 
            vp_pared, segments_subset)
        
        vp_linear_ref = vp_linear_ref.repartition(
            partition_size = "85MB").persist()

        time3 = datetime.datetime.now()
        logger.info(f"merge in segment shapes and do linear referencing: "
                    f"{time3 - time2}")
    
        operator_speeds = calculate_speed_by_segment_trip(
            vp_linear_ref)
        
        time4 = datetime.datetime.now()
        logger.info(f"calculate speed: {time4 - time3}")
                
        compute_and_export(operator_speeds, itp_id, "speeds")
        
        end_id = datetime.datetime.now()
        
        logger.info(f"ITP ID: {itp_id}: {end_id-start_id}")

    end = datetime.datetime.now()
    logger.info(f"execution time: {end-start}")
    
    #client.close()
        