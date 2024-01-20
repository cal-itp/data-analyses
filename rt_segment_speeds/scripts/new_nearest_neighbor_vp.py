import dask.dataframe as dd
import datetime
import geopandas as gpd
import gtfs_segments
import numpy as np
import pandas as pd
import sys

from dask import delayed
from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import gtfs_schedule_wrangling, helpers, neighbor
from segment_speed_utils.project_vars import SEGMENT_GCS

def rt_stop_times_nearest_neighbor(
    gdf: gpd.GeoDataFrame
) -> pd.DataFrame:
    
    results = neighbor.add_nearest_vp_idx(
        gdf).drop(columns = ["start", "geometry", "vp_idx"])
    
    return results


if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date
    
    LOG_FILE = "../logs/nearest_neighbors.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()

    # Unknown directions can be done separately for origin stop
    # we will use other methods to pare down the vp_idx 
    # maybe based on min(vp_idx) to use as the max bound
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}condensed/vp_nearest_neighbor_{analysis_date}.parquet",
        filters = [[("vp_primary_direction", "!=", "Unknown")]]
    )
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", 
                   "stop_sequence", "stop_primary_direction",
                   "geometry"],
        with_direction = True,
        get_pandas = False,
        crs = WGS84
    ).repartition(npartitions=5)
    
    stop_times = stop_times.rename(
        columns = {"geometry": "start"}
    ).set_geometry("start")
    
    gdf = dd.merge(
        stop_times,
        vp.rename(columns = {"vp_primary_direction": "stop_primary_direction"}),
        on = ["trip_instance_key", "stop_primary_direction"],
        how = "inner"
    ).repartition(npartitions=50)
    
    st_dtypes = stop_times.drop(columns = ["start"]).dtypes.to_dict()
    
    del vp
    
    results = gdf.map_partitions(
        rt_stop_times_nearest_neighbor,
        meta = {
            **st_dtypes,
            "nearest_vp_idx": "int"
        },
        align_dataframes = False
    ).repartition(npartitions=5)

    results.to_parquet(
        f"{SEGMENT_GCS}nearest/"
        f"nearest_rt_stop_times_{analysis_date}",
    )
    
    end = datetime.datetime.now()
    logger.info(f"nearest points: {end - start}")
                               