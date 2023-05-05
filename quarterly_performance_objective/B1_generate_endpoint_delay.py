"""
Download RT endpoint delay
"""
import datetime
import gcsfs
import glob
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger
from pathlib import Path
from siuba import *

from shared_utils import dask_utils, rt_utils
from update_vars import ANALYSIS_DATE, COMPILED_CACHED_GCS

fs = gcsfs.GCSFileSystem()

def import_stop_delay(filename: str, analysis_date: str) -> pd.DataFrame:
    """
    Stop delay views are by operator. 
    Parse and grab just the delay for the last stop (max stop seq).
    Grab/create relevant columns to merge delay columns back onto 
    rt_trips (which supplies avg speed)
    We need calitp_itp_id and trip_id.
    """
    itp_id = Path(filename).stem.split(f"_{analysis_date}")[0]

    df = pd.read_parquet(
        filename, 
        columns = [
           "trip_id", 
           "stop_sequence", "arrival_time",
           "delay_seconds"]
    ).assign(calitp_itp_id = int(itp_id))
    
    # Use this function to calculate, lift it from the class
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/rt_analysis/rt_filter_map_plot.py#L58-L64
    endpoint_delay_view = (df
                  >> group_by(_.calitp_itp_id, _.trip_id)
                  >> filter(_.stop_sequence == _.stop_sequence.max())
                  >> ungroup()
                  >> mutate(arrival_hour = _.arrival_time.apply(
                      lambda x: pd.to_datetime(x).hour))
                  # this inner join has to be done outside of this
                #>> inner_join(_, df >> select(_.trip_id, _.mean_speed_mph), 
                #           on = 'trip_id')
                ).drop_duplicates()
    
    return endpoint_delay_view


def concatenate_endpoint_delays(analysis_date: str) -> pd.DataFrame:
    """
    Concatenate the v2_stop_delay_views across operators
    """
    all_stop_delay = fs.ls(f"{rt_utils.GCS_FILE_PATH}v2_stop_delay_views/")
    stop_delay_for_date = [f"gs://{f}" for f in all_stop_delay 
                       if analysis_date in f]    
    
    stop_delay_dfs = [delayed(import_stop_delay)(f, analysis_date) 
                  for f in stop_delay_for_date]
    
    results = [compute(i)[0] for i in stop_delay_dfs]

    endpoint_delay = (pd.concat(results, axis=0)
                  # there will be multiple observations for same trip-stop_seq
                  # not sure why, but drop the one that is holding NaNs
                  .dropna(subset=["arrival_time", "delay_seconds"])
                  .reset_index(drop=True)
                 )
    
    return endpoint_delay


def merge_rt_trips_to_stop_delay(analysis_date: str):
    """
    Use dask_utils to read in operator rt_trip files and concatenate.
    Bring in concatenated endpoint delay df.
    Merge and use this in our quarterly metrics.
    """
    # RT trip files can be concatenated without any further processing
    all_rt_trips = fs.ls(f"{rt_utils.GCS_FILE_PATH}v2_rt_trips/")
    rt_trips_for_date = [f"gs://{f}" for f in all_rt_trips
                         if analysis_date in f]

    rt_trip = dask_utils.concatenate_list_of_files(
        rt_trips_for_date, file_type = "df")
    
    # There is a subsetting of stop delay views to get just the
    # last stop (endpoint delay)
    endpoint_delay = concatenate_endpoint_delays(analysis_date)

    keep_rt_trip_cols = [
        "organization_name", "caltrans_district", "calitp_itp_id", 
        "trip_id", "route_id", "mean_speed_mph"
    ]
    
    endpoint_delay_with_speed = pd.merge(
        rt_trip[keep_rt_trip_cols],
        endpoint_delay,
        on = ["calitp_itp_id", "trip_id"],
        how = "inner"
    )
    
    # Eric's note - use this utils to get feed_key/org_name/itp_id
    # https://cal-itp.slack.com/archives/C02H6JUSS9L/p1683245451257449?thread_ts=1683236106.569949&cid=C02H6JUSS9L
    crosswalk = rt_utils.get_speedmaps_ix_df(
        pd.to_datetime(analysis_date).date()
    )
    crosswalk = crosswalk >> distinct(_.vehicle_positions_gtfs_dataset_key,
                                      _keep_all=True)
    
    endpoint_delay_with_speed_with_feed_key = pd.merge(
        endpoint_delay_with_speed,
        crosswalk[["feed_key", "organization_itp_id", 
                   "organization_name"]
                 ].rename(columns = {"organization_itp_id": "calitp_itp_id"}), 
        on = ["organization_name", "calitp_itp_id"]
    )
    
    return endpoint_delay_with_speed_with_feed_key
    
        
if __name__=="__main__":
    
    logger.add("./logs/B1_generate_endpoint_delay.log")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
               level="INFO")
    
    logger.info(f"Analysis date: {ANALYSIS_DATE}")
    start = datetime.datetime.now()
    
    df = merge_rt_trips_to_stop_delay(ANALYSIS_DATE)    
    
    # Save in GCS
    df.to_parquet(
        f"{COMPILED_CACHED_GCS}endpoint_delays_{ANALYSIS_DATE}.parquet")
     
    end = datetime.datetime.now()
    logger.info(f"execution time: {end - start}")
