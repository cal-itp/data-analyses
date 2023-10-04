"""
Download all trips and shapes for a day for v2.

Use this to figure out why v1 and v2 aggregate service hours are so different.s
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(300_000_000_000)

import datetime as dt
import pandas as pd
import sys

from calitp.tables import tbls
from siuba import *
from loguru import logger

from shared_utils import (gtfs_utils_v2, gtfs_utils, 
                          rt_dates)
from calitp_data_analysis import utils, geography_utils
from update_vars import COMPILED_CACHED_GCS

def scheduled_operators(analysis_date: str):
    """
    This is how HQTA data is downloaded...so do the same here for backfill. 
    """
    all_operators = gtfs_utils_v2.schedule_daily_feed_to_organization(
        selected_date = analysis_date,
        keep_cols = None,
        get_df = True,
        feed_option = "use_subfeeds"
    )

    keep_cols = ["feed_key", "name"]

    operators_to_include = all_operators[keep_cols]
    
    # There shouldn't be any duplicates by name, since we got rid 
    # of precursor feeds. But, just in case, don't allow dup names.
    operators_to_include = (operators_to_include
                            .drop_duplicates(subset="name")
                            .reset_index(drop=True)
                           )
    
    return operators_to_include


if __name__=="__main__":
    logger.add("./logs/download_trips_v2_backfill.log")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    analysis_date = rt_dates.PMAC["Q2_2022"]
    VERSION = "v2"
    logger.info(f"Analysis date: {analysis_date}  warehouse {VERSION}")
    
    start = dt.datetime.now()
    
    if VERSION == "v1":
        ITP_IDS = (tbls.gtfs_schedule.agency()
                   >> distinct(_.calitp_itp_id)
                   >> filter(_.calitp_itp_id != 200) 
                   >> collect()
                  ).calitp_itp_id.tolist()
    
        IDS_TO_RUN = sorted(ITP_IDS)    
        
        logger.info(f"# operators to run: {len(IDS_TO_RUN)}")
        
        dataset = "trips"
        logger.info(f"*********** Download {dataset} data ***********")

        
        keep_trip_cols = [
            "calitp_itp_id", "calitp_url_number", 
            "service_date", "trip_key", "trip_id",
            "route_id", "direction_id", "shape_id",
            "calitp_extracted_at", "calitp_deleted_at", 
            "trip_first_departure_ts", "trip_last_arrival_ts",
            "service_hours"
        ]

        trips = gtfs_utils.get_trips(
            selected_date = analysis_date,
            itp_id_list = IDS_TO_RUN,
            trip_cols = keep_trip_cols,
            get_df = True,
        ) 

        trips.to_parquet(
            f"{COMPILED_CACHED_GCS}{dataset}_{analysis_date}_{VERSION}.parquet") 
        
        trips = pd.read_parquet(
            f"{COMPILED_CACHED_GCS}trips_{analysis_date}_{VERSION}.parquet")
        
        dataset = "routelines"
        logger.info(f"*********** Download {dataset} data ***********")

        routelines = gtfs_utils.get_route_shapes(
            selected_date = analysis_date,
            itp_id_list = IDS_TO_RUN,
            get_df = True,
            crs = geography_utils.CA_NAD83Albers,
            trip_df = trips
        )[["calitp_itp_id", "calitp_url_number", "shape_id", "geometry"]]

        utils.geoparquet_gcs_export(
            routelines, 
            COMPILED_CACHED_GCS,
            f"{dataset}_{analysis_date}_{VERSION}.parquet"
        )
        
    
    
    elif VERSION == "v2":
        operators_df = scheduled_operators(analysis_date)
    
        FEEDS_TO_RUN = sorted(operators_df.feed_key.unique().tolist())    
    
        logger.info(f"# operators to run: {len(FEEDS_TO_RUN)}")
    
        dataset = "trips"
        logger.info(f"*********** Download {dataset} data ***********")
        
        keep_trip_cols = [
            "feed_key", "name", "regional_feed_type", 
            "service_date", "trip_key", "trip_id",
            "route_key", "route_id", "route_type", 
            "route_short_name", "route_long_name", "route_desc",
            "direction_id", 
            "shape_array_key", "shape_id",
            "trip_first_departure_sec", "trip_last_arrival_sec",
            "service_hours"       
        ]

        trips = gtfs_utils_v2.get_trips(
            selected_date = analysis_date,
            operator_feeds = FEEDS_TO_RUN,
            trip_cols = keep_trip_cols,
            get_df = True,
        ) 

        trips.to_parquet(
            f"{COMPILED_CACHED_GCS}{dataset}_{analysis_date}_{VERSION}.parquet") 

        dataset = "routelines"
        logger.info(f"*********** Download {dataset} data ***********")

        keep_shape_cols = [
           "feed_key",
           "shape_id", "shape_array_key",
           "n_trips", 
           # n_trips is new column...can help if we want 
           # to choose between shape_ids
           # geometry already returned when get_df is True
        ]

        routelines = gtfs_utils_v2.get_shapes(
            selected_date = analysis_date,
            operator_feeds = FEEDS_TO_RUN,
            shape_cols = keep_shape_cols,
            get_df = True,
            crs = geography_utils.CA_NAD83Albers,
        )

        utils.geoparquet_gcs_export(
            routelines, 
            COMPILED_CACHED_GCS,
            f"{dataset}_{analysis_date}_{VERSION}.parquet"
        )
    
    end = dt.datetime.now()
    logger.info(f"execution time: {end - start}")
