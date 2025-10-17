"""
Download sample tables (2 weeks worth) that were created.
Benchmark times, get intuitive feel for how much we 
can comfortably work with.

Exploratory work around aggregations, make sure metrics make sense.
Visualize these.
Go back to tables and tweak data models.
"""
import datetime
import geopandas as gpd
import pandas as pd
import google.auth
import shapely
import sys

from loguru import logger

import warehouse_utils 
from rt_msa_utils import PREDICTIONS_GCS
credentials, project = google.auth.default()

'''
from functools import cache
from calitp_data_analysis import get_fs
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
gcsgp = GCSGeoPandas()

@cache
def gcs_geopandas():
    return GCSGeoPandas()
'''
PRODUCTION_PROJECT = "cal-itp-data-infra"
STAGING_PROJECT = "cal-itp-data-infra-staging"
PRODUCTION_MART_GTFS = "mart_gtfs"
TIFFANY_MART = "tiffany_mart_gtfs"

def download_daily_metrics(
    table_name: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Download daily stop or trip metrics.
    """
    t0 = datetime.datetime.now()
    
    if table_name == "fct_trip_updates_stop_metrics":
        sql_query = f"""
            SELECT 
                * EXCEPT(prediction_error_by_minute_array, minutes_until_arrival_array) 
            FROM `{PRODUCTION_PROJECT}.{PRODUCTION_MART_GTFS}.{table_name}`
        """
    else:
        sql_query = warehouse_utils.basic_sql_query(
            PRODUCTION_PROJECT, PRODUCTION_MART_GTFS, table_name
        )     
    
    where_condition = warehouse_utils.add_sql_date_filter("service_date", start_date, end_date)
    
    df = warehouse_utils.download_table_with_date(
        f"{sql_query} {where_condition}",
        date_col = "service_date"
    )
    
    df.to_parquet(
        f"{PREDICTIONS_GCS}{table_name}_{start_date}_{end_date}.parquet",
        engine = "pyarrow"
    )
    
    t1 = datetime.datetime.now()
    logger.info(f"download {table_name}: {start_date}-{end_date}: {t1 - t0}")

    return


def download_staging_table(
    project_name: str = STAGING_PROJECT,
    dataset_name = TIFFANY_MART,
    table_name: str = "",
    date_col: str = "",
    start_date: str = "",
    end_date: str = "",
    get_df: bool = False
) -> pd.DataFrame:
    """
    Download staging table
    """
    t0 = datetime.datetime.now()
    
    sql_query = warehouse_utils.basic_sql_query(project_name, dataset_name, table_name)
    where_condition = warehouse_utils.add_sql_date_filter(date_col, start_date, end_date)
    
    df = warehouse_utils.download_table_with_date(
        f"{sql_query} {where_condition}",
        date_col = date_col
    )
    
    if get_df:
        return df
    else:
        df.to_parquet(
            f"{PREDICTIONS_GCS}{table_name}_{start_date}_{end_date}.parquet"
        )  
        t1 = datetime.datetime.now()
        logger.info(f"download {table_name}: {start_date}-{end_date}: {t1 - t0}")

        return

def download_staging_table_with_geom(
    project_name: str = STAGING_PROJECT,
    dataset_name = TIFFANY_MART,
    table_name: str = "",
    date_col: str = "",
    start_date: str = "",
    end_date: str = "",
    geom_col: str = "",
    geom_type: str = ""
):
    """
    Download table with geom, save out as gdf
    """
    t0 = datetime.datetime.now()
    
    sql_query = warehouse_utils.basic_sql_query(project_name, dataset_name, table_name)
    where_condition = warehouse_utils.add_sql_date_filter(date_col, start_date, end_date)
    
    gdf = warehouse_utils.download_table_with_date_geom(
        f"{sql_query} {where_condition}",
        date_col = date_col,
        geom_col = geom_col,
        geom_type = geom_type
    )
       
    warehouse_utils.geoparquet_gcs_export(
        gdf,
        f"{PREDICTIONS_GCS}",
        table_name
    )
    
    t1 = datetime.datetime.now()
    logger.info(f"download {table_name}: {start_date}-{end_date}: {t1 - t0}")
    return

def download_staging_table_no_dates(
    project_name: str = STAGING_PROJECT,
    dataset_name = TIFFANY_MART,
    table_name: str = "",
):
    """
    These are staging tables in tiffany_mart_gtfs that don't have a service_date column.
    """
    t0 = datetime.datetime.now()

    sql_query = warehouse_utils.basic_sql_query(project_name, dataset_name, table_name)
    
    df = warehouse_utils.download_table_no_date(
        sql_query
    )

    df.to_parquet(
        f"{PREDICTIONS_GCS}{table_name}.parquet"
    )  
    
    t1 = datetime.datetime.now()
    logger.info(f"download {table_name}: {t1 - t0}")
    
    return
        
    
if __name__ == "__main__":
    
    LOG_FILE = "./logs/download_warehouse.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")

    
    ## (1) Download the production ready tables
    for t in ["fct_trip_updates_stop_metrics", "fct_trip_updates_trip_metrics"]:
        download_daily_metrics(
            table_name = t,
            start_date = "2025-06-01",
            end_date = "2025-06-15",
        )
    

    ## (2) Download aggregations on trip metrics
    for t in [
        "test_schedule_rt_trip_metrics", 
        "test_schedule_rt_route_direction_metrics",
        "test_schedule_rt_route_metrics",
        "crosswalk_stop_times_route_dir",
    ]:
        download_staging_table_no_dates(
            project_name = STAGING_PROJECT,
            dataset_name = TIFFANY_MART,
            table_name = t,
        )
    
    
    ## (3) Download aggregations on stop metrics    
    download_staging_table_with_geom(
        project_name = STAGING_PROJECT,
        dataset_name = TIFFANY_MART,
        table_name = "test_monthly_schedule_rt_stop_metrics",
        date_col = "month_first_day",
        start_date = "2025-06-01",
        end_date = "2025-06-01",
        geom_col = "pt_geom",
        geom_type = "point"
    )
    