"""
Download vp_path.
"""
import datetime
import geopandas as gpd
import pandas as pd
import google.auth
import sys

from google.cloud import bigquery
from loguru import logger

from calitp_data_analysis import geography_utils, utils
from shared_utils import rt_dates
from update_vars import SEGMENT_GCS
credentials, project = google.auth.default()


def download_vp_path(
    table_name = "fct_vehicle_locations_path", 
    analysis_date: str = "",
    geom_col: str = "pt_array"
):
    """
    Download a single day's worth of vp path
    """
    t0 = datetime.datetime.now()
    
    sql_query = f"""
        SELECT * 
        FROM  `cal-itp-data-infra`.`mart_gtfs`.`{table_name}`
        WHERE service_date = DATE('{analysis_date}')
    
    """
    client = bigquery.Client()
    
    query_job = client.query(sql_query)
    
    df = query_job.result().to_dataframe()
    
    # what if this is not converted to gdf?
    # if this can be exploded as items in an array, would that work?
    '''
    df["geometry"] = df[geom_col].apply(geography_utils.make_linestring)
 
    gdf = gpd.GeoDataFrame(
        df.drop(columns = geom_col), geometry="geometry", 
        crs=geography_utils.WGS84
    )
    
    # maybe add export_file_name to catalog, and grab from there, but for now, keep names the same
    
    utils.geoparquet_gcs_export(
        gdf,
        f"{SEGMENT_GCS}",
        f"{table_name}_{analysis_date}"
    )
    
    del df, gdf
    '''
    df.to_parquet(f"{SEGMENT_GCS}{table_name}_{analysis_date}.parquet")
    t1 = datetime.datetime.now()
    logger.info(f"download {table_name}: {analysis_date}: {t1 - t0}")
    
    return 
        
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/movingpandas_pipeline.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    analysis_date_list = [
        rt_dates.DATES[f"{m}2025"] for m in ["jul"] 
    ]
    
    # aug2025 is gdf; sep2025 is df; jul2025 is df
    for analysis_date in analysis_date_list:
        download_vp_path(
            table_name = "fct_vehicle_locations_path", 
            analysis_date = analysis_date,
        )