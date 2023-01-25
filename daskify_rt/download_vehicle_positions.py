"""
Download vehicle positions for a day.
"""
import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(900_000_000_000)

import geopandas as gpd
import pandas as pd

from calitp.sql import query_sql

#from shared_utils import rt_dates

#analysis_date = rt_dates.DATES["jan2023"]
analysis_date = "2023-01-18"
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"


def download_vehicle_positions(
    date: str,
    hour_range: tuple
) -> pd.DataFrame:    
    
    start_hour = str(hour_range[0]).zfill(2)
    end_hour = str(hour_range[1]).zfill(2)

    #https://www.yuichiotsuka.com/bigquery-timestamp-datetime/
    sql_statement = f"""
        SELECT *
        FROM `cal-itp-data-infra.mart_gtfs.fct_vehicle_locations`
        WHERE
            (dt = '{date}' AND
            hour >= TIMESTAMP('{date} {start_hour}:00:00') AND
            hour <= TIMESTAMP('{date} {end_hour}:00:00') 
            )
    """

    df = query_sql(sql_statement).to_parquet(
        f"{DASK_TEST}vp_raw_{analysis_date}_"
        f"hr{start_hour}-{end_hour}.parquet")
    
    #print(sql_statement)
    #return df


if __name__ == "__main__":
    
    # Get rt_datasets that are available for that day
    '''
    rt_datasets = gtfs_utils_v2.get_transit_organizations_gtfs_dataset_keys(
        keep_cols=["key", "name", "type", "regional_feed_type"],
        custom_filtering={"type": ["vehicle_positions"]},
        get_df = True
    ) >> collect()

    rt_datasets.to_parquet("./data/rt_datasets.parquet")
    '''
    
    hour_range1 = (0, 4)
    #hour_range2 = (6, 10)
    #hour_range3 = (11, 15)
    #hour_range4 = (16, 20)
    #hour_range5 = (21, 25)
    
    df = download_vehicle_positions(analysis_date, hour_range1)
        
    df.to_parquet(
        f"{DASK_TEST}vp_raw_{analysis_date}_"
        f"hr{hour_range[0]}-{hour_range[1]}.parquet")
    
