"""
Import staging table.
"""
import datetime
import pandas as pd
import dask_bigquery

from segment_speed_utils.project_vars import SEGMENT_GCS
from shared_utils import rt_dates


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    analysis_date = rt_dates.DATES["feb2025"]

    ddf = dask_bigquery.read_gbq(
        project_id="cal-itp-data-infra-staging",
        dataset_id="tiffany_mart_gtfs",
        table_id=("vp_one_operator"),
    )
    
    ddf = ddf.astype({"service_date": "str"})
    
    ddf.to_parquet(
        f"{SEGMENT_GCS}fct_vehicle_locations_grouped_{analysis_date}",
        partition_on = "gtfs_dataset_key",
    )
    
    end = datetime.datetime.now()
    print(f"{analysis_date} exported fct vehicle locations grouped: {end - start}")
    # 0:20:00.449649