"""
Import staging table.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import pandas as pd
import geopandas as gpd
import gcsfs
import dask_bigquery
import shapely

from calitp_data_analysis import utils
from segment_speed_utils.project_vars import SEGMENT_GCS
from shared_utils import rt_dates, publish_utils, schedule_rt_utils

fs = gcsfs.GCSFileSystem()

def vp_into_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Change vehicle positions, which comes as df, into gdf.
    
    This function is in concatenate_vehicle_positions.py
    """
    # Drop Nones or else shapely will error
    df2 = df[df.location.notna()].reset_index(drop=True)
    
    geom = [shapely.wkt.loads(x) for x in df2.location]

    gdf = gpd.GeoDataFrame(
        df2, geometry=geom, 
        crs="EPSG:4326"
    ).drop(columns="location").pipe(
        schedule_rt_utils.localize_timestamp_col,
        ["location_timestamp", "moving_timestamp"]
    )
    
    return gdf

if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    analysis_date = rt_dates.DATES["feb2025"]
    FILE = f"fct_vehicle_locations_grouped"

    
    ddf = dask_bigquery.read_gbq(
        project_id="cal-itp-data-infra-staging",
        dataset_id="tiffany_mart_gtfs",
        table_id=("vp_one_operator"),
    )
    
    ddf = ddf.astype({"service_date": "str"})
    
    ddf.to_parquet(
        f"{SEGMENT_GCS}{FILE}_{analysis_date}",
        partition_on = "gtfs_dataset_key",
    )
    
    end = datetime.datetime.now()
    print(f"{analysis_date} exported fct vehicle locations grouped: {end - start}")
    # 0:20:00.449649
    
    
    operators = pd.read_parquet(
        f"{SEGMENT_GCS}{FILE}_{analysis_date}/",
        columns = ["gtfs_dataset_key"]
    ).drop_duplicates()
    
    
    for one_operator in operators.gtfs_dataset_key.unique():
        df = pd.read_parquet(
            f"{SEGMENT_GCS}{FILE}_{analysis_date}/",
            filters = [["gtfs_dataset_key", "==", one_operator]]
        ).pipe(
            vp_into_gdf,
        )
        
        df.to_parquet(f"{SEGMENT_GCS}staging/{one_operator}.parquet")
        
        del df
    
    
    files_to_concatenate = fs.ls(f"{SEGMENT_GCS}staging/")
    
    df = pd.concat(
        [gpd.read_parquet(f"gs://{f}") for f in files_to_concatenate], 
        axis=0, ignore_index=True
    )
    
    print(df.shape)
    
    utils.geoparquet_gcs_export(
        df,
        SEGMENT_GCS,
        f"{FILE}_gdf_{analysis_date}"
    )
    
    # Add line to remove staging folder
    fs.rm(f"{SEGMENT_GCS}staging/")
    
    end = datetime.datetime.now()
    print(f"turn table into gdf: {end - start}")
    