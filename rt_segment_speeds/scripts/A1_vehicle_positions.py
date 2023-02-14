"""
Concatenate vehicle positions for operators on a single day.

Note: vehicle positions get changed back to tabular df 
because adding a geometry column makes the file large.
This script could be changed to just do the concatenation
without making the geometry, and save the geometry making for later.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import pandas as pd
import gcsfs
import geopandas as gpd
import shapely

from shared_utils import utils
from update_vars import SEGMENT_GCS, analysis_date


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    # Append individual operator vehicle position parquets together
    # and cache a single vehicle positions parquet
    fs = gcsfs.GCSFileSystem()
    fs_list = fs.ls(f"{SEGMENT_GCS}")

    vp_files = [i for i in fs_list if "vp_raw" in i 
                and f"{analysis_date}_batch" in i]
    
    df = pd.DataFrame()
    
    for f in vp_files:
        batch_df = pd.read_parquet(f"gs://{f}")
        df = pd.concat([df, batch_df], axis=0)
    
    time1 = datetime.datetime.now()
    print(f"appended all batch parquets: {time1 - start}")
    
    # Drop Nones or else shapely will error
    df = df[df.location.notna()].reset_index(drop=True)
    
    geom = [shapely.wkt.loads(x) for x in df.location]

    gdf = gpd.GeoDataFrame(
        df, geometry=geom, 
        crs="EPSG:4326").drop(columns="location")
    
    time2 = datetime.datetime.now()
    print(f"change to gdf: {time2 - time1}")
    
    utils.geoparquet_gcs_export(
        gdf, 
        SEGMENT_GCS,
        f"vp_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end-start}")
