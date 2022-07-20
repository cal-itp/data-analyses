"""
Compile points.

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd
import pandas as pd


def prep_rail_ferry_brt_stops():
    hq_stops = dask_geopandas.read_parquet(
        f"{bus_corridors.TEST_GCS_FILE_PATH}rail_brt_ferry.parquet")
    
    hq_stops = hq_stops.assign(
        hqta_type = "major_transit_stop"
    )
    
    return hq_stops



def get_rail_ferry_brt_extract(df):
    keep_cols = ["calitp_itp_id", "stop_id", 
                 "hqta_type", "route_type", "geometry"]
                            
    ddf = (df[keep_cols].assign(
            hqta_type = df.route_type.map(
                lambda x: "major_stop_rail" if x in ["0", "1", "2"]
                else "major_stop_brt" if x == "3" 
                else "major_stop_ferry")
        ).rename(columns = {"calitp_itp_id": "calitp_itp_primary"})
       .drop(columns = "route_type")
    )

    return ddf


df = prep_rail_ferry_brt_stops()
df2 = get_rail_ferry_brt_extract(df)
