"""
Compare car `duration_in_traffic` with bus `service_hours`

Run `make_gmaps_requests` to get and cache Google Directions API results.
Pull in cached results and compile.
"""
import geopandas as gpd
import glob
import os
import pandas as pd

os.environ["CALITP_BQ_MAX_BYTES"] = str(50_000_000_000)

from calitp.tables import tbl
from datetime import datetime
from siuba import *

import utils
import shared_utils

DATA_PATH = "./gmaps_cache/"
GCS_FILE_PATH = f"{utils.GCS_FILE_PATH}gmaps_cache/"


def grab_cached_results(df: pd.DataFrame) -> (list, list):
    result_ids = list(df.identifier)

    successful_ids = []
    durations = []

    for i in result_ids:
        try:
            json_dict = utils.open_request_json(i, DATA_PATH = DATA_PATH, 
                                    GCS_FILE_PATH = GCS_FILE_PATH
                                   )
            duration_in_sec = json_dict["legs"][0]["duration_in_traffic"]["value"]
            durations.append(duration_in_sec)
            successful_ids.append(i)
        except:
            print(f"Not found: {i}")
    
    return successful_ids, durations 
            
def double_check_lists_match(successful_ids: list, 
                             durations: list) -> pd.DataFrame:            
    # Double check lengths match
    print(f"# results_ids: {len(successful_ids)}")
    print(f"# durations: {len(durations)}")

    if len(successful_ids) == len(durations):
        results_df = pd.DataFrame(
            {'identifier': successful_ids,
             'duration_in_sec': durations,
            })
        
        return results_df
     
def compare_travel_time_by_mode(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        car_duration_hours = df.duration_in_sec.divide(60 * 60)
    )
    
    df = df.assign(
        competitive = df.apply(lambda x: 
                               1 if x.service_hours <= (x.car_duration_hours * 2)
                               else 0,  axis=1)
    )
    return df


def assemble_route_geom(df: pandas.DataFrame, SELECTED_DATE: str) -> gpd.GeoDataFrame:
    operators = list(df.calitp_itp_id.unique())

    routes = shared_utils.geography_utils.make_routes_gdf(
        SELECTED_DATE, 
        CRS="EPSG:4326",
        ITP_ID_LIST = operators
    )
    
    # Keep only the shape_ids we find in our results
    routes2 = (routes 
           >> filter(_.shape_id.isin(df.shape_id))
          )
    
    # Keep only 1 obs across ITP_ID, drop url_number
    routes3 = (routes2.sort_values(["calitp_itp_id", "shape_id", "calitp_url_number"])
               .drop_duplicates(subset=["calitp_itp_id", "shape_id"])
               .drop(columns = ["calitp_url_number", "pt_array"])
              )
    
    return routes3


if __name__ == "__main__":    
    time0 = datetime.now()
    
    df = pd.read_parquet(f"{utils.GCS_FILE_PATH}gmaps_df.parquet")
    
    successful_ids, durations = grab_cached_results(df)
    print("Grabbed cached results")
    
    results = double_check_lists_match(successful_ids, durations)

    df2 = pd.merge(df, 
                 results, 
                 on = "identifier",
                 how = "left", 
                 validate = "1:1"
                )
    
    df2 = compare_travel_time_by_mode(df2)
    
    time1 = datetime.now()
    print(f"Compiled results: {time1 - time0}")
    
    
    SELECTED_DATE = '2022-1-6' #warehouse_queries.dates["thurs"]
    routes = assemble_route_geom(df2, SELECTED_DATE)
    time2 = datetime.now()
    print(f"Grabbed line geom {time2 - time1}")
    
    gdf = pd.merge(
        routes,
        df2,
        on= ["calitp_itp_id", "shape_id"],
        how = "inner",
        # There are 6 routes where same shape_id, trip_id combo exists, 
        # but has different route_id values. Allow this for now.
        validate = "1:m",
    )
    
    # Let's make this polygon to take advantage of map_utils
    gdf = gdf.assign(
        geometry = (gdf.to_crs(shared_utils.geography_utils.CA_StatePlane)
                    .geometry.buffer(200).simplify(tolerance=25)
                    .to_crs(shared_utils.geography_utils.WGS84)
                   ),
        service_hours = gdf.service_hours.round(2),
        car_duration_hours = gdf.car_duration_hours.round(2),
    )

    shared_utils.utils.geoparquet_gcs_export(gdf, utils.GCS_FILE_PATH, "gmaps_results")
    
    time3 = datetime.now()
    print(f"Exported to GCS {time3 - time2}")
    print(f"Total execution: {time3 - time0}")
    
