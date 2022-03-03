import datetime as dt
import geopandas as gpd
import os
import pandas as pd

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

import calitp
from calitp.tables import tbl
from siuba import *

# Tweak this and find out why there are some NoneType
# Address it in shared_utils later
import geog_utils
import create_parallel_corridors
from shared_utils import geography_utils


TODAY_DATE = str(dt.date.today())

def grab_current_transit_route_shapes(DATA_PATH, FILE_NAME):
    
    agencies = (
        tbl.gtfs_schedule.agency()
        >> select(_.calitp_itp_id)
        >> distinct()
        >> collect()
    )
    
    ITP_ID_LIST = list(agencies.calitp_itp_id.unique())
    print(ITP_ID_LIST)
    
    routes = geog_utils.make_routes_shapefile(
        ITP_ID_LIST = ITP_ID_LIST, alternate_df=None)

    routes.to_parquet(f"{DATA_PATH}{FILE_NAME}_{TODAY_DATE}.parquet")


def add_route_info(df):
    trip_cols = ["calitp_itp_id", "calitp_url_number", 
                 "route_id", "shape_id"]

    trips = (tbl.gtfs_schedule.trips()
         >> select(*trip_cols)
         >> distinct()
         >> collect()
        )
    
    df2 = pd.merge(df, 
                   trips, 
                   on = ["calitp_itp_id", "calitp_url_number", 
                         "shape_id"],
                   how = "inner",
                   # Allow 1:m merge since trips maybe has multiple shape_ids
                   # Can address later by picking 1 route
                   validate = "1:m"
                  ).rename(columns = {"calitp_itp_id": "itp_id"})
    
    return df2


if __name__ == "__main__":
    # Create routes shapefile for current day
    #grab_current_transit_route_shapes("./data/", FILE_NAME = "shapes")
    
    # Create parallel corridors
    transit_routes = gpd.read_parquet("./data/shapes_2022-03-02.parquet")
    
    # Need route_id attached for parallel_corridors stuff to work
    transit_routes2 = add_route_info(transit_routes)
    
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet= geography_utils.FEET_PER_MI, 
        alternate_df = transit_routes2,
        pct_route_threshold = 0.3,
        pct_highway_threshold = 0.1,
        DATA_PATH = create_parallel_corridors.DATA_PATH, 
        FILE_NAME = "parallel_or_intersecting_2022-03-02"
    )
    
    # 50 ft buffers, get routes that are 
    create_parallel_corridors.make_analysis_data(
        hwy_buffer_feet=50, alternate_df = transit_routes2,
        pct_route_threshold = 0.1, pct_highway_threshold = 0,
        DATA_PATH = create_parallel_corridors.DATA_PATH, 
        FILE_NAME = "routes_on_shn_2022-03-02"
    )