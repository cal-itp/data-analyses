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
import geog_utils as geography_utils

TODAY_DATE = str(dt.date.today())

def grab_current_transit_route_shapes(DATA_PATH, FILE_NAME):
    
    agencies = pd.read_parquet(f"{DATA_PATH}temp_agencies.parquet")
    
    ITP_ID_LIST = list(agencies.calitp_itp_id.unique())
    print(ITP_ID_LIST)
    
    routes = geography_utils.make_routes_shapefile(
        ITP_ID_LIST = ITP_ID_LIST, alternate_df=None)

    routes.to_parquet(f"{DATA_PATH}{FILE_NAME}_{TODAY_DATE}.parquet")


    
grab_current_transit_route_shapes("./data/", FILE_NAME = "shapes")