import geopandas as gpd
import pandas as pd
import glob
import os

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

from calitp.tables import tbl
from calitp import query_sql
from siuba import *

import prep_data2 as prep_data

DATA_PATH = "./data/test/"

from datetime import datetime

time0 = datetime.now()

# Read in local parquets
stops = pd.read_parquet(f"{DATA_PATH}stops.parquet")
trips = pd.read_parquet(f"{DATA_PATH}trips.parquet")
route_info = pd.read_parquet(f"{DATA_PATH}route_info.parquet")
routes = gpd.read_parquet(f"{DATA_PATH}routes.parquet")
latest_itp_id = pd.read_parquet(f"{DATA_PATH}latest_itp_id.parquet")

time1 = datetime.now()
print(f"Read in data: {time1-time0}")

# Left only means in trips, but shape_id not found in shapes.txt
# right only means in routes, but no route that has that shape_id 
# We probably should keep how = "left"?
# left only means we can assemble from stop sequence?
routes1 = pd.merge(
        trips,
        routes,
        on = ["calitp_itp_id", "shape_id"],
        how = "left",
        validate = "m:1",
        indicator=True
    )

missing_shapes = (routes1[routes1._merge=="left_only"]
      .drop(columns = ["geometry", "_merge"])
      .reset_index(drop=True)
     )

SELECTED_DATE = "2022-3-15"

trip_cols = ["calitp_itp_id", "route_id", "shape_id"]

dim_trips = (tbl.views.gtfs_schedule_dim_trips()
             >> filter((_.calitp_itp_id == 4) | 
                       (_.calitp_itp_id == 301))
             >> select(*trip_cols, _.trip_key)
             >> distinct()
            )

missing_trips = (
    tbl.views.gtfs_schedule_fact_daily_trips()
    >> filter(_.service_date == SELECTED_DATE, 
           _.is_in_service==True)
    >> select(_.trip_key, _.trip_id)
    >> inner_join(_, dim_trips, on = "trip_key")
    >> distinct()
)

stop_info = (
    tbl.views.gtfs_schedule_dim_stop_times()
    >> filter((_.calitp_itp_id == 4) | 
              (_.calitp_itp_id == 301))
    >> inner_join(_,
                  tbl.views.gtfs_schedule_dim_stops(), 
                  on = ["calitp_itp_id", "stop_id"])
    >> select(_.calitp_itp_id, _.trip_id, 
              _.stop_id, _.stop_sequence,
              _.stop_lon, _.stop_lat)
)

time2 = datetime.now()
print(f"Trips and stops query: {time2-time1}")

missing_trips_df = missing_trips >> collect()
stop_df = stop_info >> collect()

time3 = datetime.now()
print(f"Collect: {time3-time2}")

missing_trips_df2 = (
    missing_trips_df.sort_values(group_cols + ["trip_id"])
    .drop_duplicates(subset=group_cols)
)


# Now merge this df back with stop_info
missing_stops = pd.merge(
    missing_trips_df2,
    stops_df,
    on = ["calitp_itp_id", "trip_id"],
    how = "inner"
)

time4 = datetime.now()
print(f"Merge in stop data: {time4-time3}")

missing_stops.to_parquet(f"{DATA_PATH}test_missing_stops.parquet")
time5 = datetime.now()
print(f"Execution time: {time5-time0}")