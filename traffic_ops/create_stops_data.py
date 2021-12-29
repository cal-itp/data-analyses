"""
Create stops file with identifiers including
route_id, route_name, agency_id, agency_name.

Stops need point geometry.
"""
import geopandas as gpd
import pandas as pd
import os

from shared_utils import geography_utils
import prep_data

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)
pd.set_option("display.max_rows", 20)

from calitp.tables import tbl
from calitp import query_sql
from datetime import datetime
from siuba import *

time0 = datetime.now()

# Read in local parquets
stops = pd.read_parquet("./stops.parquet")
route_info = pd.read_parquet("./route_info.parquet")
agencies = pd.read_parquet("./agencies.parquet")


# Grab stops dataset and turn it from df to gdf
stops = geography_utils.create_point_geometry(stops, 
                            longitude_col = "stop_lon", 
                            latitude_col = "stop_lat", 
                            crs = geography_utils.WGS84
                        )

# There are a couple of duplicates when looking at ID-stop_id (but diff stop_code)
# Drop these, since stop_id is used to merge with route_id
stops = (stops
         .sort_values(["calitp_itp_id", "stop_id", "stop_code"])
         .drop_duplicates(subset=["calitp_itp_id", "stop_id"])
         .reset_index(drop=True)
)

time1 = datetime.now()
print(f"Create stop geometry: {time1-time0}")
    

# Attach all the various route information    
# gtfs_schedule.stop_times merged with gtfs_schedule.trips gives route_id (via trip_id)
stops_with_route = (
    tbl.gtfs_schedule.stop_times()    
    >> select(_.calitp_itp_id, _.stop_id, _.trip_id)
    # join on trips table using trip_id to get route_id
    >> inner_join(_, 
                  (tbl.gtfs_schedule.trips()
                   >> select(_.calitp_itp_id, _.route_id, _.trip_id)
                  ),
                  ["calitp_itp_id", "trip_id"]
                 )
    # Keep stop_id and route_id, no longer need trip info
    >> select(_.calitp_itp_id, _.stop_id, _.route_id)
    >> distinct()
    >> collect()
)

# Attach route_id to stops df using stop_id
stops_with_route2 = pd.merge(
    stops,
    stops_with_route,
    on = ["calitp_itp_id", "stop_id"],
    # About 6,000 rows that are left_only (stop_id) not linked with route
    # Drop these, we want full information
    how = "inner",
    validate = "1:m",
)

# Attach route info (long/short names) using route_id
stops_with_route3 = prep_data.attach_route_name(stops_with_route2, route_info)

# Attach agency_id and agency_name using calitp_itp_id
stops_with_route4 = prep_data.attach_agency_info(stops_with_route3, agencies)

# Should calitp_itp_id==0 be dropped? There are stop_ids present though.
stops_with_route4 = (stops_with_route4
                     .sort_values(["calitp_itp_id", "route_id", 
                                   "route_long_name", "stop_id"])
                     .reset_index(drop=True)
                    )
time2 = datetime.now()
print(f"Attach route and operator info to stops: {time2-time1}")
       

stops_with_route5 = (prep_data.drop_itp_id_zero(stops_with_route4, 
                                                itp_id_col = "calitp_itp_id")
            .rename(columns = prep_data.RENAME_COLS)
          )

stops_with_route5.to_parquet("./stops_assembled.parquet")

print(f"Stops script total execution time: {time2-time0}")