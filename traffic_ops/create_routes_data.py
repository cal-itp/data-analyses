"""
Create routes file with identifiers including
route_id, route_name, agency_id, agency_name.

Operator-routes in shapes.txt need route line geometry.
Operator-routes not in shapes.txt use stop sequence 
to generate route line geometry.
"""
import geopandas as gpd
import pandas as pd
import os

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)
pd.set_option("display.max_rows", 20)

from calitp.tables import tbl
from calitp import query_sql
from datetime import datetime
from siuba import *

from shared_utils import geography_utils
import prep_data

time0 = datetime.now()
        
# Read in local parquets
stops = pd.read_parquet("./stops.parquet")
trips = pd.read_parquet("./trips.parquet")
route_info = pd.read_parquet("./route_info.parquet")
agencies = pd.read_parquet("./agencies.parquet")
routes = gpd.read_parquet("./routes.parquet")

time1 = datetime.now()
print(f"Read in data: {time1-time0}")

# Attach route_id from gtfs_schedule.trips, using shape_id
routes1 = pd.merge(
    routes,
    trips,
    on = ["calitp_itp_id", "shape_id"],
    # There are shape_ids that are left_only (1,600 obs)
    how = "inner",
    validate = "1:m",
    )

routes_part1 = prep_data.attach_route_name(routes1, route_info)

time2 = datetime.now()
print(f"Part 1 - routes for operator-routes in shapes.txt: {time2-time1}")
    

def grab_missing_stops(ITP_ID, missing_trips_df, stops_df):
    df = (
        tbl.gtfs_schedule.stop_times()
        # Loop through individual operators, then do the join to find those trips
        # until the issue that makes kernel restarts is fixed
        >> filter(_.calitp_itp_id == ITP_ID)
        >> select(_.calitp_itp_id, _.stop_id, _.stop_sequence, _.trip_id)
        # Can't do isin without the collect()
        # But collect() is what is making kernel restart / shutting down notebook
        >> distinct()
        >> collect()
        >> inner_join(_, missing_trips_df, ["calitp_itp_id", "trip_id"])
        >> inner_join(_, stops_df, ["calitp_itp_id", "stop_id"])
    )
    
    return df


# Find the stops that aren't in `shapes.txt`
missing_trips = (
    tbl.gtfs_schedule.trips()
    >> select(_.calitp_itp_id, _.route_id, _.shape_id, _.trip_id)
    >> distinct()
    >> collect()
    >> filter(~_.shape_id.isin(routes_part1.shape_id))
)

LOOP_ME = missing_trips.calitp_itp_id.unique().tolist()
print(f"operators: {LOOP_ME}, # operators: {len(LOOP_ME)}")

missing_trips_stops = pd.DataFrame()
for ITP_ID in LOOP_ME:
    df = grab_missing_stops(ITP_ID, missing_trips, stops)
    missing_trips_stops = (missing_trips_stops.append(df)
                           .sort_values(["calitp_itp_id", "trip_id", "stop_sequence"])
                           .reset_index(drop=True)
                          )

# Rename colums to match what's used in geography_utils
missing_trips_stops = missing_trips_stops.rename(
    columns = {"stop_lon": "shape_pt_lon", 
              "stop_lat": "shape_pt_lat",
               "stop_sequence": "shape_pt_sequence",
              }
)
    
missing_trips_stops2 = geography_utils.make_routes_shapefile(
    LOOP_ME, CRS = geography_utils.WGS84, 
    alternate_df=missing_trips_stops
)

missing_trips_stops2.to_parquet("./missing_routes.parquet")

routes2 = pd.merge(
    missing_trips_stops2,
    (missing_trips_stops[["calitp_itp_id", "route_id", "shape_id"]]
     .assign(shape_id = missing_trips_stops.route_id)
    .drop_duplicates()
    ),
    on = ["calitp_itp_id", "shape_id"],
    how = "inner",
    validate = "1:m",
)

# In geography_utils.make_routes_shapefile, when alternate_df is set,
# the shape_id is replaced with route_id (since shape_id is None)
# Change it back after route line geometry created
routes2 = routes2.assign(shape_id = None)

routes_part2 = prep_data.attach_route_name(routes2, route_info)

time3 = datetime.now()
print(f"Part 2 - routes for operator-routes not in shapes.txt: {time3-time2}")


# Assemble routes file
routes_assembled = (routes_part1.append(routes_part2)
                        .sort_values(["calitp_itp_id", "route_id"])
                        .drop_duplicates()
                        .reset_index(drop=True)
                       )
    
# Attach agency_id and agency_name using calitp_itp_id
routes_assembled2 = prep_data.attach_agency_info(routes_assembled, agencies)

routes_assembled2 = (prep_data.drop_itp_id_zero(routes_assembled2, 
                                                itp_id_col = "calitp_itp_id")
                    # Any renaming to be done before exporting
                    .rename(columns = prep_data.RENAME_COLS)
                   )

routes_assembled2.to_parquet("./routes_assembled.parquet")

print(f"Routes script total execution time: {time3-time0}")