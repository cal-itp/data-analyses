"""
Functions to query GTFS schedule data, 
save locally as parquets, 
then clean up at the end of the script.
"""
import pandas as pd
import glob
import os

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

from calitp.tables import tbl
from calitp import query_sql
from datetime import datetime
from siuba import *
import utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/traffic_ops/"

def create_local_parquets():
    time0 = datetime.now()
    
    stops = (
        tbl.gtfs_schedule.stops()
        >> select(_.calitp_itp_id, _.stop_id, 
                  _.stop_lat, _.stop_lon, 
                  _.stop_name, _.stop_code
                 )
        >> distinct()
        >> collect()
    )
    stops.to_parquet("./stops.parquet")

    trips = (
        tbl.gtfs_schedule.trips()
        >> select(_.calitp_itp_id, _.route_id, _.shape_id)
        >> distinct()
        >> collect()
    )
    trips.to_parquet("./trips.parquet")

    route_info = (
        tbl.gtfs_schedule.routes()
        # NB/SB may share same route_id, but different short/long names
        >> select(_.calitp_itp_id, _.route_id, 
                  _.route_short_name, _.route_long_name)
        >> distinct()
        >> collect()
    )
    route_info.to_parquet("./route_info.parquet")

    agencies = (
        tbl.gtfs_schedule.agency()
        >> select(_.calitp_itp_id, _.agency_id, _.agency_name)
        >> distinct()
        >> collect()
    )
    agencies.to_parquet("./agencies.parquet")

    # Filter to the ITP_IDs present in the latest agencies.yml
    latest_itp_id = (tbl.views.gtfs_schedule_dim_feeds()
                     >> filter(_.calitp_id_in_latest==True)
                     >> select(_.calitp_itp_id)
                     >> distinct()
                     >> collect()
                    )
    latest_itp_id.to_parquet("./latest_itp_id.parquet")
    
    time1 = datetime.now()
    print(f"Part 1: Queries and create local parquets: {time1-time0}")
    
    # Use geography_utils to assemble routes from shapes.txt
    # This takes about 37 min to run and create this routes shapefile
    ITP_ID_LIST = list(agencies.calitp_itp_id.unique())
    
    routes = utils.make_routes_shapefile(
                ITP_ID_LIST, CRS = utils.WGS84, 
                alternate_df=None)
    routes.to_parquet("./routes.parquet")
    
    time2 = datetime.now()
    print(f"Part 2: Create routes shapefile: {time2-time1}")

    print(f"Total execution time for all queries: {time2-time0}")
    
    
# Function to delete local parquets
def delete_local_parquets():
    # Find all local parquets
    FILES = [f for f in glob.glob("*.parquet")]
    print(f"list of parquet files to delete: {FILES}")
    
    for file_name in FILES:
        os.remove(f"{file_name}")

        
## These functions are used in `create_routes_data.py` and `create_stops_data.py`

# Define column names, must fit ESRI 10 character limits
RENAME_COLS = {
    "calitp_itp_id": "itp_id",
    "route_short_name": "route_name",
    "route_long_name": "route_full",
    # Ideally, we want to include _list because 
    # these columns aren't the same as agency_id and agency_name
    "agency_name": "agency",
}


# Define function to attach route_info using route_id
def attach_route_name(df, route_info_df):
    """
    Parameters:
    df: pandas.DataFrame
        each row is unique to itp_id-route_id
    route_info_df: pandas.DataFrame
                    each row is unique to route_id-route_long_name-route_short_name
    """
    # Attach route info from gtfs_schedule.routes, using route_id
    routes = pd.merge(
        df, 
        route_info_df,
        on = ["calitp_itp_id", "route_id"],
        how = "left",
        # route_id can have multiple long/short names
        validate = "m:m",
    )

    return routes


# Define function to attach agency_id, agency_name using calitp_itp_id
def attach_agency_info(df, agency_info):
    # Turn df from long, and condense values into list
    # They'll want to look at stops by ID, but see the list of agencies it's associated with
    agency_info2 = (agency_info.groupby("calitp_itp_id")
                 .agg(pd.Series.tolist)
                 .reset_index()
                )
    
    # Turn list into string, since ESRI can't handle lists
    # #https://stackoverflow.com/questions/45306988/column-of-lists-convert-list-to-string-as-a-new-column

    for c in ["agency_id", "agency_name"]: 
        agency_info2[c] = agency_info2[c].apply(lambda x: ", ".join([str(i) for i in x]))
    
    df2 = pd.merge(
        df,
        agency_info2,
        on = "calitp_itp_id",
        how = "left",
        validate = "m:1",
    )
    
    return df2


# Function to filter to latest ITP_ID in agencies.yml
# Also, embed dropping calitp_itp_id==0 as a step (print how many obs)
def filter_latest_itp_id(df, latest_itp_id_df, itp_id_col = "calitp_itp_id"):
    starting_length = len(df)
    print(f"# rows to start: {starting_length}")
    print(f"# operators to start: {df[itp_id_col].nunique()}")
    
    df = (df[df[itp_id_col] != 0]
          .sort_values([itp_id_col, "route_id"])
          .reset_index(drop=True)
         )
    
    no_zeros = len(df)
    print(f"# rows after ITP_ID==0 dropped: {no_zeros}")
    print(f"# operators after ITP_ID==0 dropped: {df[itp_id_col].nunique()}")
    
    # Drop ITP_IDs if not found in the latest_itp_id
    if itp_id_col != "calitp_itp_id":
        latest_itp_id_df = latest_itp_id_df.rename(columns = {
            "calitp_itp_id": itp_id_col})
    
    df = (df[df[itp_id_col].isin(latest_itp_id_df[itp_id_col])]
          .reset_index(drop=True)
         )
    
    only_latest_id = len(df)
    print(f"# rows with only latest agencies.yml: {only_latest_id}")
    print(f"# operators with only latest agencies.yml: {df[itp_id_col].nunique()}")
    print(f"# rows dropped-: {only_latest_id - starting_length}")
    
    return df