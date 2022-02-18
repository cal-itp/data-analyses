# Warehouse queries
import os
import pandas as pd

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

from calitp.tables import tbl
from siuba import *

import utils

#----------------------------------------------------------#
## Stops and Trips
#----------------------------------------------------------#
# Stop query
stops = (tbl.gtfs_schedule.stops()
      # unique on itp_id and url_number
      # but there are handful of agencies where same stop has multiple url's
         >> select(_.calitp_itp_id, _.calitp_url_number, 
                   _.stop_id, _.stop_lat, _.stop_lon, 
                   _.wheelchair_boarding, 
                  )
         >> distinct()
)


# Trip query
trips = (tbl.gtfs_schedule.trips()
         >> select(_.calitp_itp_id, _.calitp_url_number, 
                   _.route_id, _.trip_id, 
                   _.wheelchair_accessible)
         >> arrange(_.calitp_itp_id)
         >> distinct()
)


#----------------------------------------------------------#
## Validations for Critical Errors
#----------------------------------------------------------#
validations_query = (
    tbl.views.validation_fact_daily_feed_notices()
    >> select(_.feed_key, _.calitp_itp_id, _.calitp_url_number,
              _.date, _.code)
    >> inner_join(_, 
                  (tbl.views.validation_code_descriptions()
                   >> select(_.code)
                  ), on = ["code"]
                 )
    >> group_by(_.feed_key, _.calitp_itp_id, _.calitp_url_number,
                _.date, _.code)
    # Calculate total number of errors for that validation code
    >> mutate(num_errors = _.code.count())
    >> group_by(_.feed_key, _.calitp_itp_id, _.calitp_url_number,
               _.date)
    # Calculate number of unique critical errors
    >> mutate(critical_errors = _.code.nunique())
    # Calculate number of unique errors that day across feeds
    >> group_by(_.date)
    >> mutate(num_unique_errors = _.code.nunique())
    >> arrange(_.calitp_itp_id, _.calitp_url_number, _.date, _.code)
    # Get distinct, otherwise for a feed-date-code, there can be multiple entries
    # because that's what is captured in num_errors
    >> distinct()
    #>> collect()
)

def add_daily_feeds(df):
    # For a given day, grab all the feeds (exclude info about errors)
    daily_feed = (
        tbl.views.validation_fact_daily_feed_notices()
        >> distinct(_.feed_key, _.date,
                    _.calitp_itp_id, _.calitp_url_number,
                   )
        >> arrange(_.calitp_itp_id, _.calitp_url_number, _.date)
        >> collect()
    )
    
    df = pd.merge(
        daily_feed,
        df,
        on = ["feed_key", "date", "calitp_itp_id", "calitp_url_number"],
        how = "left",
        validate = "1:m"
    )
    
    df = df.assign(
        critical_errors = df.critical_errors.fillna(0).astype(int),
        num_errors = df.num_errors.fillna(0).astype(int),
        date = pd.to_datetime(df.date),
        total_feeds = df.groupby("date")["feed_key"].transform("count"),
        num_unique_errors = df.num_unique_errors.fillna(0).astype(int),
    )
    
    return df


#----------------------------------------------------------#
## Fares v2
#----------------------------------------------------------#
dim_feeds = (tbl.views.gtfs_schedule_dim_feeds()
    >> select(_.feed_key, 
              _.calitp_itp_id, _.calitp_url_number, 
              _.calitp_agency_name, 
              _.calitp_feed_name, _.feed_publisher_name,
             )
)


#----------------------------------------------------------#
## Create local parquets
#----------------------------------------------------------#
def create_local_parquets():
    validations_df = validations_query >> collect()
    validations = add_daily_feeds(validations_df)
    
    fares_feeds = (
        tbl.views.gtfs_schedule_fact_daily_feed_files()
        >> filter(_.file_key=="fare_leg_rules.txt", 
                  _.is_interpolated == False)
        >> select(_.feed_key, _.date, )
        >> inner_join(_, dim_feeds, "feed_key")
        >> collect()
    )
    
    validations.to_parquet(f"{utils.GCS_FILE_PATH}validations.parquet")
    fares_feeds.to_parquet(f"{utils.GCS_FILE_PATH}fares_feeds.parquet")
    print("Created and exported validations and fares v2 datasets")
    

#create_local_parquets()